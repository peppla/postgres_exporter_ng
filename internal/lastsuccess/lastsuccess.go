// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// New in postgres_exporter_ng, 2026. No copyright line: this is newly written
// code and its holder is not settled, so none is asserted.

// When each database and collector was last collected without error.
//
// The coverage metrics say what this scrape did. They cannot say how long a
// database has been failing, and that is the question an operator asks first:
// a database that failed once is a blip, and one that has not been collected
// for an hour is an outage. Answering it needs state that outlives a request,
// because the handler builds a fresh exporter for every scrape.
package lastsuccess

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Key identifies what succeeded: one connection, one database, and the job
// that collected it.
//
// Target is an opaque per-process identity for the connection rather than its
// host and port, because several targets share a host and port and a last
// success recorded against one of them is not true of the others.
//
// Collector is the job within an engine: the built-in exporter and each
// custom-query resolution on one side, each namespaced collector on the other.
// One store serves all of them, so without the engine and the job an hourly
// job's success answers for a failing five-second one.
//
// About says what the record is about, because a database-level record and a
// collector-level one can otherwise write the same key. A request selecting
// exactly one collector has a selection equal to that collector's name, so the
// two would overwrite each other. Its zero value is the collector level, so
// keys written before this field existed keep their meaning.
type Key struct {
	// Engine separates the two metric engines, which collect the same
	// databases over the same connections and succeed and fail independently.
	Engine string
	// About is the collector level by default, where Collector names one
	// collector, or the database level, where Collector holds the canonical
	// selection that completed.
	About     Level
	Target    string
	Database  string
	Collector string
}

// Level is what a record is about.
type Level int

const (
	// OneCollector is one collector on one database. The zero value.
	OneCollector Level = iota
	// WholeSelection is every database-local collector one request selected,
	// on one database. Collector then holds that selection, canonically.
	WholeSelection
)

// Selection is the canonical form of a set of collector names, for Key.Collector
// at the WholeSelection level.
//
// Sorted, so the same collectors in another order are the same context and
// read the record the first request wrote. Built from the names rather than
// from a count, so an enabled set that gains or loses a collector cannot reuse
// a record written for a different set.
//
// One record per distinct selection per database, which is bounded by how many
// collect[] profiles are scraped. Bounding it is part of the bounded-state
// work rather than a cap here.
func Selection(names []string) string {
	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}

// The engines, as Key.Engine.
const (
	// EngineQuery is the custom-query engine, whose jobs are the built-in
	// exporter and each resolution.
	EngineQuery = "query"
	// EngineCollector is the namespaced collectors, whose jobs are the
	// collectors themselves.
	EngineCollector = "collector"
)

// Store remembers the last success for each key.
//
// Safe for concurrent use: the per-database fan-out records from several
// goroutines at once.
type Store struct {
	mtx  sync.Mutex
	seen map[Key]record
	// held counts the scrapes currently working on each target, so a cleanup
	// cannot remove the history of work that is still running.
	held map[string]int
	// live says whether a connection is still reachable by something other
	// than a relation: one this process is configured with, or one holding a
	// slot of its own. Nil means nothing is.
	//
	// A relation is how one connection says it built another, and a
	// connection can be live without any relation pointing at it. A probe
	// target is its own connection and derives nothing, so another context
	// retiring and finding no relation left on it would take its history even
	// while it is being probed. The store cannot know either of those things,
	// so it asks.
	live func(string) bool
	// pending is the connections a retirement could not finish because work
	// was holding them. The hold being dropped is what finishes it, so that a
	// retirement deferred is not a retirement abandoned.
	pending map[string]struct{}
	// edges says which seeds built each derived connection and which database
	// it opens.
	//
	// Here rather than beside the identities, because a catalogue read can
	// only act on a record through this relation, and the two have to move
	// together. Registering an edge next to publishing a record is two
	// moments, and a cleanup running between them leaves a record nothing
	// owns, which no later catalogue read can reach. One lock over both
	// closes that.
	edges map[string]edge
}

// edge is the seeds a derived connection was built from and the database it
// opens.
//
// Every seed is kept, not the first. Two seeds on one server as one role build
// the same connection string for the same database, and that is one connection
// with two ways into it, so either one's catalogue has to be able to answer
// for it. Keeping only the first leaves the cleanup working just while that
// seed is scraped.
type edge struct {
	seeds    map[string]struct{}
	database string
}

// record is what is kept about one key.
//
// The two times answer different questions and only one of them is public. A
// success is the operator's answer to how long since this worked. A
// confirmation is this process noting that a complete catalogue still listed
// the database, which says nothing about whether collecting from it worked.
//
// They are separate because a cleanup needs to know the database was still
// there a moment ago, and the only way to say that with one field would be to
// move the success time forward on work that did not succeed. The exported
// staleness would then stop being true at exactly the point an operator is
// reading it to find out what is broken.
type record struct {
	// success is when this key was last collected without error.
	success time.Time
	// confirmed is the generation of the newest complete inventory that
	// listed this key's database.
	confirmed time.Time
}

// New returns an empty store.
func New() *Store {
	return &Store{seen: make(map[Key]record), held: map[string]int{},
		edges: map[string]edge{}, pending: map[string]struct{}{}}
}

// Record notes that this key was collected without error at t.
//
// A later time wins, so a recording that arrives out of order, which the
// fan-out can produce, does not move the answer backwards.
func (s *Store) Record(key Key, t time.Time) {
	if s == nil {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	previous := s.seen[key]
	if !previous.success.IsZero() && !t.After(previous.success) {
		return
	}
	previous.success = t
	s.seen[key] = previous
}

// At returns when this key last succeeded, and whether it ever has.
//
// Never having succeeded is not the same as having succeeded at time zero: one
// is a database nothing has read yet and the other is a database that stopped
// being read in 1970. The caller publishes nothing rather than a zero.
func (s *Store) At(key Key) (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	rec, ok := s.seen[key]
	if !ok || rec.success.IsZero() {
		// A key that has only ever been confirmed present has never
		// succeeded, and saying otherwise would publish a zero time as a
		// collection that worked in 1970.
		return time.Time{}, false
	}
	return rec.success, true
}

// Latest returns the most recent success for a target and job, with the
// database it was on.
//
// For a scrape that cannot name the database it is collecting: without
// discovery the name comes from the server, and a scrape whose connection
// failed has no server to ask. The database that last succeeded over this
// connection is the one the diagnostic belongs to, and losing the series at
// exactly the moment the connection breaks would lose it when it matters.
// about is matched as well as the job. Without it a database-level record
// could answer a question about one collector, which is the collision Key.About
// exists to prevent, and matching three fields out of four would reintroduce it
// here after the key had closed it.
func (s *Store) Latest(engine string, about Level, target, collector string) (string, time.Time, bool) {
	if s == nil {
		return "", time.Time{}, false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var database string
	var newest time.Time
	var found bool
	for key, rec := range s.seen {
		if key.Engine != engine || key.About != about || key.Target != target || key.Collector != collector {
			continue
		}
		if rec.success.IsZero() {
			continue
		}
		if !found || rec.success.After(newest) {
			database, newest, found = key.Database, rec.success, true
		}
	}
	return database, newest, found
}

// Len reports how many keys are held.
//
// One entry per connection, database and job this process has collected. That
// grows with databases created and dropped over a long-running process, and
// nothing prunes it yet: pruning needs to know a database is gone, which only
// a discovery that succeeded can say, and a failing or deferred database must
// be kept because its last success is the answer an operator wants.
func (s *Store) Len() int {
	if s == nil {
		return 0
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return len(s.seen)
}

// Census is one complete, successful reading of what a server contains.
//
// It carries the moment it was started rather than the moment it finished,
// because what it may remove is what was already there when it began looking.
// A record written or updated while it was reading is newer than anything it
// saw, and a census that finished late must not erase what a later scrape has
// just established.
type Census struct {
	// Target is the seed this was read through, as its identity. Its own
	// records are covered, by the database each one names.
	Target string
	// Taken is when the read started.
	Taken time.Time
	// Databases is every database the server reported, whether or not this
	// role may collect from it. A filtered list cannot authorise removal,
	// because a database that lost CONNECT is absent from it for a reason that
	// has nothing to do with existing.
	Databases map[string]struct{}
}

// Hold protects a target while a scrape is working on it, and returns the
// release.
//
// A census taken before that work started can finish during it, and its
// catalogue is then older than whatever the work is in the middle of doing. A
// target being collected right now is not a target to decide is gone.
//
// Counted rather than flagged, because several of this process's requests can
// be working on one target at once and the first to finish must not drop the
// others' protection. The entry goes when the count reaches zero, so this
// holds one entry per target being collected rather than one per target ever
// collected.
func (s *Store) Hold(target string) func() {
	if s == nil || target == "" {
		return func() {}
	}
	s.mtx.Lock()
	s.held[target]++
	s.mtx.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { s.release(target) })
	}
}

// Own registers that target is the connection seed built to open database, and
// holds it until the returned release is called.
//
// The two happen together because they have to. A request reads a connection's
// identity while discovering it and records under that identity later, and a
// catalogue read finishing in between would remove the relation, leaving a
// record no later catalogue read can reach. Registering and holding under one
// lock means a connection a request is working with cannot be retired
// underneath it, so a record without an owner cannot exist whatever the order
// of events.
//
// The hold is released when the request that discovered the connection is
// done with it, not when one database's collection finishes. A request that
// discovers a connection may record under it at any point before it ends.
//
// Calling this twice for one connection adds the second seed rather than
// replacing the first.
func (s *Store) Own(seed, target, database string) func() {
	if s == nil || seed == "" || target == "" {
		return func() {}
	}

	s.mtx.Lock()
	e, ok := s.edges[target]
	if !ok {
		e = edge{seeds: map[string]struct{}{}, database: database}
		s.edges[target] = e
	}
	e.seeds[seed] = struct{}{}
	s.held[target]++
	s.mtx.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { s.release(target) })
	}
}

// Owned returns the connections one seed built, as identity to the database
// each opens. For the tests that check what a catalogue read has authority
// over.
func (s *Store) Owned(seed string) map[string]string {
	if s == nil {
		return nil
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	out := map[string]string{}
	for target, e := range s.edges {
		if _, ok := e.seeds[seed]; ok {
			out[target] = e.database
		}
	}
	return out
}

// WatchLive tells the store how to ask whether a connection is reachable by
// something other than a relation.
//
// Set once, by whoever knows the configuration and the slots. It is used only
// to finish a retirement that work was holding up, and it is called with this
// store's lock not held: answering it reaches into the admission, and the
// admission is what calls into here, so taking the two in both orders is how
// that stops.
func (s *Store) WatchLive(live func(string) bool) {
	if s == nil {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.live = live
}

// drop lets go of one hold and says whether a retirement was waiting on it.
// The caller holds the lock.
func (s *Store) drop(target string) (finish bool) {
	if s.held[target] > 1 {
		s.held[target]--
		return false
	}
	delete(s.held, target)
	if _, waiting := s.pending[target]; waiting {
		delete(s.pending, target)
		return true
	}
	return false
}

// release lets go of one hold and finishes any retirement that was waiting on
// it. The caller must not hold the lock.
func (s *Store) release(target string) {
	s.mtx.Lock()
	finish := s.drop(target)
	s.mtx.Unlock()
	if finish {
		s.finishPending(target)
	}
}

// finishPending completes a retirement that work was holding up.
//
// With no lock held, because the first thing it does reaches outside this
// store. A connection taken up again while that question was being asked goes
// back to waiting rather than being retired underneath whoever took it.
func (s *Store) finishPending(target string) {
	if s.live != nil && s.live(target) {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.held[target] > 0 {
		s.pending[target] = struct{}{}
		return
	}
	if s.hasOwner(target) {
		return
	}
	s.retire(map[string]struct{}{target: {}})
}

// hasOwner says whether any seed still claims to have built this connection.
// The caller holds the lock.
func (s *Store) hasOwner(target string) bool {
	e, ok := s.edges[target]
	return ok && len(e.seeds) > 0
}

// retire removes the records and relations of connections nothing reaches,
// and returns how many records went. The caller holds the lock.
func (s *Store) retire(targets map[string]struct{}) int {
	removed := 0
	for key := range s.seen {
		if _, ok := targets[key.Target]; !ok {
			continue
		}
		delete(s.seen, key)
		removed++
	}
	for target := range targets {
		delete(s.edges, target)
	}
	return removed
}

// ForgetProfile removes what one collection profile alone was keeping.
//
// For the admission that bounds the profiles nothing else can bound. When a
// profile stops being scraped for long enough to lose its slot, the records
// that exist only because it was scraped go with it.
//
// Only the whole-selection records of that selection. A per-collector record
// is about one collector on one database and is read by every profile that
// includes that collector, so it belongs to none of them and is bounded by the
// collectors and the connections rather than by the profiles. The ownership
// relations stay for the same reason: another profile may still be collecting
// that connection, and a relation removed under a live record is a record
// nothing owns.
//
// Nothing here consults the holds, and it must not. A hold says some request
// is working on that connection, which under another profile is true while
// this profile is being retired and says nothing about this profile's records.
// Skipping on it left a retired profile's record alive for as long as any
// other profile kept the connection busy, with no slot and nothing scheduled
// to come back for it. The protection this needs is the slot's own: a slot in
// use is never released, so a profile reaching here has no request of its own
// running.
//
// It returns how many records it removed.
func (s *Store) ForgetProfile(engine, selection string) int {
	if s == nil || selection == "" {
		return 0
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	removed := 0
	for key := range s.seen {
		if key.Engine != engine || key.About != WholeSelection || key.Collector != selection {
			continue
		}
		delete(s.seen, key)
		removed++
	}
	return removed
}

// ForgetContext removes the state a connection context alone was keeping, and
// returns how many records went.
//
// For the admission that bounds the probe endpoint's connections. A context
// that stops being probed for long enough to lose its slot takes with it the
// connections it built and its own, and nothing else.
//
// What decides is whether anything still reaches a connection. Another seed
// that built it, which is a relation. This process being configured with it,
// or a slot of its own being held, neither of which is a relation: a probe
// target derives nothing, so a connection can be live with nothing pointing at
// it. Those two are what live answers, and a retirement without that
// answer takes a configured connection's history because another context
// happened to discover it. It is passed in rather than asked for, because
// what answers it is the admission, and the admission is what called this.
//
// Work in flight defers rather than skips. A connection being collected is
// left alone and retired when that work lets go, so a retirement deferred is
// not a retirement abandoned.
func (s *Store) ForgetContext(target string, live func(string) bool) int {
	if s == nil || target == "" {
		return 0
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	candidates := map[string]struct{}{}
	for child, e := range s.edges {
		if _, built := e.seeds[target]; !built {
			continue
		}
		delete(e.seeds, target)
		candidates[child] = struct{}{}
	}
	// The context's own connection, which may be one something else derived
	// or one this process is configured with.
	candidates[target] = struct{}{}

	gone := map[string]struct{}{}
	for candidate := range candidates {
		if s.hasOwner(candidate) {
			continue
		}
		if live != nil && live(candidate) {
			continue
		}
		if s.held[candidate] > 0 {
			// Work is using it. Finished when that work lets go, rather than
			// left to nothing.
			s.pending[candidate] = struct{}{}
			continue
		}
		gone[candidate] = struct{}{}
	}
	return s.retire(gone)
}

// Apply records what a complete inventory saw and removes what it proves gone.
//
// Both halves are one step under one lock. Between them the store would say a
// database is absent while still holding its history, and a second census
// running in that gap would read a state neither census produced.
//
// Confirming is what makes an older census safe. A complete inventory that
// lists a database says the database was there when the inventory was taken,
// and that is true whether or not collecting from it worked. Without recording
// it, the only evidence of recent presence is the last success, so a database
// that exists and keeps failing looks exactly like a database that is gone,
// and an older census finishing later removes history that a newer one had
// just confirmed. The confirmation is kept apart from the success time because
// a failed collection must not be made to look like a successful one in order
// to protect what it failed at.
//
// A record is removed only when all of these hold. The census has authority
// over its target, which means the target is the seed itself or a connection
// derived from it. The database that target is about is not in the inventory.
// Nothing has recorded a success or a newer inventory for it since this census
// began. And no scrape is working on that target right now.
//
// Every profile of an owned connection is covered, including profiles nothing
// is scraping now. Leaving a dropped database behind under an idle profile
// would make the cleanup do nothing in the case it exists for, and a narrow
// request still cannot remove a wider profile's records, because what decides
// is the connection and its database rather than the selection.
//
// It returns how many records it confirmed and how many it removed, which is
// what a caller can report.
func (s *Store) Apply(c Census) (confirmed, removed int) {
	if s == nil || c.Target == "" || c.Databases == nil {
		return 0, 0
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for key, rec := range s.seen {
		database, owned := s.about(c, key)
		if !owned {
			continue
		}
		if _, present := c.Databases[database]; present {
			if c.Taken.After(rec.confirmed) {
				rec.confirmed = c.Taken
				s.seen[key] = rec
				confirmed++
			}
			continue
		}
		if !rec.success.Before(c.Taken) || !rec.confirmed.Before(c.Taken) {
			// Written, or confirmed present, since this census began. The
			// census never saw whatever that is about.
			continue
		}
		if s.held[key.Target] > 0 {
			continue
		}
		delete(s.seen, key)
		removed++
	}

	// Then the relations themselves, which are state too and would otherwise
	// be the thing that grows.
	//
	// A relation goes only when every record under its connection has gone
	// with it. One profile's record being removed says nothing about another
	// profile's record under the same connection, and a record protected by a
	// newer inventory or by work in flight is still a record. Dropping the
	// relation under it would leave that record with no owner, which is the
	// state nothing can reach.
	//
	// A connection a scrape is holding is left alone, relation and all. That
	// is a removal deferred rather than done, and the next complete inventory
	// after the work finishes is what does it.
	for target, e := range s.edges {
		if _, built := e.seeds[c.Target]; !built {
			continue
		}
		if _, present := c.Databases[e.database]; present {
			continue
		}
		if s.held[target] > 0 {
			continue
		}
		if s.anyRecordUnder(target) {
			continue
		}
		delete(s.edges, target)
	}
	return confirmed, removed
}

// anyRecordUnder says whether anything is still held about a connection. The
// caller holds the lock.
func (s *Store) anyRecordUnder(target string) bool {
	for key := range s.seen {
		if key.Target == target {
			return true
		}
	}
	return false
}

// about says which database a key is about and whether this census may act on
// it.
//
// A derived connection is about the database it was built to open, which is
// recorded where it was built rather than read back out of the record. A
// record under the seed's own connection is about the database it names, and
// one that names none is about the connection itself, which no catalogue can
// report missing.
func (s *Store) about(c Census, key Key) (string, bool) {
	if e, ok := s.edges[key.Target]; ok {
		if _, built := e.seeds[c.Target]; built {
			return e.database, true
		}
		return "", false
	}
	if key.Target == c.Target {
		return key.Database, key.Database != ""
	}
	return "", false
}
