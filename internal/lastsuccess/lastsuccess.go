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
	return &Store{seen: make(map[Key]record), held: map[string]int{}}
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
	// Children is every connection built from that seed, as identity to the
	// database it opens.
	//
	// This is where the authority actually lands. A database is collected
	// over its own connection, so its records are keyed by that connection's
	// identity and not by the seed's. Without this the census owned nothing
	// but the seed's own database, which is the one database a connected
	// catalogue read can never report missing, so the cleanup could not
	// remove anything at all.
	//
	// Matching on host, port or database name instead would let a catalogue
	// read through one connection remove the history of another, which may be
	// a different role seeing a different server.
	Children map[string]string
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
		once.Do(func() {
			s.mtx.Lock()
			defer s.mtx.Unlock()
			if s.held[target] <= 1 {
				delete(s.held, target)
				return
			}
			s.held[target]--
		})
	}
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
		database, owned := c.about(key)
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
	return confirmed, removed
}

// about says which database a key is about and whether this census may act on
// it.
//
// A derived connection is about the database it was built to open, which is
// recorded where it was built rather than read back out of the record. A
// record under the seed's own connection is about the database it names, and
// one that names none is about the connection itself, which no catalogue can
// report missing.
func (c Census) about(key Key) (string, bool) {
	if database, ok := c.Children[key.Target]; ok {
		return database, true
	}
	if key.Target == c.Target {
		return key.Database, key.Database != ""
	}
	return "", false
}
