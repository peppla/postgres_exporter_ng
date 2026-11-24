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
type Key struct {
	// Engine separates the two metric engines, which collect the same
	// databases over the same connections and succeed and fail independently.
	Engine    string
	Target    string
	Database  string
	Collector string
}

// WholeDatabase is Key.Collector for a record about a database rather than
// about one collector in it: every database-local collector the engine has,
// run without error on that database in one scrape.
//
// No collector has an empty name, so this cannot collide with one. It is kept
// as a named constant because an empty string in a key field otherwise reads
// as a field nobody filled in.
//
// A request that selected only some collectors does not record it. Such a
// request has not shown that the rest would have worked, and a database-level
// time written from a narrow request would claim it had.
const WholeDatabase = ""

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
	seen map[Key]time.Time
}

// New returns an empty store.
func New() *Store {
	return &Store{seen: make(map[Key]time.Time)}
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
	if previous, ok := s.seen[key]; ok && !t.After(previous) {
		return
	}
	s.seen[key] = t
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
	t, ok := s.seen[key]
	return t, ok
}

// Latest returns the most recent success for a target and job, with the
// database it was on.
//
// For a scrape that cannot name the database it is collecting: without
// discovery the name comes from the server, and a scrape whose connection
// failed has no server to ask. The database that last succeeded over this
// connection is the one the diagnostic belongs to, and losing the series at
// exactly the moment the connection breaks would lose it when it matters.
func (s *Store) Latest(engine, target, collector string) (string, time.Time, bool) {
	if s == nil {
		return "", time.Time{}, false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var database string
	var newest time.Time
	var found bool
	for key, t := range s.seen {
		if key.Engine != engine || key.Target != target || key.Collector != collector {
			continue
		}
		if !found || t.After(newest) {
			database, newest, found = key.Database, t, true
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
