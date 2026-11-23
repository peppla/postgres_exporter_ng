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

// Key identifies what succeeded: one connection, one database, and one
// collector where the engine has collectors to name.
//
// Target is an opaque per-process identity for the connection rather than its
// host and port, because several targets share a host and port and a last
// success recorded against one of them is not true of the others.
type Key struct {
	Target    string
	Database  string
	Collector string
}

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

// Len reports how many keys are held, for tests and for seeing that the store
// is bounded.
func (s *Store) Len() int {
	if s == nil {
		return 0
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return len(s.seen)
}

// Forget drops every key whose target is not in keep.
//
// Called with the targets a scrape knows about, so that a server removed from
// the configuration stops being remembered. Databases are not pruned here: a
// database that is failing is exactly the one whose last success matters, and
// it is absent from a scrape's successes for as long as it fails.
func (s *Store) Forget(keep map[string]bool) {
	if s == nil {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for key := range s.seen {
		if !keep[key.Target] {
			delete(s.seen, key)
		}
	}
}
