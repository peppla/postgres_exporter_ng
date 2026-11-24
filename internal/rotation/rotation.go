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

// Package rotation decides which selected database a scrape starts with.
//
// Both engines work through their selected databases in sorted order, every
// scrape. A selected set that does not fit the budget therefore gives the same
// prefix every opportunity and the rest none: measured on three databases with
// one connection permit and a budget none of them fit, four scrapes in a row
// attempted the first and deferred the other two, every time.
//
// What rotates is the order in which databases are offered an attempt. It is
// not a change to what an attempt comes to. A deferral still means nothing was
// tried, a failure still means work started and did not finish, and the reasons
// keep the meanings admission gives them. The order appears in no label.
package rotation

import (
	"sync"
)

// Key is the schedule one sequence belongs to.
//
// Scoped so that two collection profiles do not advance each other's schedule.
// A request for a narrow collect[] set has its own order, because the set it
// walks is its own, and the two engines are separate because they choose their
// databases independently.
type Key struct {
	// Engine tells the collector engine's sequence from the query engine's.
	Engine string
	// Target is the connection this schedule is about. It is the process's own
	// identity for a DSN, not a hostname, so it is not stable across restarts.
	Target string
	// Profile is the selection the request made, which is the set this
	// sequence walks.
	Profile string
}

// Store holds one sequence per schedule, for the life of the process.
//
// It belongs to the process rather than to a request, because a request that
// ends takes its own idea of where it got to with it, and the whole point is
// for the next request to start somewhere else.
//
// It holds one small entry per schedule, so it grows with the number of
// distinct collect[] profiles scraped against a target rather than with the
// number of databases. Nothing removes an entry while the process runs, which
// it shares with the last-success store, and bounded cleanup covers both.
type Store struct {
	mu sync.Mutex
	at map[Key]uint64
}

// New returns an empty store.
func New() *Store {
	return &Store{at: map[Key]uint64{}}
}

// Take hands this scrape its starting offset and moves the schedule on.
//
// Each call gets a different value, so two requests overlapping on one schedule
// start in different places rather than both attempting the same databases and
// leaving the same ones out.
//
// The sequence counts and is never reduced to the size of the selected set, so
// discovery adding or removing a database changes which database an offset
// lands on without putting the schedule back to the beginning. It also does not
// depend on how much of the work finished: a scrape that completed nothing has
// still used its turn, and leaving the schedule where it was is how the same
// databases keep being offered first.
func (s *Store) Take(k Key) uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	at := s.at[k]
	s.at[k] = at + 1
	return at
}

// Len is how many schedules are held, for the tests that bound this.
func (s *Store) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.at)
}

// Order returns the positions 0 to n-1 rotated so that the offset comes first.
//
// A rotation rather than a subset: every selected database is still visited, so
// the ones that get no attempt are still reported as deferred by name. Leaving
// one out here would make a selected database vanish from the output, which
// reads the same as a database nobody selected.
func Order(offset uint64, n int) []int {
	if n <= 0 {
		return nil
	}
	start := int(offset % uint64(n))
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, (start+i)%n)
	}
	return out
}
