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

// Store holds one schedule per key, for the life of the process.
//
// It belongs to the process rather than to a request, because a request that
// ends takes its own idea of where it got to with it, and the whole point is
// for the next request to start somewhere else.
//
// What it remembers is the name of the database offered the first attempt, not
// a count. A counter reduced by the size of the current set is not continuity:
// with the selected set alternating between two sizes, the counter advances
// every scrape while the position it lands on cycles through a subset of the
// members, so a database selected every time can go indefinitely without ever
// being offered the first attempt. Measured on a set alternating between two
// and four members: over thirty-two scrapes one database that was selected
// throughout came first not once.
//
// It holds one small entry per schedule, so it grows with the number of
// distinct collect[] profiles scraped against a target rather than with the
// number of databases. Nothing removes an entry while the process runs, which
// it shares with the last-success store, and bounded cleanup covers both.
type Store struct {
	mu sync.Mutex
	// last is the database this schedule offered the first attempt to.
	last map[Key]string
}

// New returns an empty store.
func New() *Store {
	return &Store{last: map[Key]string{}}
}

// Next picks which of the current members is offered the first attempt, and
// moves the schedule on.
//
// members is the selected set in the order the engine walks it, which is sorted
// by name. The successor of the last database chosen is taken from the members
// present now, so a database added or removed between scrapes moves who comes
// next without putting the schedule back to the beginning and without stepping
// over a database that stayed selected.
//
// Two requests overlapping on one schedule are served one after the other and
// get different databases, because the choice and the record of it happen
// together under the lock. More overlapping requests than there are members
// will still repeat a database, which is the set being smaller than the
// concurrency rather than the schedule failing.
//
// Nothing here is told how much of the work finished. A scrape that completed
// nothing has still used its turn, because leaving the schedule where it was is
// how the same databases keep being offered first.
func (s *Store) Next(k Key, members []string) int {
	if len(members) == 0 {
		return 0
	}
	if s == nil {
		// No schedule, so the sorted order every scrape. This is the shape
		// that starves the tail of a set that does not fit the budget, so
		// every shipped request path is given the process's store.
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The first member after the one chosen last time. With no record, or with
	// the last one at or after the end of the current set, this is the first
	// member, which is also what a fresh process does.
	chosen := 0
	if last, ok := s.last[k]; ok {
		chosen = len(members)
		for i, name := range members {
			if name > last {
				chosen = i
				break
			}
		}
		if chosen == len(members) {
			chosen = 0
		}
	}
	s.last[k] = members[chosen]
	return chosen
}

// Len is how many schedules are held, for the tests that bound this.
func (s *Store) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.last)
}

// Order returns the positions 0 to n-1 rotated so that start comes first.
//
// A rotation rather than a subset: every selected database is still visited, so
// the ones that get no attempt are still reported as deferred by name. Leaving
// one out here would make a selected database vanish from the output, which
// reads the same as a database nobody selected.
func Order(start, n int) []int {
	if n <= 0 {
		return nil
	}
	if start < 0 || start >= n {
		start = 0
	}
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, (start+i)%n)
	}
	return out
}
