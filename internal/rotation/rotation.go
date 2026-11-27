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
	"strings"
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
// What it remembers is which databases are still waiting for their turn in the
// current cycle, in the order they will get it. Two simpler things were tried
// and both starve a database that stays selected while the set around it
// changes.
//
// A counter reduced by the size of the current set advances every scrape while
// the position it lands on cycles through a subset of the members. On a set
// alternating between two and four members, one database selected throughout
// came first not once in thirty-two scrapes.
//
// Remembering the last database chosen and taking the first current name after
// it fixes that and fails another: with exactly two members every scrape, one
// that stays and one replaced each time by a newcomer sorting between the
// previous choice and the one that stays, the newcomer is chosen every time.
// Thirty-two scrapes, thirty-two newcomers, and the member selected throughout
// came first not once. The set size never changed, so it is not about a growing
// number of databases competing for capacity.
//
// What both miss is that a database already waiting has an outstanding turn,
// and a database arriving later must not take it. So the waiting list is what
// is kept, newcomers join the back of it, and departures leave it.
//
// One entry per schedule, holding at most as many names as the profile selects,
// so it grows with the distinct collect[] profiles scraped against a target
// times the databases each selects. Nothing removes an entry while the process
// runs, which it shares with the last-success store, and bounded cleanup covers
// both.
type Store struct {
	mu sync.Mutex
	// waiting is the databases yet to have their turn this cycle, in order.
	waiting map[Key][]string
}

// New returns an empty store.
func New() *Store {
	return &Store{waiting: map[Key][]string{}}
}

// Next picks which of the current members is offered the first attempt, and
// moves the schedule on.
//
// members is the selected set, named so that no two entries in one call are the
// same, in whatever order the caller walks it. The order of the input does not
// decide who is chosen: what decides is which members still have a turn
// outstanding from earlier calls, and those keep their places. An earlier
// version did assume the input was sorted and chose the first name after the
// one chosen last, which silently skipped a database when the caller put the
// configured connection database first and so handed in a list that was not
// sorted at all.
//
// A member that leaves the selection loses its place. One that joins, or that
// has already had its turn this cycle, goes behind the ones still waiting.
// That is what keeps this store from growing, so every call has to reach it,
// including the ones where there is nothing to choose between. A caller that
// skips the schedule when it has one member or none leaves the departed
// members in it: a thousand-database selection collapsing to one left nine
// hundred and ninety-nine names behind, and ten more calls did not shift them.
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
	if s == nil {
		// No schedule, so the sorted order every scrape. This is the shape
		// that starves the tail of a set that does not fit the budget, so
		// every shipped request path is given the process's store.
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(members) == 0 {
		// Nothing is selected, so nothing is waiting. Returning before this
		// left the whole list behind: a selection that collapses to none kept
		// every name it had, for as long as the process ran.
		delete(s.waiting, k)
		return 0
	}

	selected := make(map[string]struct{}, len(members))
	for _, name := range members {
		selected[name] = struct{}{}
	}

	// Whoever is still waiting and is still selected, in the order they were
	// going to be served. A database that left the selection leaves the list
	// with it.
	waiting := make([]string, 0, len(members))
	stillWaiting := make(map[string]struct{}, len(members))
	for _, name := range s.waiting[k] {
		if _, ok := selected[name]; ok {
			waiting = append(waiting, name)
			stillWaiting[name] = struct{}{}
		}
	}

	// Then everyone else, at the back. A database that has just arrived, or
	// that had its turn earlier in the cycle, goes behind the ones with a turn
	// outstanding. Taking the members in their own order keeps this
	// deterministic, and that order is the sorted one.
	for _, name := range members {
		if _, ok := stillWaiting[name]; !ok {
			waiting = append(waiting, name)
		}
	}

	// The front of the list has waited longest. It is served and the rest keep
	// their places, so the next scrape carries on rather than starting again.
	chosen := waiting[0]
	s.waiting[k] = waiting[1:]

	for i, name := range members {
		if name == chosen {
			return i
		}
	}
	return 0
}

// Len is how many schedules are held, for the tests that bound this.
func (s *Store) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiting)
}

// ForgetProfile removes the schedules one collection profile alone was
// keeping, and returns how many went.
//
// For the admission that bounds the profiles. A schedule exists because a
// profile is being scraped, and a profile that has stopped being scraped for
// long enough to lose its slot takes its waiting lists with it. Another
// profile's schedule over the same connections is a different key and is left
// alone.
func (s *Store) ForgetProfile(engine, profile string) int {
	if s == nil || profile == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for k := range s.waiting {
		if k.Engine != engine || k.Profile != profile {
			continue
		}
		delete(s.waiting, k)
		removed++
	}
	return removed
}

// ForgetContext removes the schedules of one connection context, and returns
// how many went.
//
// For the admission that bounds the probe endpoint's connections. A key's
// Target is built by joining connection identities with separators, and an
// identity is fixed-width hexadecimal, so it cannot contain one. Matching
// whole fields is therefore exact: a context is in a key or it is not, and no
// identity is a prefix of another.
func (s *Store) ForgetContext(id string) int {
	if s == nil || id == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for k := range s.waiting {
		if !mentions(k.Target, id) {
			continue
		}
		delete(s.waiting, k)
		removed++
	}
	return removed
}

// mentions says whether a key's target is built from this identity.
func mentions(target, id string) bool {
	for _, field := range strings.FieldsFunc(target, func(r rune) bool {
		return r == ' ' || r == ','
	}) {
		if field == id {
			return true
		}
	}
	return false
}

// Waiting is how many names a schedule is holding, for the tests that bound
// this.
func (s *Store) Waiting(k Key) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiting[k])
}

// Keys is every schedule held, in no particular order.
//
// For the cleanup that will bound this store, which has to decide per schedule
// whether anything still reaches it, and for the tests that check what a key
// is allowed to carry.
func (s *Store) Keys() []Key {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Key, 0, len(s.waiting))
	for k := range s.waiting {
		out = append(out, k)
	}
	return out
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
