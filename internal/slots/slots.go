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

// Package slots admits a bounded number of the things nothing else can bound.
//
// Most of what this exporter remembers between scrapes is bounded by what a
// server says it has, and a complete catalogue read is what removes the rest.
// Two things have no such authority. A collection profile is whatever set of
// collectors a request asks for, and a probe target is whatever connection a
// request names. Both are minted by whoever can reach the endpoint, so without
// a limit the state behind them grows for as long as the process runs.
//
// What this does is admission, not eviction. A name that holds a slot keeps it
// until it stops being asked for, and a name arriving when every slot is held
// is refused rather than let in at the cost of one already there.
//
// Least recently used was tried on paper and gives no guarantee at all. With
// two slots and three profiles asked for in turn, every one of them is regular
// and every one loses its state before its next request, and a stream of new
// names evicts an established one just as easily. An eviction rule that can be
// driven from outside is not a rule.
//
// What admission guarantees is that nothing another requester does takes a
// slot away. The only thing that frees one is its own holder going quiet for
// longer than the idle window.
//
// That guarantee is conditional on the window, and the condition matters. A
// job whose interval is longer than the window loses its slot between its own
// requests, every time, and then competes for admission like a newcomer. The
// window belongs above the longest scrape interval the deployment supports.
package slots

import (
	"sort"
	"sync"
	"time"
)

// Kind separates the two things being admitted, so a profile and a connection
// context of the same name are not the same slot and do not share a limit.
type Kind string

const (
	// Profile is one engine's canonical set of collectors for a request.
	Profile Kind = "profile"
	// Context is one connection a request named, which is the probe path.
	Context Kind = "context"
)

// Key is one thing admitted.
type Key struct {
	Kind Kind
	Name string
}

// Limits is how many of each may hold a slot, and how long a slot survives
// without being asked for.
//
// Zero means no limit is configured, and then nothing is refused and nothing
// is released. The three values are operational settings rather than
// engineering constants, so this package picks none of them.
type Limits struct {
	Profiles int
	Contexts int
	Idle     time.Duration
}

// Store holds the slots.
type Store struct {
	mtx    sync.Mutex
	limits Limits
	slots  map[Key]*slot
	// now is the clock, which tests replace. Times are only ever compared
	// with each other through Sub, which uses the monotonic reading, so a
	// system clock that steps does not release a slot early or hold one
	// forever.
	now func() time.Time
	// onRelease is told about a slot that went idle, so its owner can drop
	// what that name alone was keeping.
	//
	// It is handed the names that still hold a slot, because it runs with
	// this store's lock held and so cannot ask. Asking would mean reaching
	// back into here from the stores it is about to change, and those are the
	// stores this call reaches into, which is the pair of orders a deadlock
	// is made of.
	onRelease func(k Key, stillHeld map[Key]struct{})
}

type slot struct {
	// inFlight counts the requests holding this slot right now. A slot in use
	// is never released, whatever the window says.
	inFlight int
	// used is when a request last asked for this slot.
	used time.Time
}

// New returns a store. onRelease may be nil.
func New(l Limits, onRelease func(k Key, stillHeld map[Key]struct{})) *Store {
	return &Store{limits: l, slots: map[Key]*slot{}, now: time.Now, onRelease: onRelease}
}

// WithClock replaces the clock, for tests.
func (s *Store) WithClock(now func() time.Time) *Store {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.now = now
	return s
}

// Admit takes every slot a request needs, or none of them.
//
// All of them together, because a request that holds a profile slot and is
// then refused a context slot has reserved state it will never use, and
// repeating that is a way to fill the store with names nothing is scraping.
// On a refusal everything this call took is given back before it returns, so a
// rejected request leaks nothing.
//
// The same key twice in one call is one slot held once.
//
// It returns the key that could not be admitted, which is what a refusal
// should say. The release is safe to call once; calling it on a refusal does
// nothing.
func (s *Store) Admit(keys ...Key) (release func(), refused Key, ok bool) {
	if s == nil {
		return func() {}, Key{}, true
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.admit(keys)
}

// admit does the work. The caller holds the lock.
func (s *Store) admit(keys []Key) (release func(), refused Key, ok bool) {
	now := s.now()
	taken := make([]Key, 0, len(keys))
	// The slots this call brought into being, as against the ones it joined.
	// A rollback that only gave back the holds would leave those behind, so a
	// client sending requests that are always refused would still fill the
	// store with the names it asked for first.
	created := make(map[Key]struct{}, len(keys))
	rollback := func() {
		for _, k := range taken {
			if _, mine := created[k]; mine {
				delete(s.slots, k)
				continue
			}
			if held, ok := s.slots[k]; ok && held.inFlight > 0 {
				held.inFlight--
			}
		}
	}

	seen := make(map[Key]struct{}, len(keys))
	for _, k := range keys {
		if _, repeat := seen[k]; repeat {
			continue
		}
		seen[k] = struct{}{}

		if held, ok := s.slots[k]; ok {
			held.inFlight++
			held.used = now
			taken = append(taken, k)
			continue
		}
		if !s.admitNew(k, now) {
			rollback()
			return func() {}, k, false
		}
		s.slots[k] = &slot{inFlight: 1, used: now}
		created[k] = struct{}{}
		taken = append(taken, k)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mtx.Lock()
			defer s.mtx.Unlock()
			for _, k := range taken {
				if held, ok := s.slots[k]; ok && held.inFlight > 0 {
					held.inFlight--
				}
			}
		})
	}, Key{}, true
}

// admitNew makes room for a name that has no slot. The caller holds the lock.
func (s *Store) admitNew(k Key, now time.Time) bool {
	limit := s.limitFor(k.Kind)
	if limit <= 0 {
		// No limit configured, so nothing is refused.
		return true
	}
	if s.countOf(k.Kind) < limit {
		return true
	}
	// Full. A slot nobody has asked for in longer than the window, and which
	// no request is holding, can go. Oldest first, so the one that has been
	// quiet longest is the one that makes room.
	if s.limits.Idle <= 0 {
		return false
	}
	idle := s.idleOf(k.Kind, now)
	if len(idle) == 0 {
		return false
	}
	s.retire(idle[0])
	return true
}

// retire removes a slot and drops what that name alone was keeping, with the
// lock still held.
//
// Under the lock on purpose. Removing the slot and then dropping its state
// outside the lock leaves a gap in which another request can be admitted under
// the same name and publish, and the callback that follows then deletes what
// the new admission has just written while its lease is held. Keeping the two
// together means a name is never half retired.
//
// The lock order is this one first. What a release does is reach into the
// stores that hold records and schedules, and nothing in those reaches back
// here, so there is no second order for the two to be taken in.
func (s *Store) retire(k Key) {
	delete(s.slots, k)
	if s.onRelease == nil {
		return
	}
	held := make(map[Key]struct{}, len(s.slots))
	for name := range s.slots {
		held[name] = struct{}{}
	}
	s.onRelease(k, held)
}

// idleOf returns the slots of one kind that nothing is holding and that have
// not been asked for within the window, oldest first. The caller holds the
// lock.
func (s *Store) idleOf(kind Kind, now time.Time) []Key {
	out := make([]Key, 0, len(s.slots))
	for k, held := range s.slots {
		if k.Kind != kind || held.inFlight > 0 {
			continue
		}
		if now.Sub(held.used) < s.limits.Idle {
			continue
		}
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := s.slots[out[i]].used, s.slots[out[j]].used
		if a.Equal(b) {
			return out[i].Name < out[j].Name
		}
		return a.Before(b)
	})
	return out
}

// Sweep releases every slot that has gone quiet, whether or not anything is
// waiting for room, and returns how many went.
//
// Admission alone would keep an idle name's state until some other name needed
// the room, which on a deployment that never fills its slots is for ever.
func (s *Store) Sweep() int {
	if s == nil || s.limits.Idle <= 0 {
		return 0
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := s.now()
	var going []Key
	for _, kind := range []Kind{Profile, Context} {
		going = append(going, s.idleOf(kind, now)...)
	}
	for _, k := range going {
		s.retire(k)
	}
	return len(going)
}

// While runs f with the slots held still, handing it a way to ask what holds
// one.
//
// For a caller that has to act on the answer, not just read it. Asking and
// then acting leaves a gap, and a name admitted in that gap is a name whose
// state the caller is about to delete on the strength of an answer that was
// true before it arrived. Nothing can be admitted or retired while f runs.
//
// f may take the locks of the stores this one's releases reach into, which is
// the order those releases already take them in. It must not call back into
// this store.
func (s *Store) While(f func(holds func(Key) bool)) {
	if s == nil {
		f(func(Key) bool { return false })
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	f(func(k Key) bool {
		_, ok := s.slots[k]
		return ok
	})
}

// Holds says whether this name has a slot right now.
//
// For the stores that have to know a connection is still being asked about
// before they retire what it was keeping. A name being retired has already
// lost its slot when its release runs, so this answers false for it and true
// for every other name that still holds one.
func (s *Store) Holds(k Key) bool {
	if s == nil {
		return false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	_, ok := s.slots[k]
	return ok
}

// Held is how many slots of one kind are held, for the tests that bound this.
func (s *Store) Held(kind Kind) int {
	if s == nil {
		return 0
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.countOf(kind)
}

// countOf counts one kind. The caller holds the lock.
func (s *Store) countOf(kind Kind) int {
	n := 0
	for k := range s.slots {
		if k.Kind == kind {
			n++
		}
	}
	return n
}

func (s *Store) limitFor(kind Kind) int {
	switch kind {
	case Profile:
		return s.limits.Profiles
	case Context:
		return s.limits.Contexts
	}
	return 0
}
