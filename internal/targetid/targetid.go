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

// Package targetid gives each connection this process monitors a short
// identity, for keying what belongs to one of them and for labelling what is
// published about one of them.
//
// A target is one connection context, which is a connection string and
// everything in it. Host and port do not identify one: several targets share
// those, and several share host, port, database and role as well. Two
// connections to one database as one role can set different search_path
// values and read different tables through identical SQL.
//
// The identity is a number assigned on first use rather than a digest of the
// connection string, because that string holds a password and a digest of a
// password can be guessed at. It is stable while the process runs and is not
// stable across restarts, which is what a process-owned identity costs.
package targetid

import (
	"strconv"
	"sync"
)

var (
	mtx      sync.Mutex
	assigned = map[string]string{}
	last     int
	// derived records which seed a connection was built from and which
	// database it opens, keyed by the derived identity.
	//
	// Kept because a seed's complete catalogue is the only thing that can
	// authorise forgetting a database, and what it authorises forgetting is
	// the state held under that database's own connection identity rather
	// than under the seed's. Without the link from one to the other, a
	// cleanup holding a seed's inventory has nothing it is allowed to touch,
	// and matching on host, port or database name instead would let one
	// connection context remove another's history.
	derived = map[string]parent{}
)

// parent is the seed a derived connection came from and the database it opens.
type parent struct {
	seed     string
	database string
}

// ID returns the identity of a connection string, assigning one if this
// process has not seen it before. The second return reports whether it was
// assigned now, so a caller can record the mapping once rather than per
// scrape.
func ID(dsn string) (string, bool) {
	mtx.Lock()
	defer mtx.Unlock()

	if id, ok := assigned[dsn]; ok {
		return id, false
	}
	last++
	id := strconv.Itoa(last)
	assigned[dsn] = id
	return id, true
}

// Len reports how many identities have been assigned.
//
// One entry per connection string the process has seen. Discovery produces one
// per database, so a server whose databases come and go grows this by a
// connection string and a number each, and nothing drops them: an identity
// that was reused after being forgotten would relabel another target's series.
func Len() int {
	mtx.Lock()
	defer mtx.Unlock()
	return len(assigned)
}

// Derived assigns the identity for a connection built from a seed by opening a
// different database on the same server as the same role, and records where it
// came from.
//
// seed is the seed's own identity rather than its connection string, so the
// string does not travel any further than it already has. database is the name
// the derived connection opens, which the caller knows because it put the name
// into the string.
//
// Calling this twice for one connection is the same as calling it once. A
// second call with a different seed is kept as the first seed: two seeds that
// build the same connection string are the same server reached two ways, so
// either seed's catalogue answers for it, and the one already recorded is the
// one whose cleanup has been reasoning about it.
func Derived(seed, dsn, database string) (string, bool) {
	id, first := ID(dsn)

	mtx.Lock()
	defer mtx.Unlock()
	if _, ok := derived[id]; !ok {
		derived[id] = parent{seed: seed, database: database}
	}
	return id, first
}

// ChildrenOf returns the connections derived from one seed, as identity to the
// database each opens.
//
// This is what relates a seed's catalogue to the records it may remove. A
// caller holding a complete inventory read through that seed may act on the
// children whose database the inventory does not list, and on nothing else.
func ChildrenOf(seed string) map[string]string {
	mtx.Lock()
	defer mtx.Unlock()
	out := map[string]string{}
	for id, p := range derived {
		if p.seed == seed {
			out[id] = p.database
		}
	}
	return out
}

// Clear forgets every identity. For tests, which would otherwise see the
// numbers another test assigned.
func Clear() {
	mtx.Lock()
	defer mtx.Unlock()
	assigned = map[string]string{}
	derived = map[string]parent{}
	last = 0
}
