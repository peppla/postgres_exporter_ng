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
// The identity is the first 128 bits of HMAC-SHA-256 over the whole connection
// string, under a key this process reads from crypto/rand at startup, written
// as 32 hexadecimal characters.
//
// A plain digest would not do. The connection string holds a password, the
// identity appears in a published label, and a digest of a guess can be
// compared with a digest of the real thing. A publicly salted digest has the
// same problem, since the salt is published with it. Under a key that never
// leaves the process, nothing outside it can relate an identity to a
// connection string.
//
// Deriving rather than assigning is what keeps this package from holding
// anything. An assigned number needs a map from every connection string ever
// seen, that map grows for as long as the process runs, and removing an entry
// from it means a later lookup of the same string gets a different number
// while records and schedules still refer to the old one. A function has none
// of those questions: one connection string has one identity for the life of
// the process, whether or not anything is still using it.
//
// It is not stable across restarts, which is what a process-owned identity
// costs, and what the key buys.
package targetid

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// Width is how many hexadecimal characters an identity has.
//
// 128 bits of the digest. The identity goes in a label, so shorter is better,
// and a collision would merge two connection contexts into one series, so it
// has to stay out of reach. At this width it is, for any number of connections
// an exporter will see.
const Width = 32

// loggedCap is how many identities this package will remember having logged.
//
// ID says whether its caller is the first to ask for an identity, which is
// what stops the mapping being logged on every scrape. Without an assignment
// map there is nothing that knows, so this holds the answer and nothing else.
//
// Capped because it is the one thing here that could grow. Past the cap the
// mapping stops being logged, which loses a debug line for connections beyond
// the first few thousand and loses nothing else. A log line is a smaller thing
// than an identity, and this is the trade the cap exists to make.
const loggedCap = 4096

var (
	mtx    sync.Mutex
	logged = map[string]struct{}{}
	key    = newKey()
)

// newKey reads the process's key.
//
// A process that cannot read one does not start. Falling back to a fixed key,
// or to a counter, would quietly give back the property the key is there for,
// and it would do it in exactly the deployments whose random source is
// unusual.
func newKey() []byte {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		panic("targetid: cannot read a key for target identities: " + err.Error())
	}
	return k
}

// ID returns this process's identity for a connection string, and whether this
// is the first time it has been asked for one.
//
// The second return is for the caller that logs which connection an identity
// stands for. It is true at most once per connection string, and it is false
// once this package has stopped remembering, which is the cap above.
func ID(dsn string) (string, bool) {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(dsn)) // nolint: errcheck // hash.Hash never returns an error
	id := hex.EncodeToString(mac.Sum(nil))[:Width]

	mtx.Lock()
	defer mtx.Unlock()
	if _, seen := logged[id]; seen {
		return id, false
	}
	if len(logged) >= loggedCap {
		return id, false
	}
	logged[id] = struct{}{}
	return id, true
}

// Len is how many identities this package has remembered logging, for the
// tests that bound it. It is not how many connections exist.
func Len() int {
	mtx.Lock()
	defer mtx.Unlock()
	return len(logged)
}

// Clear forgets what has been logged, for tests. It does not change any
// identity, because no identity is stored.
func Clear() {
	mtx.Lock()
	defer mtx.Unlock()
	logged = map[string]struct{}{}
}

// Rekey replaces the process key, for the tests that check two processes do
// not agree. Nothing in production calls it.
func Rekey() {
	mtx.Lock()
	defer mtx.Unlock()
	key = newKey()
	logged = map[string]struct{}{}
}
