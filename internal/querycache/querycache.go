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

// What cache_seconds caches, and where it lives.
//
// A query file can say cache_seconds, which means run this query less often
// than the endpoint is scraped. That only works if what it collected outlives
// a request, and it did not: the handler builds a fresh exporter per request,
// which builds a fresh server, whose cache starts empty. Two consecutive
// scrapes of a query returning nextval produced 1 and 2 and reported an age of
// zero for both. The feature existed in a unit test that reused one server
// object and nowhere else.
//
// So the cache is owned by the process and handed to the engine, rather than
// being a field on something a request creates. It holds metrics and nothing
// else: no database handles, because a handle held here would be a connection
// outside the accounting that bounds them.
package querycache

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Key identifies one cached result.
//
// Every part of it is something that makes two results different rather than
// the same result twice. Target and Database say where it came from, because
// the same namespace against another server or another database is another
// measurement. Collector separates the resolutions, which can configure the
// same namespace with different intervals. Definition is a fingerprint of the
// query itself, so editing a query file does not serve answers from the
// version before the edit.
type Key struct {
	Target     string
	Database   string
	Collector  string
	Namespace  string
	Definition string
}

type entry struct {
	metrics     []prometheus.Metric
	collectedAt time.Time
	expiresAt   time.Time
}

// Cache holds collected metrics for as long as their query asked.
//
// Safe for concurrent use, because two scrapes of the same endpoint overlap
// and both may be reading and writing the same namespace.
type Cache struct {
	mtx     sync.Mutex
	entries map[Key]entry

	// now is a field so the sweep and the expiry check agree on one reading,
	// rather than taking two.
	now func() time.Time
}

// New returns an empty cache.
func New() *Cache {
	return &Cache{entries: make(map[Key]entry), now: time.Now}
}

// NewWithClock returns an empty cache that reads the time from now.
//
// For tests. The engine reads the time through the cache rather than calling
// time.Now itself, so moving this clock moves the one reading both of them
// use. A clock only the cache could see would let them disagree about the
// present.
func NewWithClock(now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	return &Cache{entries: make(map[Key]entry), now: now}
}

// Now reports the time the cache is reading, which is what the engine stamps
// collections with and measures ages against. A nil cache reads the wall
// clock, so a caller holding no cache still gets a usable answer.
// now is assigned once, at construction, so it is read without the lock. A
// clock that a test moves has to do its own synchronising, which it needs
// anyway to move it from one goroutine and read it from another.
func (c *Cache) Now() time.Time {
	if c == nil {
		return time.Now()
	}
	return c.now()
}

// Get returns a cached result and when it was collected.
//
// An entry past its expiry is not returned and is removed, so a namespace
// whose query file stopped setting cache_seconds does not leave a result
// behind for the next reader of that key.
func (c *Cache) Get(key Key) ([]prometheus.Metric, time.Time, bool) {
	if c == nil {
		return nil, time.Time{}, false
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()

	found, ok := c.entries[key]
	if !ok {
		return nil, time.Time{}, false
	}
	if !c.now().Before(found.expiresAt) {
		delete(c.entries, key)
		return nil, time.Time{}, false
	}
	return found.metrics, found.collectedAt, true
}

// Put stores a result for the given lifetime, unless a newer one is already
// held.
//
// A lifetime of zero or less stores nothing and clears whatever was there,
// which is what a namespace that no longer asks to be cached should leave
// behind.
//
// Overlapping scrapes both miss and both run the query, and they finish in the
// order the server answers rather than the order they started, so an
// unconditional store keeps whichever observation was written last. Refusing
// the older write is preferred to serialising refreshes per key, which would
// make one scrape wait on another scrape's slow query and spend a request
// budget on work that request did not ask for.
func (c *Cache) Put(key Key, metrics []prometheus.Metric, collectedAt time.Time, lifetime time.Duration) {
	if c == nil {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if lifetime <= 0 {
		delete(c.entries, key)
		return
	}
	if held, ok := c.entries[key]; ok && !collectedAt.After(held.collectedAt) {
		return
	}
	c.entries[key] = entry{
		metrics:     metrics,
		collectedAt: collectedAt,
		expiresAt:   collectedAt.Add(lifetime),
	}

	// Expired entries are dropped on the way past. Without this the map grows
	// with every database and every query file an exporter ever saw, and
	// nothing would ever read those keys again to notice.
	c.sweepLocked()
}

// Len reports how many entries are held, for tests and for seeing that the
// cache is bounded.
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return len(c.entries)
}

// Clear forgets everything.
func (c *Cache) Clear() {
	if c == nil {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.entries = make(map[Key]entry)
}

func (c *Cache) sweepLocked() {
	now := c.now()
	for key, held := range c.entries {
		if !now.Before(held.expiresAt) {
			delete(c.entries, key)
		}
	}
}

// CollectedAt reports when an entry was collected, whether or not it has
// expired, for tests that need to compare against the timestamp a sample
// carries.
func (c *Cache) CollectedAt(key Key) (time.Time, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	held, ok := c.entries[key]
	return held.collectedAt, ok
}
