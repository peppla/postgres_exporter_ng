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

// The server version, looked up once a minute per server.
//
// Every scrape needs it, so without a cache every scrape pays a round trip
// before collecting anything. The cache is keyed by host and port rather than
// by database, because the version is a property of the server.
package versioncache

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/blang/semver/v4"
)

// LoadVersion asks a server for its version.
//
// It takes a context because the lookup happens inside a scrape's budget. A
// version query without one is a query the collection deadline cannot stop,
// and a slow one held the whole request open past its deadline while the
// statements it was supposed to precede were cancelled correctly.
type LoadVersion func(context.Context, *sql.DB, string) (semver.Version, string, error)

type versionInfo struct {
	semantic semver.Version
	full     string
	cachedAt time.Time
}

// entry is one server's cached version and the right to refresh it.
//
// loading is a lock held only while that server's version is being read. It
// is a channel rather than a mutex so a caller can wait on it with a context:
// a mutex gives no way to stop waiting, so a request whose budget expired
// would keep queueing behind someone else's slow lookup.
type entry struct {
	loading chan struct{}

	mtx   sync.Mutex
	info  versionInfo
	valid bool
}

var (
	entriesMtx sync.Mutex
	entries    = make(map[string]*entry)

	// CurrentTime is replaced in tests to age the cache without waiting.
	CurrentTime = time.Now
)

const TTL = 1 * time.Minute

// GetVersion returns the server's version, from the cache when it is fresh.
//
// Waiting is bounded by ctx in both places it can happen: acquiring the right
// to refresh, and the lookup itself. Only callers asking about the same server
// wait for each other, where a single lock across the whole cache made every
// server queue behind whichever one was slowest.
func GetVersion(ctx context.Context, db *sql.DB, server string, loadVersion LoadVersion) (semver.Version, string, error) {
	e := entryFor(server)

	if info, ok := e.fresh(); ok {
		return info.semantic, info.full, nil
	}

	select {
	case e.loading <- struct{}{}:
		defer func() { <-e.loading }()
	case <-ctx.Done():
		return semver.Version{}, "", ctx.Err()
	}

	// Someone else may have refreshed it while this call was waiting, which
	// is the common case when several scrapes start together.
	if info, ok := e.fresh(); ok {
		return info.semantic, info.full, nil
	}

	semanticVersion, versionString, err := loadVersion(ctx, db, server)
	if err != nil {
		return semver.Version{}, "", err
	}

	e.store(versionInfo{semantic: semanticVersion, full: versionString, cachedAt: CurrentTime()})
	return semanticVersion, versionString, nil
}

// Clear forgets every cached version.
func Clear() {
	entriesMtx.Lock()
	defer entriesMtx.Unlock()
	entries = make(map[string]*entry)
}

func entryFor(server string) *entry {
	entriesMtx.Lock()
	defer entriesMtx.Unlock()
	if e, ok := entries[server]; ok {
		return e
	}
	e := &entry{loading: make(chan struct{}, 1)}
	entries[server] = e
	return e
}

func (e *entry) fresh() (versionInfo, bool) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if !e.valid || CurrentTime().Sub(e.info.cachedAt) >= TTL {
		return versionInfo{}, false
	}
	return e.info, true
}

func (e *entry) store(info versionInfo) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	e.info, e.valid = info, true
}
