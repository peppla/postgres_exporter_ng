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

// What cache_seconds means over consecutive scrapes.

package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// collectNamespaces runs one scrape of the namespace engine and returns what
// it emitted, keyed by metric name.
func collectNamespaces(t *testing.T, server *Server, cache *querycache.Cache) (map[string][]*dto.Metric, map[string]error) {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	errs := queryNamespaceMappings(context.Background(), ch, server, cache, "custom_query.lr")
	close(ch)

	out := map[string][]*dto.Metric{}
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("writing a metric: %s", err)
		}
		out[nameOf(m)] = append(out[nameOf(m)], pb)
	}
	return out, errs
}

// nameOf pulls the family name out of a descriptor's string form, which is
// the only place dto.Metric does not carry it.
func nameOf(m prometheus.Metric) string {
	desc := m.Desc().String()
	const marker = `fqName: "`
	i := len(marker)
	start := indexOf(desc, marker)
	if start < 0 {
		return desc
	}
	rest := desc[start+i:]
	end := indexOf(rest, `"`)
	if end < 0 {
		return desc
	}
	return rest[:end]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

const cachedNamespace = "cached_thing"
const cachedQuery = "SELECT value FROM cached_thing"

// testClock is a clock a test moves by hand.
//
// The cache is built on it and the engine reads the time through the cache, so
// moving it moves the one reading both of them use. Ageing an entry by writing
// it back with an older timestamp was the alternative, and it needs a store
// that accepts an older observation, which is the defect the store now
// refuses.
type testClock struct {
	mtx sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Now()}
}

func (c *testClock) Now() time.Time {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.now = c.now.Add(d)
}

// TestCacheServesTheCollectionTimeNotTheScrapeTime is the contract line this
// exists for: old values must never be republished with fresh timestamps as
// though they had just been collected.
//
// Two scrapes, the second within the cache lifetime. The first query runs and
// the second does not, and both responses describe that one collection with
// the same timestamp, which is when it happened.
func TestCacheServesTheCollectionTimeNotTheScrapeTime(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	// One query, for the first scrape only. A second ExpectQuery is not set,
	// so a second query would fail the scrape and be visible here.
	mock.ExpectQuery("FROM cached_thing").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(42.0))

	clock := newTestClock()
	collectedAt := clock.Now()
	cache := querycache.NewWithClock(clock.Now)
	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 300},
		},
	}

	first, errs := collectNamespaces(t, server, cache)
	if len(errs) != 0 {
		t.Fatalf("the first scrape failed: %v", errs)
	}
	if got := first["pg_exporter_namespace_cache_age_seconds"]; len(got) != 1 {
		t.Fatalf("no age was reported on the first scrape")
	} else if age := got[0].GetGauge().GetValue(); age != 0 {
		t.Errorf("a freshly collected namespace reported an age of %v", age)
	}

	sample := first[cachedNamespace+"_value"]
	if len(sample) != 1 {
		t.Fatalf("the first scrape produced %d samples, want 1", len(sample))
	}
	// Stamped on the response that collected it as well, so that one
	// collection is one instant however many responses carry it. Unstamped,
	// this sample would be recorded at whatever moment the scraper read it,
	// and the next response would carry the collection time instead.
	if sample[0].TimestampMs == nil {
		t.Fatal("the collecting response carried no timestamp, so this collection is recorded at two different instants")
	}
	if got, want := sample[0].GetTimestampMs(), collectedAt.UnixMilli(); got != want {
		t.Errorf("the collected sample is stamped %d, want %d, which is when it was collected", got, want)
	}

	// Well inside the lifetime, so the second scrape serves what the first
	// collected.
	clock.advance(time.Minute)

	second, errs := collectNamespaces(t, server, cache)
	if len(errs) != 0 {
		t.Fatalf("the second scrape failed: %v", errs)
	}

	cachedSample := second[cachedNamespace+"_value"]
	if len(cachedSample) != 1 {
		t.Fatalf("the second scrape produced %d samples, want 1", len(cachedSample))
	}
	if cachedSample[0].TimestampMs == nil {
		t.Fatal("a cached sample was served with no timestamp, so it would be recorded as collected now")
	}
	if got, want := cachedSample[0].GetTimestampMs(), sample[0].GetTimestampMs(); got != want {
		t.Errorf("the cached sample is stamped %d and the collected one %d, so one collection is recorded as two points", got, want)
	}

	if got := second["pg_exporter_namespace_cache_age_seconds"]; len(got) != 1 {
		t.Fatal("no age was reported on the second scrape")
	} else if age := got[0].GetGauge().GetValue(); age != 60 {
		t.Errorf("the reported age is %v seconds, want 60", age)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestAFailedQueryIsNotCached covers the other half.
//
// A failed query used to be cached as an empty result, so one bad moment
// blanked a namespace for the whole cache lifetime and only the first scrape
// reported an error. The rest looked like a namespace with nothing in it.
func TestAFailedQueryIsNotCached(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	failure := errors.New("the connection went away")
	mock.ExpectQuery("FROM cached_thing").WillReturnError(failure)
	// The retry has to happen on the very next scrape, so a second query is
	// expected. Without the fix it never comes and this expectation fails.
	mock.ExpectQuery("FROM cached_thing").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(7.0))

	cache := querycache.New()
	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 300},
		},
	}

	failed, errs := collectNamespaces(t, server, cache)
	if len(errs) != 1 {
		t.Fatalf("a failed query produced %d errors, want 1", len(errs))
	}

	if cached := cache.Len(); cached != 0 {
		t.Errorf("a failed query was cached, so the namespace is blank until it expires")
	}

	// No samples, so no age. Published as zero, it read as freshly collected,
	// which is how a namespace nothing could be read from looked like one read
	// in full.
	if got := failed[cachedNamespace+"_value"]; len(got) != 0 {
		t.Fatalf("a failed query emitted %d samples", len(got))
	}
	if ages := failed["pg_exporter_namespace_cache_age_seconds"]; len(ages) != 0 {
		t.Errorf("failed query with no samples reports cache age %v",
			ages[0].GetGauge().GetValue())
	}

	second, errs := collectNamespaces(t, server, cache)
	if len(errs) != 0 {
		t.Fatalf("the retry failed: %v", errs)
	}
	if got := second[cachedNamespace+"_value"]; len(got) != 1 {
		t.Fatalf("the retry produced %d samples, want 1", len(got))
	} else if got[0].GetUntyped().GetValue() != 7 {
		t.Errorf("the retry produced %v, want 7", got[0].GetUntyped().GetValue())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestAnExpiredEntryIsRecollected checks the lifetime itself, which nothing
// else here exercises: past cache_seconds the query runs again.
func TestAnExpiredEntryIsRecollected(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM cached_thing").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(1.0))
	mock.ExpectQuery("FROM cached_thing").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(2.0))

	clock := newTestClock()
	cache := querycache.NewWithClock(clock.Now)
	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 10},
		},
	}

	if _, errs := collectNamespaces(t, server, cache); len(errs) != 0 {
		t.Fatalf("the first scrape failed: %v", errs)
	}

	// Past the ten-second lifetime it was stored with.
	clock.advance(30 * time.Second)
	recollectedAt := clock.Now()

	second, errs := collectNamespaces(t, server, cache)
	if len(errs) != 0 {
		t.Fatalf("the second scrape failed: %v", errs)
	}
	sample := second[cachedNamespace+"_value"]
	if len(sample) != 1 {
		t.Fatalf("the second scrape produced %d samples, want 1", len(sample))
	}
	if got := sample[0].GetUntyped().GetValue(); got != 2 {
		t.Errorf("an expired entry served %v, want the recollected 2", got)
	}
	if got, want := sample[0].GetTimestampMs(), recollectedAt.UnixMilli(); got != want {
		t.Errorf("the recollected sample is stamped %d, want %d, which is when it was recollected", got, want)
	}
	if got := second["pg_exporter_namespace_cache_age_seconds"]; len(got) != 1 {
		t.Fatal("no age was reported after recollection")
	} else if age := got[0].GetGauge().GetValue(); age != 0 {
		t.Errorf("a recollected namespace reported an age of %v, so it was served rather than recollected", age)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestANamespaceWithoutCacheSecondsIsUnchanged keeps the fix off the queries
// it does not apply to.
//
// A namespace that does not ask to be cached is collected every scrape, so its
// samples carry no timestamp and the scraper records them when it reads them,
// exactly as before any of this.
func TestANamespaceWithoutCacheSecondsIsUnchanged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM cached_thing").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(1.0))
	mock.ExpectQuery("FROM cached_thing").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(2.0))

	cache := querycache.New()
	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 0},
		},
	}

	for scrape, want := range []float64{1, 2} {
		out, errs := collectNamespaces(t, server, cache)
		if len(errs) != 0 {
			t.Fatalf("scrape %d failed: %v", scrape+1, errs)
		}
		sample := out[cachedNamespace+"_value"]
		if len(sample) != 1 {
			t.Fatalf("scrape %d produced %d samples, want 1", scrape+1, len(sample))
		}
		if got := sample[0].GetUntyped().GetValue(); got != want {
			t.Errorf("scrape %d served %v, want %v, so it did not run its own query", scrape+1, got, want)
		}
		if sample[0].TimestampMs != nil {
			t.Errorf("scrape %d carried an explicit timestamp of %d, so a query collected every scrape is no longer recorded when it is read",
				scrape+1, sample[0].GetTimestampMs())
		}
		// No age either. It could only ever read zero here, and a series per
		// namespace per database saying so is the cost of the diagnostic
		// without the diagnostic.
		if ages := out["pg_exporter_namespace_cache_age_seconds"]; len(ages) != 0 {
			t.Errorf("scrape %d published %d age series for a namespace without cache_seconds", scrape+1, len(ages))
		}
	}

	if cached := cache.Len(); cached != 0 {
		t.Errorf("a namespace without cache_seconds left %d entries in the cache", cached)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestTwoTargetsOnOneHostDoNotShareAnEntry covers the identity the key is
// built from.
//
// Two connections to one host and port are two targets, not one. Keyed by the
// host and port, the second served what the first collected.
//
// Two login roles against one database, which is the case the database name in
// the key cannot separate: same host, same port, same database, and the only
// difference is the role the connection string names. Two databases on one
// host are separated by the name as well, so they would not show the key
// using an identity that is not the fingerprint.
//
// The engine builds the key, so this fails on the production path rather than
// on two Key values a test wrote out by hand.
func TestTwoTargetsOnOneHostDoNotShareAnEntry(t *testing.T) {
	newTarget := func(t *testing.T, dsn string, value float64) (*Server, sqlmock.Sqlmock, func()) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("opening a stub connection: %s", err)
		}
		mock.ExpectQuery("FROM cached_thing").WillReturnRows(
			sqlmock.NewRows([]string{"value"}).AddRow(value))
		server := &Server{
			db:  db,
			dsn: dsn,
			// The same fingerprint for both, because host and port are all a
			// fingerprint is, and the same database, which is all the key
			// carries besides.
			datname:        "app",
			labels:         prometheus.Labels{"server": "198.51.100.7:5432"},
			queryOverrides: map[string]string{cachedNamespace: cachedQuery},
			metricMap: map[string]MetricMapNamespace{
				cachedNamespace: {cacheSeconds: 300},
			},
		}
		return server, mock, func() { db.Close() }
	}

	first, firstMock, closeFirst := newTarget(t, "postgresql://reader_a@198.51.100.7:5432/app", 11)
	defer closeFirst()
	second, secondMock, closeSecond := newTarget(t, "postgresql://reader_b@198.51.100.7:5432/app", 22)
	defer closeSecond()

	cache := querycache.New()

	firstOut, errs := collectNamespaces(t, first, cache)
	if len(errs) != 0 {
		t.Fatalf("the first target failed: %v", errs)
	}
	secondOut, errs := collectNamespaces(t, second, cache)
	if len(errs) != 0 {
		t.Fatalf("the second target failed: %v", errs)
	}

	if got := firstOut[cachedNamespace+"_value"]; len(got) != 1 || got[0].GetUntyped().GetValue() != 11 {
		t.Fatalf("the first target did not collect its own value, so there is nothing for the second to be served by mistake")
	}
	if got := secondOut[cachedNamespace+"_value"]; len(got) != 1 {
		t.Fatalf("the second target produced %d samples, want 1", len(got))
	} else if value := got[0].GetUntyped().GetValue(); value != 22 {
		t.Errorf("the second target served %v, want its own 22: it was served the other target's cached result", value)
	}
	if held := cache.Len(); held != 2 {
		t.Errorf("two targets left %d cache entries, want one each", held)
	}

	// Each target ran its own query exactly once.
	if err := firstMock.ExpectationsWereMet(); err != nil {
		t.Errorf("the first target: %s", err)
	}
	if err := secondMock.ExpectationsWereMet(); err != nil {
		t.Errorf("the second target: %s", err)
	}
}
