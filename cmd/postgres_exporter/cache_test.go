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
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// collectNamespaces runs one scrape of the namespace engine and returns what
// it emitted, keyed by metric name.
func collectNamespaces(t *testing.T, server *Server) (map[string][]*dto.Metric, map[string]error) {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	errs := queryNamespaceMappings(ch, server)
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

// TestCacheServesTheCollectionTimeNotTheScrapeTime is the contract line this
// exists for: old values must never be republished with fresh timestamps as
// though they had just been collected.
//
// Two scrapes, the second within the cache lifetime. The first query runs and
// the second does not, and the samples the second scrape serves carry the time
// the first one collected them.
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

	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricCache:    map[string]cachedMetrics{},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 300},
		},
	}

	first, errs := collectNamespaces(t, server)
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
	if sample[0].TimestampMs != nil {
		t.Errorf("a freshly collected sample carried an explicit timestamp of %d, so it would not be stamped with the scrape time",
			sample[0].GetTimestampMs())
	}

	// Make the cached entry look a minute old, which is well inside the
	// lifetime, so the second scrape serves it.
	server.cacheMtx.Lock()
	entry := server.metricCache[cachedNamespace]
	collectedAt := time.Now().Add(-time.Minute)
	entry.lastScrape = collectedAt
	server.metricCache[cachedNamespace] = entry
	server.cacheMtx.Unlock()

	second, errs := collectNamespaces(t, server)
	if len(errs) != 0 {
		t.Fatalf("the second scrape failed: %v", errs)
	}

	sample = second[cachedNamespace+"_value"]
	if len(sample) != 1 {
		t.Fatalf("the second scrape produced %d samples, want 1", len(sample))
	}
	if sample[0].TimestampMs == nil {
		t.Fatal("a cached sample was served with no timestamp, so it would be recorded as collected now")
	}
	if got, want := sample[0].GetTimestampMs(), collectedAt.UnixMilli(); got != want {
		t.Errorf("the cached sample is stamped %d, want %d, which is when it was collected", got, want)
	}

	if got := second["pg_exporter_namespace_cache_age_seconds"]; len(got) != 1 {
		t.Fatal("no age was reported on the second scrape")
	} else if age := got[0].GetGauge().GetValue(); age < 59 || age > 61 {
		t.Errorf("the reported age is %v seconds, want about 60", age)
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

	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricCache:    map[string]cachedMetrics{},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 300},
		},
	}

	_, errs := collectNamespaces(t, server)
	if len(errs) != 1 {
		t.Fatalf("a failed query produced %d errors, want 1", len(errs))
	}

	server.cacheMtx.Lock()
	cached := len(server.metricCache)
	server.cacheMtx.Unlock()
	if cached != 0 {
		t.Errorf("a failed query was cached, so the namespace is blank until it expires")
	}

	second, errs := collectNamespaces(t, server)
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

	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{cachedNamespace: cachedQuery},
		metricCache:    map[string]cachedMetrics{},
		metricMap: map[string]MetricMapNamespace{
			cachedNamespace: {cacheSeconds: 10},
		},
	}

	if _, errs := collectNamespaces(t, server); len(errs) != 0 {
		t.Fatalf("the first scrape failed: %v", errs)
	}

	// Older than the lifetime.
	server.cacheMtx.Lock()
	entry := server.metricCache[cachedNamespace]
	entry.lastScrape = time.Now().Add(-30 * time.Second)
	server.metricCache[cachedNamespace] = entry
	server.cacheMtx.Unlock()

	second, errs := collectNamespaces(t, server)
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
	if sample[0].TimestampMs != nil {
		t.Error("a recollected sample carried an explicit timestamp")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
