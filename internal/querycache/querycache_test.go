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

package querycache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func sample(value float64) []prometheus.Metric {
	desc := prometheus.NewDesc("cache_test_metric", "a metric to cache", nil, nil)
	return []prometheus.Metric{prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)}
}

// TestEachPartOfTheKeyMakesADifferentEntry is what stops one database's
// result being served for another.
//
// Every field exists because two results that differ in it are different
// measurements. A key missing one of them would serve a cached answer from the
// wrong server, the wrong database, the wrong resolution, or the version of a
// query file before it was edited.
func TestEachPartOfTheKeyMakesADifferentEntry(t *testing.T) {
	base := Key{
		Target:     "db1:5432",
		Database:   "app",
		Collector:  "custom_query.lr",
		Namespace:  "a_namespace",
		Definition: "abc123",
	}

	for _, tc := range []struct {
		field  string
		change func(Key) Key
	}{
		{"Target", func(k Key) Key { k.Target = "db2:5432"; return k }},
		{"Database", func(k Key) Key { k.Database = "other"; return k }},
		{"Collector", func(k Key) Key { k.Collector = "custom_query.hr"; return k }},
		{"Namespace", func(k Key) Key { k.Namespace = "another"; return k }},
		{"Definition", func(k Key) Key { k.Definition = "def456"; return k }},
	} {
		t.Run(tc.field, func(t *testing.T) {
			c := New()
			c.Put(base, sample(1), time.Now(), time.Minute)

			if _, _, ok := c.Get(tc.change(base)); ok {
				t.Errorf("a key differing only in %s read the other entry", tc.field)
			}
			if _, _, ok := c.Get(base); !ok {
				t.Errorf("the original entry went missing")
			}
		})
	}
}

// TestAnExpiredEntryIsNotServedAndIsRemoved covers the lifetime.
func TestAnExpiredEntryIsNotServedAndIsRemoved(t *testing.T) {
	c := New()
	key := Key{Namespace: "a_namespace"}

	// Collected a minute ago with a ten-second lifetime, so already past it.
	c.Put(key, sample(1), time.Now().Add(-time.Minute), 10*time.Second)

	if _, _, ok := c.Get(key); ok {
		t.Error("an expired entry was served")
	}
	if c.Len() != 0 {
		t.Errorf("an expired entry was left behind, %d entries remain", c.Len())
	}
}

// TestAnOlderObservationDoesNotReplaceANewer covers overlapping refreshes.
//
// Two scrapes that both miss run the query at the same time and finish in the
// order the server answers. The one that started first can finish last while
// holding a value it read before the other one started, and storing it then
// serves a superseded measurement until it expires.
func TestAnOlderObservationDoesNotReplaceANewer(t *testing.T) {
	c := New()
	key := Key{Namespace: "a_namespace"}

	newer := time.Now()
	older := newer.Add(-time.Second)

	// The slow scrape started first and finishes last, so it writes second.
	c.Put(key, sample(2), newer, time.Minute)
	c.Put(key, sample(1), older, time.Minute)

	metrics, collectedAt, ok := c.Get(key)
	if !ok {
		t.Fatal("the entry went missing")
	}
	if got := metricValue(t, metrics); got != 2 {
		t.Errorf("the cache holds %v, want the newer 2", got)
	}
	if !collectedAt.Equal(newer) {
		t.Errorf("the entry is dated %s, want %s, which is when the value it holds was collected", collectedAt, newer)
	}

	// A newer one still replaces it, or nothing would ever be refreshed.
	c.Put(key, sample(3), newer.Add(time.Second), time.Minute)
	metrics, _, ok = c.Get(key)
	if !ok {
		t.Fatal("the entry went missing")
	}
	if got := metricValue(t, metrics); got != 3 {
		t.Errorf("the cache holds %v after a newer observation, want 3", got)
	}
}

// metricValue reads the one value a sample carries.
func metricValue(t *testing.T, metrics []prometheus.Metric) float64 {
	t.Helper()
	if len(metrics) != 1 {
		t.Fatalf("the entry holds %d metrics, want 1", len(metrics))
	}
	pb := &dto.Metric{}
	if err := metrics[0].Write(pb); err != nil {
		t.Fatalf("writing a metric: %s", err)
	}
	return pb.GetGauge().GetValue()
}

// TestAZeroLifetimeStoresNothing covers a namespace whose query file stopped
// asking to be cached.
//
// Leaving the old entry would serve a result the file no longer wants kept,
// for as long as the lifetime it was stored with.
func TestAZeroLifetimeStoresNothing(t *testing.T) {
	c := New()
	key := Key{Namespace: "a_namespace"}

	c.Put(key, sample(1), time.Now(), time.Minute)
	if _, _, ok := c.Get(key); !ok {
		t.Fatal("the entry was not stored, so this test proves nothing")
	}

	c.Put(key, sample(2), time.Now(), 0)
	if _, _, ok := c.Get(key); ok {
		t.Error("a zero lifetime left the previous entry in place")
	}
}

// TestExpiredEntriesAreSweptOnWrite is what keeps the cache bounded.
//
// Entries are keyed by database, and databases come and go. Without a sweep
// the map grows with every database and every query file the exporter ever
// saw, and nothing would read those keys again to notice they had expired.
func TestExpiredEntriesAreSweptOnWrite(t *testing.T) {
	c := New()
	stale := time.Now().Add(-time.Hour)

	for i := 0; i < 50; i++ {
		c.Put(Key{Database: fmt.Sprintf("gone_%d", i)}, sample(1), stale, time.Minute)
	}
	// Each Put sweeps, so the expired ones never accumulate.
	if c.Len() > 1 {
		t.Errorf("%d expired entries are still held", c.Len())
	}

	// A live entry survives the sweep.
	c.Put(Key{Database: "current"}, sample(1), time.Now(), time.Minute)
	if _, _, ok := c.Get(Key{Database: "current"}); !ok {
		t.Error("the sweep removed a live entry")
	}
}

// TestConcurrentScrapesShareTheCacheSafely covers overlapping scrapes, which
// is the normal case for an endpoint two Prometheus jobs are reading.
func TestConcurrentScrapesShareTheCacheSafely(t *testing.T) {
	c := New()
	var wg sync.WaitGroup

	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := Key{Database: fmt.Sprintf("db_%d", worker%3), Namespace: "shared"}
			for i := 0; i < 200; i++ {
				c.Put(key, sample(float64(i)), time.Now(), time.Minute)
				c.Get(key)
				c.Len()
			}
		}(worker)
	}
	wg.Wait()

	if c.Len() == 0 {
		t.Error("nothing survived the concurrent writers")
	}
}

// TestANilCacheIsUsable so an exporter built without one, which every test
// that does not pass one is, does not have to guard every call.
func TestANilCacheIsUsable(t *testing.T) {
	var c *Cache
	c.Put(Key{Namespace: "x"}, sample(1), time.Now(), time.Minute)
	if _, _, ok := c.Get(Key{Namespace: "x"}); ok {
		t.Error("a nil cache returned an entry")
	}
	if c.Len() != 0 {
		t.Error("a nil cache reported entries")
	}
	c.Clear()
}
