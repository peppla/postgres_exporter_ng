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

package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// TestSelectDatabases pins the selection rule, which decides coverage and had
// no test before. The rule is: exclusion wins, and a non-empty inclusion list
// is a whitelist.
func TestSelectDatabases(t *testing.T) {
	names := []string{"postgres", "app", "tenant_1", "template0"}

	for _, tc := range []struct {
		name    string
		exclude []string
		include []string
		want    []string
	}{
		{
			name: "no filters keeps everything in order",
			want: []string{"postgres", "app", "tenant_1", "template0"},
		},
		{
			name:    "exclusion removes by exact name",
			exclude: []string{"template0", "postgres"},
			want:    []string{"app", "tenant_1"},
		},
		{
			name:    "a non-empty inclusion list is a whitelist",
			include: []string{"app"},
			want:    []string{"app"},
		},
		{
			name:    "exclusion beats inclusion for the same name",
			exclude: []string{"app"},
			include: []string{"app", "tenant_1"},
			want:    []string{"tenant_1"},
		},
		{
			name:    "inclusion naming nothing present selects nothing",
			include: []string{"absent"},
			want:    []string{},
		},
		{
			name:    "excluding everything selects nothing",
			exclude: names,
			want:    []string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectDatabases(names, tc.exclude, tc.include)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
			// The input must not be modified: the caller reuses it.
			if !reflect.DeepEqual(names, []string{"postgres", "app", "tenant_1", "template0"}) {
				t.Errorf("selectDatabases modified its input: %q", names)
			}
		})
	}
}

func gathered(t *testing.T, c *coverageMetrics) map[string]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.CollectorFunc(func(ch chan<- prometheus.Metric) {
		c.collect(ch)
	}))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %s", err)
	}
	// Gather returns written protobufs, so the value is read straight off them.
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			out[mf.GetName()] = m.GetGauge().GetValue()
		}
	}
	return out
}

// TestCoverageMetricNames pins the published names. They are the interface an
// operator and an alert both depend on, so a rename has to be a deliberate act
// that fails this test rather than a quiet edit.
func TestCoverageMetricNames(t *testing.T) {
	c := newCoverageMetrics(nil)
	c.setSelection(true, true, 10, 8)
	c.setOutcome(8, 1)

	want := []string{
		"pg_exporter_database_discovery_enabled",
		"pg_exporter_database_discovery_success",
		"pg_exporter_databases_discovered",
		"pg_exporter_databases_selected",
		"pg_exporter_databases_scraped",
		"pg_exporter_databases_failed",
	}
	got := gathered(t, c)
	if len(got) != len(want) {
		t.Fatalf("published %d metrics, expected %d: %v", len(got), len(want), got)
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("%s was not published", name)
		}
	}
}

// TestCoverageInvariant is the property the whole set rests on. If scraped and
// failed do not account for selected, the exporter has lost track of its own
// work and no alert built on these numbers can be trusted.
func TestCoverageInvariant(t *testing.T) {
	for _, tc := range []struct{ selected, failed int }{
		{1, 0}, {1, 1}, {10, 0}, {10, 3}, {10, 10}, {120, 40}, {0, 0},
	} {
		c := newCoverageMetrics(nil)
		c.setSelection(true, true, tc.selected, tc.selected)
		c.setOutcome(tc.selected, tc.failed)

		g := gathered(t, c)
		scraped := g["pg_exporter_databases_scraped"]
		failed := g["pg_exporter_databases_failed"]
		selected := g["pg_exporter_databases_selected"]

		if scraped+failed != selected {
			t.Errorf("selected=%d failed=%d: scraped(%v) + failed(%v) = %v, want %v",
				tc.selected, tc.failed, scraped, failed, scraped+failed, selected)
		}
		if failed != float64(tc.failed) {
			t.Errorf("selected=%d failed=%d: published failed %v", tc.selected, tc.failed, failed)
		}
	}
}

// TestCoverageReportsDiscoveryFailure is the case the metrics exist for. When
// discovery fails, the DSN list silently falls back to the configured database:
// one database covered where fifty were expected, and nothing in the old output
// said so. pg_up stays 1, because the one database that is left works.
func TestCoverageReportsDiscoveryFailure(t *testing.T) {
	c := newCoverageMetrics(nil)
	// Discovery enabled, discovery failed, so only the seed is known and only
	// the seed is selected.
	c.setSelection(true, false, 1, 1)
	c.setOutcome(1, 0)

	g := gathered(t, c)
	if g["pg_exporter_database_discovery_enabled"] != 1 {
		t.Error("discovery_enabled should be 1 when auto-discovery is configured")
	}
	if g["pg_exporter_database_discovery_success"] != 0 {
		t.Error("discovery_success should be 0 when discovery failed")
	}
	if g["pg_exporter_databases_scraped"] != 1 {
		t.Error("the seed database is still scraped after a discovery failure")
	}

	// And the healthy case must be distinguishable from it, which is the whole
	// point: a scrape of one database because there is one database, against a
	// scrape of one database because discovery broke.
	healthy := newCoverageMetrics(nil)
	healthy.setSelection(false, true, 1, 1)
	healthy.setOutcome(1, 0)
	h := gathered(t, healthy)

	if h["pg_exporter_database_discovery_success"] == g["pg_exporter_database_discovery_success"] {
		t.Error("a discovery failure is indistinguishable from discovery being switched off")
	}
}

// TestCoverageSelectionRetainedWithoutOutcome covers helper state only: values
// set by setSelection are still there when setOutcome never runs.
//
// It does NOT show that a client would receive them. Collect runs scrape
// synchronously and emits the gauges after it returns, so a scrape that never
// finishes delivers nothing at all. An earlier revision of this test was named
// as though it proved otherwise.
func TestCoverageSelectionRetainedWithoutOutcome(t *testing.T) {
	c := newCoverageMetrics(nil)
	c.setSelection(true, true, 120, 120)
	// No setOutcome: the scrape was cut short.

	g := gathered(t, c)
	if g["pg_exporter_databases_discovered"] != 120 || g["pg_exporter_databases_selected"] != 120 {
		t.Errorf("selection was lost when no outcome followed: %v", g)
	}
}

// TestDiscoveryFailsOnUnparseableSeed covers a seed that never reaches the
// server. Both parse paths used to drop the DSN and still report that
// determining the set succeeded, so a single typo could publish
// "nothing to cover, and that is correct".
func TestDiscoveryFailsOnUnparseableSeed(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
	}{
		{"malformed URI", "postgresql://user:pass@host:not-a-port/db"},
		{"neither URI nor keyword form", "this is not a dsn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Exporter{dsn: []string{tc.dsn}}
			dsns, discovered, ok := e.discoverDatabaseDSNs()
			if ok {
				t.Error("discovery reported success for a seed it could not parse")
			}
			if len(dsns) != 0 {
				t.Errorf("expected no DSNs, got %q", dsns)
			}
			if discovered != 0 {
				t.Errorf("expected nothing discovered, got %d", discovered)
			}
		})
	}
}

// TestCoverageCountsUnacquiredWorkAsFailed drives the real scrape path with an
// exhausted semaphore and a cancelled context, so every database is skipped
// before anything is attempted.
//
// This is the case that made the first revision of these metrics lie: the
// skipped work returned without touching the error counter, and scraped is
// derived from selected minus errors, so databases that were never attempted
// were published as successfully covered. No database is needed to show it,
// because acquisition fails first.
func TestCoverageCountsUnacquiredWorkAsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // every Acquire now fails immediately

	dsns := []string{
		"postgresql://u:p@127.0.0.1:1/one?sslmode=disable",
		"postgresql://u:p@127.0.0.1:1/two?sslmode=disable",
		"postgresql://u:p@127.0.0.1:1/three?sslmode=disable",
	}
	e := NewExporter(dsns,
		WithContext(ctx),
		WithConnectionsSemaphore(semaphore.NewWeighted(1)),
	)

	ch := make(chan prometheus.Metric, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	e.scrape(ch)
	close(ch)
	<-done

	g := gathered(t, e.coverage)
	if g["pg_exporter_databases_selected"] != float64(len(dsns)) {
		t.Errorf("selected %v, want %d", g["pg_exporter_databases_selected"], len(dsns))
	}
	if g["pg_exporter_databases_scraped"] != 0 {
		t.Errorf("scraped %v: work that was never attempted was reported as covered",
			g["pg_exporter_databases_scraped"])
	}
	if g["pg_exporter_databases_failed"] != float64(len(dsns)) {
		t.Errorf("failed %v, want %d", g["pg_exporter_databases_failed"], len(dsns))
	}
}
