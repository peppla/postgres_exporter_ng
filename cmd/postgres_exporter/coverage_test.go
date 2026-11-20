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
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// targetsFor builds the scrape targets for a list of database names, the way
// discovery would when it knows every name.
func targetsFor(names ...string) []scrapeTarget {
	out := make([]scrapeTarget, 0, len(names))
	for _, n := range names {
		out = append(out, scrapeTarget{dsn: "postgresql://u:p@h:5432/" + n, datname: n})
	}
	return out
}

// TestSelectDatabases pins the selection rule, which decides coverage and had
// no test before. The rule is: exclusion wins, an inclusion list that was
// given is a whitelist, and an inclusion list that was given empty selects
// nothing rather than everything.
func TestSelectDatabases(t *testing.T) {
	names := []string{"postgres", "app", "tenant_1", "template0"}

	for _, tc := range []struct {
		name    string
		exclude databaseList
		include databaseList
		want    []string
	}{
		{
			name: "no filters keeps everything in order",
			want: []string{"postgres", "app", "tenant_1", "template0"},
		},
		{
			name:    "exclusion removes by exact name",
			exclude: listOf([]string{"template0", "postgres"}),
			want:    []string{"app", "tenant_1"},
		},
		{
			name:    "a non-empty inclusion list is a whitelist",
			include: listOf([]string{"app"}),
			want:    []string{"app"},
		},
		{
			name:    "exclusion beats inclusion for the same name",
			exclude: listOf([]string{"app"}),
			include: listOf([]string{"app", "tenant_1"}),
			want:    []string{"tenant_1"},
		},
		{
			name:    "inclusion naming nothing present selects nothing",
			include: listOf([]string{"absent"}),
			want:    []string{},
		},
		{
			name:    "excluding everything selects nothing",
			exclude: listOf(names),
			want:    []string{},
		},
		{
			// Pep, 2026-11-20. The opposite reading, that an empty list means
			// no restriction, is what a plain []string gives you by accident.
			name:    "an inclusion list given empty selects nothing",
			include: listOf([]string{}),
			want:    []string{},
		},
		{
			name:    "an inclusion list given empty beats an exclusion list too",
			exclude: listOf([]string{"app"}),
			include: listOf([]string{}),
			want:    []string{},
		},
		{
			name:    "an exclusion list given empty excludes nothing",
			exclude: listOf([]string{}),
			want:    []string{"postgres", "app", "tenant_1", "template0"},
		},
		{
			// The inherited flag surface: unset arrives as one empty string.
			name:    "an unset comma-separated flag is an absent list",
			exclude: listFromFlag([]string{""}),
			include: listFromFlag([]string{""}),
			want:    []string{"postgres", "app", "tenant_1", "template0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectDatabases(names, tc.exclude, tc.include)
			if !reflect.DeepEqual(got.selected, tc.want) {
				t.Errorf("got %q, want %q", got.selected, tc.want)
			}
			// Everything the rule left out is accounted for, as one or the
			// other. A database that fell out of both would be invisible.
			if len(got.selected)+len(got.excluded)+got.notIncluded != len(names) {
				t.Errorf("%d selected + %d excluded + %d not included != %d offered",
					len(got.selected), len(got.excluded), got.notIncluded, len(names))
			}
			for _, name := range got.excluded {
				if !contains(tc.exclude.names, name) {
					t.Errorf("%q reported as excluded but is not in the exclude list", name)
				}
			}
			// The input must not be modified: the caller reuses it.
			if !reflect.DeepEqual(names, []string{"postgres", "app", "tenant_1", "template0"}) {
				t.Errorf("selectDatabases modified its input: %q", names)
			}
		})
	}
}

// series keys a sample by name and sorted labels, so vector families with
// several samples do not collapse into one entry the way gathered's do.
func series(t *testing.T, c *coverageMetrics) map[string]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.CollectorFunc(func(ch chan<- prometheus.Metric) {
		c.collect(ch)
	}))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %s", err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			parts := make([]string, 0, len(m.Label))
			for _, l := range m.Label {
				parts = append(parts, l.GetName()+"="+l.GetValue())
			}
			sort.Strings(parts)
			key := mf.GetName()
			if len(parts) > 0 {
				key += "{" + strings.Join(parts, ",") + "}"
			}
			out[key] = m.GetGauge().GetValue()
		}
	}
	return out
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
	c.setSelection(true, true, 10, targetsFor("one", "two"), []string{"skipped"}, 7)
	c.setOutcome(2, 1)
	c.setDatabaseOutcome("one", "")
	c.setDatabaseOutcome("two", reasonConnect)

	want := []string{
		"pg_exporter_database_discovery_enabled",
		"pg_exporter_database_discovery_success",
		"pg_exporter_databases_discovered",
		"pg_exporter_databases_selected",
		"pg_exporter_databases_scraped",
		"pg_exporter_databases_failed",
		"pg_exporter_databases_excluded",
		"pg_exporter_database_selected{datname=one}",
		"pg_exporter_database_selected{datname=two}",
		"pg_exporter_database_scrape_success{datname=one}",
		"pg_exporter_database_scrape_success{datname=two}",
		"pg_exporter_database_scrape_error{datname=two,reason=connect}",
		"pg_exporter_database_excluded{datname=skipped,reason=exclude_list}",
	}
	got := series(t, c)
	if len(got) != len(want) {
		t.Fatalf("published %d series, expected %d: %v", len(got), len(want), got)
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
		c.setSelection(true, true, tc.selected, make([]scrapeTarget, tc.selected), nil, 0)
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
	c.setSelection(true, false, 1, targetsFor("postgres"), nil, 0)
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
	healthy.setSelection(false, true, 1, targetsFor("postgres"), nil, 0)
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
	c.setSelection(true, true, 120, make([]scrapeTarget, 120), nil, 0)
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
			d := e.discoverDatabaseDSNs()
			if d.ok {
				t.Error("discovery reported success for a seed it could not parse")
			}
			if len(d.targets) != 0 {
				t.Errorf("expected no targets, got %v", d.targets)
			}
			if d.discovered != 0 {
				t.Errorf("expected nothing discovered, got %d", d.discovered)
			}
		})
	}
}

// TestCoverageCountsUnacquiredWorkAsFailed drives the real scrape path with a
// cancelled context, so every database is skipped before anything is
// attempted.
//
// The semaphore is NOT exhausted: its single permit is free the whole time.
// What fails the acquisition is the cancelled context, because Acquire checks
// ctx.Done() before capacity and prefers to fail even when it could take the
// permit without blocking (golang.org/x/sync/semaphore). The semaphore still
// has to be present, since a nil connSema skips the acquisition entirely and
// the path under test would never run.
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

// drain consumes a metric channel while the scrape writes to it.
func drain() (chan prometheus.Metric, func()) {
	ch := make(chan prometheus.Metric, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	return ch, func() { close(ch); <-done }
}

// TestDatabaseOutcomeNamesTheFailure is the question the counts cannot answer.
// Three databases, one of them unreachable: the counts say two of three, and
// only the per-database series says which one went dark.
func TestDatabaseOutcomeNamesTheFailure(t *testing.T) {
	c := newCoverageMetrics(nil)
	targets := targetsFor("app", "tenant_1", "tenant_2")
	c.setSelection(true, true, 3, targets, nil, 0)
	c.setDatabaseOutcome("app", "")
	c.setDatabaseOutcome("tenant_1", "")
	c.setDatabaseOutcome("tenant_2", reasonConnect)
	c.setOutcome(3, 1)

	g := series(t, c)
	if g["pg_exporter_databases_scraped"] != 2 || g["pg_exporter_databases_failed"] != 1 {
		t.Fatalf("counts wrong: %v", g)
	}
	if g["pg_exporter_database_scrape_success{datname=tenant_2}"] != 0 {
		t.Error("the database that failed is reported as a success")
	}
	if g["pg_exporter_database_scrape_error{datname=tenant_2,reason=connect}"] != 1 {
		t.Error("no error series names the database that failed")
	}
	for _, ok := range []string{"app", "tenant_1"} {
		if g["pg_exporter_database_scrape_success{datname="+ok+"}"] != 1 {
			t.Errorf("%s was covered but is not reported as a success", ok)
		}
		if _, found := g["pg_exporter_database_scrape_error{datname="+ok+",reason=connect}"]; found {
			t.Errorf("%s was covered but has an error series", ok)
		}
	}
}

// TestDatabaseSeriesClearedBetweenScrapes covers the failure mode a gauge
// vector invites. A database that is dropped from the server stops appearing
// in the selection, and without a reset its last value stays in the output
// forever: a deleted database reported as healthy, indefinitely.
func TestDatabaseSeriesClearedBetweenScrapes(t *testing.T) {
	c := newCoverageMetrics(nil)

	c.setSelection(true, true, 2, targetsFor("app", "going_away"), nil, 0)
	c.setDatabaseOutcome("app", "")
	c.setDatabaseOutcome("going_away", "")
	c.setOutcome(2, 0)
	if g := series(t, c); g["pg_exporter_database_scrape_success{datname=going_away}"] != 1 {
		t.Fatalf("setup: going_away was not reported as covered: %v", g)
	}

	// Next scrape: the database is gone.
	c.setSelection(true, true, 1, targetsFor("app"), nil, 0)
	c.setDatabaseOutcome("app", "")
	c.setOutcome(1, 0)

	g := series(t, c)
	if _, found := g["pg_exporter_database_selected{datname=going_away}"]; found {
		t.Error("a database that is no longer selected is still reported as selected")
	}
	if _, found := g["pg_exporter_database_scrape_success{datname=going_away}"]; found {
		t.Error("a database that is gone is still reported as successfully scraped")
	}
}

// TestExcludedIsNeverFailed holds the line the contract draws: leaving a
// database out on purpose is a decision, and a decision is not a fault. An
// exclusion counted as a failure would page somebody for working as
// configured.
func TestExcludedIsNeverFailed(t *testing.T) {
	c := newCoverageMetrics(nil)
	c.setSelection(true, true, 5, targetsFor("app"), []string{"scratch", "archive"}, 2)
	c.setDatabaseOutcome("app", "")
	c.setOutcome(1, 0)

	g := series(t, c)
	if g["pg_exporter_databases_failed"] != 0 {
		t.Errorf("excluded databases were counted as failures: %v", g["pg_exporter_databases_failed"])
	}
	// Two named in the exclude list, two more left out by the include list.
	if g["pg_exporter_databases_excluded"] != 4 {
		t.Errorf("excluded count is %v, want 4", g["pg_exporter_databases_excluded"])
	}
	for _, name := range []string{"scratch", "archive"} {
		if g["pg_exporter_database_excluded{datname="+name+",reason=exclude_list}"] != 1 {
			t.Errorf("%s is not reported as excluded", name)
		}
		if _, found := g["pg_exporter_database_scrape_error{datname="+name+",reason=connect}"]; found {
			t.Errorf("%s is excluded but has a failure series", name)
		}
		if _, found := g["pg_exporter_database_selected{datname="+name+"}"]; found {
			t.Errorf("%s is excluded but is reported as selected", name)
		}
	}
	// The databases an include list left out are counted, not named. A series
	// per database would be a series per database on the whole server.
	if n := len(g); n > 12 {
		t.Errorf("%d series for one selected database and four exclusions: %v", n, g)
	}
}

// TestUnreachableDatabasesAreNamed drives the real fan-out against a port with
// nothing listening, with named targets, and checks that each one is reported
// by name with a reason.
//
// The counts for this case were already covered. What was not is that the
// reason is a closed label value rather than whatever the driver put in the
// error string.
func TestUnreachableDatabasesAreNamed(t *testing.T) {
	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/app?sslmode=disable"})
	targets := targetsForDSN("127.0.0.1:1", "app", "tenant_1")
	e.coverage.setSelection(true, true, 2, targets, nil, 0)

	ch, stop := drain()
	failed, _ := e.scrapeTargets(ch, targets)
	stop()

	if failed != len(targets) {
		t.Fatalf("%d of %d targets failed, want all of them", failed, len(targets))
	}
	g := series(t, e.coverage)
	for _, name := range []string{"app", "tenant_1"} {
		if g["pg_exporter_database_scrape_success{datname="+name+"}"] != 0 {
			t.Errorf("%s is unreachable but reported as a success", name)
		}
		found := ""
		for _, reason := range []string{reasonConnect, reasonScrape, reasonNotAttempted} {
			if _, ok := g["pg_exporter_database_scrape_error{datname="+name+",reason="+reason+"}"]; ok {
				found = reason
			}
		}
		if found == "" {
			t.Errorf("%s failed with no reason series: %v", name, g)
		}
	}
}

// TestUnattemptedWorkIsNamedNotAttempted is the cancelled-context case with
// names attached. Work that never started must not be reported as covered, and
// must say that it never started rather than that it failed to connect: the
// two call for different responses.
func TestUnattemptedWorkIsNamedNotAttempted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // every Acquire now fails immediately

	targets := targetsForDSN("127.0.0.1:1", "one", "two")
	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/one?sslmode=disable"},
		WithContext(ctx),
		WithConnectionsSemaphore(semaphore.NewWeighted(1)),
	)
	e.coverage.setSelection(true, true, 2, targets, nil, 0)

	ch, stop := drain()
	failed, connFailures := e.scrapeTargets(ch, targets)
	stop()

	if failed != len(targets) {
		t.Fatalf("%d of %d targets failed, want all of them", failed, len(targets))
	}
	if connFailures != 0 {
		t.Errorf("%d connection failures: nothing was connected to, so pg_up must keep its meaning", connFailures)
	}
	g := series(t, e.coverage)
	for _, name := range []string{"one", "two"} {
		if g["pg_exporter_database_scrape_error{datname="+name+",reason="+reasonNotAttempted+"}"] != 1 {
			t.Errorf("%s was never attempted but is not reported as such: %v", name, g)
		}
		if g["pg_exporter_database_scrape_success{datname="+name+"}"] != 0 {
			t.Errorf("%s was never attempted but is reported as a success", name)
		}
	}
}

// targetsForDSN builds targets whose DSNs point at a given host:port, so a
// test can choose somewhere that will not answer.
func targetsForDSN(hostport string, names ...string) []scrapeTarget {
	out := make([]scrapeTarget, 0, len(names))
	for _, n := range names {
		out = append(out, scrapeTarget{
			dsn:     "postgresql://u:p@" + hostport + "/" + n + "?sslmode=disable",
			datname: n,
		})
	}
	return out
}
