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
	"sort"
	"strings"
	"testing"

	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// targetsFor builds the scrape targets for a list of database names on one
// server, the way discovery would when it knows every name.
func targetsFor(names ...string) []scrapeTarget {
	return targetsOn("h:5432", names...)
}

// targetsOn builds targets on a named server, so a test can have the same
// database name on two of them.
func targetsOn(server string, names ...string) []scrapeTarget {
	out := make([]scrapeTarget, 0, len(names))
	for _, n := range names {
		out = append(out, scrapeTarget{
			dsn:         "postgresql://u:p@" + server + "/" + n,
			databaseRef: databaseRef{server: server, datname: n},
		})
	}
	return out
}

// ref names one database on the default test server.
func ref(datname string) databaseRef { return databaseRef{server: "h:5432", datname: datname} }

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
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	c.setSelection(true, true, 10, targetsFor("one", "two"), []databaseRef{ref("skipped")}, 7)
	c.setOutcome(2, 1, 0)
	c.setDatabaseOutcome(ref("one"), "")
	c.setDatabaseOutcome(ref("two"), reasonConnect)

	want := []string{
		"pg_exporter_database_discovery_enabled",
		"pg_exporter_database_discovery_success",
		"pg_exporter_databases_discovered",
		"pg_exporter_databases_selected",
		"pg_exporter_databases_scraped",
		"pg_exporter_databases_failed",
		"pg_exporter_databases_excluded",
		"pg_exporter_database_selected{datname=one,server=h:5432}",
		"pg_exporter_database_selected{datname=two,server=h:5432}",
		"pg_exporter_database_scrape_success{datname=one,server=h:5432}",
		"pg_exporter_database_scrape_success{datname=two,server=h:5432}",
		"pg_exporter_database_scrape_error{datname=two,reason=connect,server=h:5432}",
		"pg_exporter_database_excluded{datname=skipped,reason=exclude_list,server=h:5432}",
		"pg_exporter_databases_attempted",
		"pg_exporter_databases_deferred",
		// The database that succeeded keeps a last-success time; the one that
		// failed has none to keep, because this process has never collected it.
		"pg_exporter_database_last_success_timestamp_seconds{datname=one,server=h:5432,target=}",
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

// TestCoverageInvariant is the property the whole set rests on. If the
// outcomes do not account for what was selected, the exporter has lost track
// of its own work and no alert built on these numbers can be trusted.
//
// Two invariants, because a deferral is neither a success nor a failure:
// attempted plus deferred is selected, and scraped plus failed is attempted.
func TestCoverageInvariant(t *testing.T) {
	for _, tc := range []struct{ selected, failed, deferred int }{
		{1, 0, 0}, {1, 1, 0}, {1, 0, 1}, {10, 0, 0}, {10, 3, 0}, {10, 3, 2},
		{10, 0, 10}, {10, 10, 0}, {120, 40, 40}, {0, 0, 0},
	} {
		c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
		c.setSelection(true, true, tc.selected, make([]scrapeTarget, tc.selected), nil, 0)
		c.setOutcome(tc.selected, tc.failed, tc.deferred)

		g := gathered(t, c)
		scraped := g["pg_exporter_databases_scraped"]
		failed := g["pg_exporter_databases_failed"]
		selected := g["pg_exporter_databases_selected"]
		attempted := g["pg_exporter_databases_attempted"]
		deferred := g["pg_exporter_databases_deferred"]

		if attempted+deferred != selected {
			t.Errorf("selected=%d failed=%d deferred=%d: attempted(%v) + deferred(%v) = %v, want %v",
				tc.selected, tc.failed, tc.deferred, attempted, deferred, attempted+deferred, selected)
		}
		if scraped+failed != attempted {
			t.Errorf("selected=%d failed=%d deferred=%d: scraped(%v) + failed(%v) = %v, want attempted %v",
				tc.selected, tc.failed, tc.deferred, scraped, failed, scraped+failed, attempted)
		}
		if deferred != float64(tc.deferred) {
			t.Errorf("selected=%d deferred=%d: published deferred %v", tc.selected, tc.deferred, deferred)
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
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	// Discovery enabled, discovery failed, so only the seed is known and only
	// the seed is selected.
	c.setSelection(true, false, 1, targetsFor("postgres"), nil, 0)
	c.setOutcome(1, 0, 0)

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
	healthy := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	healthy.setSelection(false, true, 1, targetsFor("postgres"), nil, 0)
	healthy.setOutcome(1, 0, 0)
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
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
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
func TestCoverageCountsUnacquiredWorkAsDeferred(t *testing.T) {
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
	if g["pg_exporter_databases_failed"] != 0 {
		t.Errorf("failed %v: work that was never attempted was reported as a failure of the database",
			g["pg_exporter_databases_failed"])
	}
	if g["pg_exporter_databases_deferred"] != float64(len(dsns)) {
		t.Errorf("deferred %v, want %d", g["pg_exporter_databases_deferred"], len(dsns))
	}
	if g["pg_exporter_databases_attempted"] != 0 {
		t.Errorf("attempted %v, want none: no permit was available for any of them",
			g["pg_exporter_databases_attempted"])
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
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	targets := targetsFor("app", "tenant_1", "tenant_2")
	c.setSelection(true, true, 3, targets, nil, 0)
	c.setDatabaseOutcome(ref("app"), "")
	c.setDatabaseOutcome(ref("tenant_1"), "")
	c.setDatabaseOutcome(ref("tenant_2"), reasonConnect)
	c.setOutcome(3, 1, 0)

	g := series(t, c)
	if g["pg_exporter_databases_scraped"] != 2 || g["pg_exporter_databases_failed"] != 1 {
		t.Fatalf("counts wrong: %v", g)
	}
	if g["pg_exporter_database_scrape_success{datname=tenant_2,server=h:5432}"] != 0 {
		t.Error("the database that failed is reported as a success")
	}
	if g["pg_exporter_database_scrape_error{datname=tenant_2,reason=connect,server=h:5432}"] != 1 {
		t.Error("no error series names the database that failed")
	}
	for _, ok := range []string{"app", "tenant_1"} {
		if g["pg_exporter_database_scrape_success{datname="+ok+",server=h:5432}"] != 1 {
			t.Errorf("%s was covered but is not reported as a success", ok)
		}
		if _, found := g["pg_exporter_database_scrape_error{datname="+ok+",reason=connect,server=h:5432}"]; found {
			t.Errorf("%s was covered but has an error series", ok)
		}
	}
}

// TestDatabaseSeriesClearedBetweenScrapes covers the failure mode a gauge
// vector invites. A database that is dropped from the server stops appearing
// in the selection, and without a reset its last value stays in the output
// forever: a deleted database reported as healthy, indefinitely.
func TestDatabaseSeriesClearedBetweenScrapes(t *testing.T) {
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")

	c.setSelection(true, true, 2, targetsFor("app", "going_away"), nil, 0)
	c.setDatabaseOutcome(ref("app"), "")
	c.setDatabaseOutcome(ref("going_away"), "")
	c.setOutcome(2, 0, 0)
	if g := series(t, c); g["pg_exporter_database_scrape_success{datname=going_away,server=h:5432}"] != 1 {
		t.Fatalf("setup: going_away was not reported as covered: %v", g)
	}

	// Next scrape: the database is gone.
	c.setSelection(true, true, 1, targetsFor("app"), nil, 0)
	c.setDatabaseOutcome(ref("app"), "")
	c.setOutcome(1, 0, 0)

	g := series(t, c)
	if _, found := g["pg_exporter_database_selected{datname=going_away,server=h:5432}"]; found {
		t.Error("a database that is no longer selected is still reported as selected")
	}
	if _, found := g["pg_exporter_database_scrape_success{datname=going_away,server=h:5432}"]; found {
		t.Error("a database that is gone is still reported as successfully scraped")
	}
}

// TestExcludedIsNeverFailed holds the line the contract draws: leaving a
// database out on purpose is a decision, and a decision is not a fault. An
// exclusion counted as a failure would page somebody for working as
// configured.
func TestExcludedIsNeverFailed(t *testing.T) {
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	c.setSelection(true, true, 5, targetsFor("app"), []databaseRef{ref("scratch"), ref("archive")}, 2)
	c.setDatabaseOutcome(ref("app"), "")
	c.setOutcome(1, 0, 0)

	g := series(t, c)
	if g["pg_exporter_databases_failed"] != 0 {
		t.Errorf("excluded databases were counted as failures: %v", g["pg_exporter_databases_failed"])
	}
	// Two named in the exclude list, two more left out by the include list.
	if g["pg_exporter_databases_excluded"] != 4 {
		t.Errorf("excluded count is %v, want 4", g["pg_exporter_databases_excluded"])
	}
	for _, name := range []string{"scratch", "archive"} {
		if g["pg_exporter_database_excluded{datname="+name+",reason=exclude_list,server=h:5432}"] != 1 {
			t.Errorf("%s is not reported as excluded", name)
		}
		if _, found := g["pg_exporter_database_scrape_error{datname="+name+",reason=connect,server=h:5432}"]; found {
			t.Errorf("%s is excluded but has a failure series", name)
		}
		if _, found := g["pg_exporter_database_selected{datname="+name+",server=h:5432}"]; found {
			t.Errorf("%s is excluded but is reported as selected", name)
		}
	}
	// The databases an include list left out are counted, not named. A series
	// per database would be a series per database on the whole server.
	if n := len(g); n > 14 {
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
	failed, _, _ := e.scrapeTargets(ch, targets)
	stop()

	if failed != len(targets) {
		t.Fatalf("%d of %d targets failed, want all of them", failed, len(targets))
	}
	g := series(t, e.coverage)
	for _, name := range []string{"app", "tenant_1"} {
		if g["pg_exporter_database_scrape_success{datname="+name+",server=127.0.0.1:1}"] != 0 {
			t.Errorf("%s is unreachable but reported as a success", name)
		}
		found := ""
		for _, reason := range []string{reasonConnect, reasonScrape, reasonNotAttempted} {
			if _, ok := g["pg_exporter_database_scrape_error{datname="+name+",reason="+reason+",server=127.0.0.1:1}"]; ok {
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
	failed, connFailures, deferred := e.scrapeTargets(ch, targets)
	stop()

	if deferred != len(targets) {
		t.Fatalf("%d of %d targets were deferred, want all of them", deferred, len(targets))
	}
	if failed != 0 {
		t.Errorf("%d targets failed: nothing was tried, so nothing is known to be wrong with them", failed)
	}
	if connFailures != 0 {
		t.Errorf("%d connection failures: nothing was connected to, so pg_up must keep its meaning", connFailures)
	}
	g := series(t, e.coverage)
	for _, name := range []string{"one", "two"} {
		if g["pg_exporter_database_deferred{datname="+name+",reason="+dbselect.ReasonNoPermit+",server=127.0.0.1:1}"] != 1 {
			t.Errorf("%s was never attempted but is not reported as deferred: %v", name, g)
		}
		if g["pg_exporter_database_scrape_success{datname="+name+",server=127.0.0.1:1}"] != 0 {
			t.Errorf("%s was never attempted but is reported as a success", name)
		}
		// Not an error. A deferral says the exporter ran out of room, and an
		// alert on the error series must not fire for it.
		for _, reason := range []string{reasonConnect, reasonScrape, reasonNotAttempted} {
			if _, found := g["pg_exporter_database_scrape_error{datname="+name+",reason="+reason+",server=127.0.0.1:1}"]; found {
				t.Errorf("%s was deferred and carries a %s error series", name, reason)
			}
		}
	}
}

// targetsForDSN builds targets whose DSNs point at a given host:port, so a
// test can choose somewhere that will not answer.
func targetsForDSN(hostport string, names ...string) []scrapeTarget {
	out := make([]scrapeTarget, 0, len(names))
	for _, n := range names {
		out = append(out, scrapeTarget{
			dsn:         "postgresql://u:p@" + hostport + "/" + n + "?sslmode=disable",
			databaseRef: databaseRef{server: hostport, datname: n},
		})
	}
	return out
}

// TestSameDatabaseNameOnTwoServersStaysApart covers the collision a datname
// label alone invites. The exporter accepts several DSNs, and two servers can
// each hold a database called postgres. Keyed on the name only, the second
// target overwrites the first: a failure on one server is erased by a success
// on another, and the aggregate count says two databases while one series
// claims to describe them both.
func TestSameDatabaseNameOnTwoServersStaysApart(t *testing.T) {
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	targets := append(targetsOn("10.0.0.1:5432", "postgres"), targetsOn("10.0.0.2:5432", "postgres")...)
	c.setSelection(true, true, 2, targets, nil, 0)
	c.setDatabaseOutcome(targets[0].databaseRef, "")
	c.setDatabaseOutcome(targets[1].databaseRef, reasonConnect)
	c.setOutcome(2, 1, 0)

	g := series(t, c)
	if g["pg_exporter_database_scrape_success{datname=postgres,server=10.0.0.1:5432}"] != 1 {
		t.Error("the healthy server's database is not reported as covered")
	}
	if g["pg_exporter_database_scrape_success{datname=postgres,server=10.0.0.2:5432}"] != 0 {
		t.Error("the failing server's database was overwritten by the healthy one")
	}
	if g["pg_exporter_database_scrape_error{datname=postgres,reason=connect,server=10.0.0.2:5432}"] != 1 {
		t.Error("the failure on the second server has no error series")
	}

	// The named series and the aggregate count have to agree about how many
	// databases there were. One named series for two counted targets is the
	// shape this collision takes in a response.
	named := 0
	for key := range g {
		if strings.HasPrefix(key, "pg_exporter_database_selected{") {
			named++
		}
	}
	if float64(named) != g["pg_exporter_databases_selected"] {
		t.Errorf("%d named series for %v selected databases", named, g["pg_exporter_databases_selected"])
	}
}

// TestExcludedSameNameOnTwoServersStaysApart is the same collision on the
// exclusion family, which is keyed the same way.
func TestExcludedSameNameOnTwoServersStaysApart(t *testing.T) {
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	c.setSelection(true, true, 4, targetsOn("10.0.0.1:5432", "app"), []databaseRef{
		{server: "10.0.0.1:5432", datname: "scratch"},
		{server: "10.0.0.2:5432", datname: "scratch"},
	}, 0)

	g := series(t, c)
	for _, server := range []string{"10.0.0.1:5432", "10.0.0.2:5432"} {
		if g["pg_exporter_database_excluded{datname=scratch,reason=exclude_list,server="+server+"}"] != 1 {
			t.Errorf("scratch on %s is not reported as excluded", server)
		}
	}
	if g["pg_exporter_databases_excluded"] != 2 {
		t.Errorf("excluded count is %v, want 2", g["pg_exporter_databases_excluded"])
	}
}

// TestLastSuccessSurvivesAFailingScrape is what the series exists for.
//
// The counts say what this scrape did. They cannot say how long a database has
// been failing, and that is the first question an operator asks: a database
// that failed once is a blip, and one that has not been collected for an hour
// is an outage. The store is the process's, so the answer survives the
// exporter the handler builds and throws away for each request.
func TestLastSuccessSurvivesAFailingScrape(t *testing.T) {
	store := lastsuccess.New()
	target := scrapeTarget{
		dsn:         "postgresql://u:p@h:5432/app",
		databaseRef: databaseRef{server: "h:5432", datname: "app", identity: "1"},
	}

	// A scrape that worked.
	working := newCoverageMetrics(nil, store, "exporter")
	working.setSelection(true, true, 1, []scrapeTarget{target}, nil, 0)
	working.setDatabaseOutcome(target.databaseRef, "")
	working.setOutcome(1, 0, 0)

	first := gathered(t, working)
	recorded := first["pg_exporter_database_last_success_timestamp_seconds"]
	if recorded == 0 {
		t.Fatalf("a successful scrape recorded no last success: %v", first)
	}

	// A later scrape that failed, from a fresh set of metrics, which is what
	// the next request builds.
	failing := newCoverageMetrics(nil, store, "exporter")
	failing.setSelection(true, true, 1, []scrapeTarget{target}, nil, 0)
	failing.setDatabaseOutcome(target.databaseRef, reasonConnect)
	failing.setOutcome(1, 1, 0)

	second := gathered(t, failing)
	if got := second["pg_exporter_database_scrape_success"]; got != 0 {
		t.Errorf("the failing scrape reports success %v", got)
	}
	kept := second["pg_exporter_database_last_success_timestamp_seconds"]
	if kept != recorded {
		t.Errorf("the last success reads %v after a failing scrape, want the %v it recorded before",
			kept, recorded)
	}
}

// TestADatabaseNeverCollectedHasNoLastSuccess keeps the absent case absent. A
// zero there says the database was last collected at the epoch, which is a
// different claim from never having been collected.
func TestADatabaseNeverCollectedHasNoLastSuccess(t *testing.T) {
	target := scrapeTarget{
		dsn:         "postgresql://u:p@h:5432/app",
		databaseRef: databaseRef{server: "h:5432", datname: "app", identity: "1"},
	}
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")
	c.setSelection(true, true, 1, []scrapeTarget{target}, nil, 0)
	c.setDatabaseOutcome(target.databaseRef, reasonConnect)
	c.setOutcome(1, 1, 0)

	for name := range series(t, c) {
		if strings.HasPrefix(name, "pg_exporter_database_last_success_timestamp_seconds") {
			t.Errorf("a database nothing has ever collected reports %s", name)
		}
	}
}

// TestOneJobDoesNotInheritAnothersLastSuccess is ChatGPT's case, found by
// writing it against the draft.
//
// One store serves the built-in exporter and each custom-query resolution, and
// they all reach the same database over the same connection. They succeed and
// fail independently, so a key without the job lets an hourly job's success
// answer for a five-second one that is failing.
func TestOneJobDoesNotInheritAnothersLastSuccess(t *testing.T) {
	store := lastsuccess.New()
	target := scrapeTarget{
		dsn:         "postgresql://u:p@h:5432/app",
		databaseRef: databaseRef{server: "h:5432", datname: "app", identity: "1"},
	}

	hr := newCoverageMetrics(nil, store, "custom_query.hr")
	hr.setSelection(true, true, 1, []scrapeTarget{target}, nil, 0)
	hr.setDatabaseOutcome(target.databaseRef, "")
	hr.setOutcome(1, 0, 0)
	if gathered(t, hr)["pg_exporter_database_last_success_timestamp_seconds"] == 0 {
		t.Fatal("the high-resolution job recorded no success, so there is nothing for the other to inherit")
	}

	lr := newCoverageMetrics(nil, store, "custom_query.lr")
	lr.setSelection(true, true, 1, []scrapeTarget{target}, nil, 0)
	lr.setDatabaseOutcome(target.databaseRef, reasonConnect)
	lr.setOutcome(1, 1, 0)

	for name := range series(t, lr) {
		if strings.HasPrefix(name, "pg_exporter_database_last_success_timestamp_seconds") {
			t.Errorf("the low-resolution job never succeeded and reports %s", name)
		}
	}
}

// TestTwoTargetsOnOneDatabaseKeepSeparateLastSuccess is the other case ChatGPT
// found: internal identity alone does not separate what is published.
//
// Two connections to the same database, which an operator configures to
// monitor it as two roles. Both succeed, and the series have to be two, or one
// connection's time is reported for the other and the registry keeps whichever
// came last.
func TestTwoTargetsOnOneDatabaseKeepSeparateLastSuccess(t *testing.T) {
	store := lastsuccess.New()
	first := scrapeTarget{
		dsn:         "postgresql://a:p@h:5432/app",
		databaseRef: databaseRef{server: "h:5432", datname: "app", identity: "1"},
	}
	second := scrapeTarget{
		dsn:         "postgresql://b:p@h:5432/app",
		databaseRef: databaseRef{server: "h:5432", datname: "app", identity: "2"},
	}

	c := newCoverageMetrics(nil, store, "exporter")
	c.setSelection(true, true, 2, []scrapeTarget{first, second}, nil, 0)
	c.setDatabaseOutcome(first.databaseRef, "")
	c.setDatabaseOutcome(second.databaseRef, "")
	c.setOutcome(2, 0, 0)

	published := 0
	for name := range series(t, c) {
		if strings.HasPrefix(name, "pg_exporter_database_last_success_timestamp_seconds") {
			published++
		}
	}
	if published != 2 {
		t.Errorf("two connections to one database published %d last-success series, want 2", published)
	}
}
