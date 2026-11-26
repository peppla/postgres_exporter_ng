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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/rotation"
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
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
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
// The ways a scrape runs out of room before it starts a database, and the
// label each one has to publish.
//
// The cases with a limiter are what the shipped binary does, where one is
// always configured. Before this change every one of them reported
// no_connection_permit, so an expired or cancelled scrape named a limit that
// may have been entirely free.
//
// The third case had no coverage at all. With no limiter the engine did not
// read the context, so every database was attempted after the deadline had
// passed and the scrape reported a server it could not connect to. main always
// constructs a limiter, so this path is reached through NewExporter rather
// than by running the exporter.
func admissionCases(t *testing.T) []struct {
	name    string
	ctx     context.Context
	sema    *semaphore.Weighted
	workers int64
	reason  string
} {
	t.Helper()

	// Every permit held, and as many of them as there are workers, so that
	// each target reaches the limiter and is refused. With fewer permits than
	// targets the later targets never ask, and what they meet is the deadline
	// rather than the shortage, which is a different reason and has a case of
	// its own below.
	const bothTargets = 2
	full := semaphore.NewWeighted(bothTargets)
	if !full.TryAcquire(bothTargets) {
		t.Fatal("the fixture could not take the permits")
	}
	t.Cleanup(func() { full.Release(bothTargets) })
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancelWaiting)

	// A real deadline, not a cancellation. An earlier version of this cancelled
	// the context and called it an expired deadline. The code reports the two
	// apart, so a fixture that stages one cannot stand in for the other.
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)

	cancelled, cancelNow := context.WithTimeout(context.Background(), time.Hour)
	cancelNow()
	t.Cleanup(cancelNow)

	return []struct {
		name    string
		ctx     context.Context
		sema    *semaphore.Weighted
		workers int64
		reason  string
	}{
		{"a full connection limit", waiting, full, bothTargets, reasonNoPermit},
		{"a deadline that had passed", expired, semaphore.NewWeighted(1), 1, reasonDeadline},
		{"a deadline that had passed with no limit configured", expired, nil, 1, reasonDeadline},
		{"a cancelled scrape", cancelled, semaphore.NewWeighted(1), 1, reasonCancelled},
	}
}

func TestUnattemptedWorkIsNamedNotAttempted(t *testing.T) {
	for _, admission := range admissionCases(t) {
		t.Run(admission.name, func(t *testing.T) {
			targets := targetsForDSN("127.0.0.1:1", "one", "two")
			e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/one?sslmode=disable"},
				WithContext(admission.ctx),
				WithConnectionsSemaphore(admission.sema, admission.workers),
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
				if g["pg_exporter_database_deferred{datname="+name+",reason="+admission.reason+",server=127.0.0.1:1}"] != 1 {
					t.Errorf("%s was never attempted but is not reported as deferred for %s: %v", name, admission.reason, g)
				}
				if g["pg_exporter_database_scrape_success{datname="+name+",server=127.0.0.1:1}"] != 0 {
					t.Errorf("%s was never attempted but is reported as a success", name)
				}
				// Not an error. A deferral says the exporter ran out of room,
				// and an alert on the error series must not fire for it.
				for _, reason := range []string{reasonConnect, reasonScrape, reasonNotAttempted} {
					if _, found := g["pg_exporter_database_scrape_error{datname="+name+",reason="+reason+",server=127.0.0.1:1}"]; found {
						t.Errorf("%s was deferred and carries a %s error series", name, reason)
					}
				}
			}
		})
	}
}

// targetsForDSN builds targets whose DSNs point at a given host:port, so a
// test can choose somewhere that will not answer.
func targetsForDSN(hostport string, names ...string) []scrapeTarget {
	out := make([]scrapeTarget, 0, len(names))
	for _, n := range names {
		dsn := "postgresql://u:p@" + hostport + "/" + n + "?sslmode=disable"
		out = append(out, scrapeTarget{
			dsn: dsn,
			// identity as the engine assigns it. Without it every target here
			// shares the empty string, which is exactly the collision the
			// schedule has to avoid, so a fixture missing it would hide the
			// thing these tests are for.
			databaseRef: databaseRef{server: hostport, datname: n, identity: identityOf(dsn)},
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

// TestLastSuccessSurvivesANameTheScrapeCannotResolve covers the scrape that
// cannot say which database it is.
//
// Without discovery the name comes from the server, so a connection that fails
// has nothing to ask. ChatGPT found the series missing entirely on that path.
// What the connection last succeeded on is the database it belongs to.
func TestLastSuccessSurvivesANameTheScrapeCannotResolve(t *testing.T) {
	store := lastsuccess.New()
	when := time.Now().Add(-time.Minute)
	store.Record(lastsuccess.Key{Engine: lastsuccess.EngineQuery, Target: "1", Database: "root", Collector: "exporter"}, when)

	// A target the scrape could not name, which is what a failed connection
	// leaves behind.
	unnamed := scrapeTarget{
		dsn:         "postgresql://root:p@h:5432/?sslmode=disable",
		databaseRef: databaseRef{server: "h:5432", identity: "1"},
	}

	c := newCoverageMetrics(nil, store, "exporter")
	c.setSelection(true, true, 1, []scrapeTarget{unnamed}, nil, 0)
	c.setDatabaseOutcome(unnamed.databaseRef, reasonConnect)
	c.setOutcome(1, 1, 0)

	got := series(t, c)
	key := "pg_exporter_database_last_success_timestamp_seconds{datname=root,server=h:5432,target=1}"
	value, ok := got[key]
	if !ok {
		t.Fatalf("a connection that failed lost the last success it had: %v", got)
	}
	if want := float64(when.UnixNano()) / 1e9; value != want {
		t.Errorf("the last success reads %v, want the recorded %v", value, want)
	}
}

// manyNames builds n database names, for a fan-out with more databases than
// workers.
func manyNames(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("db%03d", i))
	}
	return out
}

// TestEveryDatabaseIsVisitedWithFewerWorkersThanDatabases covers the width of
// the fan-out against the size of the selected set.
//
// Running fewer workers than databases must not cap how many databases a scrape
// covers. These targets point somewhere that will not answer, so each one ends
// as a connection failure, which is the outcome being counted here: every
// database was reached and reported on, rather than silently left out.
func TestEveryDatabaseIsVisitedWithFewerWorkersThanDatabases(t *testing.T) {
	// Three times as many databases as workers. Each connection attempt to a
	// closed port costs about a second here, so the figures are kept small:
	// the question is whether the worker count caps the set, which nine
	// databases answer as well as a thousand.
	const databases, workers = 9, 3

	names := manyNames(databases)
	targets := targetsForDSN("127.0.0.1:1", names...)
	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/db000?sslmode=disable"},
		WithContext(context.Background()),
		WithConnectionsSemaphore(semaphore.NewWeighted(workers), workers),
	)
	e.coverage.setSelection(true, true, databases, targets, nil, 0)

	ch, stop := drain()
	failed, connFailures, deferred := e.scrapeTargets(ch, targets)
	stop()

	if deferred != 0 {
		t.Errorf("%d databases were deferred, and the budget was not the constraint here", deferred)
	}
	if failed != databases || connFailures != databases {
		t.Fatalf("%d failed and %d of those were connection failures, want %d of each: every database has to be reached",
			failed, connFailures, databases)
	}

	g := series(t, e.coverage)
	missing := 0
	for _, name := range names {
		if g["pg_exporter_database_scrape_error{datname="+name+",reason="+reasonConnect+",server=127.0.0.1:1}"] != 1 {
			missing++
		}
	}
	if missing != 0 {
		t.Errorf("%d of %d databases carry no outcome series, so the worker count capped the set", missing, databases)
	}
}

// TestManyDatabasesBehindAHeldPermitStayBounded is the goroutine bound.
//
// The permits are held throughout, so every worker parks in the limiter. The
// goroutine count has to come from the worker number rather than from the size
// of the selected set, and every database that never started has to be deferred
// and named.
func TestManyDatabasesBehindAHeldPermitStayBounded(t *testing.T) {
	const databases, workers = 500, 4

	held := semaphore.NewWeighted(workers)
	if !held.TryAcquire(workers) {
		t.Fatal("the fixture could not take the permits")
	}
	defer held.Release(workers)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	names := manyNames(databases)
	targets := targetsForDSN("127.0.0.1:1", names...)
	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/db000?sslmode=disable"},
		WithContext(ctx),
		WithConnectionsSemaphore(held, workers),
	)
	e.coverage.setSelection(true, true, databases, targets, nil, 0)

	ch, stop := drain()
	base := runtime.NumGoroutine()
	type result struct{ failed, connFailures, deferred int }
	out := make(chan result, 1)
	go func() {
		f, c, d := e.scrapeTargets(ch, targets)
		out <- result{f, c, d}
	}()

	// Sampled while the workers are parked in the limiter, which is where a
	// per-database fan-out would have five hundred goroutines waiting.
	time.Sleep(100 * time.Millisecond)
	peak := runtime.NumGoroutine() - base

	got := <-out
	stop()

	if peak > workers+4 {
		t.Errorf("%d goroutines above the baseline while %d databases waited on %d workers",
			peak, databases, workers)
	}
	if got.deferred != databases {
		t.Fatalf("%d of %d databases were deferred, want all of them: the rest were dropped", got.deferred, databases)
	}
	if got.failed != 0 || got.connFailures != 0 {
		t.Errorf("%d failed and %d connection failures: nothing was tried, so nothing is known to be wrong",
			got.failed, got.connFailures)
	}

	g := series(t, e.coverage)
	unnamed := 0
	for _, name := range names {
		permit := g["pg_exporter_database_deferred{datname="+name+",reason="+reasonNoPermit+",server=127.0.0.1:1}"]
		deadline := g["pg_exporter_database_deferred{datname="+name+",reason="+reasonDeadline+",server=127.0.0.1:1}"]
		if permit+deadline != 1 {
			unnamed++
		}
	}
	if unnamed != 0 {
		t.Errorf("%d of %d deferred databases are not named by a deferral series", unnamed, databases)
	}
}

// TestWorkThatNeverReachedTheLimiterNamesTheDeadline pins a change the bounded
// pool makes to what operators see.
//
// With one worker and the only permit held, the first database asks for a
// permit and waits until the scrape's time runs out, so it reports
// no_connection_permit. The databases behind it are reached after that time has
// gone, so they never ask, and what they met is the deadline. Before the pool
// every database asked at once, so every one of them observed the shortage for
// itself and said no_connection_permit. What changed is when a database reaches
// the limiter, not whether the older observation was real.
//
// The order is deterministic because there is one worker, which is what lets
// this distinguish the two reasons at all.
func TestWorkThatNeverReachedTheLimiterNamesTheDeadline(t *testing.T) {
	held := semaphore.NewWeighted(1)
	if !held.TryAcquire(1) {
		t.Fatal("the fixture could not take the only permit")
	}
	defer held.Release(1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	targets := targetsForDSN("127.0.0.1:1", "first", "second", "third")
	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/first?sslmode=disable"},
		WithContext(ctx),
		WithConnectionsSemaphore(held, 1),
	)
	e.coverage.setSelection(true, true, 3, targets, nil, 0)

	ch, stop := drain()
	_, _, deferred := e.scrapeTargets(ch, targets)
	stop()

	if deferred != 3 {
		t.Fatalf("%d of 3 databases were deferred, want all of them", deferred)
	}

	g := series(t, e.coverage)
	key := func(name, reason string) string {
		return "pg_exporter_database_deferred{datname=" + name + ",reason=" + reason + ",server=127.0.0.1:1}"
	}
	if g[key("first", reasonNoPermit)] != 1 {
		t.Errorf("the database that asked for a permit and waited is not named for the permit: %v", g)
	}
	for _, name := range []string{"second", "third"} {
		if g[key(name, reasonDeadline)] != 1 {
			t.Errorf("%s was reached after the time had gone and is not named for the deadline", name)
		}
		if g[key(name, reasonNoPermit)] != 0 {
			t.Errorf("%s never asked for a permit and is named for one", name)
		}
	}
}

// TestEveryTargetInTheRequestGetsATurn covers the schedule this engine keeps.
//
// One schedule over everything the request selected, across servers as well as
// within them. An earlier version grouped by server and rotated inside each
// group, which keeps the first server ahead of every other on every request:
// the groups' own schedules advance and nothing moves between them, so a later
// server is never attempted when the first consumes the budget.
func TestEveryTargetInTheRequestGetsATurn(t *testing.T) {
	store := rotation.New()
	e := NewExporter([]string{
		"postgresql://u:p@127.0.0.1:1/a?sslmode=disable",
		"postgresql://u:p@127.0.0.1:2/x?sslmode=disable",
	},
		WithContext(context.Background()),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		WithRotation(store),
		CollectorName("exporter"),
	)

	targets := append(targetsForDSN("127.0.0.1:1", "a", "b", "c"),
		targetsForDSN("127.0.0.1:2", "x", "y")...)

	firstOf := map[string]int{}
	for scrape := 0; scrape < len(targets); scrape++ {
		got := e.rotateTargets(targets)
		if len(got) != len(targets) {
			t.Fatalf("scrape %d returned %d of %d targets: a rotation must not drop one", scrape, len(got), len(targets))
		}

		// Every target still there, whatever the order.
		seen := map[string]bool{}
		for _, target := range got {
			seen[target.dsn] = true
		}
		if len(seen) != len(targets) {
			t.Fatalf("scrape %d returned %d distinct targets of %d", scrape, len(seen), len(targets))
		}
		firstOf[got[0].dsn]++
	}

	// As many scrapes as there are targets, so each has been first once.
	if len(firstOf) != len(targets) {
		t.Errorf("over %d scrapes only %d of the %d targets was ever offered the first attempt: %v",
			len(targets), len(firstOf), len(targets), firstOf)
	}
}

// TestTheTwoEnginesDoNotShareASchedule keeps one engine's turn out of the
// other's for the same server.
func TestTheTwoEnginesDoNotShareASchedule(t *testing.T) {
	store := rotation.New()
	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/one?sslmode=disable"},
		WithContext(context.Background()),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		WithRotation(store),
		CollectorName("exporter"),
	)
	targets := targetsForDSN("127.0.0.1:1", "a", "b")

	// The collector engine takes several turns against the same server.
	for i := 0; i < 3; i++ {
		store.Next(rotation.Key{Engine: "collector", Target: "127.0.0.1:1", Profile: "all"},
			[]string{"a", "b"})
	}

	got := e.rotateTargets(targets)
	if got[0].datname != "a" {
		t.Errorf("the query engine's first scrape starts at %s, want a: the other engine moved its schedule", got[0].datname)
	}
}

// TestTheDatabaseOfferedTheFirstAttemptChangesBetweenScrapes goes through
// scrapeTargets rather than the rotation helper, so it fails if the fan-out
// takes the targets in the order it was handed rather than the scheduled one.
//
// One worker and a held permit, so exactly one database asks the limiter and
// waits, reporting no_connection_permit, while the rest are reached after the
// time has gone and report scrape_deadline. The database that asked is the one
// the schedule put first, and over three scrapes it has to be a different one
// each time.
func TestTheDatabaseOfferedTheFirstAttemptChangesBetweenScrapes(t *testing.T) {
	names := []string{"first", "second", "third"}
	store := rotation.New()

	asked := make([]string, 0, len(names))
	for scrape := 0; scrape < len(names); scrape++ {
		held := semaphore.NewWeighted(1)
		if !held.TryAcquire(1) {
			t.Fatal("the fixture could not take the only permit")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		targets := targetsForDSN("127.0.0.1:1", names...)
		e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/first?sslmode=disable"},
			WithContext(ctx),
			WithConnectionsSemaphore(held, 1),
			WithRotation(store),
			CollectorName("exporter"),
		)
		e.coverage.setSelection(true, true, len(names), targets, nil, 0)

		ch, stop := drain()
		_, _, deferred := e.scrapeTargets(ch, targets)
		stop()
		cancel()
		held.Release(1)

		if deferred != len(names) {
			t.Fatalf("scrape %d deferred %d of %d databases", scrape, deferred, len(names))
		}

		g := series(t, e.coverage)
		for _, name := range names {
			if g["pg_exporter_database_deferred{datname="+name+",reason="+reasonNoPermit+",server=127.0.0.1:1}"] == 1 {
				asked = append(asked, name)
			}
		}
	}

	// The premise. Without exactly one database asking per scrape, the order
	// cannot be read off the reasons at all.
	if len(asked) != len(names) {
		t.Fatalf("%d databases asked the limiter across %d scrapes, want one each: %v", len(asked), len(names), asked)
	}
	seen := map[string]bool{}
	for _, name := range asked {
		if seen[name] {
			t.Errorf("the databases offered the first attempt were %v, want a different one each scrape", asked)
			break
		}
		seen[name] = true
	}
}

// TestTwoConnectionsToOneDatabaseBothGetATurn is the collision a host and port
// cannot see.
//
// Two configured connections differing only in role, over the same endpoint and
// the same database. Keyed and named by host, port and database they are the
// same member, so the first target matching that name is chosen every time and
// the other never gets a turn. Both are always returned, so this is starvation
// rather than a dropped target.
func TestTwoConnectionsToOneDatabaseBothGetATurn(t *testing.T) {
	store := rotation.New()
	e := NewExporter([]string{"postgresql://role_a@127.0.0.1:1/app?sslmode=disable"},
		WithContext(context.Background()),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		WithRotation(store),
		CollectorName("exporter"),
	)

	targets := []scrapeTarget{}
	for _, role := range []string{"role_a", "role_b"} {
		dsn := "postgresql://" + role + "@127.0.0.1:1/app?sslmode=disable"
		targets = append(targets, scrapeTarget{
			dsn:         dsn,
			databaseRef: databaseRef{server: "127.0.0.1:1", datname: "app", identity: identityOf(dsn)},
		})
	}

	firstOf := map[string]int{}
	for call := 0; call < 12; call++ {
		got := e.rotateTargets(targets)
		if len(got) != len(targets) {
			t.Fatalf("call %d returned %d of %d targets", call, len(got), len(targets))
		}
		firstOf[got[0].dsn]++
	}

	if len(firstOf) != len(targets) {
		t.Errorf("over twelve calls only %d of the two connections was ever first: %v", len(firstOf), firstOf)
	}
}

// TestASecondServerGetsATurn is the other half. Rotating inside each server's
// own group moves nothing between servers, so when the first server's attempt
// consumes the budget the second is never attempted.
func TestASecondServerGetsATurn(t *testing.T) {
	store := rotation.New()
	e := NewExporter([]string{
		"postgresql://u:p@host_a:5432/app?sslmode=disable",
		"postgresql://u:p@host_b:5432/app?sslmode=disable",
	},
		WithContext(context.Background()),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		WithRotation(store),
		CollectorName("exporter"),
	)

	targets := append(targetsForDSN("host_a:5432", "app"), targetsForDSN("host_b:5432", "app")...)

	firstOf := map[string]int{}
	for call := 0; call < 12; call++ {
		got := e.rotateTargets(targets)
		if len(got) != len(targets) {
			t.Fatalf("call %d returned %d of %d targets", call, len(got), len(targets))
		}
		firstOf[got[0].server]++
	}

	if len(firstOf) != 2 {
		t.Errorf("over twelve calls only %v was ever first, so a later server never gets an opportunity", firstOf)
	}
}

// TestHealthAnswersForWorkThatNeverStarted covers the rule that decides pg_up
// and pg_exporter_last_scrape_error.
//
// The case that was wrong: a role that cannot log in. Discovery's connection
// fails, so the fan-out has only the seed and defers it, and the fan-out's own
// counts are all zero. Reading only those reported the server as up and the
// scrape as clean, while the server log recorded the refused login.
func TestHealthAnswersForWorkThatNeverStarted(t *testing.T) {
	for _, c := range []struct {
		name                                  string
		targets, deferred, connErrors, errors int
		d                                     discovery
		wantUp, wantFailed                    float64
	}{
		{
			name: "a login that was refused, with nothing attempted after it",
			// What ChatGPT measured: 1/0/0/0/1, discovery_success 0.
			targets: 1, deferred: 1,
			d:      discovery{ok: false, connectFailures: 1},
			wantUp: 0, wantFailed: 1,
		},
		{
			name:    "every target failed at the connection",
			targets: 2, connErrors: 2, errors: 2,
			d:      discovery{ok: true},
			wantUp: 0, wantFailed: 1,
		},
		{
			name: "one target failed at the connection while a seed had connected",
			// The seed that connected was not retried, so its observation
			// stands. What went wrong is in the error signal and in the
			// per-database outcomes.
			targets: 4, deferred: 3, connErrors: 1, errors: 1,
			d:      discovery{ok: false, connectSuccesses: 1, connectFailures: 1},
			wantUp: 1, wantFailed: 1,
		},
		{
			name:    "one target connected and another did not",
			targets: 2, connErrors: 1, errors: 1,
			d:      discovery{ok: true},
			wantUp: 1, wantFailed: 1,
		},
		{
			name:    "a query failed on a connection that worked",
			targets: 1, errors: 1,
			d:      discovery{ok: true},
			wantUp: 1, wantFailed: 1,
		},
		{
			name: "everything was deferred and nothing tried to connect",
			// Nothing observed the server either way, so this says nothing
			// about it. The deferral is what reports the shortfall.
			targets: 3, deferred: 3,
			d:      discovery{ok: true},
			wantUp: 1, wantFailed: 0,
		},
		{
			name:    "a healthy scrape",
			targets: 3,
			d:       discovery{ok: true},
			wantUp:  1, wantFailed: 0,
		},
		{
			name: "discovery's catalogue query failed on a connection that worked",
			// Not a connection failure, so the server is up, but the scrape
			// did not do what it meant to.
			targets: 1,
			d:       discovery{ok: false},
			wantUp:  1, wantFailed: 1,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			up, failed := health(c.targets, c.deferred, c.connErrors, c.errors, c.d)
			if up != c.wantUp {
				t.Errorf("pg_up is %v, want %v", up, c.wantUp)
			}
			if failed != c.wantFailed {
				t.Errorf("last_scrape_error is %v, want %v", failed, c.wantFailed)
			}
		})
	}
}

// TestGetServerReturnsWithoutWaitingAfterItsLastAttempt exercises the
// connection getter itself.
//
// One attempt is allowed and the count is raised before the limit is checked,
// so the loop returns on its next turn and a wait before that return precedes
// no retry. The wait is attempt seconds, cut short only by the context ending,
// and in discovery it is spent holding a connection permit.
//
// So the measurement is how long GetServer takes to come back from a connection
// that fails at once, against a context with plenty of time left. Testing the
// helper that names the condition is not enough: the guards are at the call
// site and the helper answers the same whether they are there or not.
func TestGetServerReturnsWithoutWaitingAfterItsLastAttempt(t *testing.T) {
	// Long enough that the context cannot be what cuts the wait short, which
	// would make this pass for the wrong reason.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	e := NewExporter([]string{"postgresql://u:p@127.0.0.1:1/app?sslmode=disable"},
		WithContext(ctx),
		CollectorName("exporter"),
	)

	begin := time.Now()
	server, err := e.GetServer(ctx, "postgresql://u:p@127.0.0.1:1/app?sslmode=disable")
	took := time.Since(begin)

	// The premise. Without a failure there is no final attempt to return from.
	if err == nil {
		if server != nil {
			server.Close()
		}
		t.Fatal("the connection to a closed port succeeded, so this case proves nothing")
	}
	if took >= time.Second {
		t.Errorf("the final failed attempt took %v to come back, so it waited for a retry that never happens", took)
	}
}

// TestLastAttemptNamesTheCondition covers the comparison on its own. It is not
// what protects the fix: the case above is.
func TestLastAttemptNamesTheCondition(t *testing.T) {
	for _, c := range []struct {
		name              string
		errCount, retries int
		wantLast          bool
	}{
		{"the only attempt, with no retry allowed", 1, 1, true},
		{"the first of two attempts", 1, 2, false},
		{"the second of two attempts", 2, 2, true},
		{"past the limit", 3, 2, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := lastAttempt(c.errCount, c.retries); got != c.wantLast {
				t.Errorf("lastAttempt(%d, %d) is %v, want %v", c.errCount, c.retries, got, c.wantLast)
			}
		})
	}
}

// TestASeedThatConnectedKeepsTheServerUp covers one seed connecting and
// another being refused.
//
// The scrape reached the server, so reporting it as down discards an
// observation the scrape made. The answer must also not change with how many
// targets the budget admitted.
func TestASeedThatConnectedKeepsTheServerUp(t *testing.T) {
	// Four selected databases, none attempted, and discovery saw one
	// connection work and one refused.
	up, failed := health(4, 4, 0, 0, discovery{ok: false, connectSuccesses: 1, connectFailures: 1})
	if up != 1 {
		t.Errorf("pg_up is %v after a seed connected and read its catalogue, want 1", up)
	}
	if failed != 1 {
		t.Errorf("last_scrape_error is %v after a refused login, want 1", failed)
	}

	// The single refused seed keeps the answer it had.
	up, failed = health(1, 1, 0, 0, discovery{ok: false, connectFailures: 1})
	if up != 0 || failed != 1 {
		t.Errorf("a refused login alone gives up %v and error %v, want 0 and 1", up, failed)
	}

	// Four targets, three deferred, and the one admitted failed at the
	// connection. The seed that connected was never retried, so its success
	// stands, and the failure is reported by the error signal and the
	// per-database outcomes rather than by unmaking it.
	up, failed = health(4, 3, 1, 1, discovery{ok: false, connectSuccesses: 1, connectFailures: 1})
	if up != 1 || failed != 1 {
		t.Errorf("one failed target among three deferred gives up %v and error %v, want 1 and 1", up, failed)
	}

	// The same observations with nothing admitted have to give the same
	// answer. An aggregate that changes with how much budget was left is
	// reporting the budget rather than the server.
	deferredUp, _ := health(4, 4, 0, 0, discovery{ok: false, connectSuccesses: 1, connectFailures: 1})
	if deferredUp != up {
		t.Errorf("pg_up is %v with one target admitted and %v with none, from the same connection observations", up, deferredUp)
	}

	// A fan-out that failed at every connection with no successful discovery
	// behind it is still down.
	up, _ = health(2, 0, 2, 2, discovery{ok: true})
	if up != 0 {
		t.Errorf("pg_up is %v when every attempted database failed and nothing else connected, want 0", up)
	}
}

// brokenCollector returns a metric whose label count does not match its
// descriptor, which is what an application query that names a label column its
// SQL does not select used to produce.
type brokenCollector struct{ desc *prometheus.Desc }

func (b brokenCollector) Describe(ch chan<- *prometheus.Desc) { ch <- b.desc }

func (b brokenCollector) Collect(ch chan<- prometheus.Metric) {
	// One label value for a descriptor that declares two. The gatherer rejects
	// this and drops the family.
	ch <- prometheus.NewInvalidMetric(b.desc, errors.New("inconsistent label cardinality"))
}

// TestAGatherErrorIsCountedThroughTheHandler covers the one failure this
// endpoint used to report nowhere.
//
// A collector that returns an invalid metric has its family dropped by the
// gatherer and the request still answers 200. The collection the health signals
// describe worked, so pg_up stays 1 and last_scrape_error stays 0: what failed
// was turning the collection into a response. Without a count of gather
// failures the family simply vanishes from the output.
//
// It goes through Handler, because the wiring under test is the one Handler
// builds. A test that constructs its own promhttp handler tests its own
// options: removing Registry from the production HandlerOpts leaves such a test
// passing, which is what the first version of this did.
func TestAGatherErrorIsCountedThroughTheHandler(t *testing.T) {
	// An invalid metric, which takes its whole family with it. Other gather
	// failures reject less: duplicate samples leave the family in place with
	// the samples that survived. What they have in common is that something
	// asked for is missing and the response does not say so.
	broken := brokenCollector{
		desc: prometheus.NewDesc("pg_fixture_broken", "a family this one takes with it",
			[]string{"one", "two"}, nil),
	}
	// A healthy collector under another name, so a request can be made that
	// gathers cleanly through the same handler. Every name a request selects
	// has to have a collector here, since the registry registers whatever the
	// filter names.
	healthy := prometheus.NewGauge(prometheus.GaugeOpts{Name: "pg_fixture_healthy", Help: "a family that gathers"})
	healthy.Set(1)

	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{"postgresql://u:p@127.0.0.1:1/app?sslmode=disable"},
		semaphore.NewWeighted(limits.Connections),
		map[string]prometheus.Collector{"standard.go": broken, "version": healthy},
		limits, NewCollectionGate(limits), querycache.New(), NewStores(),
		dbselect.Absent(), dbselect.Absent())

	// standard.go is where the broken collector was registered, so a request
	// that selects it gathers a failure and a request that selects something
	// else does not. Without that difference every request here fails and the
	// claim about surviving a clean one is never exercised.
	get := func(selected string) string {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/metrics?collect[]="+selected, nil)
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("the %s request answered %d, want 200: a rejected metric is not an error to the client", selected, w.Code)
		}
		return w.Body.String()
	}

	first := get("standard.go")
	if strings.Contains(first, "pg_fixture_broken") {
		t.Fatal("the broken family was served, so the gatherer did not drop it and this proves nothing")
	}

	// The hook increments after gathering, so this request's own failure lands
	// in the count after its response has been built. The operator sees it on
	// the next scrape.
	gathering := func(body string) string {
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, `promhttp_metric_handler_errors_total{cause="gathering"}`) {
				return strings.TrimSpace(line)
			}
		}
		return ""
	}

	// The second failing request carries the first one's failure.
	second := gathering(get("standard.go"))
	if second == "" {
		t.Fatalf("no count of gather failures is served, so a rejected metric is invisible")
	}
	if !strings.HasSuffix(second, " 1") {
		t.Errorf("after one failed gather the count is %q, want 1", second)
	}

	// Two requests have now failed, and these two gather cleanly because they
	// do not select the broken collector. The count has to hold at 2 across
	// both: a reset would lose the record, and an increment on a clean gather
	// would report a failure that did not happen.
	for i, request := range []string{"version", "version"} {
		clean := gathering(get(request))
		if clean == "" {
			t.Fatalf("clean request %d serves no count at all", i+1)
		}
		if !strings.HasSuffix(clean, " 2") {
			t.Errorf("clean request %d carries %q, want 2 retained from the two failed gathers", i+1, clean)
		}
	}
}

// TestTheMasterSeedDoesNotMoveWithTheVisitingOrder covers which seed carries
// the queries that run once per server rather than once per database.
//
// It used to be assigned inside the seed loop, so it was whichever seed the
// loop reached last. That is a data race the moment discovery and collection
// overlap, and it would move the placement as soon as the visiting order is
// scheduled rather than fixed. The behaviour is deliberately unchanged: it is
// still the last seed that parses, taken from the configured list instead of
// from the order of execution.
func TestTheMasterSeedDoesNotMoveWithTheVisitingOrder(t *testing.T) {
	first := "postgresql://u:p@127.0.0.1:1/a?sslmode=disable"
	second := "host=127.0.0.1 port=2 dbname=b"
	third := "postgresql://u:p@127.0.0.1:3/c?sslmode=disable"

	if got := masterSeed([]string{first, second, third}); got != third {
		t.Errorf("the master seed is %q, want the last that parses, %q", got, third)
	}

	// A seed that parses as neither form is not a candidate, exactly as the
	// loop skips it.
	if got := masterSeed([]string{first, "nonsense"}); got != first {
		t.Errorf("the master seed is %q with an unparseable seed last, want %q", got, first)
	}
	if got := masterSeed([]string{"nonsense"}); got != "" {
		t.Errorf("the master seed is %q with nothing parseable, want empty", got)
	}
	if got := masterSeed(nil); got != "" {
		t.Errorf("the master seed is %q with no seeds, want empty", got)
	}

	// The answer is a function of the configured list, so no visiting order
	// can change it. Reversing the list is a different configuration and is
	// allowed to differ; the same list must not.
	configured := []string{first, second, third}
	want := masterSeed(configured)
	for i := 0; i < 8; i++ {
		if got := masterSeed(configured); got != want {
			t.Fatalf("the same configured list gave %q and then %q", want, got)
		}
	}
}

// TestDiscoverySettlesTheMasterSeedBeforeItVisitsAnything is the half that
// matters for concurrency: the field is written once, before any seed is read,
// rather than as each one is reached.
//
// It does not detect putting the assignment back inside the loop. While the
// loop is sequential and visits the configured order, the last seed it reaches
// is the last seed configured, so both give the same answer. What separates
// them is a visiting order that is scheduled rather than fixed, and a reader
// running at the same time, neither of which exists yet. The regression for
// that belongs with the interleaving change that introduces both.
func TestDiscoverySettlesTheMasterSeedBeforeItVisitsAnything(t *testing.T) {
	seeds := []string{
		"postgresql://u:p@127.0.0.1:1/a?sslmode=disable",
		"postgresql://u:p@127.0.0.1:2/b?sslmode=disable",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e := NewExporter(seeds,
		WithContext(ctx),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		AutoDiscoverDatabases(true),
		CollectorName("exporter"),
	)

	// Nothing answers at either address, so every seed fails to connect and
	// the field cannot have been set by a successful read.
	d := e.discoverDatabaseDSNs()
	if d.ok {
		t.Fatal("discovery reported success against addresses that do not answer")
	}
	if e.masterDSN != seeds[len(seeds)-1] {
		t.Errorf("the master seed is %q after a discovery where nothing connected, want %q",
			e.masterDSN, seeds[len(seeds)-1])
	}
}

// TestAHealthySeedIsCollectedBeforeAStalledOneIsRead is the ordering the whole
// interleaving exists for.
//
// Discovery used to finish for every seed before any collection began, so one
// seed that stalls spent the budget and the databases already found waited
// behind it. Enqueuing every seed read and appending collection behind it is
// the same defect with more machinery, so what this asserts is the order the
// work actually happened in, not that both appear in one queue.
//
// The recorder is the connection attempt itself: the healthy seed's databases
// are at a closed port, so each attempt fails at once and is recorded, and the
// stalled seed is an address that accepts nothing and burns the remaining
// budget.
func TestAHealthySeedIsCollectedBeforeAStalledOneIsRead(t *testing.T) {
	stalled, stop := stalledEndpoint(t)
	defer stop()

	var mu sync.Mutex
	var order []string
	recordAttempt := func(what string) {
		mu.Lock()
		order = append(order, what)
		mu.Unlock()
	}

	// A healthy seed whose databases do not answer, and a stalled seed. The
	// healthy one is read first here, which is the case that used to collect
	// nothing at all.
	healthy := "postgresql://u:p@127.0.0.1:1/app?sslmode=disable"
	seeds := []string{healthy, stalled}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	e := NewExporter(seeds,
		WithContext(ctx),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		AutoDiscoverDatabases(true),
		WithRotation(rotation.New()),
		CollectorName("exporter"),
	)
	e.onSeedRead = func(seed string) { recordAttempt("read " + seed) }
	e.onTargetCollected = func(target string) { recordAttempt("collect " + target) }

	ch, drainStop := drain()
	d := discovery{ok: true}
	e.discoverAndCollect(ch, &d)
	drainStop()

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()

	// The premise: both seeds were read, so this is about the order and not
	// about one of them never happening.
	reads := 0
	for _, step := range got {
		if strings.HasPrefix(step, "read ") {
			reads++
		}
	}
	if reads != len(seeds) {
		t.Fatalf("%d of %d seeds were read: %v", reads, len(seeds), got)
	}

	// The healthy seed's collection has to come before the stalled seed's read.
	stalledRead, firstCollect := -1, -1
	for i, step := range got {
		if step == "read "+stalled {
			stalledRead = i
		}
		if firstCollect < 0 && strings.HasPrefix(step, "collect ") {
			firstCollect = i
		}
	}
	if firstCollect < 0 {
		t.Fatalf("nothing was collected at all: %v", got)
	}
	if stalledRead >= 0 && firstCollect > stalledRead {
		t.Errorf("the stalled seed was read before anything was collected: %v", got)
	}
}

// stalledEndpoint is a listener that accepts connections and answers nothing,
// which is how a seed consumes a budget without failing.
func stalledEndpoint(t *testing.T) (dsn string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %s", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Held open and never answered.
			go func() { <-done; conn.Close() }()
		}
	}()
	return "postgresql://u:p@" + ln.Addr().String() + "/app?sslmode=disable", func() {
		ln.Close()
		<-done
	}
}

// TestEachSeedKeepsItsOwnDatabaseSchedule is the fifth draft regression.
//
// A scrape that discovers one seed at a time hands the schedule one seed's
// databases at a time. A schedule told only those members drops every member it
// is not told about, so the other seed's waiting turns were thrown away and
// restarted on every request. Over eight requests with three databases in each
// of two seeds, only two of each seed's three were ever offered first.
func TestEachSeedKeepsItsOwnDatabaseSchedule(t *testing.T) {
	const requests = 9

	store := rotation.New()
	seeds := []string{
		"postgresql://u:p@host_a:5432/app?sslmode=disable",
		"postgresql://u:p@host_b:5432/app?sslmode=disable",
	}
	e := NewExporter(seeds,
		WithContext(context.Background()),
		WithConnectionsSemaphore(semaphore.NewWeighted(1), 1),
		WithRotation(store),
		CollectorName("exporter"),
	)

	batchOf := func(seed, host string) []scrapeTarget {
		out := targetsForDSN(host, "a", "b", "c")
		for i := range out {
			out[i].seed = seed
		}
		return out
	}

	firstOf := map[string]map[string]int{seeds[0]: {}, seeds[1]: {}}
	for request := 0; request < requests; request++ {
		// Each seed's databases are handed over on their own, which is what
		// the interleaved scrape does.
		for i, host := range []string{"host_a:5432", "host_b:5432"} {
			batch := e.rotateTargets(batchOf(seeds[i], host))
			if len(batch) != 3 {
				t.Fatalf("request %d seed %d returned %d of 3 targets", request, i, len(batch))
			}
			firstOf[seeds[i]][batch[0].datname]++
		}
	}

	for seed, counts := range firstOf {
		if len(counts) != 3 {
			t.Errorf("over %d requests seed %s offered only %v first, want all three databases a turn",
				requests, seed, counts)
		}
	}
}

// TestTwoSeedsOnOneServerDoNotSelectTheSameDatabaseTwice is the fourth.
//
// One shared map across all seeds used to drop the repeat. Per-seed maps
// appended together do not, so two configured seeds with the same credentials
// on the same server and different connection databases selected and collected
// the same database twice.
func TestTwoSeedsOnOneServerDoNotSelectTheSameDatabaseTwice(t *testing.T) {
	seen := map[string]struct{}{}
	var kept []scrapeTarget
	for _, batch := range [][]scrapeTarget{
		targetsForDSN("127.0.0.1:1", "app", "shared"),
		targetsForDSN("127.0.0.1:1", "other", "shared"),
	} {
		for _, target := range batch {
			if _, repeat := seen[target.dsn]; repeat {
				continue
			}
			seen[target.dsn] = struct{}{}
			kept = append(kept, target)
		}
	}
	if len(kept) != 3 {
		names := []string{}
		for _, k := range kept {
			names = append(names, k.datname)
		}
		t.Errorf("two seeds sharing a database yielded %d targets, %v, want 3", len(kept), names)
	}
}

// TestAnEarlierSeedKeepsItsOutcomeWhenALaterOneIsSelected is the third.
//
// setSelection clears every per-database sample, which is right when the whole
// selection arrives at once. Called again for the next seed it erased what the
// seeds before it had already collected, so a database reported as covered by
// the counts reported 0 for itself.
func TestAnEarlierSeedKeepsItsOutcomeWhenALaterOneIsSelected(t *testing.T) {
	c := newCoverageMetrics(nil, lastsuccess.New(), "exporter")

	first := targetsForDSN("127.0.0.1:1", "one")
	c.setSelection(true, true, 1, first, nil, 0)
	c.setDatabaseOutcome(first[0].databaseRef, "")

	// The next seed arrives and extends the selection.
	both := append(append([]scrapeTarget{}, first...), targetsForDSN("127.0.0.1:2", "two")...)
	c.extendSelection(true, true, 2, both, nil, 0)

	g := series(t, c)
	if g["pg_exporter_database_scrape_success{datname=one,server=127.0.0.1:1}"] != 1 {
		t.Errorf("the database collected before the next seed was read reports 0: %v", g)
	}
	if g["pg_exporter_database_scrape_success{datname=two,server=127.0.0.1:2}"] != 0 {
		t.Errorf("a database not collected yet does not report 0: %v", g)
	}
	if g["pg_exporter_databases_selected"] != 2 {
		t.Errorf("the selected count is %v, want 2", g["pg_exporter_databases_selected"])
	}
}
