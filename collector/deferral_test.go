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

// A database the scrape never started work on.
//
// Deferral and failure call for different responses. A failure says something
// is wrong with that database. A deferral says the exporter ran out of room,
// and the database may well be fine. Reported as the same thing, an operator
// alerting on failures is woken by a busy exporter, and an operator watching a
// selected set that cannot be kept up with has nothing to watch.

package collector

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// deferralEngine builds an engine around one context and one connection limit.
//
// The collectors are the caller's, because what the engine reports depends on
// which scopes were selected. An engine with no collectors at all asked for no
// database-local work, so every case about a deferred database has to select
// a database-scoped one.
// workers is how many databases the fan-out works on at once, which in a
// running exporter is the connection limit. The fixtures below pass it
// explicitly so a case that stages a full limiter stages the width that goes
// with it: with fewer workers than databases, the later databases are reached
// after the scrape's time has gone and meet the deadline rather than the
// shortage, which is a different reason.
func deferralEngine(ctx context.Context, sema *semaphore.Weighted, workers int, collectors map[string]Collector) PostgresCollector {
	return PostgresCollector{
		Collectors: collectors,
		logger:     log.NewNopLogger(),
		ctx:        ctx,
		connSema:   sema,
		dbWorkers:  workers,
		instance:   &instance{dsn: "postgresql://probe@198.51.100.9:5432/app", name: "198.51.100.9:5432"},
	}
}

// fullLimitEngine is an engine whose only connection permit is taken, so work
// that asks for one waits and the wait ends without it.
//
// The permit is held for the whole test and the context carries a short
// deadline, which is how this ends in a running exporter: the work queues
// behind other databases and the scrape runs out of time while it waits.
func fullLimitEngine(t *testing.T, collectors map[string]Collector) PostgresCollector {
	t.Helper()
	sema := semaphore.NewWeighted(1)
	if !sema.TryAcquire(1) {
		t.Fatal("the fixture could not take the only permit")
	}
	t.Cleanup(func() { sema.Release(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	return deferralEngine(ctx, sema, 1, collectors)
}

// expiredScrapeEngine is an engine whose deadline has already passed, with a
// connection permit free.
//
// A real deadline, not a cancellation. An earlier version of this fixture
// cancelled the context instead. The code reports the two apart, so a fixture
// that stages one cannot stand in for the other. Two versions of this file in
// a row named a cause they were not staging, which is why each case below pins
// the reason rather than only the outcome.
func expiredScrapeEngine(t *testing.T, collectors map[string]Collector) PostgresCollector {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return deferralEngine(ctx, semaphore.NewWeighted(1), 1, collectors)
}

// cancelledScrapeEngine is an engine whose scrape was cancelled, with a
// connection permit free and time still on the clock.
func cancelledScrapeEngine(t *testing.T, collectors map[string]Collector) PostgresCollector {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	cancel()
	t.Cleanup(cancel)
	return deferralEngine(ctx, semaphore.NewWeighted(1), 1, collectors)
}

// The ways a scrape stops before it starts a database, and the label each one
// publishes.
//
// Every case below runs against all of them. A deferral that names the wrong
// cause is worse than one that names none, because it describes a constraint
// the exporter did not meet.
var deferralCauses = []struct {
	name   string
	engine func(*testing.T, map[string]Collector) PostgresCollector
	reason string
}{
	{"a full connection limit", fullLimitEngine, dbselect.ReasonNoPermit},
	{"a deadline that had passed", expiredScrapeEngine, dbselect.ReasonDeadline},
	{"a deadline that had passed with no limit configured", unlimitedExpiredEngine, dbselect.ReasonDeadline},
	{"a cancelled scrape", cancelledScrapeEngine, dbselect.ReasonCancelled},
}

// unlimitedExpiredEngine is an engine with no connection limit at all, whose
// scrape has run out of time.
//
// This is the case that had no coverage. With no limiter to ask, the engine
// did not read the context, so work was started after the deadline had passed,
// its connection failed on the dead context, and the database was reported as
// one the exporter could not connect to.
//
// main always constructs a limiter, so the shipped binary does not take this
// path. NewPostgresCollector is exported and the limiter is an option, so
// something embedding this package does.
func unlimitedExpiredEngine(t *testing.T, collectors map[string]Collector) PostgresCollector {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return deferralEngine(ctx, nil, 1, collectors)
}

// publishedByName runs a collector function and returns what reached the
// channel, by metric name.
func publishedByName(emit func(ch chan<- prometheus.Metric)) map[string]MetricResult {
	ch := make(chan prometheus.Metric, 32)
	emit(ch)
	close(ch)
	out := map[string]MetricResult{}
	for m := range ch {
		out[familyName(m)] = readMetric(m)
	}
	return out
}

// familyName pulls the metric name out of a descriptor's string form, which is
// the only place a written-out metric does not carry it.
func familyName(m prometheus.Metric) string {
	desc := m.Desc().String()
	const marker = `fqName: "`
	start := strings.Index(desc, marker)
	if start < 0 {
		return desc
	}
	rest := desc[start+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return desc
	}
	return rest[:end]
}

// TestADeferredDatabaseIsNotAFailure covers the per-database fan-out.
func TestADeferredDatabaseIsNotAFailure(t *testing.T) {
	for _, cause := range deferralCauses {
		t.Run(cause.name, func(t *testing.T) {
			p := cause.engine(t, map[string]Collector{})
			target := dbTarget{datname: "app", dsn: "postgresql://probe@198.51.100.9:5432/app"}

			var outcome databaseOutcome
			published := publishedByName(func(ch chan<- prometheus.Metric) {
				outcome = p.collectDatabase(ch, target, map[string]Collector{}, "198.51.100.9:5432", "")
			})

			if outcome != outcomeDeferred {
				t.Errorf("a database the scrape had no room for was reported to its caller as %v, want deferred", outcome)
			}
			deferred, ok := published["pg_collector_database_deferred"]
			if !ok {
				t.Fatalf("no deferral was published: %v", published)
			}
			if deferred.labels["reason"] != cause.reason {
				t.Errorf("the deferral reason is %q, want %q", deferred.labels["reason"], cause.reason)
			}
			if deferred.labels["datname"] != "app" {
				t.Errorf("the deferral names %q", deferred.labels["datname"])
			}
			if _, ok := published["pg_collector_database_error"]; ok {
				t.Error("a deferral was published as an error, so an alert on failures fires for a busy exporter")
			}
			// Still not reached, which is what database_up says.
			up, ok := published["pg_collector_database_up"]
			if !ok {
				t.Fatal("no database_up was published for a deferred database, so it reads as a database nobody selected")
			}
			if up.value != 0 {
				t.Errorf("database_up is %v for a database nothing connected to", up.value)
			}
		})
	}
}

// TestADeferredControlConnectionIsNotAFailure covers the other path: the
// control connection itself never gets a permit, so the engine collects
// nothing at all.
//
// A database-scoped collector is selected, so this scrape did mean to cover a
// database and the counts say so.
func TestADeferredControlConnectionIsNotAFailure(t *testing.T) {
	for _, cause := range deferralCauses {
		t.Run(cause.name, func(t *testing.T) {
			withScopedCollector(t, "deferral_test_local", scopeDatabase)
			p := cause.engine(t, map[string]Collector{"deferral_test_local": quietCollector{}})

			published := publishedByName(func(ch chan<- prometheus.Metric) {
				p.Collect(ch)
			})

			deferred, ok := published["pg_collector_database_deferred"]
			if !ok {
				t.Fatalf("no deferral was published: %v", published)
			}
			if deferred.labels["reason"] != cause.reason {
				t.Errorf("the deferral reason is %q, want %q", deferred.labels["reason"], cause.reason)
			}
			if _, ok := published["pg_collector_database_error"]; ok {
				t.Error("a deferred control connection was published as an error")
			}
			count, ok := published["pg_collector_databases_deferred"]
			if !ok {
				t.Fatal("no deferral count was published")
			}
			if count.value != 1 {
				t.Errorf("the deferral count is %v, want the one database this engine meant to cover", count.value)
			}
			// The set it meant to cover is still reported, because a shortfall with no
			// record of the intent is indistinguishable from no shortfall.
			if _, ok := published["pg_collector_databases_selected"]; !ok {
				t.Error("nothing was published about what this scrape intended to cover")
			}
			if _, ok := published["pg_collector_databases_discovered"]; ok {
				t.Error("a discovered total was published although the catalogue was never read")
			}
			// Database-local work was selected and its set could not be determined,
			// which is what the discovery status is for.
			status, ok := published["pg_collector_database_discovery_success"]
			if !ok {
				t.Error("nothing was said about a set of databases this scrape wanted and could not get")
			} else if status.value != 0 {
				t.Errorf("discovery_success is %v when no connection was available to read the catalogue", status.value)
			}
		})
	}
}

// TestAClusterOnlyDeferralSelectsNoDatabase is the same path with only a
// cluster-scoped collector selected.
//
// The connection was deferred, so the per-connection diagnostic names it. No
// database was meant to be covered, so the count beside it is zero and nothing
// is said about a set of databases. The two describe different things here,
// which the help text says.
func TestAClusterOnlyDeferralSelectsNoDatabase(t *testing.T) {
	for _, cause := range deferralCauses {
		t.Run(cause.name, func(t *testing.T) {
			withScopedCollector(t, "deferral_test_cluster", scopeCluster)
			p := cause.engine(t, map[string]Collector{"deferral_test_cluster": quietCollector{}})

			published := publishedByName(func(ch chan<- prometheus.Metric) {
				p.Collect(ch)
			})

			deferred, ok := published["pg_collector_database_deferred"]
			if !ok {
				t.Fatalf("the deferred connection was not named: %v", published)
			}
			if deferred.labels["reason"] != cause.reason {
				t.Errorf("the deferral reason is %q, want %q", deferred.labels["reason"], cause.reason)
			}
			count, ok := published["pg_collector_databases_deferred"]
			if !ok {
				t.Fatal("no deferral count was published")
			}
			if count.value != 0 {
				t.Errorf("the deferral count is %v, want none: no database-local work was selected", count.value)
			}
			selected, ok := published["pg_collector_databases_selected"]
			if !ok {
				t.Fatal("nothing was published about what this scrape intended to cover")
			}
			if selected.value != 0 {
				t.Errorf("a cluster-only scrape reported %v databases selected", selected.value)
			}
			if _, ok := published["pg_collector_database_discovery_success"]; ok {
				t.Error("a cluster-only deferral reports discovery status despite no discovery requested")
			}
			if _, ok := published["pg_collector_database_error"]; ok {
				t.Error("a deferral was published as an error")
			}
		})
	}
}

// TestDeferralReasonsAreAClosedSet keeps the label from becoming an error
// string, which is how a bounded label set becomes unbounded.
func TestDeferralReasonsAreAClosedSet(t *testing.T) {
	for _, reason := range []string{dbselect.ReasonNoPermit, dbselect.ReasonNotAttempted, dbselect.ReasonDeadline, dbselect.ReasonCancelled} {
		if strings.ContainsAny(reason, " .:\"") {
			t.Errorf("%q does not look like a closed-set value", reason)
		}
	}
}

// fanOutTargets builds n database targets at one address.
func fanOutTargets(hostport string, n int) []dbTarget {
	out := make([]dbTarget, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("db%03d", i)
		out = append(out, dbTarget{
			datname: name,
			dsn:     "postgresql://probe@" + hostport + "/" + name + "?sslmode=disable",
		})
	}
	return out
}

// publishedPerDatabase collects what the fan-out wrote, grouped by the database
// each series names.
func publishedPerDatabase(emit func(ch chan<- prometheus.Metric)) map[string]map[string]MetricResult {
	ch := make(chan prometheus.Metric, 1<<16)
	emit(ch)
	close(ch)
	out := map[string]map[string]MetricResult{}
	for m := range ch {
		r := readMetric(m)
		name := r.labels["datname"]
		if out[name] == nil {
			out[name] = map[string]MetricResult{}
		}
		out[name][familyName(m)] = r
	}
	return out
}

// TestFewerWorkersThanDatabasesStillCoversThemAll is half of the acceptance
// check for the bounded pool.
//
// Running fewer workers than databases must not cap how many databases a scrape
// reaches. The permits are free and the clock is not the constraint, so every
// database here is reached and ends as a connection failure against a closed
// port. The failure is the point: it means the database was reached rather than
// quietly left out.
func TestFewerWorkersThanDatabasesStillCoversThemAll(t *testing.T) {
	const databases, workers = 9, 3

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	p := deferralEngine(ctx, semaphore.NewWeighted(workers), workers, map[string]Collector{})
	targets := fanOutTargets("127.0.0.1:1", databases)

	var failed, deferred int
	published := publishedPerDatabase(func(ch chan<- prometheus.Metric) {
		failed, deferred = p.collectDatabases(ch, targets, map[string]Collector{}, "127.0.0.1:1", "")
	})

	if deferred != 0 {
		t.Errorf("%d databases were deferred, and neither a permit nor the clock was the constraint", deferred)
	}
	if failed != databases {
		t.Fatalf("%d of %d databases were reached, so the worker count capped the set", failed, databases)
	}

	// Counted is not the same as named. A count that adds up while a database
	// has no series of its own is the shortfall the per-database series exist
	// to prevent.
	for _, target := range targets {
		series := published[target.datname]
		if _, ok := series["pg_collector_database_error"]; !ok {
			t.Errorf("%s is in the failed count and has no error series of its own", target.datname)
		}
		if series["pg_collector_database_error"].labels["reason"] != dbselect.ReasonConnect {
			t.Errorf("%s failed with reason %q, want %q", target.datname,
				series["pg_collector_database_error"].labels["reason"], dbselect.ReasonConnect)
		}
	}
}

// TestManyDatabasesBehindHeldPermitsStayBounded is the goroutine bound, for the
// engine rather than for the pool on its own.
//
// Every permit is held, so each worker parks in the limiter while the remaining
// databases wait for a worker. A goroutine per database would show four hundred
// of them waiting here.
func TestManyDatabasesBehindHeldPermitsStayBounded(t *testing.T) {
	const databases, workers = 400, 4

	held := semaphore.NewWeighted(workers)
	if !held.TryAcquire(workers) {
		t.Fatal("the fixture could not take the permits")
	}
	t.Cleanup(func() { held.Release(workers) })

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	t.Cleanup(cancel)
	p := deferralEngine(ctx, held, workers, map[string]Collector{})
	targets := fanOutTargets("127.0.0.1:1", databases)

	ch := make(chan prometheus.Metric, 1<<16)
	base := runtime.NumGoroutine()
	type result struct{ failed, deferred int }
	out := make(chan result, 1)
	go func() {
		f, d := p.collectDatabases(ch, targets, map[string]Collector{}, "127.0.0.1:1", "")
		out <- result{f, d}
	}()

	// Sampled while the workers are parked in the limiter, which is where a
	// goroutine per database would show four hundred of them waiting.
	time.Sleep(100 * time.Millisecond)
	peak := runtime.NumGoroutine() - base

	got := <-out
	close(ch)

	if peak > workers+4 {
		t.Errorf("%d goroutines above the baseline while %d databases waited on %d workers",
			peak, databases, workers)
	}
	if got.deferred != databases {
		t.Fatalf("%d of %d databases were deferred, want all of them: the rest were dropped", got.deferred, databases)
	}
	if got.failed != 0 {
		t.Errorf("%d databases failed: nothing was tried, so nothing is known to be wrong with them", got.failed)
	}

	// Every one of them named, with a reason from the closed set.
	reasons := map[string]int{}
	named := 0
	for m := range ch {
		r := readMetric(m)
		if familyName(m) != "pg_collector_database_deferred" {
			continue
		}
		reasons[r.labels["reason"]]++
		named++
	}
	if named != databases {
		t.Errorf("%d deferral series for %d deferred databases", named, databases)
	}
	for reason := range reasons {
		if reason != dbselect.ReasonNoPermit && reason != dbselect.ReasonDeadline {
			t.Errorf("a deferral was reported with reason %q, which is outside the closed set", reason)
		}
	}
}
