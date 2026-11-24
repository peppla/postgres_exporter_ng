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

// Per-collector admission against a server that would have answered.
//
// The unit tests beside this one stage the context with a stub connection, so
// they cannot tell "no SQL ran because the budget had gone" from "no SQL ran
// because nothing was reachable". Here the server is real and the healthy case
// runs first, so a collector that does not run had somewhere to run.
//
// DATA_SOURCE_NAME must point at a server.
//go:build integration

package collector

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	_ "github.com/lib/pq"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// liveDSN is the server these tests run against.
// controlName is the database every case here connects to, and controlUp is
// its own database_up entry rather than whichever database published last.
const (
	controlName   = "postgres"
	controlUp     = "pg_collector_database_up/" + controlName + "/"
	attempted     = "pg_collector_databases_attempted//"
	deferredCount = "pg_collector_databases_deferred//"
)

func liveDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATA_SOURCE_NAME")
	if dsn == "" {
		t.Skip("DATA_SOURCE_NAME is not set")
	}
	return dsn
}

// openLive returns an instance connected to the server, so a collector that
// does not run had a working connection to run on.
func openLive(t *testing.T, dsn string) *instance {
	t.Helper()
	inst, err := newInstance(dsn)
	if err != nil {
		t.Fatalf("building the instance: %s", err)
	}
	if err := inst.setup(context.Background()); err != nil {
		t.Fatalf("connecting: %s", err)
	}
	t.Cleanup(func() { inst.Close() })
	return inst
}

// queryingCollector runs real SQL, so a run and a non-run are distinguishable
// at the server as well as in the counter.
type queryingCollector struct {
	ran *atomic.Int32
}

func (c queryingCollector) Update(ctx context.Context, inst *instance, ch chan<- prometheus.Metric) error {
	c.ran.Add(1)
	var one int
	return inst.getDB().QueryRowContext(ctx, "SELECT 1").Scan(&one)
}

// TestOnARealConnectionTheHealthyCaseRunsAndTheEndedCaseDoesNot is the premise
// for every deferral case here, and the deferral case itself.
//
// The same collector, the same open connection and the same code path, run
// twice. With time on the clock the SQL runs and the collector reports success.
// With the scrape's time gone no SQL runs at all and the collector reports a
// deferral instead, with no success series beside it.
func TestOnARealConnectionTheHealthyCaseRunsAndTheEndedCaseDoesNot(t *testing.T) {
	dsn := liveDSN(t)
	inst := openLive(t, dsn)

	var ran atomic.Int32
	local := map[string]Collector{"stat_fixture_live": queryingCollector{ran: &ran}}

	live, cancelLive := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancelLive)
	var healthy localOutcome
	healthySeries := unitSeries(func(ch chan<- prometheus.Metric) {
		healthy = runLocalCollectors(live, local, inst, ch, log.NewNopLogger(), "app", lastsuccess.New())
	})
	if healthy.ran != 1 || ran.Load() != 1 {
		t.Fatalf("the healthy case did not run: outcome %+v, %d runs", healthy, ran.Load())
	}
	if healthySeries["stat_fixture_live"]["pg_scrape_collector_success"].value != 1 {
		t.Fatalf("the healthy case ran and did not report success: %v", healthySeries["stat_fixture_live"])
	}

	ran.Store(0)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)
	var ended localOutcome
	endedSeries := unitSeries(func(ch chan<- prometheus.Metric) {
		ended = runLocalCollectors(expired, local, inst, ch, log.NewNopLogger(), "app", lastsuccess.New())
	})

	if got := ran.Load(); got != 0 {
		t.Errorf("%d collectors ran SQL on a connection that worked after the scrape's time had gone", got)
	}
	if ended.deferred != 1 || ended.failed != 0 || ended.ran != 0 {
		t.Fatalf("outcome is %+v, want one deferred and nothing else", ended)
	}
	if ended.reason != dbselect.ReasonDeadline {
		t.Errorf("the reason is %q, want %q", ended.reason, dbselect.ReasonDeadline)
	}
	if _, ok := endedSeries["stat_fixture_live"]["pg_scrape_collector_success"]; ok {
		t.Error("a collector that never started published a success series, which reads as one that failed")
	}
	if _, ok := endedSeries["stat_fixture_live"]["pg_scrape_collector_deferred"]; !ok {
		t.Errorf("a collector that never started published no deferral: %v", endedSeries["stat_fixture_live"])
	}
}

// TestTheOtherDatabasePathReachesItsCollectors exercises collectDatabase itself
// against the server, rather than the function it calls.
//
// The healthy run proves the path reaches real SQL on a database the fan-out
// opened for itself. The ended run is the earlier boundary, the database's own
// admission check: the scrape's time is gone before the database is reached, so
// nothing is connected to and the database is deferred rather than attempted.
// The boundary this component protects, where the connection opens and the
// collectors are then deferred, is in
// TestTheFanOutPathDefersUnitsAfterItsConnectionWorked.
func TestTheOtherDatabasePathReachesItsCollectors(t *testing.T) {
	dsn := liveDSN(t)

	collectorScopes["stat_fixture_live"] = scopeDatabase
	t.Cleanup(func() { delete(collectorScopes, "stat_fixture_live") })

	var ran atomic.Int32
	local := map[string]Collector{"stat_fixture_live": queryingCollector{ran: &ran}}
	target := dbTarget{datname: "postgres", dsn: dsn}

	run := func(ctx context.Context) (databaseOutcome, map[string]MetricResult) {
		p := PostgresCollector{
			Collectors:  local,
			logger:      log.NewNopLogger(),
			ctx:         ctx,
			connSema:    semaphore.NewWeighted(2),
			dbWorkers:   2,
			lastSuccess: lastsuccess.New(),
			instance:    &instance{dsn: dsn, name: "live"},
		}
		var outcome databaseOutcome
		published := publishedByReason(func(ch chan<- prometheus.Metric) {
			outcome = p.collectDatabase(ch, target, local, "live", selectionOf(local))
		})
		return outcome, published
	}

	live, cancelLive := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancelLive)
	healthyOutcome, _ := run(live)
	if healthyOutcome != outcomeScraped || ran.Load() != 1 {
		t.Fatalf("the healthy run came to %v with %d collector runs, want scraped with 1", healthyOutcome, ran.Load())
	}

	// The scrape's time is gone before the database is reached, so this stops
	// at the database's own admission check. The collectors are not reached,
	// which is the stronger statement: no SQL ran and the database is deferred
	// rather than failed, because nothing was tried.
	ran.Store(0)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)
	endedOutcome, published := run(expired)

	if got := ran.Load(); got != 0 {
		t.Errorf("%d collectors ran SQL after the scrape's time had gone", got)
	}
	if endedOutcome != outcomeDeferred {
		t.Errorf("a database the scrape had no time left for came to %v, want deferred", endedOutcome)
	}
	if published[dbselect.ReasonDeadline].value != 1 {
		t.Errorf("the database does not name the deadline: %v", published)
	}
	for _, wrong := range []string{dbselect.ReasonScrape, dbselect.ReasonConnect} {
		if _, found := published[wrong]; found {
			t.Errorf("nothing was tried and the database reports %q", wrong)
		}
	}
}

// endsAtACountedCheck is a context that is live until a stated boundary and
// ended after it, in Err and in Done together.
//
// A wrapper that only overrides Err is not enough here: an already expired
// parent has a closed Done channel, so the connection setup fails at the ping
// and the collectors are never reached. The boundary has to move both.
//
// The boundary is the nth read of Err. The tests below do not assume a value
// for n: the number of reads the path makes before its collectors is not stable
// between runs, because the connection pool does not do the same work twice.
// They search for an n where the run shows a connection that opened and a
// collector that never started, which is the boundary itself, and they fail if
// no such n exists within the search. Below the window the connection fails at
// the ping, and above it the collector runs.
type endsAtACountedCheck struct {
	context.Context
	n     int64
	calls atomic.Int64
	done  chan struct{}
	once  sync.Once
}

func newCountedCheck(parent context.Context, n int64) *endsAtACountedCheck {
	return &endsAtACountedCheck{Context: parent, n: n, done: make(chan struct{})}
}

func (c *endsAtACountedCheck) Err() error {
	if c.calls.Add(1) <= c.n {
		return nil
	}
	c.once.Do(func() { close(c.done) })
	return context.DeadlineExceeded
}

func (c *endsAtACountedCheck) Done() <-chan struct{} { return c.done }

func (c *endsAtACountedCheck) Deadline() (time.Time, bool) { return time.Time{}, false }

// at returns how many reads of Err have happened so far.
func (c *endsAtACountedCheck) at() int64 { return c.calls.Load() }

// reportingCollector records the context read count at the moment it starts, so
// a calibration run can say where the collectors begin.
type reportingCollector struct {
	ran   *atomic.Int32
	atRun *atomic.Int64
	ctx   *endsAtACountedCheck
	err   error
}

func (c reportingCollector) Update(ctx context.Context, inst *instance, ch chan<- prometheus.Metric) error {
	c.ran.Add(1)
	c.atRun.Store(c.ctx.at())
	if c.err != nil {
		return c.err
	}
	var one int
	return inst.getDB().QueryRowContext(ctx, "SELECT 1").Scan(&one)
}

// TestTheFanOutPathDefersUnitsAfterItsConnectionWorked is round 180's boundary,
// on the path the fan-out takes for a database it opened for itself.
//
// The connection is opened and the database is reached, and the scrape's time
// then goes before its collectors start. That makes an attempt that did not
// finish, not a database nothing was tried on, so the database is failed rather
// than deferred, keeps database_up 1, and names the budget as the cause.
func TestTheFanOutPathDefersUnitsAfterItsConnectionWorked(t *testing.T) {
	dsn := liveDSN(t)
	collectorScopes["stat_fixture_live"] = scopeDatabase
	t.Cleanup(func() { delete(collectorScopes, "stat_fixture_live") })

	target := dbTarget{datname: "postgres", dsn: dsn}
	store := lastsuccess.New()

	run := func(n int64, err error) (databaseOutcome, map[string]MetricResult, *atomic.Int32, *atomic.Int64) {
		var ran atomic.Int32
		var atRun atomic.Int64
		ctx := newCountedCheck(context.Background(), n)
		local := map[string]Collector{"stat_fixture_live": reportingCollector{ran: &ran, atRun: &atRun, ctx: ctx, err: err}}
		p := PostgresCollector{
			Collectors:  local,
			logger:      log.NewNopLogger(),
			ctx:         ctx,
			connSema:    semaphore.NewWeighted(2),
			dbWorkers:   2,
			lastSuccess: store,
			instance:    &instance{dsn: dsn, name: "live"},
		}
		var outcome databaseOutcome
		published := publishedByFamilyAndReason(func(ch chan<- prometheus.Metric) {
			outcome = p.collectDatabase(ch, target, local, "live", selectionOf(local))
		})
		return outcome, published, &ran, &atRun
	}

	// The healthy run first, with the boundary out of reach. It records the
	// last-success times the deferred run below has to keep, and it is the
	// premise for every assertion here: the collector has somewhere to run.
	healthy, healthySeries, healthyRan, atRun := run(1<<40, nil)
	if healthy != outcomeScraped || healthyRan.Load() != 1 {
		t.Fatalf("the healthy run came to %v with %d collector runs, want scraped with 1", healthy, healthyRan.Load())
	}
	if healthySeries[controlUp].value != 1 {
		t.Fatalf("the healthy run did not report the connection as up: %v", healthySeries)
	}

	// Find the boundary rather than assume it. Too low and the connection
	// fails at the ping, too high and the collector runs, and the window
	// between is what this test is about.
	var outcome databaseOutcome
	var published map[string]MetricResult
	boundary := int64(-1)
	for n := int64(1); n <= atRun.Load()+8; n++ {
		o, p, ran, _ := run(n, nil)
		if p[controlUp].value == 1 && ran.Load() == 0 {
			outcome, published, boundary = o, p, n
			break
		}
	}
	if boundary < 0 {
		t.Fatalf("no boundary between a connection that opened and a collector that started, searching up to %d context reads", atRun.Load()+8)
	}
	t.Logf("the scrape ends at context read %d, after the connection is open and before the collector starts", boundary)
	if outcome != outcomeFailed {
		t.Errorf("a database whose connection worked and whose collectors were deferred came to %v, want an attempt that did not finish", outcome)
	}
	if published["pg_collector_database_error/"+controlName+"/"+dbselect.ReasonDeadline].value != 1 {
		t.Errorf("the database does not name the budget as the cause: %v", published)
	}
	if _, found := published["pg_collector_database_error/"+controlName+"/"+dbselect.ReasonScrape]; found {
		t.Error("no collector failed and the database reports a collector failure")
	}
	if _, found := published["pg_collector_database_deferred/"+controlName+"/"+dbselect.ReasonDeadline]; found {
		t.Error("the database is counted as both an attempt that did not finish and a database nothing was tried on")
	}
	if _, found := published["pg_scrape_collector_deferred/"+controlName+"/"+dbselect.ReasonDeadline]; !found {
		t.Errorf("the collector that never started is not named: %v", published)
	}
	if _, found := published["pg_collector_last_success_timestamp_seconds/"+controlName+"/"]; !found {
		t.Errorf("the deferred collector lost the time it last worked, which the healthy run recorded: %v", published)
	}
}

// TestTheControlDatabasePathDefersUnitsAfterItsConnectionWorked is the same
// boundary for the configured connection database, through the whole engine:
// admission, the baseline connection, the catalogue read, the scope split and
// the rotated fan-out. There is no separate control-local path any more, so
// what this covers is that database taking its turn like the others.
//
// There are two different boundaries for it and this one is the later. The
// earlier is the baseline connection succeeding and the database's own
// admission then finding no time left, which gives a deferred database that
// keeps the connection observation, up 1, and attempts nothing. The predicate
// below therefore requires an attempt as well as an open connection and no
// collector run, because without that it stops at the earlier boundary and
// then asserts things only the later one produces.
func TestTheControlDatabasePathDefersUnitsAfterItsConnectionWorked(t *testing.T) {
	dsn := liveDSN(t)
	collectorScopes["stat_fixture_live"] = scopeDatabase
	t.Cleanup(func() { delete(collectorScopes, "stat_fixture_live") })

	store := lastsuccess.New()
	run := func(n int64) (map[string]MetricResult, *atomic.Int32, *atomic.Int64) {
		var ran atomic.Int32
		var atRun atomic.Int64
		ctx := newCountedCheck(context.Background(), n)
		local := map[string]Collector{"stat_fixture_live": reportingCollector{ran: &ran, atRun: &atRun, ctx: ctx}}
		p := PostgresCollector{
			Collectors: local,
			logger:     log.NewNopLogger(),
			ctx:        ctx,
			connSema:   semaphore.NewWeighted(4),
			dbWorkers:  4,
			// Only the control database, so a collector run and a database_up
			// here belong to the control connection and to nothing else. A
			// server with a second eligible database would otherwise put a
			// fan-out database's series beside the control's, and the counts
			// below are about the control database's own attempt.
			includeDatabases: dbselect.Of([]string{controlName}),
			lastSuccess:      store,
			instance:         &instance{dsn: dsn, name: "live"},
		}
		published := publishedByFamilyAndReason(func(ch chan<- prometheus.Metric) {
			p.Collect(ch)
		})
		return published, &ran, &atRun
	}

	healthy, healthyRan, atRun := run(1 << 40)
	if healthyRan.Load() < 1 {
		t.Fatalf("the healthy run never reached a collector: %v", healthy)
	}
	if healthy[controlUp].value != 1 {
		t.Fatalf("the healthy run did not report the control connection as up: %v", healthy)
	}

	// The earlier boundary has to be reachable too, and it has its own
	// assertions. Finding it first is also what shows the predicate below is
	// choosing between two real outcomes rather than taking the only one there
	// is.
	earlier := int64(-1)
	for n := int64(1); n <= atRun.Load()+8; n++ {
		p, ran, _ := run(n)
		if p[controlUp].value == 1 && ran.Load() == 0 && p[attempted].value == 0 {
			earlier = n
			if p[deferredCount].value != 1 {
				t.Errorf("the baseline connection worked and no local attempt started, and %v databases are deferred, want 1: %v",
					p[deferredCount].value, p)
			}
			if _, found := p["pg_collector_database_error/"+controlName+"/"+dbselect.ReasonDeadline]; found {
				t.Error("nothing was attempted and the database reports an incomplete attempt")
			}
			break
		}
	}
	if earlier < 0 {
		t.Fatalf("the baseline connection and a database never attempted is not reachable, searching up to %d context reads", atRun.Load()+8)
	}
	t.Logf("the earlier boundary, a baseline connection whose local turn never started, is at context read %d", earlier)

	var published map[string]MetricResult
	boundary := int64(-1)
	for n := int64(1); n <= atRun.Load()+8; n++ {
		p, ran, _ := run(n)
		if p[controlUp].value == 1 && ran.Load() == 0 && p[attempted].value == 1 {
			published, boundary = p, n
			break
		}
	}
	if boundary < 0 {
		t.Fatalf("no boundary between a local connection that opened and a collector that started, searching up to %d context reads", atRun.Load()+8)
	}
	t.Logf("the scrape ends at context read %d, after the control connection is open and before its collectors start", boundary)
	if published["pg_collector_database_error/"+controlName+"/"+dbselect.ReasonDeadline].value != 1 {
		t.Errorf("the control database does not name the budget as the cause: %v", published)
	}
	if _, found := published["pg_collector_database_error/"+controlName+"/"+dbselect.ReasonScrape]; found {
		t.Error("no control collector failed and the control database reports a collector failure")
	}
	if _, found := published["pg_scrape_collector_deferred/"+controlName+"/"+dbselect.ReasonDeadline]; !found {
		t.Errorf("the control collector that never started is not named: %v", published)
	}
	if published["pg_collector_databases_failed//"].value != 1 {
		t.Errorf("databases_failed is %v, want 1 for the control database's unfinished attempt: %v",
			published["pg_collector_databases_failed//"].value, published)
	}
	if published["pg_collector_databases_deferred//"].value != 0 {
		t.Errorf("databases_deferred is %v, want 0: the control database was attempted",
			published["pg_collector_databases_deferred//"].value)
	}
}

// publishedByFamilyAndReason keys what was published by family, database and
// reason, so a family carrying two reasons at once keeps both and two databases
// do not collapse into one entry.
//
// The database belongs in the key. Without it a scrape covering more than one
// database kept whichever database_up arrived last, so a fixture reading that
// entry could be told the control connection was up when it was not. That is
// how the control-path case below passed against a server with one eligible
// database and failed against one with two.
func publishedByFamilyAndReason(emit func(ch chan<- prometheus.Metric)) map[string]MetricResult {
	ch := make(chan prometheus.Metric, 4096)
	emit(ch)
	close(ch)
	out := map[string]MetricResult{}
	for m := range ch {
		r := readMetric(m)
		out[familyName(m)+"/"+r.labels["datname"]+"/"+r.labels["reason"]] = r
	}
	return out
}
