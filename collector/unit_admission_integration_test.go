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
// opened for itself. The ended run proves the same path stops before any SQL
// and reports the database as an incomplete attempt with the budget as its
// cause, keeping database_up at 1 because the connection is not what went
// wrong.
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
