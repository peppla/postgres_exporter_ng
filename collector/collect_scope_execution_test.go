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

// What a scrape executes, not only what it publishes.
//
// A count of series says what was reported. It cannot say that a query was
// never sent, and "the catalogue is not read when nothing needs it" is a claim
// about what was sent. These tests drive the engine against a stub connection,
// where an unexpected query is an error and an unmet expectation is one too.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/versioncache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/sync/semaphore"
)

// quietCollector is a collector that reports nothing and reads nothing, so a
// test can choose a scope without also choosing a query.
type quietCollector struct{}

func (quietCollector) Update(context.Context, *instance, chan<- prometheus.Metric) error { return nil }

// withScopedCollector registers a collector of the given scope for the length
// of a test. The scope maps are package state, which the scope tests already
// write to the same way.
func withScopedCollector(t *testing.T, name string, scope collectorScope) {
	t.Helper()
	enabled := true
	collectorState[name] = &enabled
	collectorScopes[name] = scope
	t.Cleanup(func() {
		delete(collectorState, name)
		delete(collectorScopes, name)
	})
}

// stubConnection points the engine at a stub database for the length of a
// test, and returns the mock to set expectations on.
//
// Pings are monitored so a test can fail one, which is how the connection
// failure paths are reached without a server.
func stubConnection(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	previous := openDB
	openDB = func(string) (*sql.DB, error) { return db, nil }
	versioncache.Clear()
	t.Cleanup(func() {
		openDB = previous
		versioncache.Clear()
		db.Close()
	})
	return mock
}

// collectEngine runs one scrape of the engine and returns the metrics it
// emitted, by family name.
func collectEngine(t *testing.T, p PostgresCollector) map[string][]*dto.Metric {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	done := make(chan struct{})
	out := map[string][]*dto.Metric{}
	go func() {
		defer close(done)
		for metric := range ch {
			pb := &dto.Metric{}
			if err := metric.Write(pb); err != nil {
				t.Errorf("writing a metric: %s", err)
				continue
			}
			out[familyOf(metric)] = append(out[familyOf(metric)], pb)
		}
	}()
	p.Collect(ch)
	close(ch)
	<-done
	return out
}

func familyOf(m prometheus.Metric) string {
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

// engineWith builds an engine with the given collectors, pointed at the stub.
func engineWith(collectors map[string]Collector) PostgresCollector {
	return PostgresCollector{
		Collectors: collectors,
		logger:     log.NewNopLogger(),
		ctx:        context.Background(),
		instance:   &instance{dsn: "postgresql://probe@198.51.100.9:5432/app", name: "198.51.100.9:5432"},
	}
}

func gaugeValue(t *testing.T, metrics []*dto.Metric) float64 {
	t.Helper()
	if len(metrics) != 1 {
		t.Fatalf("expected one sample, got %d", len(metrics))
	}
	return metrics[0].GetGauge().GetValue()
}

// TestAClusterOnlyScrapeRunsNoCatalogQuery is the execution-level half of the
// coverage correction.
//
// The catalogue is read to decide which databases the fan-out runs on. With no
// database-scoped collector selected there is no fan-out, so a scrape that
// reads it is doing work nothing asked for, and a cluster-scoped collector
// waits on a query it does not need. The stub expects the version lookup and
// nothing else: the catalogue query would be an unexpected one.
func TestAClusterOnlyScrapeRunsNoCatalogQuery(t *testing.T) {
	mock := stubConnection(t)
	withScopedCollector(t, "scope_exec_cluster", scopeCluster)

	mock.ExpectPing()
	mock.ExpectQuery("SELECT version").WillReturnRows(
		sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 18.0 on x86_64-pc-linux-gnu"))
	// Expected so that running it can be seen. A query with no expectation
	// behind it is refused and the refusal is returned to the caller, which
	// this engine logs, so an unexpected query is invisible to a test. An
	// expectation that goes unmatched is not.
	mock.ExpectQuery("FROM pg_database").WillReturnRows(
		sqlmock.NewRows([]string{"datname", "is_current"}).AddRow("app", true))

	out := collectEngine(t, engineWith(map[string]Collector{"scope_exec_cluster": quietCollector{}}))

	unmatched := mock.ExpectationsWereMet()
	if unmatched == nil {
		t.Error("a cluster-only scrape read the catalogue, which only the fan-out needs")
	} else if !strings.Contains(unmatched.Error(), "pg_database") {
		t.Errorf("a cluster-only scrape left something other than the catalogue query unrun: %s", unmatched)
	}

	if got := gaugeValue(t, out["pg_collector_databases_selected"]); got != 0 {
		t.Errorf("a cluster-only scrape reported %v databases selected", got)
	}
	// Not asked for is neither known nor unknown. A zero would invent a server
	// with no databases and a one would claim a catalogue nobody read.
	if _, ok := out["pg_collector_databases_discovered"]; ok {
		t.Error("a cluster-only scrape published a discovered total without reading the catalogue")
	}
	if _, ok := out["pg_collector_database_discovery_success"]; ok {
		t.Error("a cluster-only scrape reported on a discovery it never attempted")
	}
	// The control connection worked, and that is what the cluster-wide work
	// ran on.
	if got := gaugeValue(t, out["pg_collector_database_up"]); got != 1 {
		t.Errorf("the control connection reports up=%v", got)
	}
}

// TestADatabaseScopedScrapeRunsTheCatalogQuery is what makes the test above
// mean anything: the same harness, one database-scoped collector, and the
// catalogue is read.
func TestADatabaseScopedScrapeRunsTheCatalogQuery(t *testing.T) {
	mock := stubConnection(t)
	withScopedCollector(t, "scope_exec_local", scopeDatabase)

	mock.ExpectPing()
	mock.ExpectQuery("SELECT version").WillReturnRows(
		sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 18.0 on x86_64-pc-linux-gnu"))
	mock.ExpectQuery("FROM pg_database").WillReturnRows(
		sqlmock.NewRows([]string{"datname", "is_current"}).AddRow("app", true))

	out := collectEngine(t, engineWith(map[string]Collector{"scope_exec_local": quietCollector{}}))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a database-scoped scrape did not read the catalogue: %s", err)
	}
	if got := gaugeValue(t, out["pg_collector_databases_selected"]); got != 1 {
		t.Errorf("a database-scoped scrape reported %v databases selected, want the one the catalogue holds", got)
	}
	if got := gaugeValue(t, out["pg_collector_databases_discovered"]); got != 1 {
		t.Errorf("discovered is %v, want the one database the catalogue holds", got)
	}
	if got := gaugeValue(t, out["pg_collector_database_discovery_success"]); got != 1 {
		t.Errorf("discovery_success is %v after a catalogue read that worked", got)
	}
}

// TestAClusterOnlyScrapeThatCannotConnectSelectsNoDatabase covers the failure
// path, which decided the scope after it had already reported.
//
// Measured on the first version: against an unreachable server,
// collect[]=postgres.cluster reported one database selected and none reached,
// which is a shortfall for work the request never asked for.
func TestAClusterOnlyScrapeThatCannotConnectSelectsNoDatabase(t *testing.T) {
	mock := stubConnection(t)
	withScopedCollector(t, "scope_exec_cluster", scopeCluster)

	mock.ExpectPing().WillReturnError(errors.New("the server is not listening"))
	mock.ExpectQuery("FROM pg_database").WillReturnRows(
		sqlmock.NewRows([]string{"datname", "is_current"}).AddRow("app", true))

	out := collectEngine(t, engineWith(map[string]Collector{"scope_exec_cluster": quietCollector{}}))

	unmatched := mock.ExpectationsWereMet()
	if unmatched == nil {
		t.Error("a scrape whose connection failed read the catalogue over it")
	} else if !strings.Contains(unmatched.Error(), "pg_database") {
		t.Errorf("the failed scrape left something other than the catalogue query unrun: %s", unmatched)
	}
	if got := gaugeValue(t, out["pg_collector_databases_selected"]); got != 0 {
		t.Errorf("a cluster-only scrape that could not connect reported %v databases selected", got)
	}
	// The connection diagnostics are the point of this path and stay.
	if got := gaugeValue(t, out["pg_collector_database_up"]); got != 0 {
		t.Errorf("a connection that failed reports up=%v", got)
	}
	if len(out["pg_collector_database_error"]) != 1 {
		t.Errorf("a connection that failed published %d error series, want 1",
			len(out["pg_collector_database_error"]))
	}
	// Not asked for stays not asked for on this path too. A zero here says
	// the set of databases has collapsed to the configured one, and this
	// request never wanted a set.
	if _, ok := out["pg_collector_database_discovery_success"]; ok {
		t.Error("cluster-only failure reports discovery status despite no discovery requested")
	}
}

// TestAClusterOnlyScrapeWithNoPermitSelectsNoDatabase covers the other early
// exit, where the request never got a connection permit and nothing was
// opened at all.
//
// Same rule as the connection failure above: the scope decides what is
// reported, and it is decided before either exit.
func TestAClusterOnlyScrapeWithNoPermitSelectsNoDatabase(t *testing.T) {
	mock := stubConnection(t)
	withScopedCollector(t, "scope_exec_cluster", scopeCluster)

	// Nothing is expected, because nothing should be run: without a permit
	// the engine does not open the connection, so even the version lookup
	// must not happen.
	engine := engineWith(map[string]Collector{"scope_exec_cluster": quietCollector{}})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	engine.ctx = cancelled
	engine.connSema = semaphore.NewWeighted(1)

	out := collectEngine(t, engine)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a scrape with no connection permit ran a query: %s", err)
	}
	if got := gaugeValue(t, out["pg_collector_databases_selected"]); got != 0 {
		t.Errorf("a cluster-only scrape with no permit reported %v databases selected", got)
	}
	if _, ok := out["pg_collector_database_discovery_success"]; ok {
		t.Error("a scrape with no permit reports discovery status despite no discovery requested")
	}
	if got := gaugeValue(t, out["pg_collector_database_up"]); got != 0 {
		t.Errorf("database_up is %v when nothing was opened", got)
	}
	// Deferred, not failed: nothing was tried, so nothing is known to be
	// wrong with this server. The connection is still named, because it is
	// what the cluster-wide work would have run on.
	if len(out["pg_collector_database_deferred"]) != 1 {
		t.Errorf("a scrape with no permit published %d deferral series, want 1",
			len(out["pg_collector_database_deferred"]))
	}
	if _, ok := out["pg_collector_database_error"]; ok {
		t.Error("a scrape with no permit published an error, so an alert on failures fires for a busy exporter")
	}
}

// TestADatabaseScopedScrapeThatCannotConnectSelectsItsDatabase is the other
// side of the same path: the request did ask for database-local work, so the
// control database is still what it meant to cover.
func TestADatabaseScopedScrapeThatCannotConnectSelectsItsDatabase(t *testing.T) {
	mock := stubConnection(t)
	withScopedCollector(t, "scope_exec_local", scopeDatabase)

	mock.ExpectPing().WillReturnError(errors.New("the server is not listening"))

	out := collectEngine(t, engineWith(map[string]Collector{"scope_exec_local": quietCollector{}}))

	if got := gaugeValue(t, out["pg_collector_databases_selected"]); got != 1 {
		t.Errorf("a database-scoped scrape that could not connect reported %v databases selected, want its control database", got)
	}
	if got := gaugeValue(t, out["pg_collector_database_discovery_success"]); got != 0 {
		t.Errorf("discovery_success is %v after a connection that never worked", got)
	}
}

// TestADatabaseScopedScrapeWithNoPermitReportsUnknownDiscovery is the same
// half of the rule on the admission path: database-local work was selected, so
// the set of databases was wanted and could not be determined.
func TestADatabaseScopedScrapeWithNoPermitReportsUnknownDiscovery(t *testing.T) {
	stubConnection(t)
	withScopedCollector(t, "scope_exec_local", scopeDatabase)

	engine := engineWith(map[string]Collector{"scope_exec_local": quietCollector{}})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	engine.ctx = cancelled
	engine.connSema = semaphore.NewWeighted(1)

	out := collectEngine(t, engine)

	got, ok := out["pg_collector_database_discovery_success"]
	if !ok {
		t.Fatal("a scrape that wanted the set of databases and could not get it reported nothing about discovery")
	}
	if gaugeValue(t, got) != 0 {
		t.Errorf("discovery_success is %v when no connection was available to read the catalogue", gaugeValue(t, got))
	}
}
