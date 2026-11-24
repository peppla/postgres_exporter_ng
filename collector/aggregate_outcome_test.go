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

// What the database-local work came to, counted.
//
// The engine already said which databases it meant to cover and what happened
// to each one. What it could not say is how the two compare: a reader had to
// count series to learn that nine of ten databases were covered, and a reader
// counting series cannot tell a database that was never selected from one
// whose series went missing.
//
// Two equations make the counts usable: attempted plus deferred is selected,
// and covered plus failed is attempted. Every case here checks both, because
// an exporter that has lost track of its own work is the thing these series
// exist to show, and a set of counts that can disagree with itself shows
// nothing.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/versioncache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/sync/semaphore"
)

// failsOnDatabase fails for one named database and works everywhere else.
//
// A collector that fails on every database cannot produce a mixed scrape, and
// a mixed scrape is the one where a count that averages a shortfall across a
// set is indistinguishable from a count that does not.
type failsOnDatabase struct{ datname string }

func (c failsOnDatabase) Update(_ context.Context, inst *instance, _ chan<- prometheus.Metric) error {
	if name, _ := config.DatabaseFromDSN(inst.dsn); name == c.datname {
		return errors.New("the collector could not read what it needed")
	}
	return nil
}

// stubServer points the engine at a fresh stub connection for every open.
//
// One shared stub does not survive a fan-out: the engine closes the control
// connection before opening the databases, which would close the handle under
// them and turn every database into a connection failure. A connection each
// also matches what the engine does to a server.
//
// expect is called with each new mock as it is created, numbered in the order
// the engine opened them. Number 0 is the control connection, which is opened
// before any fan-out begins. The rest are the fan-out, in whichever order its
// goroutines got there, so they are given the same expectations.
//
// Each fan-out database costs two opens, because newInstance opens a handle to
// check the connection string and closes it again before setup opens the one
// it keeps. Only the second is pinged, so a ping expectation on the first goes
// unmatched, which is why these mocks are not checked for completeness.
func stubServer(t *testing.T, expect func(n int, mock sqlmock.Sqlmock)) func() []sqlmock.Sqlmock {
	t.Helper()
	var mtx sync.Mutex
	var mocks []sqlmock.Sqlmock
	previous := openDB
	openDB = func(string) (*sql.DB, error) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			return nil, err
		}
		mtx.Lock()
		n := len(mocks)
		mocks = append(mocks, mock)
		mtx.Unlock()
		expect(n, mock)
		return db, nil
	}
	versioncache.Clear()
	t.Cleanup(func() {
		openDB = previous
		versioncache.Clear()
	})
	return func() []sqlmock.Sqlmock {
		mtx.Lock()
		defer mtx.Unlock()
		return mocks
	}
}

// catalogueOf gives every connection the version lookup and a catalogue holding
// the named databases.
//
// current is the database the connection is on, which the catalogue reports as
// its own row rather than leaving out. others are the remaining databases on
// the server.
//
// Every connection gets both, matched out of order. The version lookup is
// optional, because the engine caches it per server and only the first
// connection in a process runs it. The catalogue read is required of a control
// connection and is never reached by a fan-out one. Matched in order, a
// skipped version lookup puts the catalogue read against the wrong
// expectation, which fails it and sends the engine down its control fallback
// with nothing in the fixture to say so.
func catalogueOf(current string, others ...string) func(int, sqlmock.Sqlmock) {
	return func(_ int, mock sqlmock.Sqlmock) {
		mock.MatchExpectationsInOrder(false)
		mock.ExpectPing()
		mock.ExpectQuery("SELECT version").WillReturnRows(
			sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 18.0 on x86_64-pc-linux-gnu"))
		rows := sqlmock.NewRows([]string{"datname", "is_current"}).AddRow(current, true)
		for _, name := range others {
			rows.AddRow(name, false)
		}
		mock.ExpectQuery("FROM pg_database").WillReturnRows(rows)
	}
}

// outcome is the five counts one scrape reported.
type outcome struct {
	selected  float64
	attempted float64
	scraped   float64
	failed    float64
	deferred  float64
}

// outcomeOf reads the counts and checks that they agree with each other.
//
// The two equations are checked here rather than in one test of their own, so
// that every case below proves them for the path it covers. A count published
// on one path and omitted on another is a missing series rather than a wrong
// number, so each is required to be present.
func outcomeOf(t *testing.T, out map[string][]*dto.Metric) outcome {
	t.Helper()
	read := func(name string) float64 {
		metrics, ok := out["pg_collector_"+name]
		if !ok {
			t.Fatalf("%s was not published, so the counts cannot be read as a set", name)
		}
		return gaugeValue(t, metrics)
	}
	got := outcome{
		selected:  read("databases_selected"),
		attempted: read("databases_attempted"),
		scraped:   read("databases_scraped"),
		failed:    read("databases_failed"),
		deferred:  read("databases_deferred"),
	}
	if got.attempted+got.deferred != got.selected {
		t.Errorf("attempted %v plus deferred %v is not selected %v: the exporter has lost track of a database",
			got.attempted, got.deferred, got.selected)
	}
	if got.scraped+got.failed != got.attempted {
		t.Errorf("covered %v plus failed %v is not attempted %v: the exporter has lost track of a database",
			got.scraped, got.failed, got.attempted)
	}
	return got
}

// String is what a failure message prints, because the five numbers are only
// readable together.
func (o outcome) String() string {
	n := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	return "selected " + n(o.selected) +
		", attempted " + n(o.attempted) +
		", covered " + n(o.scraped) +
		", failed " + n(o.failed) +
		", deferred " + n(o.deferred)
}

// labelOf returns one label's value, or the empty string when the metric does
// not carry it.
func labelOf(m *dto.Metric, name string) string {
	for _, pair := range m.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}

// errorsByDatabase returns the reason published against each database.
func errorsByDatabase(out map[string][]*dto.Metric) map[string]string {
	reasons := map[string]string{}
	for _, m := range out["pg_collector_database_error"] {
		reasons[labelOf(m, "datname")] = labelOf(m, "reason")
	}
	return reasons
}

// engineOn builds an engine with the given collectors and selection rule.
func engineOn(collectors map[string]Collector, exclude, include dbselect.List) PostgresCollector {
	return PostgresCollector{
		Collectors:       collectors,
		logger:           log.NewNopLogger(),
		ctx:              context.Background(),
		excludeDatabases: exclude,
		includeDatabases: include,
		instance:         &instance{dsn: "postgresql://probe@198.51.100.9:5432/app", name: "198.51.100.9:5432"},
	}
}

// TestRepeatedScrapesDiscoverEveryDatabase holds the fixture itself honest.
//
// The engine looks up the server version once per process and caches it, so a
// second scrape does not run that query. A fixture that insists on it anyway
// pushes every later query one expectation out of step, the catalogue read
// fails, and the engine falls back to the connected database. That fallback is
// correct behaviour on a real failure and wrong as a test result: coverage
// silently drops to one database while a case that only reads the connected
// database's series still passes.
func TestRepeatedScrapesDiscoverEveryDatabase(t *testing.T) {
	stubServer(t, catalogueOf("app", "other"))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	for _, pass := range []string{"first", "second"} {
		t.Run(pass, func(t *testing.T) {
			out := collectEngine(t, engineOn(
				map[string]Collector{"outcome_local": quietCollector{}},
				dbselect.Absent(), dbselect.Absent()))

			if got := gaugeValue(t, out["pg_collector_database_discovery_success"]); got != 1 {
				t.Errorf("discovery_success is %v, so the catalogue was not read", got)
			}
			if got := gaugeValue(t, out["pg_collector_databases_discovered"]); got != 2 {
				t.Errorf("discovered is %v, want both fixture databases", got)
			}
			got := outcomeOf(t, out)
			if want := (outcome{selected: 2, attempted: 2, scraped: 2}); got != want {
				t.Errorf("the %s scrape reported %s", pass, got)
			}
		})
	}
}

// TestACoveredDatabaseIsCounted is the baseline the other cases are read
// against: one database, one collector, nothing wrong.
func TestACoveredDatabaseIsCounted(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	got := outcomeOf(t, collectEngine(t, engineOn(
		map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Absent(), dbselect.Absent())))

	want := outcome{selected: 1, attempted: 1, scraped: 1}
	if got != want {
		t.Errorf("a scrape that covered its one database reported %s", got)
	}
}

// TestAnExcludedControlDatabaseIsNotCounted covers the case the control
// connection makes awkward.
//
// That connection is opened whether or not its database is selected, because
// cluster-wide collection runs on it, and it reports database_up for itself.
// None of that makes its database one this scrape meant to cover, and counting
// it would report a database covered that the operator asked to be left alone.
func TestAnExcludedControlDatabaseIsNotCounted(t *testing.T) {
	mocks := stubServer(t, catalogueOf("app", "other"))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Of([]string{"app"}), dbselect.Absent()))
	got := outcomeOf(t, out)

	// One database selected, which is the other one, and it was covered.
	want := outcome{selected: 1, attempted: 1, scraped: 1}
	if got != want {
		t.Errorf("a scrape whose control database is excluded reported %s", got)
	}
	// The connection still worked and still says so, for both databases: the
	// control because cluster-wide collection ran over it, the other because
	// the fan-out opened it.
	if len(out["pg_collector_database_up"]) != 2 {
		t.Errorf("%d database_up series, want the control connection and the one selected database",
			len(out["pg_collector_database_up"]))
	}
	if len(mocks()) < 2 {
		t.Errorf("%d connections were opened, so the selected database was never reached", len(mocks()))
	}
	// The exclusion is an instruction about work, not only about counting.
	// One collector ran, on the one database that was selected, and the
	// control connection carried the cluster-wide work alone.
	ran := out["pg_scrape_collector_success"]
	if len(ran) != 1 {
		t.Fatalf("%d collector runs, want the one selected database's", len(ran))
	}
	if got := labelOf(ran[0], "datname"); got != "other" {
		t.Errorf("the collector ran against %q, want the database selection kept", got)
	}
	if reasons := errorsByDatabase(out); len(reasons) != 0 {
		t.Errorf("an excluded control database was reported as an error: %v", reasons)
	}
}

// TestAnEmptyIncludeListCoversNothing is the other way a database-scoped
// request ends up covering no database.
//
// An explicitly empty include list is an instruction, not an omission. The
// counts say nothing was selected and nothing was attempted, and no database
// connection is opened at all.
func TestAnEmptyIncludeListCoversNothing(t *testing.T) {
	mocks := stubServer(t, catalogueOf("app", "other"))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Absent(), dbselect.Of(nil)))
	got := outcomeOf(t, out)

	if (got != outcome{}) {
		t.Errorf("a scrape that was asked to cover no database reported %s", got)
	}
	// The counts are published as zeros rather than left out. A reader cannot
	// tell a missing series from a series nobody published, and this
	// configuration is one an operator writes on purpose.
	if got := gaugeValue(t, out["pg_collector_databases_excluded"]); got != 2 {
		t.Errorf("databases_excluded is %v, want both databases the server offered", got)
	}
	if len(mocks()) != 1 {
		t.Errorf("%d connections were opened, want the control connection alone", len(mocks()))
	}
}

// TestClusterOnlyCollectionReportsZeroedCounts covers the request that asks
// for no database-local work at all.
//
// The counts are published as zeros, which is a different claim from omitting
// them. Before this they were split: the deferral count was published when
// such a request had no connection permit and omitted when it worked, so one
// configuration reported two different shapes depending on how busy the
// exporter was.
func TestClusterOnlyCollectionReportsZeroedCounts(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "outcome_cluster", scopeCluster)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_cluster": quietCollector{}},
		dbselect.Absent(), dbselect.Absent()))
	got := outcomeOf(t, out)

	if (got != outcome{}) {
		t.Errorf("a cluster-only scrape reported %s", got)
	}
	// The cluster-wide work itself ran, on the control connection.
	if got := gaugeValue(t, out["pg_collector_database_up"]); got != 1 {
		t.Errorf("the control connection reports up=%v", got)
	}
}

// TestAFailedCollectorFailsItsDatabase covers a collector that could not read
// what it needed on a connection that worked.
//
// The connection is not what went wrong, so database_up stays 1. What the
// scrape did not do is cover that database, and the count has to say so or a
// collector failing everywhere reads as a server fully covered.
func TestAFailedCollectorFailsItsDatabase(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_local": failsOnDatabase{datname: "app"}},
		dbselect.Absent(), dbselect.Absent()))
	got := outcomeOf(t, out)

	want := outcome{selected: 1, attempted: 1, failed: 1}
	if got != want {
		t.Errorf("a scrape whose only collector failed reported %s", got)
	}
	if got := gaugeValue(t, out["pg_collector_database_up"]); got != 1 {
		t.Errorf("database_up is %v after a collector failed on a connection that worked", got)
	}
	// Named, not just counted. A count of one failure with nothing saying
	// which database failed is the shortfall averaged across a set.
	reasons := errorsByDatabase(out)
	if reasons["app"] != dbselect.ReasonScrape {
		t.Errorf("the failure is reported as %q, want %q", reasons["app"], dbselect.ReasonScrape)
	}
	if _, ok := out["pg_collector_database_deferred"]; ok {
		t.Error("a collector failure was published as a deferral, which says nothing was tried")
	}
}

// TestCoverageRequiresEverySelectedCollector is the completion rule.
//
// A database is covered when all of its selected collectors worked, not when
// the ones that ran worked. Counting the successes that arrived would let a
// scrape report full coverage while one collector failed on every database,
// and it would let a future scheduling decision drop a collector and still
// report the database as covered.
func TestCoverageRequiresEverySelectedCollector(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "outcome_local_a", scopeDatabase)
	withScopedCollector(t, "outcome_local_b", scopeDatabase)

	out := collectEngine(t, engineOn(map[string]Collector{
		"outcome_local_a": quietCollector{},
		"outcome_local_b": failsOnDatabase{datname: "app"},
	}, dbselect.Absent(), dbselect.Absent()))
	got := outcomeOf(t, out)

	want := outcome{selected: 1, attempted: 1, failed: 1}
	if got != want {
		t.Errorf("a database with one of two collectors failing reported %s", got)
	}
	// Both collectors reported for themselves, which is what makes the
	// database-level answer checkable against them.
	if len(out["pg_scrape_collector_success"]) != 2 {
		t.Errorf("%d collector_success series, want one per selected collector",
			len(out["pg_scrape_collector_success"]))
	}
}

// TestCountsAddUpAcrossDatabases is the case a single-database server cannot
// show: one database covered and another failing, in one scrape.
func TestCountsAddUpAcrossDatabases(t *testing.T) {
	stubServer(t, catalogueOf("app", "other"))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_local": failsOnDatabase{datname: "other"}},
		dbselect.Absent(), dbselect.Absent()))
	got := outcomeOf(t, out)

	want := outcome{selected: 2, attempted: 2, scraped: 1, failed: 1}
	if got != want {
		t.Errorf("a scrape of two databases where one collector failed reported %s", got)
	}
	reasons := errorsByDatabase(out)
	if len(reasons) != 1 || reasons["other"] != dbselect.ReasonScrape {
		t.Errorf("the failure is reported against %v, want the one database whose collector failed", reasons)
	}
}

// TestADeferredDatabaseIsAttemptedByNothing covers the control connection that
// never got a permit.
//
// Deferred is not failed and not covered. The equations put it outside
// attempted, so an operator watching failures is not woken by a busy exporter
// and an operator watching a selected set that cannot be kept up with has the
// number to watch.
func TestADeferredDatabaseIsAttemptedByNothing(t *testing.T) {
	withScopedCollector(t, "outcome_local", scopeDatabase)
	engine := engineOn(map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Absent(), dbselect.Absent())
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	engine.ctx = cancelled
	engine.connSema = semaphore.NewWeighted(1)

	out := collectEngine(t, engine)
	got := outcomeOf(t, out)

	want := outcome{selected: 1, deferred: 1}
	if got != want {
		t.Errorf("a scrape with no connection permit reported %s", got)
	}
	if _, ok := out["pg_collector_database_error"]; ok {
		t.Error("a deferral was published as an error, so an alert on failures fires for a busy exporter")
	}
}

// TestAControlConnectionThatFailedCountsAsAttempted is the other early exit.
//
// The connection was tried and did not work, which is a failure and not a
// deferral: something is wrong with that database or the server, and the
// equations have to place it inside attempted.
func TestAControlConnectionThatFailedCountsAsAttempted(t *testing.T) {
	stubServer(t, func(_ int, mock sqlmock.Sqlmock) {
		mock.ExpectPing().WillReturnError(errors.New("the server is not listening"))
	})
	withScopedCollector(t, "outcome_local", scopeDatabase)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Absent(), dbselect.Absent()))
	got := outcomeOf(t, out)

	want := outcome{selected: 1, attempted: 1, failed: 1}
	if got != want {
		t.Errorf("a scrape whose control connection failed reported %s", got)
	}
	if reasons := errorsByDatabase(out); reasons["app"] != dbselect.ReasonConnect {
		t.Errorf("the failure is reported as %q, want %q", reasons["app"], dbselect.ReasonConnect)
	}
}

// TestAClusterOnlyScrapeThatCannotConnectCountsNothing keeps the two rules
// from colliding.
//
// The connection that failed is still reported, because cluster-wide
// collection would have run over it. It is not a database this request meant
// to cover, so it is not counted as one that failed.
func TestAClusterOnlyScrapeThatCannotConnectCountsNothing(t *testing.T) {
	stubServer(t, func(_ int, mock sqlmock.Sqlmock) {
		mock.ExpectPing().WillReturnError(errors.New("the server is not listening"))
	})
	withScopedCollector(t, "outcome_cluster", scopeCluster)

	out := collectEngine(t, engineOn(
		map[string]Collector{"outcome_cluster": quietCollector{}},
		dbselect.Absent(), dbselect.Absent()))
	got := outcomeOf(t, out)

	if (got != outcome{}) {
		t.Errorf("a cluster-only scrape that could not connect reported %s", got)
	}
	// The connection diagnostic stays, because that is what did fail.
	if len(out["pg_collector_database_error"]) != 1 {
		t.Errorf("%d error series for a connection that failed, want 1",
			len(out["pg_collector_database_error"]))
	}
}
