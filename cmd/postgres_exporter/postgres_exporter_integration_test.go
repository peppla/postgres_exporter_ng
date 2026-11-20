// Copyright 2021 The Prometheus Authors
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

// These tests need a running PostgreSQL server, so they are built only under
// the integration tag. DATA_SOURCE_NAME must point at it. docker-compose.yml
// starts a matching server and .github/workflows/go.yml runs these tests once
// per supported major version.
//
// Several assertions here used to pass without reaching a database. A test
// that consumed its metric channel and threw everything away could report
// success while nothing had been collected, so each test now keeps what it
// produced and names what it expects to find.
//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	_ "github.com/lib/pq"
	"github.com/percona/postgres_exporter/collector"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/versioncache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	vc "github.com/prometheus/client_golang/prometheus/collectors/version"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/sync/semaphore"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type IntegrationSuite struct {
	e *Exporter
}

var _ = Suite(&IntegrationSuite{})

func (s *IntegrationSuite) SetUpSuite(c *C) {
	// Flag defaults are not applied until the command line is parsed, and
	// several of them change what the exporter produces. metric-prefix is the
	// sharp one: unparsed it is the empty string, so every built-in metric
	// comes out named _stat_database_* instead of pg_stat_database_*. The
	// collector registry is also keyed on flag values, so without this no
	// namespaced collector is enabled.
	kingpin.CommandLine.Terminate(nil)
	_, err := kingpin.CommandLine.Parse([]string{})
	c.Assert(err, IsNil)
	c.Assert(*metricPrefix, Equals, "pg")

	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	exporter := NewExporter(strings.Split(dsn, ","))
	c.Assert(exporter, NotNil)
	// Assign the exporter to the suite
	s.e = exporter

	prometheus.MustRegister(exporter)
}

// recorder keeps the metrics a collector produces instead of discarding them.
type recorder struct {
	ch chan prometheus.Metric
	wg sync.WaitGroup
	mu sync.Mutex
	in []prometheus.Metric
}

func newRecorder() *recorder {
	r := &recorder{ch: make(chan prometheus.Metric, 1024)}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for m := range r.ch {
			r.mu.Lock()
			r.in = append(r.in, m)
			r.mu.Unlock()
		}
	}()
	return r
}

// done closes the channel and returns every metric that was written to it.
func (r *recorder) done() []prometheus.Metric {
	close(r.ch)
	r.wg.Wait()
	return r.in
}

var fqNameRe = regexp.MustCompile(`fqName: "([^"]+)"`)

// metricNames returns the set of metric names in a batch of metrics. The name
// lives only in the descriptor, and the descriptor only prints it.
func metricNames(metrics []prometheus.Metric) map[string]int {
	names := make(map[string]int, len(metrics))
	for _, m := range metrics {
		if match := fqNameRe.FindStringSubmatch(m.Desc().String()); match != nil {
			names[match[1]]++
		}
	}
	return names
}

// gather registers c in a registry of its own and collects it, which is the
// path a real scrape takes and the only way to read metric family names
// without parsing descriptors.
//
// An ordinary registry, not a pedantic one: PostgresCollector.Describe
// advertises only its two status descriptors and produces the rest unchecked,
// which a pedantic registry rejects as undeclared.
func gather(c *C, coll prometheus.Collector) map[string]*dto.MetricFamily {
	reg := prometheus.NewRegistry()
	reg.MustRegister(coll)
	families, err := reg.Gather()
	c.Assert(err, IsNil)
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		byName[f.GetName()] = f
	}
	return byName
}

// TestAllNamespacesReturnResults runs the built-in namespace queries against
// the server and checks that they produced metrics.
//
// This test used to build its server with NewServer, which leaves master
// false. checkMapVersions then takes its non-master branch and leaves the
// metric map empty, so queryNamespaceMappings had nothing to run and returned
// no errors. The test asserted exactly that empty error map and passed without
// executing a single built-in namespace.
func (s *IntegrationSuite) TestAllNamespacesReturnResults(c *C) {
	for _, dsn := range s.e.dsn {
		rec := newRecorder()

		// Open a database connection
		server, err := NewServer(dsn)
		c.Assert(server, NotNil)
		c.Assert(err, IsNil)
		// The built-in metric map is built for the master server only. Without
		// this the rest of the test has nothing to execute.
		server.master = true

		// Do a version update
		err = s.e.checkMapVersions(context.Background(), rec.ch, server)
		c.Assert(err, IsNil)
		c.Assert(len(server.metricMap) > 0, Equals, true,
			Commentf("checkMapVersions produced no namespaces to query"))

		err = querySettings(context.Background(), rec.ch, server)
		if !c.Check(err, Equals, nil) {
			fmt.Println("## ERRORS FOUND")
			fmt.Println(err)
		}

		// This should never happen in our test cases.
		errMap := queryNamespaceMappings(context.Background(), rec.ch, server, querycache.New(), "test")
		if !c.Check(len(errMap), Equals, 0) {
			fmt.Println("## NAMESPACE ERRORS FOUND")
			for namespace, err := range errMap {
				fmt.Println(namespace, ":", err)
			}
		}
		server.Close()

		names := metricNames(rec.done())

		// An empty error map is only good news if something ran. These three
		// namespaces return at least one row in this fixture, a stock primary
		// reached with a superuser connection: pg_stat_database_conflicts has
		// a row per database even with no conflicts, pg_stat_archiver has its
		// row even with archiving off, and the activity override builds rows
		// from database names and known session states with zero counts where
		// nothing matches. That is a claim about this fixture, not about every
		// server: a restricted role or a managed service can hide any of them.
		// pg_stat_replication, pg_replication_slots and pg_lock_conflicts are
		// legitimately empty here, so they are not asserted.
		for _, want := range []string{
			"pg_stat_database_conflicts_confl_lock",
			"pg_stat_archiver_archived_count",
			"pg_stat_activity_count",
		} {
			c.Check(names[want] > 0, Equals, true,
				Commentf("%s was not produced, so the namespace queries did not run against this fixture", want))
		}

		// querySettings reads pg_settings and names each metric after the
		// setting, so look for one every server has.
		c.Check(names["pg_settings_max_connections"] > 0, Equals, true,
			Commentf("querySettings produced no pg_settings_* metrics"))
	}
}

// TestNamespacedCollectorsAgainstServer scrapes the namespaced collectors,
// the engine the legacy suite above never touches. Every collector test in
// collector/ uses sqlmock, so until this ran, no test in the repository had
// executed a namespaced collector's SQL against a real PostgreSQL server.
func (s *IntegrationSuite) TestNamespacedCollectorsAgainstServer(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	c.Assert(len(pc.Collectors) > 0, Equals, true,
		Commentf("no namespaced collector was enabled"))

	families := gather(c, pc)

	success, ok := families["pg_scrape_collector_success"]
	c.Assert(ok, Equals, true,
		Commentf("no collector reported a scrape status"))

	// Every enabled collector reports at least once. Database-local ones
	// report once per database they ran in, so the sample count is no longer
	// the collector count, which is what the fan-out changed.
	reported := map[string]int{}
	var failed []string
	for _, m := range success.GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		reported[labels["collector"]]++
		if m.GetGauge().GetValue() != 1 {
			failed = append(failed, labels["collector"]+"/"+labels["datname"])
		}
	}
	for name := range pc.Collectors {
		c.Check(reported[name] > 0, Equals, true,
			Commentf("%s reported no status at all", name))
	}
	c.Check(len(reported), Equals, len(pc.Collectors))
	c.Check(failed, IsNil, Commentf("collectors failed against this server: %v", failed))

	// A collector can report success and emit nothing. pg_stat_database has a
	// row per database on every server, so its families must carry samples.
	found := 0
	for name, f := range families {
		if strings.HasPrefix(name, "pg_stat_database_") && len(f.GetMetric()) > 0 {
			found++
		}
	}
	c.Check(found > 0, Equals, true,
		Commentf("every collector reported success and produced no pg_stat_database_* samples"))
}

// TestInvalidDsnDoesntCrash tests that specifying an invalid DSN doesn't crash
// the exporter. Related to https://github.com/prometheus-community/postgres_exporter/issues/93
// although not a replication of the scenario.
func (s *IntegrationSuite) TestInvalidDsnDoesntCrash(c *C) {
	rec := newRecorder()
	defer rec.done()

	// Send a bad DSN
	exporter := NewExporter([]string{"invalid dsn"})
	c.Assert(exporter, NotNil)
	exporter.scrape(rec.ch)

	// Send a DSN to a non-listening port.
	exporter = NewExporter([]string{"postgresql://nothing:nothing@127.0.0.1:1/nothing"})
	c.Assert(exporter, NotNil)
	exporter.scrape(rec.ch)
}

// TestUnknownMetricParsingDoesntCrash deliberately deletes all the column maps out
// of an exporter to test that the default metric handling code can cope with unknown columns.
func (s *IntegrationSuite) TestUnknownMetricParsingDoesntCrash(c *C) {
	rec := newRecorder()

	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	exporter := NewExporter(strings.Split(dsn, ","))
	c.Assert(exporter, NotNil)

	// Convert the default maps into a list of empty maps.
	emptyMaps := make(map[string]intermediateMetricMap, 0)
	for k := range exporter.builtinMetricMaps {
		emptyMaps[k] = intermediateMetricMap{
			columnMappings: map[string]ColumnMapping{},
			master:         true,
			cacheSeconds:   0,
		}
	}
	exporter.builtinMetricMaps = emptyMaps

	// scrape the exporter and make sure it works
	exporter.scrape(rec.ch)

	// The scrape still has to have produced something, or the empty column
	// maps were never reached and the test proves nothing.
	c.Check(len(rec.done()) > 0, Equals, true,
		Commentf("the scrape produced nothing at all"))
}

// writeQueryFile puts content in a directory of its own and returns the
// directory. loadCustomQueries reads a DIRECTORY and takes every .yml and
// .yaml file in it. The tests below used to hand it the path of a single file,
// which os.ReadDir rejects, so nothing was ever loaded.
func writeQueryFile(c *C, name, content string) string {
	dir := c.MkDir()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), IsNil)
	return dir
}

// A query that works on every supported major version and returns exactly one
// row, so the metric it produces can be asserted by value.
const fixtureQuery = `
pg_integration_fixture:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  metrics:
    - datname:
        usage: "LABEL"
        description: "Database name"
    - value:
        usage: "GAUGE"
        description: "Constant one, to prove the query ran"
`

// TestCustomQueriesProduceMetrics checks that a query file is loaded, reported
// as loaded, and that its query reaches the server and produces its metric.
//
// Loading was previously asserted by a test that could not load anything: the
// exporter was given file paths where a directory was expected, and
// resolutionEnabled was left at its zero value, which matches no resolution,
// so loadCustomQueries returned before reading anything.
func (s *IntegrationSuite) TestCustomQueriesProduceMetrics(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	dir := writeQueryFile(c, "fixture.yaml", fixtureQuery)

	exporter := NewExporter(
		strings.Split(dsn, ",")[:1],
		WithUserQueriesEnabled(LR),
		WithUserQueriesPath(map[MetricResolution]string{LR: dir}),
	)
	c.Assert(exporter, NotNil)

	families := gather(c, exporter)

	loadErr, ok := families["pg_exporter_user_queries_load_error"]
	c.Assert(ok, Equals, true,
		Commentf("the exporter reported no load status for %q, so the directory was never read", dir))
	for _, m := range loadErr.GetMetric() {
		c.Check(m.GetGauge().GetValue(), Equals, float64(0),
			Commentf("query file failed to load: %v", m.GetLabel()))
	}

	fixture, ok := families["pg_integration_fixture_value"]
	c.Assert(ok, Equals, true,
		Commentf("the fixture query produced no metric, so it did not run"))
	c.Assert(len(fixture.GetMetric()) > 0, Equals, true)
	for _, m := range fixture.GetMetric() {
		c.Check(m.GetGauge().GetValue(), Equals, float64(1))
	}
}

// TestExtendQueriesDoesntCrash feeds the exporter a query file whose columns
// are deliberately wrong and checks that the scrape survives it. The file is
// the repository's own user_queries_test.yaml fixture, which asks for
// histogram buckets that its query does not return.
func (s *IntegrationSuite) TestExtendQueriesDoesntCrash(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	broken, err := os.ReadFile("tests/user_queries_test.yaml")
	c.Assert(err, IsNil)
	dir := writeQueryFile(c, "user_queries_test.yaml", string(broken))

	exporter := NewExporter(
		strings.Split(dsn, ",")[:1],
		WithUserQueriesEnabled(HR),
		WithUserQueriesPath(map[MetricResolution]string{HR: dir}),
	)
	c.Assert(exporter, NotNil)

	// Collecting runs the scrape and then publishes the load status, which
	// scrape on its own does not. Surviving this call is what the test is for.
	families := gather(c, exporter)

	// The file parses, so it loads cleanly. The damage is in the columns and
	// shows up while metrics are built. A load status is reported only if the
	// directory was read at all.
	loadErr, ok := families["pg_exporter_user_queries_load_error"]
	c.Assert(ok, Equals, true,
		Commentf("no load status was reported, so the query directory was never read"))
	c.Assert(len(loadErr.GetMetric()) > 0, Equals, true)

	up, ok := families["pg_up"]
	c.Assert(ok, Equals, true)
	for _, m := range up.GetMetric() {
		c.Check(m.GetGauge().GetValue(), Equals, float64(1),
			Commentf("the scrape did not reach the server"))
	}
}

// TestAutoDiscoverDatabases checks that discovery finds every database the
// server will let the exporter connect to, plus the one in the DSN.
//
// The count was previously hardcoded to 2, which was a fact about
// docker-compose.yml rather than about discovery, and said nothing when it
// held.
func (s *IntegrationSuite) TestAutoDiscoverDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	exporter := NewExporter(strings.Split(dsn, ","))
	c.Assert(exporter, NotNil)

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()

	// The same catalog discovery is built from, asked for directly.
	catalog, err := queryDatabases(context.Background(), server)
	c.Assert(err, IsNil)
	c.Assert(len(catalog) > 1, Equals, true,
		Commentf("the fixture server has one database, so discovery was never exercised"))

	// The catalog query returns the connection's own database, flagged. That
	// is what gives every target a name, and it is why the counts below no
	// longer add one and hope.
	seedName, found := currentName(catalog)
	c.Assert(found, Equals, true,
		Commentf("the catalog did not report which database the connection is on: %v", catalog))
	var reported string
	c.Assert(server.db.QueryRow("SELECT current_database()").Scan(&reported), IsNil)
	c.Assert(seedName, Equals, reported)

	others := otherThanCurrent(catalog)
	c.Assert(len(others), Equals, len(catalog)-1)

	d := exporter.discoverDatabaseDSNs()
	c.Assert(d.ok, Equals, true, Commentf("discovery reported failure"))

	// Discovery keeps the DSN it was given and adds one per other database.
	c.Assert(len(d.targets), Equals, len(others)+1,
		Commentf("built %d targets from %d other databases: %v", len(d.targets), len(others), others))
	c.Assert(d.discovered, Equals, len(catalog),
		Commentf("discovery reported %d databases, the catalog has %d", d.discovered, len(catalog)))

	// Every target carries a name, including the seed. Without that, a
	// shortfall can be counted but not attributed.
	byName := map[string]bool{}
	for _, t := range d.targets {
		c.Check(t.datname, Not(Equals), "",
			Commentf("target %q has no database name", loggableDSN(t.dsn)))
		byName[t.datname] = true
	}
	c.Check(byName[seedName], Equals, true,
		Commentf("the configured database %q is not among the named targets", seedName))
	for _, name := range others {
		c.Check(byName[name], Equals, true, Commentf("%q was discovered but is not a target", name))
	}
}

// TestPerDatabaseCoverageAgainstServer checks that a real scrape publishes a
// named series per database it covered.
//
// The counts alone cannot say which database went dark, and this is the only
// test that shows the names reaching the output through the real discovery
// path rather than through targets a test built by hand.
func (s *IntegrationSuite) TestPerDatabaseCoverageAgainstServer(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	exporter := NewExporter(strings.Split(dsn, ",")[:1], AutoDiscoverDatabases(true))
	c.Assert(exporter, NotNil)

	families := gather(c, exporter)

	selected, ok := families["pg_exporter_database_selected"]
	c.Assert(ok, Equals, true, Commentf("no per-database selection was published"))
	c.Assert(len(selected.GetMetric()) > 1, Equals, true,
		Commentf("only %d database was selected, so the fan-out was not exercised", len(selected.GetMetric())))

	success, ok := families["pg_exporter_database_scrape_success"]
	c.Assert(ok, Equals, true, Commentf("no per-database outcome was published"))
	c.Assert(len(success.GetMetric()), Equals, len(selected.GetMetric()),
		Commentf("a selected database has no outcome, so coverage would be averaged away"))

	for _, m := range success.GetMetric() {
		c.Check(m.GetGauge().GetValue(), Equals, float64(1),
			Commentf("database reported as not covered: %v", m.GetLabel()))
	}

	// Every database on this server is reachable, so nothing should be
	// reported as failed or excluded.
	if errs, present := families["pg_exporter_database_scrape_error"]; present {
		c.Check(len(errs.GetMetric()), Equals, 0,
			Commentf("failures reported against a healthy server: %v", errs.GetMetric()))
	}
	counts := map[string]float64{}
	for _, name := range []string{"pg_exporter_databases_selected", "pg_exporter_databases_scraped",
		"pg_exporter_databases_failed", "pg_exporter_databases_excluded"} {
		f, present := families[name]
		c.Assert(present, Equals, true, Commentf("%s was not published", name))
		counts[name] = f.GetMetric()[0].GetGauge().GetValue()
	}
	c.Check(counts["pg_exporter_databases_scraped"]+counts["pg_exporter_databases_failed"],
		Equals, counts["pg_exporter_databases_selected"],
		Commentf("the coverage invariant does not hold against a real server: %v", counts))
	c.Check(counts["pg_exporter_databases_selected"], Equals, float64(len(selected.GetMetric())),
		Commentf("the count and the named series disagree about how many databases were selected"))
	c.Check(counts["pg_exporter_databases_excluded"], Equals, float64(0))
}

// TestMetadataFailureIsNotReportedAsCoverage drives the real collection path
// against a database where version detection cannot work, and checks that the
// database is reported as not covered.
//
// This is the defect the per-database gauges would otherwise have made worse.
// GetServer builds a fresh Server for every scrape, so when checkMapVersions
// fails there is no previous query map to fall back on: the map is nil,
// queryNamespaceMappings iterates nothing and returns no errors, Scrape
// returns nil, and the exporter reports a database it never queried as covered.
// Custom query files load in that same step, so they do not run either.
//
// The failure is arranged by revoking EXECUTE on pg_catalog.version(), which
// is a per-database privilege, so only the fixture database is affected.
func (s *IntegrationSuite) TestMetadataFailureIsNotReportedAsCoverage(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const db = "vis_metadata_fail"
	const role = "vis_metadata"
	exec := func(server *Server, stmt string) error {
		_, err := server.db.Exec(stmt)
		return err
	}
	// Leave nothing behind for the cases that run after this one.
	cleanup := func() {
		_ = exec(admin, `DROP DATABASE IF EXISTS `+db+` WITH (FORCE)`)
		_ = exec(admin, `DROP ROLE IF EXISTS `+role)
	}
	cleanup()
	defer cleanup()

	c.Assert(exec(admin, `CREATE ROLE `+role+` LOGIN PASSWORD 'secret'`), IsNil)
	c.Assert(exec(admin, `GRANT pg_monitor TO `+role), IsNil)
	c.Assert(exec(admin, `CREATE DATABASE `+db), IsNil)

	target, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], db))
	c.Assert(err, IsNil)
	defer target.Close()
	c.Assert(exec(target, `REVOKE EXECUTE ON FUNCTION pg_catalog.version() FROM PUBLIC`), IsNil)
	c.Assert(exec(target, `REVOKE EXECUTE ON FUNCTION pg_catalog.version() FROM `+role), IsNil)

	// The version cache is keyed by host:port, not by database, so a version
	// another case already looked up on this server would be served from it
	// and this database would never attempt the call that is revoked.
	versioncache.Clear()
	defer versioncache.Clear()

	exporter := NewExporter(
		[]string{"postgresql://" + role + ":secret@" + hostPort(c, dsn) + "/" + db + "?sslmode=disable"},
		AutoDiscoverDatabases(true),
		// Keep the fan-out to the one database whose metadata is broken, so
		// the counts below are unambiguous.
		IncludeDatabases(db),
	)
	families := gather(c, exporter)

	count := func(name string) float64 {
		f, ok := families[name]
		c.Assert(ok, Equals, true, Commentf("%s was not published", name))
		return f.GetMetric()[0].GetGauge().GetValue()
	}
	c.Check(count("pg_exporter_databases_selected"), Equals, float64(1))
	c.Check(count("pg_exporter_databases_scraped"), Equals, float64(0),
		Commentf("a database whose query maps could not be prepared was reported as covered"))
	c.Check(count("pg_exporter_databases_failed"), Equals, float64(1))

	success, ok := families["pg_exporter_database_scrape_success"]
	c.Assert(ok, Equals, true)
	c.Assert(len(success.GetMetric()), Equals, 1)
	c.Check(success.GetMetric()[0].GetGauge().GetValue(), Equals, float64(0),
		Commentf("per-database status claims coverage of a database that was never queried"))

	errs, ok := families["pg_exporter_database_scrape_error"]
	c.Assert(ok, Equals, true, Commentf("the failure was counted but not named"))
	c.Assert(len(errs.GetMetric()), Equals, 1)
	labels := map[string]string{}
	for _, l := range errs.GetMetric()[0].GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	c.Check(labels["datname"], Equals, db)
	// The connection worked, so this is a collection failure. Calling it a
	// connection failure would change what pg_up means.
	c.Check(labels["reason"], Equals, reasonScrape)
	c.Check(count("pg_up"), Equals, float64(1),
		Commentf("the connection succeeded, so pg_up must not report otherwise"))
}

// rewriteDatabase points a DSN at another database on the same server.
func rewriteDatabase(c *C, dsn, database string) string {
	u, err := url.Parse(dsn)
	c.Assert(err, IsNil)
	u.Path = "/" + database
	return u.String()
}

// hostPort returns the host and port of a DSN, for building another one.
func hostPort(c *C, dsn string) string {
	u, err := url.Parse(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	return u.Host
}

// TestCollectorFanOutReachesOtherDatabases is the point of the fan-out.
//
// pg_stat_user_tables shows the tables of the database the connection is
// attached to and nothing else, so before the fan-out the namespaced
// collectors reported the configured database's tables and said nothing at all
// about any other database on the server. A server with a hundred databases
// looked like a server with one.
//
// The test seeds a table in a database the DSN does not point at, and requires
// it to appear in the scrape.
func (s *IntegrationSuite) TestCollectorFanOutReachesOtherDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const other = "fanout_other"
	cleanup := func() { _, _ = admin.db.Exec(`DROP DATABASE IF EXISTS ` + other + ` WITH (FORCE)`) }
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE DATABASE ` + other)
	c.Assert(err, IsNil)

	seed, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], other))
	c.Assert(err, IsNil)
	defer seed.Close()
	_, err = seed.db.Exec(`CREATE TABLE fanout_marker (id int)`)
	c.Assert(err, IsNil)
	// pg_stat_user_tables only shows a relation once statistics exist for it.
	_, err = seed.db.Exec(`SELECT count(*) FROM fanout_marker`)
	c.Assert(err, IsNil)

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	// The seeded table, in a database the DSN does not name.
	found := false
	var seenDatabases []string
	for name, f := range families {
		if !strings.HasPrefix(name, "pg_stat_user_tables_") {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			seenDatabases = append(seenDatabases, labels["datname"])
			if labels["datname"] == other && labels["relname"] == "fanout_marker" {
				found = true
			}
		}
	}
	c.Check(found, Equals, true,
		Commentf("a table in %q did not reach the scrape; databases seen: %v", other, uniq(seenDatabases)))

	// The engine reports its own coverage, separately from the legacy engine.
	for _, name := range []string{
		"pg_collector_databases_discovered",
		"pg_collector_databases_selected",
		"pg_collector_databases_excluded",
		"pg_collector_database_discovery_success",
	} {
		_, ok := families[name]
		c.Check(ok, Equals, true, Commentf("%s was not published", name))
	}
	c.Check(families["pg_collector_database_discovery_success"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(1))
	selected := families["pg_collector_databases_selected"].GetMetric()[0].GetGauge().GetValue()
	c.Check(selected > 1, Equals, true,
		Commentf("the collectors selected %v databases, so the fan-out was not exercised", selected))

	up, ok := families["pg_collector_database_up"]
	c.Assert(ok, Equals, true, Commentf("no per-database connection status was published"))
	c.Check(len(up.GetMetric()), Equals, int(selected),
		Commentf("a selected database has no connection status"))
	for _, m := range up.GetMetric() {
		c.Check(m.GetGauge().GetValue(), Equals, float64(1),
			Commentf("database reported unreachable: %v", m.GetLabel()))
	}
}

// TestCollectorSelectionExcludesDatabases checks that the collectors' own
// selection is applied, and that an excluded database is reported as a
// decision rather than as a fault.
func (s *IntegrationSuite) TestCollectorSelectionExcludesDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()
	catalog, err := queryDatabases(context.Background(), server)
	c.Assert(err, IsNil)
	others := otherThanCurrent(catalog)
	c.Assert(len(others) > 0, Equals, true,
		Commentf("the fixture server has one database, so exclusion cannot be exercised"))
	excluded := others[0]

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
		collector.WithDatabaseSelection(dbselect.Of([]string{excluded}), dbselect.Absent()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	c.Check(families["pg_collector_databases_excluded"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(1))

	named, ok := families["pg_collector_database_excluded"]
	c.Assert(ok, Equals, true, Commentf("the excluded database was counted but not named"))
	c.Assert(len(named.GetMetric()), Equals, 1)
	labels := map[string]string{}
	for _, l := range named.GetMetric()[0].GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	c.Check(labels["datname"], Equals, excluded)
	c.Check(labels["reason"], Equals, dbselect.ReasonExcludeList)

	// An excluded database is not connected to and not reported as a failure.
	for _, m := range families["pg_collector_database_up"].GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "datname" {
				c.Check(l.GetValue(), Not(Equals), excluded,
					Commentf("an excluded database was connected to anyway"))
			}
		}
	}
}

func uniq(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// TestCollectorDiscoveryFailureKeepsExclusion is the first of three
// regressions for defects found in review of the fan-out.
//
// When the catalog could not be read, the control database became a target
// without ever being put through the selection rule. An operator who had
// excluded it got its tables collected anyway, and only on the scrape where
// something else had already gone wrong.
//
// SELECT on pg_database is revoked from the monitoring role, which is a
// per-database privilege, so discovery fails while the connection is healthy.
func (s *IntegrationSuite) TestCollectorDiscoveryFailureKeepsExclusion(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const role = "nodiscovery"
	const marker = "discovery_failure_marker"
	control, hasControl := config.DatabaseFromDSN(strings.Split(dsn, ",")[0])
	c.Assert(hasControl, Equals, true)

	cleanup := func() {
		_, _ = admin.db.Exec(`DROP TABLE IF EXISTS ` + marker)
		_, _ = admin.db.Exec(`GRANT SELECT ON pg_catalog.pg_database TO PUBLIC`)
		_, _ = admin.db.Exec(`DROP OWNED BY ` + role)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()

	_, err = admin.db.Exec(`CREATE TABLE ` + marker + ` (id int)`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`SELECT count(*) FROM ` + marker)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`REVOKE SELECT ON pg_catalog.pg_database FROM PUBLIC`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`REVOKE SELECT ON pg_catalog.pg_database FROM ` + role)
	c.Assert(err, IsNil)

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		"postgresql://"+role+":secret@"+hostPort(c, dsn)+"/"+control+"?sslmode=disable",
		[]string{},
		collector.WithContext(context.Background()),
		collector.WithDatabaseSelection(dbselect.Of([]string{control}), dbselect.Absent()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	c.Assert(families["pg_collector_database_discovery_success"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0), Commentf("discovery was supposed to fail in this fixture"))

	// The exclusion still applies.
	c.Check(families["pg_collector_databases_selected"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0),
		Commentf("an excluded database was selected because discovery failed"))
	for name, f := range families {
		if !strings.HasPrefix(name, "pg_stat_user_tables_") {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() != "relname" {
					continue
				}
				c.Check(l.GetValue(), Not(Equals), marker,
					Commentf("a table in the excluded database reached the scrape through %s", name))
			}
		}
	}

	// A discovery failure publishes no discovered total, because there is no
	// total to report and the one it would invent is 1.
	_, present := families["pg_collector_databases_discovered"]
	c.Check(present, Equals, false,
		Commentf("a database count was published although the catalog could not be read"))

	// Cluster-wide collection still works, which is the point of keeping the
	// control connection a target.
	success, ok := families["pg_scrape_collector_success"]
	c.Assert(ok, Equals, true)
	cluster := 0
	for _, m := range success.GetMetric() {
		if m.GetGauge().GetValue() == 1 {
			cluster++
		}
	}
	c.Check(cluster > 0, Equals, true,
		Commentf("no collector succeeded, so cluster-wide collection did not survive the discovery failure"))
}

// TestCollectorDatabaseUpRequiresAConnection is the second regression.
//
// instance.setup called sql.Open, which does not connect, and then asked the
// version cache, which answers from any database on the same server. So a
// database that refuses connections was published as up while every collector
// on it failed. CONNECTION LIMIT 0 is the cheapest way to refuse one.
func (s *IntegrationSuite) TestCollectorDatabaseUpRequiresAConnection(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const closed = "no_connections_allowed"
	const role = "limited"
	cleanup := func() {
		_, _ = admin.db.Exec(`DROP DATABASE IF EXISTS ` + closed + ` WITH (FORCE)`)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE DATABASE ` + closed + ` CONNECTION LIMIT 0`)
	c.Assert(err, IsNil)
	// A connection limit does not apply to superusers, so the scrape has to
	// run as an ordinary monitoring role for the database to refuse it.
	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)

	control, hasControl := config.DatabaseFromDSN(strings.Split(dsn, ",")[0])
	c.Assert(hasControl, Equals, true)
	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		"postgresql://"+role+":secret@"+hostPort(c, dsn)+"/"+control+"?sslmode=disable",
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	up, ok := families["pg_collector_database_up"]
	c.Assert(ok, Equals, true)
	found := false
	for _, m := range up.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "datname" && l.GetValue() == closed {
				found = true
				c.Check(m.GetGauge().GetValue(), Equals, float64(0),
					Commentf("a database that refuses connections was reported as up"))
			}
		}
	}
	c.Assert(found, Equals, true,
		Commentf("the unreachable database produced no connection status at all"))

	errs, ok := families["pg_collector_database_error"]
	c.Assert(ok, Equals, true, Commentf("the unreachable database was not named with a reason"))
	named := false
	for _, m := range errs.GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["datname"] == closed {
			named = true
			c.Check(labels["reason"], Equals, dbselect.ReasonConnect)
		}
	}
	c.Check(named, Equals, true)
}

// TestCollectorControlFailureIsReported is the third regression.
//
// With a control connection that cannot be opened, Collect returned before
// publishing anything, so the response carried no coverage and no failure. A
// scrape that collected nothing looked the same as one never configured to
// collect.
func (s *IntegrationSuite) TestCollectorControlFailureIsReported(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		"postgresql://nobody:wrong@"+hostPort(c, dsn)+"/postgres?sslmode=disable",
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	c.Assert(len(families) > 0, Equals, true,
		Commentf("a failed control connection produced an empty response"))
	c.Check(families["pg_collector_database_discovery_success"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0))
	c.Check(families["pg_collector_database_up"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0))

	errs, ok := families["pg_collector_database_error"]
	c.Assert(ok, Equals, true, Commentf("the failure was not named with a reason"))
	labels := map[string]string{}
	for _, l := range errs.GetMetric()[0].GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	c.Check(labels["reason"], Equals, dbselect.ReasonConnect)
	c.Check(labels["datname"], Equals, "postgres")

	// No discovered total, because discovery never ran.
	_, present := families["pg_collector_databases_discovered"]
	c.Check(present, Equals, false,
		Commentf("a database count was published although nothing was discovered"))
}

// TestCollectorExclusionWithAmbiguousDSN covers a connection string that names
// two different databases, in its path and in a dbname parameter.
//
// The exporter used to decide selection from the name it parsed out of the
// DSN, and it read the path first. lib/pq emits both as dbname keywords and
// sorts its output, and libpq takes the last occurrence, so the database
// actually connected to is whichever name sorts later. With discovery working
// the catalog corrects the name, so the mistake only surfaced when discovery
// had already failed: an excluded database was collected from, and its
// connection status was labelled with the database it was not connected to.
//
// The fixture makes both true at once. The DSN resolves to the database that
// sorts later, SELECT on pg_database is revoked so discovery fails, and that
// database is excluded.
func (s *IntegrationSuite) TestCollectorExclusionWithAmbiguousDSN(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const role = "ambiguous"
	const marker = "precedence_marker"
	// The fixture server has postgres and root. "root" sorts after "postgres",
	// so a DSN naming both resolves to root whichever half it comes from.
	const connected = "root"
	const other = "postgres"

	target, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], connected))
	c.Assert(err, IsNil)
	defer target.Close()

	// Catalog privileges are per database, and this fixture connects to
	// `connected`, so the revoke has to happen there rather than on the admin
	// connection. Revoking in the wrong database leaves discovery working and
	// the test passing for the wrong reason.
	cleanup := func() {
		_, _ = target.db.Exec(`GRANT SELECT ON pg_catalog.pg_database TO PUBLIC`)
		_, _ = target.db.Exec(`DROP TABLE IF EXISTS ` + marker)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()

	_, err = target.db.Exec(`CREATE TABLE ` + marker + ` (id int)`)
	c.Assert(err, IsNil)
	_, err = target.db.Exec(`SELECT count(*) FROM ` + marker)
	c.Assert(err, IsNil)

	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)
	_, err = target.db.Exec(`REVOKE SELECT ON pg_catalog.pg_database FROM PUBLIC`)
	c.Assert(err, IsNil)
	_, err = target.db.Exec(`REVOKE SELECT ON pg_catalog.pg_database FROM ` + role)
	c.Assert(err, IsNil)

	ambiguous := "postgresql://" + role + ":secret@" + hostPort(c, dsn) +
		"/" + other + "?dbname=" + connected + "&sslmode=disable"

	// The premise: this DSN really does connect to the database that is not in
	// its path. Without this the rest of the test could pass for the wrong
	// reason.
	probe, err := NewServer(ambiguous)
	c.Assert(err, IsNil)
	defer probe.Close()
	var reached string
	c.Assert(probe.db.QueryRow(`SELECT current_database()`).Scan(&reached), IsNil)
	c.Assert(reached, Equals, connected,
		Commentf("the fixture DSN does not reach %q, so it does not test what it claims", connected))

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		ambiguous,
		[]string{},
		collector.WithContext(context.Background()),
		collector.WithDatabaseSelection(dbselect.Of([]string{connected}), dbselect.Absent()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	c.Assert(families["pg_collector_database_discovery_success"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0), Commentf("discovery was supposed to fail in this fixture"))
	c.Check(families["pg_collector_databases_selected"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0),
		Commentf("the excluded database was selected, so the exclusion was applied to the wrong name"))
	c.Check(families["pg_collector_databases_excluded"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(1))

	// The connection status names the database that was actually connected to.
	up, ok := families["pg_collector_database_up"]
	c.Assert(ok, Equals, true)
	c.Assert(len(up.GetMetric()), Equals, 1)
	for _, l := range up.GetMetric()[0].GetLabel() {
		if l.GetName() == "datname" {
			c.Check(l.GetValue(), Equals, connected,
				Commentf("connection status is labelled with a database this scrape never connected to"))
		}
	}

	// And nothing from the excluded database reached the scrape.
	for name, f := range families {
		if !strings.HasPrefix(name, "pg_stat_user_tables_") {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() != "relname" {
					continue
				}
				c.Check(l.GetValue(), Not(Equals), marker,
					Commentf("a table in the excluded database reached the scrape through %s", name))
			}
		}
	}
}

// TestCollectorIncludeWithAmbiguousDSN is the same fixture read the other way.
// An include list naming the database that is actually connected to must
// select it, which it cannot do if the name came from the wrong half of the
// connection string.
func (s *IntegrationSuite) TestCollectorIncludeWithAmbiguousDSN(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const role = "ambiguous_include"
	const connected = "root"
	const other = "postgres"

	// Per-database privileges again: revoke in the database this DSN reaches.
	target, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], connected))
	c.Assert(err, IsNil)
	defer target.Close()

	cleanup := func() {
		_, _ = target.db.Exec(`GRANT SELECT ON pg_catalog.pg_database TO PUBLIC`)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()

	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)
	_, err = target.db.Exec(`REVOKE SELECT ON pg_catalog.pg_database FROM PUBLIC`)
	c.Assert(err, IsNil)
	_, err = target.db.Exec(`REVOKE SELECT ON pg_catalog.pg_database FROM ` + role)
	c.Assert(err, IsNil)

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		"postgresql://"+role+":secret@"+hostPort(c, dsn)+"/"+other+"?dbname="+connected+"&sslmode=disable",
		[]string{},
		collector.WithContext(context.Background()),
		collector.WithDatabaseSelection(dbselect.Absent(), dbselect.Of([]string{connected})),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	c.Check(families["pg_collector_databases_selected"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(1),
		Commentf("an include list naming the connected database did not select it"))
	c.Check(families["pg_collector_databases_excluded"].GetMetric()[0].GetGauge().GetValue(),
		Equals, float64(0))
}

// withCollector enables a collector that ships disabled, for the duration of
// one test, and restores the defaults afterwards.
//
// Collector enablement lives in package-level flag state, so this mutates
// something shared. gocheck runs the cases in a suite one at a time, and the
// restore is deferred, so the blast radius is this test.
func withCollector(c *C, name string) func() {
	_, err := kingpin.CommandLine.Parse([]string{"--collector." + name})
	c.Assert(err, IsNil)
	return func() {
		_, err := kingpin.CommandLine.Parse([]string{})
		c.Assert(err, IsNil)
	}
}

// TestIndexIOFansOutAcrossDatabases covers the port that made the index I/O
// collector fannable.
//
// pg_statio_user_indexes carried no datname label, so two databases each
// holding a public.accounts.accounts_pkey produced label sets identical in
// every respect. The registry rejects those as duplicates, which is why the
// collector ran on the configured connection alone and reported nothing about
// any other database.
func (s *IntegrationSuite) TestIndexIOFansOutAcrossDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	defer withCollector(c, "statio_user_indexes")()

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const other = "index_io_other"
	const table = "index_io_marker"
	cleanup := func() {
		_, _ = admin.db.Exec(`DROP DATABASE IF EXISTS ` + other + ` WITH (FORCE)`)
		_, _ = admin.db.Exec(`DROP TABLE IF EXISTS ` + table)
	}
	cleanup()
	defer cleanup()

	// The same table and index name in two databases, which is the collision
	// the label exists to resolve.
	_, err = admin.db.Exec(`CREATE TABLE ` + table + ` (id int PRIMARY KEY)`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`CREATE DATABASE ` + other)
	c.Assert(err, IsNil)
	seed, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], other))
	c.Assert(err, IsNil)
	defer seed.Close()
	_, err = seed.db.Exec(`CREATE TABLE ` + table + ` (id int PRIMARY KEY)`)
	c.Assert(err, IsNil)
	for _, server := range []*Server{admin, seed} {
		// Touch the index so statistics exist for it.
		_, err = server.db.Exec(`SELECT count(*) FROM ` + table + ` WHERE id = 1`)
		c.Assert(err, IsNil)
	}

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	c.Assert(pc.Collectors["statio_user_indexes"], NotNil,
		Commentf("the collector under test is not enabled"))

	families := gather(c, pc)
	f, ok := families["pg_statio_user_indexes_idx_blks_read_total"]
	c.Assert(ok, Equals, true, Commentf("the index I/O collector produced nothing"))

	databases := map[string]bool{}
	for _, m := range f.GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		c.Check(labels["datname"], Not(Equals), "",
			Commentf("an index I/O sample does not say which database it describes"))
		if labels["relname"] == table {
			databases[labels["datname"]] = true
		}
	}
	c.Check(databases[other], Equals, true,
		Commentf("the index in %q did not reach the scrape; saw %v", other, databases))
	c.Check(len(databases), Equals, 2,
		Commentf("the same index name in two databases produced %d series, want 2: %v", len(databases), databases))
}

// TestIndexStatsCollectorAgainstServer covers the ported index statistics
// collector running against a real server, in a database the DSN does not
// point at.
//
// The collector reproduces the body of pg_stat_user_indexes rather than
// selecting from it, so that the index's relpages and reltuples come off a
// join the view already performs. That makes it worth running against each
// supported version rather than trusting sqlmock, which cannot tell whether
// the query is valid SQL on PostgreSQL 15 or 19.
func (s *IntegrationSuite) TestIndexStatsCollectorAgainstServer(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const other = "index_stats_other"
	const table = "index_stats_marker"
	cleanup := func() { _, _ = admin.db.Exec(`DROP DATABASE IF EXISTS ` + other + ` WITH (FORCE)`) }
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE DATABASE ` + other)
	c.Assert(err, IsNil)

	seed, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], other))
	c.Assert(err, IsNil)
	defer seed.Close()
	_, err = seed.db.Exec(`CREATE TABLE ` + table + ` (id int PRIMARY KEY)`)
	c.Assert(err, IsNil)
	_, err = seed.db.Exec(`INSERT INTO ` + table + ` SELECT generate_series(1, 10)`)
	c.Assert(err, IsNil)
	_, err = seed.db.Exec(`SELECT count(*) FROM ` + table + ` WHERE id = 1`)
	c.Assert(err, IsNil)

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	c.Assert(pc.Collectors["stat_user_indexes"], NotNil,
		Commentf("the index statistics collector is not enabled by default"))

	families := gather(c, pc)
	for _, name := range []string{
		"pg_stat_user_indexes_idx_scan",
		"pg_stat_user_indexes_idx_tup_read",
		"pg_stat_user_indexes_idx_tup_fetch",
		"pg_stat_user_indexes_relpages",
		"pg_stat_user_indexes_reltuples",
	} {
		f, ok := families[name]
		c.Assert(ok, Equals, true, Commentf("%s was not produced", name))
		found := false
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			c.Check(labels["datname"], Not(Equals), "",
				Commentf("%s does not say which database it describes", name))
			if labels["datname"] == other && labels["relname"] == table {
				found = true
			}
		}
		c.Check(found, Equals, true,
			Commentf("%s carries nothing for the index seeded in %q", name, other))
	}

	// The collector reports every collector status it owns, and none failed.
	for _, m := range families["pg_scrape_collector_success"].GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["collector"] == "stat_user_indexes" {
			c.Check(m.GetGauge().GetValue(), Equals, float64(1),
				Commentf("the index statistics collector failed against %q", labels["datname"]))
		}
	}
}

// TestExtensionsFanOutAcrossDatabases is the last of the two collectors that
// could not fan out.
//
// pg_extension is a per-database catalog, and pg_available_extensions reports
// its installed_version from the connection's own database, so both describe
// one database while neither said which. Two databases produced identical
// label sets, so the collector ran on the configured connection alone.
//
// An extension installed in one database and not another is what makes the
// difference visible.
func (s *IntegrationSuite) TestExtensionsFanOutAcrossDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const other = "extensions_other"
	// pg_trgm ships with the server and is not installed anywhere by default.
	const ext = "pg_trgm"
	cleanup := func() { _, _ = admin.db.Exec(`DROP DATABASE IF EXISTS ` + other + ` WITH (FORCE)`) }
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE DATABASE ` + other)
	c.Assert(err, IsNil)

	seed, err := NewServer(rewriteDatabase(c, strings.Split(dsn, ",")[0], other))
	c.Assert(err, IsNil)
	defer seed.Close()
	_, err = seed.db.Exec(`CREATE EXTENSION ` + ext)
	c.Assert(err, IsNil)

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)

	families := gather(c, pc)
	installed, ok := families["pg_extensions"]
	c.Assert(ok, Equals, true, Commentf("the extensions collector produced nothing"))

	seen := map[string]map[string]bool{}
	for _, m := range installed.GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		c.Check(labels["datname"], Not(Equals), "",
			Commentf("an installed-extension sample does not say which database it describes"))
		if seen[labels["datname"]] == nil {
			seen[labels["datname"]] = map[string]bool{}
		}
		seen[labels["datname"]][labels["name"]] = true
	}

	c.Check(len(seen) > 1, Equals, true,
		Commentf("extensions were reported for %d database, so the collector did not fan out", len(seen)))
	c.Check(seen[other][ext], Equals, true,
		Commentf("%s is installed in %q but was not reported there: %v", ext, other, seen))

	// The point of the label: the same extension list is not reported for
	// every database. plpgsql is in both, pg_trgm is in one.
	control, hasControl := config.DatabaseFromDSN(strings.Split(dsn, ",")[0])
	c.Assert(hasControl, Equals, true)
	c.Check(seen[control][ext], Equals, false,
		Commentf("%s is not installed in %q but was reported there", ext, control))
	c.Check(seen[control]["plpgsql"], Equals, true)
	c.Check(seen[other]["plpgsql"], Equals, true)

	// pg_available_extensions is reported per database too, because its
	// installed_version differs between them.
	available, ok := families["pg_available_extensions"]
	c.Assert(ok, Equals, true)
	installedVersions := map[string]string{}
	for _, m := range available.GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["name"] == ext {
			installedVersions[labels["datname"]] = labels["installed_version"]
		}
	}
	c.Check(installedVersions[other], Not(Equals), "",
		Commentf("%s reports no installed version in the database that has it", ext))
	c.Check(installedVersions[control], Equals, "",
		Commentf("%s reports an installed version in a database that does not have it", ext))
}

// TestApplicationQueryDatabaseSelection drives the per-query databases rule
// against a real server, which is where the contract says it has to be
// proved rather than at parse time.
//
// Four queries in one file, against a server with more than one database:
// one with no list, one naming a single database, one with an explicit empty
// list, and one with master: true and a list that does not contain the
// configured database.
func (s *IntegrationSuite) TestApplicationQueryDatabaseSelection(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()
	catalog, err := queryDatabases(context.Background(), server)
	c.Assert(err, IsNil)
	control, found := currentName(catalog)
	c.Assert(found, Equals, true)
	others := otherThanCurrent(catalog)
	c.Assert(len(others) > 0, Equals, true,
		Commentf("the fixture server has one database, so per-query selection cannot be exercised"))
	other := others[0]

	const fixture = `
app_everywhere:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  metrics:
    - datname: {usage: "LABEL", description: "Database"}
    - value: {usage: "GAUGE", description: "Constant one"}
app_one_database:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  databases:
    - %s
  metrics:
    - datname: {usage: "LABEL", description: "Database"}
    - value: {usage: "GAUGE", description: "Constant one"}
app_no_databases:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  databases: []
  metrics:
    - datname: {usage: "LABEL", description: "Database"}
    - value: {usage: "GAUGE", description: "Constant one"}
app_master_only:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  master: true
  databases:
    - %s
  metrics:
    - datname: {usage: "LABEL", description: "Database"}
    - value: {usage: "GAUGE", description: "Constant one"}
`
	// The master query's list names a database that is not the configured
	// one, so a list that could suppress or redirect a master query would
	// show up here.
	dir := writeQueryFile(c, "app.yaml", fmt.Sprintf(fixture, other, other))

	exporter := NewExporter(
		strings.Split(dsn, ",")[:1],
		AutoDiscoverDatabases(true),
		WithUserQueriesEnabled(LR),
		WithUserQueriesPath(map[MetricResolution]string{LR: dir}),
	)
	c.Assert(exporter, NotNil)
	families := gather(c, exporter)

	where := func(metric string) []string {
		f, ok := families[metric]
		if !ok {
			return nil
		}
		var names []string
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "datname" {
					names = append(names, l.GetValue())
				}
			}
		}
		sort.Strings(names)
		return names
	}

	// An omitted list means every database the engine reaches.
	everywhere := where("app_everywhere_value")
	c.Check(len(everywhere), Equals, len(catalog),
		Commentf("a query with no databases list ran in %v, the server has %d connectable databases", everywhere, len(catalog)))

	// A list of one means that one.
	c.Check(where("app_one_database_value"), DeepEquals, []string{other},
		Commentf("a query naming %q ran somewhere else", other))

	// An explicit empty list means nowhere, which is the opposite of omitted
	// and the distinction a plain slice cannot carry.
	c.Check(where("app_no_databases_value"), IsNil,
		Commentf("a query with databases: [] ran anyway"))

	// master: true means the configured connection database and only it, even
	// though the list names a different one.
	c.Check(where("app_master_only_value"), DeepEquals, []string{control},
		Commentf("a master query did not run on the configured database alone"))
}

// TestApplicationQueryDatabasesWithoutDiscovery covers the case the name
// resolution exists for.
//
// With discovery on, every target already carries a database name and the
// per-query list can be applied straight away. With discovery off there is one
// target and no name, so a list would match nothing and an application query
// restricted to the database it is actually pointed at would silently never
// run. The exporter asks the server instead, once, and only when some loaded
// query has a list to apply.
func (s *IntegrationSuite) TestApplicationQueryDatabasesWithoutDiscovery(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()
	var control string
	c.Assert(server.db.QueryRow(`SELECT current_database()`).Scan(&control), IsNil)

	const fixture = `
app_here:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  databases:
    - %s
  metrics:
    - datname: {usage: "LABEL", description: "Database"}
    - value: {usage: "GAUGE", description: "Constant one"}
app_elsewhere:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  databases:
    - a_database_that_does_not_exist
  metrics:
    - datname: {usage: "LABEL", description: "Database"}
    - value: {usage: "GAUGE", description: "Constant one"}
`
	dir := writeQueryFile(c, "app.yaml", fmt.Sprintf(fixture, control))

	// Discovery deliberately off: one target, and no name from the catalog.
	exporter := NewExporter(
		strings.Split(dsn, ",")[:1],
		WithUserQueriesEnabled(LR),
		WithUserQueriesPath(map[MetricResolution]string{LR: dir}),
	)
	c.Assert(exporter, NotNil)
	families := gather(c, exporter)

	_, here := families["app_here_value"]
	c.Check(here, Equals, true,
		Commentf("a query naming the database it is pointed at did not run, so the name was never resolved"))
	_, elsewhere := families["app_elsewhere_value"]
	c.Check(elsewhere, Equals, false,
		Commentf("a query naming a database that does not exist ran anyway"))
}

// TestApplicationQueryNameLookupFailureIsNotSuccess is the regression for a
// defect in the first version of per-query database selection.
//
// When the database name could not be resolved, the scrape logged a warning
// and carried on with an empty name. Every query with a databases list then
// matched nothing, so none of them ran, while the scrape returned no error and
// coverage reported the database as covered. An unknown identity is not an
// intentional non-match, and the two must not produce the same output.
//
// EXECUTE on pg_catalog.current_database() is revoked for the role, which is a
// per-database privilege, so the connection, version detection and ordinary
// queries all keep working and only the name lookup fails.
//
// The connection string names no database, which is what leaves the name to be
// asked for. A string that names one is read instead of the server being
// asked, so the revoked privilege would never be reached. libpq then falls
// back to the role name, and a database of that name is created here so the
// fallback reaches a real one.
func (s *IntegrationSuite) TestApplicationQueryNameLookupFailureIsNotSuccess(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	// The role and the database it reaches by fallback share this name.
	const role = "noname"

	cleanup := func() {
		_, _ = admin.db.Exec(`DROP DATABASE IF EXISTS ` + role + ` WITH (FORCE)`)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()

	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`CREATE DATABASE ` + role)
	c.Assert(err, IsNil)

	// Revoked in the database the role connects to, which is the database
	// being created here and not the one this suite is configured with.
	// EXECUTE is a per-database privilege, so revoking it anywhere else
	// leaves the lookup working and the test proving nothing.
	targetDSN, err := config.DSNWithDatabase(strings.Split(dsn, ",")[0], role)
	c.Assert(err, IsNil)
	onTarget, err := NewServer(targetDSN)
	c.Assert(err, IsNil)
	defer onTarget.Close()
	_, err = onTarget.db.Exec(`REVOKE EXECUTE ON FUNCTION pg_catalog.current_database() FROM PUBLIC`)
	c.Assert(err, IsNil)
	_, err = onTarget.db.Exec(`REVOKE EXECUTE ON FUNCTION pg_catalog.current_database() FROM ` + role)
	c.Assert(err, IsNil)

	// The query names the database it is pointed at, so it would run if the
	// name could be resolved.
	fixture := fmt.Sprintf(`
app_lookup:
  query: "SELECT 1::float8 AS value"
  databases:
    - %s
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
`, role)
	dir := writeQueryFile(c, "app.yaml", fixture)

	// Discovery off, so the name is not supplied by the catalog and has to be
	// asked for.
	exporter := NewExporter(
		[]string{"postgresql://" + role + ":secret@" + hostPort(c, dsn) + "/?sslmode=disable"},
		WithUserQueriesEnabled(LR),
		WithUserQueriesPath(map[MetricResolution]string{LR: dir}),
		DisableDefaultMetrics(true),
		DisableSettingsMetrics(true),
	)
	c.Assert(exporter, NotNil)
	families := gather(c, exporter)

	count := func(name string) float64 {
		f, ok := families[name]
		c.Assert(ok, Equals, true, Commentf("%s was not published", name))
		return f.GetMetric()[0].GetGauge().GetValue()
	}
	c.Check(count("pg_exporter_databases_scraped"), Equals, float64(0),
		Commentf("the database was reported as covered although no restricted query could run"))
	c.Check(count("pg_exporter_databases_failed"), Equals, float64(1))

	_, ran := families["app_lookup_value"]
	c.Check(ran, Equals, false,
		Commentf("the fixture is wrong: the query ran, so the name lookup did not fail"))
}

// TestApplicationQueryWithoutListsNeedsNoName is the other half. A query file
// whose queries all decide where they run without a database name must not
// fail when that name cannot be resolved, and must not pay a round trip
// asking for it.
//
// Two queries, with the lookup revoked. One omits databases, so it runs
// everywhere. One writes an explicit empty list, so it runs nowhere. Neither
// decision refers to a name, so the scrape must succeed and report the
// database as covered.
//
// The empty list is the case that regressed. The lookup was demanded whenever
// a list was present at all, so a file saying that a query runs nowhere
// demanded a name on that query's behalf, and where the name could not be
// read the whole scrape failed for a query that was never going to run.
func (s *IntegrationSuite) TestApplicationQueryWithoutListsNeedsNoName(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const role = "noname_nolist"
	control, hasControl := config.DatabaseFromDSN(strings.Split(dsn, ",")[0])
	c.Assert(hasControl, Equals, true)

	cleanup := func() {
		_, _ = admin.db.Exec(`GRANT EXECUTE ON FUNCTION pg_catalog.current_database() TO PUBLIC`)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()

	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`REVOKE EXECUTE ON FUNCTION pg_catalog.current_database() FROM PUBLIC`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`REVOKE EXECUTE ON FUNCTION pg_catalog.current_database() FROM ` + role)
	c.Assert(err, IsNil)

	dir := writeQueryFile(c, "app.yaml", `
app_unrestricted:
  query: "SELECT 1::float8 AS value"
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
app_nowhere:
  query: "SELECT 1::float8 AS value"
  databases: []
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
`)

	exporter := NewExporter(
		[]string{"postgresql://" + role + ":secret@" + hostPort(c, dsn) + "/" + control + "?sslmode=disable"},
		WithUserQueriesEnabled(LR),
		WithUserQueriesPath(map[MetricResolution]string{LR: dir}),
		DisableDefaultMetrics(true),
		DisableSettingsMetrics(true),
	)
	families := gather(c, exporter)

	_, ran := families["app_unrestricted_value"]
	c.Check(ran, Equals, true,
		Commentf("an unrestricted query did not run, so the name lookup was demanded when nothing needed it"))

	_, ranNowhere := families["app_nowhere_value"]
	c.Check(ranNowhere, Equals, false,
		Commentf("a query with an explicit empty list ran somewhere"))

	scraped, ok := families["pg_exporter_databases_scraped"]
	c.Assert(ok, Equals, true, Commentf("pg_exporter_databases_scraped was not published"))
	c.Check(scraped.GetMetric()[0].GetGauge().GetValue(), Equals, float64(1),
		Commentf("the scrape failed although no query needed the database name"))
}

// TestApplicationQueryVersionRestriction covers per-query version gating,
// which was parsed and then ignored.
//
// runonserver was read from a Server field that nothing ever assigned, and
// compared against a version that was also never assigned, so every query in a
// file shared one restriction and that restriction was always empty. A query
// written for PostgreSQL 16 and later ran on 15, and one written for 13 only
// ran on 19.
//
// Three queries against the running server: one restricted to a range that
// includes it, one to a range that excludes it, and one unrestricted. The
// excluded one must not run while the other two do, which is also what shows
// the restriction belongs to the query rather than to the file.
func (s *IntegrationSuite) TestApplicationQueryVersionRestriction(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()
	var major int
	c.Assert(server.db.QueryRow(`SELECT current_setting('server_version_num')::int / 10000`).Scan(&major), IsNil)

	fixture := fmt.Sprintf(`
app_supported:
  query: "SELECT 1::float8 AS value"
  runonserver: ">=%d.0.0"
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
app_unsupported:
  query: "SELECT 1::float8 AS value"
  runonserver: ">=%d.0.0"
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
app_any_version:
  query: "SELECT 1::float8 AS value"
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
`, major, major+10)
	dir := writeQueryFile(c, "app.yaml", fixture)

	exporter := NewExporter(
		strings.Split(dsn, ",")[:1],
		WithUserQueriesEnabled(LR),
		WithUserQueriesPath(map[MetricResolution]string{LR: dir}),
		DisableDefaultMetrics(true),
		DisableSettingsMetrics(true),
	)
	families := gather(c, exporter)

	_, supported := families["app_supported_value"]
	c.Check(supported, Equals, true,
		Commentf("a query restricted to >=%d did not run on PostgreSQL %d", major, major))

	_, unsupported := families["app_unsupported_value"]
	c.Check(unsupported, Equals, false,
		Commentf("a query restricted to >=%d ran on PostgreSQL %d", major+10, major))

	_, any := families["app_any_version_value"]
	c.Check(any, Equals, true,
		Commentf("an unrestricted query did not run, so one query's restriction reached another"))
}

// TestTableCatalogMetricsAgainstServer checks the rewritten table collector
// against a real server, including its version gates.
//
// The collector reads pg_class directly instead of querying
// pg_stat_user_tables and joining back to it, and assembles its query per
// version because PostgreSQL resolves missing columns and functions at parse
// time. A query that is wrong for a version fails at parse, which sqlmock
// cannot see: it never parses anything.
func (s *IntegrationSuite) TestTableCatalogMetricsAgainstServer(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const table = "catalog_metrics_marker"
	cleanup := func() { _, _ = admin.db.Exec(`DROP TABLE IF EXISTS ` + table) }
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE TABLE ` + table + ` (id int PRIMARY KEY)`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`INSERT INTO ` + table + ` SELECT generate_series(1, 100)`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`VACUUM ANALYZE ` + table)
	c.Assert(err, IsNil)

	var major int
	c.Assert(admin.db.QueryRow(`SELECT current_setting('server_version_num')::int / 10000`).Scan(&major), IsNil)

	pc, err := collector.NewPostgresCollector(
		log.NewNopLogger(),
		[]string{},
		strings.Split(dsn, ",")[0],
		[]string{},
		collector.WithContext(context.Background()),
	)
	c.Assert(err, IsNil)
	families := gather(c, pc)

	carries := func(name string) bool {
		f, ok := families[name]
		if !ok {
			return false
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "relname" && l.GetValue() == table {
					return true
				}
			}
		}
		return false
	}

	// Present on every supported version.
	for _, name := range []string{
		"pg_stat_user_tables_n_ins_since_vacuum",
		"pg_stat_user_tables_relpages",
		"pg_stat_user_tables_reltuples",
		"pg_stat_user_tables_relallvisible",
		"pg_stat_user_tables_relfrozenxid_age",
		"pg_stat_user_tables_relminmxid_age",
	} {
		c.Check(carries(name), Equals, true,
			Commentf("%s carries nothing for the seeded table on PostgreSQL %d", name, major))
	}

	// Version gates. A gate that is wrong in the permissive direction fails
	// the query at parse time and takes the whole collector with it, which the
	// collector-success assertion below would catch; a gate wrong in the
	// restrictive direction silently drops a metric, which is what this
	// checks.
	c.Check(carries("pg_stat_user_tables_n_tup_newpage_upd"), Equals, major >= 16,
		Commentf("n_tup_newpage_upd on PostgreSQL %d", major))
	c.Check(carries("pg_stat_user_tables_relallfrozen"), Equals, major >= 18,
		Commentf("relallfrozen on PostgreSQL %d", major))

	for _, m := range families["pg_scrape_collector_success"].GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["collector"] == "stat_user_tables" {
			c.Check(m.GetGauge().GetValue(), Equals, float64(1),
				Commentf("the table collector failed against %q on PostgreSQL %d", labels["datname"], major))
		}
	}
}

// withQueryDirectory points one resolution's custom-query directory at a
// fixture and switches that resolution on, then puts both back.
//
// The handler reads these from package-level flag state rather than from
// arguments, so a test going through the handler has to set them there. Two
// pieces of state, not one: the directory, and the boolean that decides
// whether the exporter built from it does anything. Setting only the directory
// produces a registered exporter that collects nothing, an empty body and a
// scrape that looks instant, which is how the first version of the
// cancellation test below passed while measuring nothing at all.
func withQueryDirectory(c *C, res MetricResolution, dir string) func() {
	var path *string
	var enabled *bool
	switch res {
	case HR:
		path, enabled = collectCustomQueryHrDirectory, collectCustomQueryHr
	case MR:
		path, enabled = collectCustomQueryMrDirectory, collectCustomQueryMr
	case LR:
		path, enabled = collectCustomQueryLrDirectory, collectCustomQueryLr
	default:
		c.Fatalf("unknown resolution %v", res)
	}
	previousPath, previousEnabled := *path, *enabled
	*path, *enabled = dir, true
	return func() { *path, *enabled = previousPath, previousEnabled }
}

// scrapeHTTP serves one request through the real handler and returns the
// status, the body and how long it took.
func scrapeHTTP(dsn string, limits budget.Limits, header string, collect ...string) (int, string, time.Duration) {
	q := url.Values{}
	for _, v := range collect {
		q.Add("collect[]", v)
	}
	req := httptest.NewRequest("GET", "/metrics?"+q.Encode(), nil)
	if header != "" {
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", header)
	}
	rec := httptest.NewRecorder()

	h := Handler(log.NewNopLogger(), strings.Split(dsn, ","),
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{}, limits, NewCollectionGate(limits), querycache.New())

	start := time.Now()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String(), time.Since(start)
}

// TestRequestBudgetCancelsALongQuery is the check the earlier budget tests
// could not make.
//
// They held a connection open against a listener that never answered, which
// exercises connect_timeout and nothing else. A query that has already started
// on a real server is cancelled only if the collection context reaches it, and
// the legacy engine ran its queries through a context-free Query, so a
// two-second statement outran a one-second budget and the scrape returned
// after two seconds with a 200.
func (s *IntegrationSuite) TestRequestBudgetCancelsALongQuery(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	dir := writeQueryFile(c, "slow.yaml", `
slow_application_query:
  query: "SELECT 1::float8 AS value FROM pg_sleep(5)"
  metrics:
    - value: {usage: "GAUGE", description: "a query that takes five seconds"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	limits := budget.Defaults()
	limits.RequestTimeout = 30 * time.Second
	limits.MaxRequestTimeout = 60 * time.Second

	// The premise. Without this the rest measures a query that never ran: a
	// scrape that collects nothing is fast and produces no sample, which is
	// exactly what a cancelled one looks like.
	code, body, elapsed := scrapeHTTP(dsn, limits, "", "custom_query.lr")
	c.Assert(code, Equals, 200)
	c.Assert(strings.Contains(body, "slow_application_query_value"), Equals, true,
		Commentf("the slow query did not run with a thirty-second budget, so this test cannot show it being cancelled"))
	c.Assert(elapsed > 4*time.Second, Equals, true,
		Commentf("the slow query returned in %s, so it is not slow and nothing below is a cancellation", elapsed))

	for _, tc := range []struct {
		name   string
		header string
	}{
		// Whole seconds, which the old parser read.
		{"an integer budget", "1"},
		// A fraction, which the old parser threw away entirely, falling back
		// to ten seconds for a scraper that asked for a quarter of one.
		{"a fractional budget", "0.25"},
	} {
		code, body, elapsed := scrapeHTTP(dsn, limits, tc.header, "custom_query.lr")

		c.Check(code, Equals, 200, Commentf("%s", tc.name))
		c.Check(elapsed < 4*time.Second, Equals, true,
			Commentf("%s: the scrape took %s, so the five-second query was not cancelled", tc.name, elapsed))
		c.Check(strings.Contains(body, "slow_application_query_value"), Equals, false,
			Commentf("%s: a cancelled query produced a sample", tc.name))
	}
}

// TestInvalidTimeoutHeaderIsRefused covers the other half of the same defect.
//
// A header that cannot be read used to be ignored, so a scraper that sent
// something malformed silently got the configured fallback instead of the
// budget it meant. Refusing it is the only way the sender finds out.
func (s *IntegrationSuite) TestInvalidTimeoutHeaderIsRefused(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	for _, tc := range []struct{ name, header string }{
		{"not a number", "garbage"},
		{"empty-ish", " "},
		{"zero", "0"},
		{"negative", "-1"},
		{"not finite", "NaN"},
		{"infinite", "Inf"},
	} {
		code, _, _ := scrapeHTTP(dsn, budget.Defaults(), tc.header, "exporter")
		c.Check(code, Equals, 400,
			Commentf("%s: header %q returned %d, want 400", tc.name, tc.header, code))
	}

	// An absent header is not an error. Older agents send none and the
	// configured fallback exists for them.
	code, _, _ := scrapeHTTP(dsn, budget.Defaults(), "", "exporter")
	c.Check(code, Equals, 200, Commentf("a scrape with no timeout header was refused"))
}

// TestMetadataLookupObeysTheBudget covers the phase that ran before any
// collector and outside every deadline.
//
// Cancelling the statements a scrape issues does not help if the scrape has
// already spent its budget working out which version it is talking to. The
// version lookup, the distribution detection and the ping all ran without a
// context, so a slow answer at that phase held the request open however short
// its budget was.
//
// The version function is made slow by fault injection, the same way ChatGPT
// reproduced it: a public.version() that sleeps and then returns the real one,
// with the role's search_path putting public first. The lookup is unqualified,
// so it finds the slow one. This is not a claim that pg_catalog.version() is
// ever slow.
func (s *IntegrationSuite) TestMetadataLookupObeysTheBudget(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const role = "slow_version_role"
	control, hasControl := config.DatabaseFromDSN(strings.Split(dsn, ",")[0])
	c.Assert(hasControl, Equals, true)

	cleanup := func() {
		_, _ = admin.db.Exec(`DROP FUNCTION IF EXISTS public.version()`)
		_, _ = admin.db.Exec(`REASSIGN OWNED BY ` + role + ` TO CURRENT_USER`)
		_, _ = admin.db.Exec(`DROP OWNED BY ` + role)
		_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	cleanup()
	defer cleanup()

	_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`CREATE FUNCTION public.version() RETURNS text AS $$
		SELECT pg_sleep(5); SELECT pg_catalog.version();
	$$ LANGUAGE sql`)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`ALTER ROLE ` + role + ` SET search_path = public, pg_catalog`)
	c.Assert(err, IsNil)

	// Nothing may be cached from an earlier test, or the slow phase is
	// skipped and this measures the wrong thing.
	versioncache.Clear()
	defer versioncache.Clear()

	slowDSN := "postgresql://" + role + ":secret@" + hostPort(c, dsn) + "/" + control + "?sslmode=disable"
	limits := budget.Defaults()
	limits.RequestTimeout = 30 * time.Second

	// The premise: with a long budget the slow version function is reached
	// and the scrape does take five seconds. Without this the assertions
	// below would pass against a fixture where nothing was slow.
	code, _, elapsed := scrapeHTTP(slowDSN, limits, "", "exporter")
	c.Assert(code, Equals, 200)
	c.Assert(elapsed > 4*time.Second, Equals, true,
		Commentf("the injected slow version function was not reached: the scrape took %s", elapsed))

	versioncache.Clear()

	code, _, elapsed = scrapeHTTP(slowDSN, limits, "0.25", "exporter")
	c.Check(code, Equals, 200)
	c.Check(elapsed < 3*time.Second, Equals, true,
		Commentf("a quarter-second scrape spent %s in the version lookup", elapsed))
}

// TestCacheSecondsHoldsAcrossHTTPScrapes is the test the earlier cache work
// should have had.
//
// The unit tests reused one Server object, which is the one arrangement where
// a per-request cache appears to work. Through the handler nothing was cached
// at all: a fresh exporter per request builds a fresh server whose cache
// starts empty, so two consecutive scrapes of a query returning nextval
// returned 1 and 2 and both reported an age of zero.
//
// A sequence is used rather than a clock, because what has to be shown is
// that the query did not run again, and a sequence says so from the server's
// side.
func (s *IntegrationSuite) TestCacheSecondsHoldsAcrossHTTPScrapes(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const seq = "cache_probe_seq"
	cleanup := func() { _, _ = admin.db.Exec(`DROP SEQUENCE IF EXISTS ` + seq) }
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE SEQUENCE ` + seq)
	c.Assert(err, IsNil)

	const uncachedSeq = "uncached_probe_seq"
	cleanupUncached := func() { _, _ = admin.db.Exec(`DROP SEQUENCE IF EXISTS ` + uncachedSeq) }
	cleanupUncached()
	defer cleanupUncached()
	_, err = admin.db.Exec(`CREATE SEQUENCE ` + uncachedSeq)
	c.Assert(err, IsNil)

	// Two namespaces in one file: one cached, one not. The second is here to
	// hold the treatment of everything that does not ask to be cached, which
	// is every built-in namespace and most application queries.
	dir := writeQueryFile(c, "cached.yaml", `
cached_probe:
  query: "SELECT nextval('`+seq+`')::float8 AS value"
  cache_seconds: 300
  metrics:
    - value: {usage: "GAUGE", description: "a sequence, so a second run is visible"}
uncached_probe:
  query: "SELECT nextval('`+uncachedSeq+`')::float8 AS value"
  metrics:
    - value: {usage: "GAUGE", description: "a sequence, collected every scrape"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	// One cache for both requests, which is what main gives the handler.
	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), strings.Split(dsn, ","),
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	scrape := func() string {
		req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200)
		return rec.Body.String()
	}

	first := scrape()
	c.Assert(strings.Contains(first, "cached_probe_value"), Equals, true,
		Commentf("the query did not run at all, so nothing below is about caching"))
	firstValue, firstTimestamped := sampleValue(c, first, "cached_probe_value")
	// Stamped on the response that collected it as well as on the ones that
	// serve it. Left unstamped, this sample is recorded at the moment the
	// scraper read it and the next response carries the collection time, which
	// are two instants for one measurement.
	c.Check(firstTimestamped, Equals, true,
		Commentf("the response that collected a cacheable namespace carried no timestamp"))

	second := scrape()
	secondValue, secondTimestamped := sampleValue(c, second, "cached_probe_value")

	c.Check(sampleTimestamp(c, second, "cached_probe_value"),
		Equals, sampleTimestamp(c, first, "cached_probe_value"),
		Commentf("the two responses stamped one collection with different times, so it is stored as two points"))

	// What a namespace without cache_seconds does, which is what it did
	// before any of this: run every scrape, and carry no timestamp, so the
	// scraper records it when it reads it.
	firstUncached, uncachedTimestamped := sampleValue(c, first, "uncached_probe_value")
	c.Check(uncachedTimestamped, Equals, false,
		Commentf("a namespace that does not ask to be cached carried an explicit timestamp"))
	secondUncached, _ := sampleValue(c, second, "uncached_probe_value")
	c.Check(secondUncached > firstUncached, Equals, true,
		Commentf("a namespace without cache_seconds served %v twice, so it was cached anyway", firstUncached))

	c.Check(secondValue, Equals, firstValue,
		Commentf("the sequence advanced from %v to %v, so the query ran again instead of being served from the cache",
			firstValue, secondValue))

	// A cached sample carries the time it was collected, so the timestamp is
	// a second, independent signal that this came from the cache.
	c.Check(secondTimestamped, Equals, true,
		Commentf("the second sample carried no timestamp, so it was collected rather than served"))
	c.Check(strings.Contains(second, "pg_exporter_namespace_cache_age_seconds"), Equals, true,
		Commentf("no age was published for a cached namespace"))

	// Emptying the cache makes the query run again, which shows the first two
	// results came from the cache rather than from a query that happens to
	// return the same number twice.
	cache.Clear()
	third := scrape()
	thirdValue, _ := sampleValue(c, third, "cached_probe_value")
	c.Check(thirdValue > firstValue, Equals, true,
		Commentf("after clearing the cache the sequence did not advance, so the query is not what produces this value"))
}

// sampleValue reads one sample's value out of an exposition body, and says
// whether the sample carried an explicit timestamp.
//
// The value is the field after the name and labels, not the last field. A
// cached sample carries its collection time after the value, and reading the
// last field gave that timestamp instead: the first version of this test
// compared a sequence number against 1795433380488 and reported it as the
// query having run twice.
func sampleValue(c *C, body, family string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, family+"{") && !strings.HasPrefix(line, family+" ") {
			continue
		}
		fields := strings.Fields(line)
		c.Assert(len(fields) >= 2, Equals, true, Commentf("malformed sample %q", line))
		value, err := strconv.ParseFloat(fields[1], 64)
		c.Assert(err, IsNil)
		return value, len(fields) > 2
	}
	c.Fatalf("%s is not in the response", family)
	return 0, false
}

// sampleTimestamp reads the explicit timestamp a sample carries, in
// milliseconds, and returns zero when it carries none.
func sampleTimestamp(c *C, body, family string) int64 {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, family+"{") && !strings.HasPrefix(line, family+" ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return 0
		}
		ts, err := strconv.ParseInt(fields[2], 10, 64)
		c.Assert(err, IsNil)
		return ts
	}
	c.Fatalf("%s is not in the response", family)
	return 0
}

// labelValues reads one label from every sample of a family, sorted.
//
// Each where argument is text the sample's line has to contain, so a test can
// read one namespace's series out of a response that carries others.
func labelValues(body, family, label string, where ...string) []string {
	re := regexp.MustCompile(label + `="([^"]*)"`)
	var found []string
line:
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, family+"{") {
			continue
		}
		for _, want := range where {
			if !strings.Contains(line, want) {
				continue line
			}
		}
		if m := re.FindStringSubmatch(line); m != nil {
			found = append(found, m[1])
		}
	}
	sort.Strings(found)
	return found
}

// TestTwoDatabasesOnOneHostKeepSeparateCacheEntries covers what identifies a
// cached result.
//
// Two targets on one host and port are two measurements, and keyed by the host
// and port they were one. Measured before the fix: the first response carried
// both databases, and the second served one target's result for both, so the
// other database left exposition and the value published under its name was
// another database's.
//
// Discovery is off and no query restricts itself by database, which is the
// arrangement where nothing else supplies a name. An operator should not have
// to add a databases list to make this correct.
func (s *IntegrationSuite) TestTwoDatabasesOnOneHostKeepSeparateCacheEntries(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]

	admin, err := NewServer(configured)
	c.Assert(err, IsNil)
	defer admin.Close()
	catalog, err := queryDatabases(context.Background(), admin)
	c.Assert(err, IsNil)
	control, found := currentName(catalog)
	c.Assert(found, Equals, true)
	others := otherThanCurrent(catalog)
	c.Assert(len(others) > 0, Equals, true,
		Commentf("the fixture server has one database, so two targets on one host cannot be configured"))
	other := others[0]
	secondTarget, err := config.DSNWithDatabase(configured, other)
	c.Assert(err, IsNil)

	dir := writeQueryFile(c, "two.yaml", `
two_targets:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  cache_seconds: 300
  metrics:
    - datname: {usage: "LABEL", description: "The database this ran on"}
    - value: {usage: "GAUGE", description: "Constant one"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{configured, secondTarget},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	scrape := func() string {
		req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200)
		return rec.Body.String()
	}

	want := []string{control, other}
	sort.Strings(want)

	first := scrape()
	c.Assert(labelValues(first, "two_targets_value", "datname"), DeepEquals, want,
		Commentf("the first response carried %v, want both databases, so nothing below is about the cache",
			labelValues(first, "two_targets_value", "datname")))
	c.Assert(cache.Len(), Equals, 2,
		Commentf("two targets left %d cache entries, want one each", cache.Len()))

	served := scrape()
	c.Check(labelValues(served, "two_targets_value", "datname"), DeepEquals, want,
		Commentf("the second response carried %v, want both databases: one target was served the other's cached result",
			labelValues(served, "two_targets_value", "datname")))

	// The age series names the database it belongs to, so two targets do not
	// report one collection between them.
	c.Check(labelValues(served, "pg_exporter_namespace_cache_age_seconds", "datname"), DeepEquals, want,
		Commentf("the age series did not identify both databases"))

	// Both were served rather than collected again, which is what makes the
	// check above a check about the cache.
	ages := 0
	for _, line := range strings.Split(served, "\n") {
		if !strings.HasPrefix(line, "pg_exporter_namespace_cache_age_seconds{") {
			continue
		}
		ages++
		fields := strings.Fields(line)
		c.Assert(len(fields) >= 2, Equals, true, Commentf("malformed sample %q", line))
		age, err := strconv.ParseFloat(fields[1], 64)
		c.Assert(err, IsNil)
		c.Check(age > 0, Equals, true,
			Commentf("a namespace reported an age of %v on the second response, so it collected again rather than being served: %s", age, line))
	}
	c.Check(ages, Equals, 2, Commentf("%d age series were published for two targets", ages))
}

// TestTwoRolesOnOneDatabaseKeepSeparateCacheEntries covers the half of target
// identity that a database name cannot carry.
//
// Two connections to the same database as different roles are two targets, and
// everything the key held before identified neither of them apart: the host
// and port are the same and so is the database. Measured before the fix: the
// second response carried one role twice, so one target published the other's
// result under its own name.
//
// A monitoring role sees different rows from an application role, and an
// exporter configured with both is how an operator separates what each can
// read. Serving one's result for the other is not a stale answer, it is
// another target's answer.
func (s *IntegrationSuite) TestTwoRolesOnOneDatabaseKeepSeparateCacheEntries(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]
	parsed, err := url.Parse(configured)
	c.Assert(err, IsNil)
	c.Assert(parsed.Host, Not(Equals), "",
		Commentf("DATA_SOURCE_NAME is not a URL, so two roles cannot be pointed at one database here"))

	admin, err := NewServer(configured)
	c.Assert(err, IsNil)
	defer admin.Close()

	const password = "cache_identity_probe"
	roles := []string{"cache_identity_a", "cache_identity_b"}
	cleanup := func() {
		for _, role := range roles {
			_, _ = admin.db.Exec(`DROP ROLE IF EXISTS ` + role)
		}
	}
	cleanup()
	defer cleanup()
	for _, role := range roles {
		_, err = admin.db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD '` + password + `'`)
		c.Assert(err, IsNil)
		_, err = admin.db.Exec(`GRANT pg_monitor TO ` + role)
		c.Assert(err, IsNil)
	}

	asRole := func(role string) string {
		u := *parsed
		u.User = url.UserPassword(role, password)
		return u.String()
	}

	dir := writeQueryFile(c, "roles.yaml", `
role_targets:
  query: "SELECT current_user AS usename, 1::float8 AS value"
  cache_seconds: 300
  metrics:
    - usename: {usage: "LABEL", description: "The role this ran as"}
    - value: {usage: "GAUGE", description: "Constant one"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{asRole(roles[0]), asRole(roles[1])},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	scrape := func() string {
		req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200)
		return rec.Body.String()
	}

	first := scrape()
	c.Assert(labelValues(first, "role_targets_value", "usename"), DeepEquals, roles,
		Commentf("the first response carried %v, want both roles, so nothing below is about the cache",
			labelValues(first, "role_targets_value", "usename")))
	c.Assert(cache.Len(), Equals, 2,
		Commentf("two roles left %d cache entries, want one each", cache.Len()))

	served := scrape()
	c.Check(labelValues(served, "role_targets_value", "usename"), DeepEquals, roles,
		Commentf("the second response carried %v, want both roles: one target was served the other's cached result",
			labelValues(served, "role_targets_value", "usename")))

	// The diagnostic has to say which collection it describes. Labelled by
	// namespace and database alone, two roles against one database produced
	// one label set, the registry refused the second, and the two collections
	// reported one age between them.
	c.Check(labelValues(served, "pg_exporter_namespace_cache_age_seconds", "usename"), DeepEquals, roles,
		Commentf("the age series carried %v, want one per role",
			labelValues(served, "pg_exporter_namespace_cache_age_seconds", "usename")))
}

// TestTwoSearchPathsOnOneDatabaseAreTwoCollections covers the connection
// context that no name in the series can carry.
//
// Both targets are the same host, port, database and role. They differ in a
// search_path set through the connection string's options, so identical SQL
// reads a different table in each, and their collections have different
// values. Labelled by namespace, database and role alone, the two reported one
// age between them: the registry refused the second as a duplicate.
//
// ChatGPT measured this case against the role-labelled version, which is why
// the diagnostic carries the identity the cached result is keyed by.
func (s *IntegrationSuite) TestTwoSearchPathsOnOneDatabaseAreTwoCollections(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]
	parsed, err := url.Parse(configured)
	c.Assert(err, IsNil)
	c.Assert(parsed.Host, Not(Equals), "",
		Commentf("DATA_SOURCE_NAME is not a URL, so two search paths cannot be configured here"))

	admin, err := NewServer(configured)
	c.Assert(err, IsNil)
	defer admin.Close()

	schemas := []string{"cache_context_a", "cache_context_b"}
	values := map[string]float64{schemas[0]: 11, schemas[1]: 22}
	cleanup := func() {
		for _, schema := range schemas {
			_, _ = admin.db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		}
	}
	cleanup()
	defer cleanup()
	for _, schema := range schemas {
		_, err = admin.db.Exec(`CREATE SCHEMA ` + schema)
		c.Assert(err, IsNil)
		_, err = admin.db.Exec(`CREATE TABLE ` + schema + `.context_values (value double precision)`)
		c.Assert(err, IsNil)
		_, err = admin.db.Exec(fmt.Sprintf(`INSERT INTO %s.context_values VALUES (%v)`, schema, values[schema]))
		c.Assert(err, IsNil)
	}

	withSearchPath := func(schema string) string {
		u := *parsed
		q := u.Query()
		q.Set("options", "-csearch_path="+schema)
		u.RawQuery = q.Encode()
		return u.String()
	}

	// current_schema() reports where the unqualified table name resolved, so
	// the sample says which connection collected it.
	dir := writeQueryFile(c, "context.yaml", `
context_probe:
  query: "SELECT current_schema() AS context, value FROM context_values"
  cache_seconds: 300
  metrics:
    - context: {usage: "LABEL", description: "The schema this resolved in"}
    - value: {usage: "GAUGE", description: "A value only this schema has"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{withSearchPath(schemas[0]), withSearchPath(schemas[1])},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	scrape := func() string {
		req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200)
		return rec.Body.String()
	}

	first := scrape()
	c.Assert(labelValues(first, "context_probe_value", "context"), DeepEquals, schemas,
		Commentf("the first response carried %v, want one sample per search path",
			labelValues(first, "context_probe_value", "context")))
	c.Assert(cache.Len(), Equals, 2,
		Commentf("two search paths left %d cache entries, want one each", cache.Len()))

	served := scrape()
	c.Check(labelValues(served, "context_probe_value", "context"), DeepEquals, schemas,
		Commentf("the second response carried %v, want one sample per search path",
			labelValues(served, "context_probe_value", "context")))

	// Two collections, so two age series. The identities are this process's
	// and their values are not worth asserting, only that there are two of
	// them and that they differ.
	identities := labelValues(served, "pg_exporter_namespace_cache_age_seconds", "target")
	c.Check(len(identities), Equals, 2,
		Commentf("two collections published %d age series: %v", len(identities), identities))
	if len(identities) == 2 {
		c.Check(identities[0], Not(Equals), identities[1],
			Commentf("both age series carry the identity %q", identities[0]))
	}
}

// TestAnOmittedDatabaseIsResolvedForTheAgeSeries covers the other half of the
// per-database diagnostic.
//
// A connection string need not name a database. libpq then falls back to the
// role name, and the exporter knows the name only by asking. Without asking,
// the age series carried datname="" while the query running over the same
// connection reported the real one, so the diagnostic named no database for a
// collection that had one.
//
// The name is asked for because a namespace here asks to be cached. A file
// with no cache_seconds and no databases list still pays nothing.
func (s *IntegrationSuite) TestAnOmittedDatabaseIsResolvedForTheAgeSeries(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]
	parsed, err := url.Parse(configured)
	c.Assert(err, IsNil)
	c.Assert(parsed.Host, Not(Equals), "",
		Commentf("DATA_SOURCE_NAME is not a URL, so the database cannot be omitted from it here"))

	// No path and no dbname, which is a supported connection form.
	withoutDatabase := *parsed
	withoutDatabase.Path = "/"
	q := withoutDatabase.Query()
	q.Del("dbname")
	withoutDatabase.RawQuery = q.Encode()
	target := withoutDatabase.String()
	_, named := config.DatabaseFromDSN(target)
	c.Assert(named, Equals, false,
		Commentf("the connection string still names a database, so nothing has to be resolved"))

	dir := writeQueryFile(c, "fallback.yaml", `
fallback_probe:
  query: "SELECT current_database() AS datname, 1::float8 AS value"
  cache_seconds: 300
  metrics:
    - datname: {usage: "LABEL", description: "The database this ran on"}
    - value: {usage: "GAUGE", description: "Constant one"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{target},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), querycache.New())

	req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	c.Assert(rec.Code, Equals, 200)
	body := rec.Body.String()

	// What the server says it is, which is the answer the label has to match.
	fromQuery := labelValues(body, "fallback_probe_value", "datname")
	c.Assert(len(fromQuery), Equals, 1,
		Commentf("the query produced %v, want one sample", fromQuery))
	c.Assert(fromQuery[0], Not(Equals), "",
		Commentf("the query reported no database, so there is nothing to compare the label against"))

	c.Check(labelValues(body, "pg_exporter_namespace_cache_age_seconds", "datname"),
		DeepEquals, fromQuery,
		Commentf("the age series reported %v for a collection the query says ran on %v",
			labelValues(body, "pg_exporter_namespace_cache_age_seconds", "datname"), fromQuery))
}

// globalCollectorsForTest builds the process-wide collectors main builds, for
// the tests that ask for every collector.
func globalCollectorsForTest() map[string]prometheus.Collector {
	return map[string]prometheus.Collector{
		"standard.process": collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		"standard.go":      collectors.NewGoCollector(),
		"version":          vc.NewCollector(exporterName),
	}
}

// TestTwoResolutionsSharingANamespaceAreTwoCollections covers the last part of
// the cache key the diagnostic was missing.
//
// The resolutions are separate engines with separate query directories and
// separate intervals, and each can define the same namespace with its own
// columns. The cache has always kept them apart, because the resolution is
// part of the key. The age series did not, so two collections published one
// diagnostic and the registry refused the other.
//
// Both the selected form and plain /metrics are checked, because they build
// the engine set by different paths.
func (s *IntegrationSuite) TestTwoResolutionsSharingANamespaceAreTwoCollections(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]

	// One namespace, defined twice, with a different column each time so the
	// application samples are two families rather than one duplicated series.
	hrDir := writeQueryFile(c, "hr.yaml", `
resolution_probe:
  query: "SELECT 11::float8 AS high"
  cache_seconds: 60
  metrics:
    - high: {usage: "GAUGE", description: "The high resolution's own column"}
`)
	lrDir := writeQueryFile(c, "lr.yaml", `
resolution_probe:
  query: "SELECT 22::float8 AS low"
  cache_seconds: 60
  metrics:
    - low: {usage: "GAUGE", description: "The low resolution's own column"}
`)
	restoreHR := withQueryDirectory(c, HR, hrDir)
	defer restoreHR()
	restoreLR := withQueryDirectory(c, LR, lrDir)
	defer restoreLR()

	cache := querycache.New()
	limits := budget.Defaults()
	// The collectors main supplies, because one of the requests below is a
	// plain /metrics, which asks for all of them. An empty map here would
	// make the fixture a test of what happens when a selected collector is
	// absent, which is not what this is about.
	h := Handler(log.NewNopLogger(), []string{configured},
		semaphore.NewWeighted(limits.Connections), globalCollectorsForTest(),
		limits, NewCollectionGate(limits), cache)

	scrape := func(query string) string {
		req := httptest.NewRequest("GET", "/metrics"+query, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200, Commentf("GET /metrics%s", query))
		return rec.Body.String()
	}

	want := []string{"custom_query.hr", "custom_query.lr"}
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"both resolutions selected", "?collect[]=custom_query.hr&collect[]=custom_query.lr"},
		{"plain /metrics", ""},
	} {
		body := scrape(tc.query)

		// The premise: both engines ran and each published its own column.
		// sampleValue fails when the family is absent, so this is presence as
		// well as value.
		high, _ := sampleValue(c, body, "resolution_probe_high")
		c.Assert(high, Equals, float64(11), Commentf("%s: the high resolution served %v", tc.name, high))
		low, _ := sampleValue(c, body, "resolution_probe_low")
		c.Assert(low, Equals, float64(22), Commentf("%s: the low resolution served %v", tc.name, low))

		got := labelValues(body, "pg_exporter_namespace_cache_age_seconds", "collector",
			`namespace="resolution_probe"`)
		c.Check(got, DeepEquals, want,
			Commentf("%s: the age series for one namespace carried %v, want one per resolution", tc.name, got))
	}
}

// TestASlowCollectionDoesNotReplaceANewerOne covers overlapping refreshes
// through the handler, which is where they happen: two scrapes of one endpoint
// overlap, and a mutex or the race detector says nothing about which
// observation is kept.
//
// The query takes a sequence value and then sleeps on its first call, so the
// request that started first finishes last holding the older observation.
// Measured before the fix: the fast request observed 2, the slow one observed
// 1, and every scrape for the next five minutes served 1.
func (s *IntegrationSuite) TestASlowCollectionDoesNotReplaceANewerOne(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]

	admin, err := NewServer(configured)
	c.Assert(err, IsNil)
	defer admin.Close()

	const seq = "cache_overlap_seq"
	const fn = "cache_overlap_probe"
	cleanup := func() {
		_, _ = admin.db.Exec(`DROP FUNCTION IF EXISTS ` + fn + `()`)
		_, _ = admin.db.Exec(`DROP SEQUENCE IF EXISTS ` + seq)
	}
	cleanup()
	defer cleanup()
	_, err = admin.db.Exec(`CREATE SEQUENCE ` + seq)
	c.Assert(err, IsNil)
	_, err = admin.db.Exec(`CREATE FUNCTION ` + fn + `() RETURNS double precision AS $$
DECLARE taken bigint;
BEGIN
	taken := nextval('` + seq + `');
	IF taken = 1 THEN PERFORM pg_sleep(1.5); END IF;
	RETURN taken;
END $$ LANGUAGE plpgsql`)
	c.Assert(err, IsNil)

	dir := writeQueryFile(c, "overlap.yaml", `
overlap_probe:
  query: "SELECT `+fn+`() AS value"
  cache_seconds: 300
  metrics:
    - value: {usage: "GAUGE", description: "a sequence, so each collection is distinguishable"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{configured},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	scrape := func() string {
		req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200)
		return rec.Body.String()
	}

	type finished struct {
		body string
		at   time.Time
	}
	slow := make(chan finished, 1)
	go func() {
		body := scrape()
		slow <- finished{body, time.Now()}
	}()

	// Wait until the slow query has taken its sequence value. Starting the
	// second request on a timer instead would make the overlap a guess.
	taken := func() int64 {
		var last int64
		var called bool
		c.Assert(admin.db.QueryRow(`SELECT last_value, is_called FROM `+seq).Scan(&last, &called), IsNil)
		if !called {
			return 0
		}
		return last
	}
	deadline := time.Now().Add(10 * time.Second)
	for taken() < 1 {
		c.Assert(time.Now().Before(deadline), Equals, true,
			Commentf("the first request never reached the query, so there is no overlap to test"))
		time.Sleep(5 * time.Millisecond)
	}

	fastBody := scrape()
	fastAt := time.Now()
	slowResult := <-slow

	fastValue, _ := sampleValue(c, fastBody, "overlap_probe_value")
	slowValue, _ := sampleValue(c, slowResult.body, "overlap_probe_value")

	// The run has to have the shape the test is about, or what follows proves
	// nothing: two collections, the older one written second.
	c.Assert(slowValue, Equals, float64(1),
		Commentf("the first request observed %v, want the first sequence value", slowValue))
	c.Assert(fastValue, Equals, float64(2),
		Commentf("the second request observed %v, want the second sequence value: it was served from the cache instead of collecting", fastValue))
	c.Assert(slowResult.at.After(fastAt), Equals, true,
		Commentf("the first request finished before the second, so this run did not write the older observation last"))

	served := scrape()
	servedValue, _ := sampleValue(c, served, "overlap_probe_value")
	c.Check(servedValue, Equals, fastValue,
		Commentf("the cache holds %v, want the newer %v: a collection that started earlier replaced a later one when it finished",
			servedValue, fastValue))
	c.Check(sampleTimestamp(c, served, "overlap_probe_value"),
		Equals, sampleTimestamp(c, fastBody, "overlap_probe_value"),
		Commentf("the served sample is dated differently from the collection it came from"))
}

// TestEachNamespaceIsDatedWhenItWasCollected covers what a collection time
// means when a scrape collects more than one namespace.
//
// The engine runs them one after another on one connection, so the second
// finishes a query's worth of time after the first starts. Dated from a single
// time taken before the scrape, every namespace in it carried the same
// timestamp, and the later ones were dated to before the work that preceded
// them. Two namespaces that each take about a second make that visible
// whichever order the map yields them in.
func (s *IntegrationSuite) TestEachNamespaceIsDatedWhenItWasCollected(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")
	configured := strings.Split(dsn, ",")[0]

	dir := writeQueryFile(c, "slow.yaml", `
slow_one:
  query: "SELECT 1::float8 AS value FROM pg_sleep(0.6)"
  cache_seconds: 300
  metrics:
    - value: {usage: "GAUGE", description: "Constant one, slowly"}
slow_two:
  query: "SELECT 2::float8 AS value FROM pg_sleep(0.6)"
  cache_seconds: 300
  metrics:
    - value: {usage: "GAUGE", description: "Constant two, slowly"}
`)
	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), []string{configured},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	c.Assert(rec.Code, Equals, 200)
	body := rec.Body.String()

	one := sampleTimestamp(c, body, "slow_one_value")
	two := sampleTimestamp(c, body, "slow_two_value")
	c.Assert(one > 0, Equals, true, Commentf("slow_one carried no timestamp"))
	c.Assert(two > 0, Equals, true, Commentf("slow_two carried no timestamp"))

	gap := one - two
	if gap < 0 {
		gap = -gap
	}
	c.Check(gap >= 400, Equals, true,
		Commentf("two collections about 600ms apart are dated %d ms apart, so both were dated from one time taken before the scrape", gap))
}

// TestEditingAQueryFileInvalidatesItsCache covers the stale answer an edit
// would otherwise leave behind.
//
// A cached result describes the question that was asked. Change the question
// and the old answer is not a slow answer, it is the wrong one, and with a
// long cache_seconds it would be served for as long as the file said.
func (s *IntegrationSuite) TestEditingAQueryFileInvalidatesItsCache(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	dir := c.MkDir()
	path := filepath.Join(dir, "edited.yaml")
	write := func(value string) {
		c.Assert(os.WriteFile(path, []byte(`
edited_probe:
  query: "SELECT `+value+`::float8 AS value"
  cache_seconds: 300
  metrics:
    - value: {usage: "GAUGE", description: "a value the file decides"}
`), 0o600), IsNil)
	}
	write("11")

	restore := withQueryDirectory(c, LR, dir)
	defer restore()

	cache := querycache.New()
	limits := budget.Defaults()
	h := Handler(log.NewNopLogger(), strings.Split(dsn, ","),
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), cache)

	scrape := func() string {
		req := httptest.NewRequest("GET", "/metrics?collect[]=custom_query.lr", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		c.Assert(rec.Code, Equals, 200)
		return rec.Body.String()
	}

	value, _ := sampleValue(c, scrape(), "edited_probe_value")
	c.Assert(value, Equals, float64(11),
		Commentf("the query did not run, so this test cannot show an edit taking effect"))

	// Served from the cache, unchanged, while the file is unchanged.
	cached, timestamped := sampleValue(c, scrape(), "edited_probe_value")
	c.Assert(cached, Equals, float64(11))
	c.Assert(timestamped, Equals, true,
		Commentf("the second scrape collected rather than serving, so the cache is not in play"))

	// Now the question changes.
	write("22")

	edited, _ := sampleValue(c, scrape(), "edited_probe_value")
	c.Check(edited, Equals, float64(22),
		Commentf("the edited query returned %v, so a result from before the edit was served", edited))
}

// scrapeWith serves one /metrics request with the given collect[] values and
// returns the status and the body.
//
// This goes through the HTTP handler rather than gathering a collector
// directly, because what is being tested is the selection a scrape asks for,
// and that lives in the request.
func scrapeWith(dsn string, collect ...string) (int, string) {
	code, body, _ := scrapeHTTP(dsn, budget.Defaults(), "", collect...)
	return code, body
}

// hasFamily reports whether a scrape's body carries any sample of a family.
//
// The body is read rather than parsed. expfmt's text parser needs a name
// validation scheme that this version of prometheus/common leaves unset, and
// setting a global to read one response is a worse trade than matching the
// sample lines, which is all this needs.
func hasFamily(body, name string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			return true
		}
	}
	return false
}

// TestCollectSelectsCollectorsByScope covers the per-collector and per-scope
// collect[] selection against a running server.
//
// The two groups are what let one exporter serve two Prometheus jobs at
// different intervals, with the per-database collectors on the slower one. For
// that to work each group has to collect its own side and not the other's, so
// the test names a family from each side and requires it present on one and
// absent on the other.
func (s *IntegrationSuite) TestCollectSelectsCollectorsByScope(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	const clusterFamily = "pg_stat_database_blks_read"
	// pg_extensions rather than a table family: the connected database may
	// hold no user tables, and a collector with nothing to report looks the
	// same as a collector that was not selected. Every database has plpgsql.
	const databaseFamily = "pg_extensions"

	// The premise first. Without it, a test that finds neither family would
	// report that selection works when in fact nothing was collected at all.
	code, all := scrapeWith(dsn, "postgres")
	c.Assert(code, Equals, 200)
	c.Assert(hasFamily(all, clusterFamily), Equals, true,
		Commentf("%s is missing from an unfiltered scrape, so the fixture is wrong", clusterFamily))
	c.Assert(hasFamily(all, databaseFamily), Equals, true,
		Commentf("%s is missing from an unfiltered scrape, so the fixture is wrong", databaseFamily))

	for _, tc := range []struct {
		name        string
		collect     []string
		wantCluster bool
		wantLocal   bool
	}{
		{"the cluster group collects its own side only",
			[]string{"postgres.cluster"}, true, false},
		{"the database group collects its own side only",
			[]string{"postgres.database"}, false, true},
		{"both groups together collect everything",
			[]string{"postgres.cluster", "postgres.database"}, true, true},
		{"one collector by name collects only itself",
			[]string{"postgres.collector.extensions"}, false, true},
		{"a bare postgres overrides a narrower selection",
			[]string{"postgres.collector.extensions", "postgres"}, true, true},
	} {
		code, body := scrapeWith(dsn, tc.collect...)
		c.Check(code, Equals, 200, Commentf("%s: %v", tc.name, tc.collect))

		gotCluster := hasFamily(body, clusterFamily)
		gotLocal := hasFamily(body, databaseFamily)
		c.Check(gotCluster, Equals, tc.wantCluster,
			Commentf("%s: %s present=%v, want %v", tc.name, clusterFamily, gotCluster, tc.wantCluster))
		c.Check(gotLocal, Equals, tc.wantLocal,
			Commentf("%s: %s present=%v, want %v", tc.name, databaseFamily, gotLocal, tc.wantLocal))
	}
}

// TestClusterOnlyScrapeCoversNoDatabases covers what the coverage counts mean
// when a scrape asks for cluster-scoped collectors and nothing else.
//
// pg_collector_databases_selected says how many databases the namespaced
// collectors meant to run database-local collectors against. A scrape that
// selected no database-scoped collector meant none, and reporting the whole
// catalogue there reads as a shortfall for work the request never asked for:
// the fan-out does not run, so every database is selected and none is
// reached.
func (s *IntegrationSuite) TestClusterOnlyScrapeCoversNoDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()
	catalog, err := queryDatabases(context.Background(), server)
	c.Assert(err, IsNil)
	c.Assert(len(catalog) > 1, Equals, true,
		Commentf("the fixture server has one database, so a count of zero cannot be told from a count of one"))

	const selected = "pg_collector_databases_selected"
	const discovered = "pg_collector_databases_discovered"

	// The database group first, which is the premise: the count is the
	// catalogue when database-local collectors were asked for.
	code, body := scrapeWith(dsn, "postgres.database")
	c.Assert(code, Equals, 200)
	c.Assert(sampleValueOf(c, body, discovered), Equals, float64(len(catalog)),
		Commentf("discovery did not read the catalogue, so nothing below is about selection"))
	c.Assert(sampleValueOf(c, body, selected), Equals, float64(len(catalog)),
		Commentf("the database group selected %v of %d databases", sampleValueOf(c, body, selected), len(catalog)))

	// Cluster-scoped collectors only. No database-local work, so no set of
	// databases to cover, and the catalogue is not read at all: an inventory
	// is not part of every cluster-only profile, and reporting one here would
	// mean a query nothing asked for.
	code, body = scrapeWith(dsn, "postgres.cluster")
	c.Assert(code, Equals, 200)
	c.Check(sampleValueOf(c, body, selected), Equals, float64(0),
		Commentf("a scrape that selected no database-scoped collector reported %v databases selected",
			sampleValueOf(c, body, selected)))
	// Not asked for is neither known nor unknown, so neither is published. A
	// zero would invent a server with no databases and a one would claim a
	// catalogue nobody read. Whether the query ran is checked where it can be
	// checked, in collector/collect_scope_execution_test.go.
	c.Check(hasFamily(body, discovered), Equals, false,
		Commentf("the cluster group published a discovered total"))
	c.Check(hasFamily(body, "pg_collector_database_discovery_success"), Equals, false,
		Commentf("the cluster group reported on a discovery it never attempted"))
}

// sampleValueOf reads the value of a family's one sample, and fails when the
// family is absent rather than reporting a zero that nothing published.
//
// The value is the field after the name and labels, not the last field: a
// sample can carry an explicit timestamp after its value.
func sampleValueOf(c *C, body, family string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, family+"{") && !strings.HasPrefix(line, family+" ") {
			continue
		}
		fields := strings.Fields(line)
		c.Assert(len(fields) >= 2, Equals, true, Commentf("malformed sample %q", line))
		value, err := strconv.ParseFloat(fields[1], 64)
		c.Assert(err, IsNil)
		return value
	}
	c.Fatalf("%s is not in the response", family)
	return 0
}

// TestCollectRejectsAnUnusableSelection covers the failure that used to look
// like a success.
//
// A collect[] naming a collector that does not exist, or one that is turned
// off, used to produce a scrape that returned 200 with no metrics in it.
// Prometheus records that as a healthy target reporting nothing, which is the
// same shape as a database with nothing to report.
func (s *IntegrationSuite) TestCollectRejectsAnUnusableSelection(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	for _, tc := range []struct {
		name    string
		collect string
	}{
		{"an unknown collector name", "postgres.collector.no_such_collector"},
		{"a collector that ships disabled", "postgres.collector.stat_wal_receiver"},
		{"an unknown value in the postgres namespace", "postgres.nonsense"},
		{"the old second-level collector form", "postgres.stat_user_tables"},
	} {
		code, _ := scrapeWith(dsn, tc.collect)
		c.Check(code, Equals, 400, Commentf("%s: %s returned %d", tc.name, tc.collect, code))
	}
}

// withConfigFile installs a parsed configuration file as the one the exporter
// reads, and restores what was there afterwards.
//
// The package-level handler is named c, which a gocheck method shadows with
// its own parameter, so this lives outside the method.
func withConfigFile(path string) (func(), error) {
	h := &config.Handler{}
	if err := h.ReloadConfig(path, log.NewNopLogger()); err != nil {
		return func() {}, err
	}
	previous := c.GetConfig()
	c.Config = h.GetConfig()
	return func() { c.Config = previous }, nil
}

// TestMonitoringDatabaseKeysSelectDatabases is the end-to-end half of the
// configuration keys.
//
// Parsing them is checked in the config package. What this adds is that the
// parsed value reaches the collector and changes which databases are
// collected, which is the step a unit test cannot see: a key can be read
// perfectly and never passed on.
//
// The registry is built the way a scrape builds it, because that is where the
// configuration is read.
func (s *IntegrationSuite) TestMonitoringDatabaseKeysSelectDatabases(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	server, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer server.Close()
	catalog, err := queryDatabases(context.Background(), server)
	c.Assert(err, IsNil)
	others := otherThanCurrent(catalog)
	c.Assert(len(others) > 0, Equals, true,
		Commentf("the fixture server has one database, so selection cannot be exercised"))
	chosen := others[0]

	path := filepath.Join(c.MkDir(), "postgres_exporter.yml")
	c.Assert(os.WriteFile(path,
		[]byte("monitoring:\n  databases:\n    include: ["+chosen+"]\n"), 0o600), IsNil)
	restore, err := withConfigFile(path)
	c.Assert(err, IsNil)
	defer restore()

	limits := budget.Defaults()
	registry, err := makeRegistry(context.Background(), strings.Split(dsn, ","),
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		Filters{EnablePostgresCollector: true}, limits, querycache.New())
	c.Assert(err, IsNil)
	families, err := registry.Gather()
	c.Assert(err, IsNil)

	byName := map[string]*dto.MetricFamily{}
	for _, f := range families {
		byName[f.GetName()] = f
	}

	count, ok := byName["pg_collector_databases_selected"]
	c.Assert(ok, Equals, true, Commentf("nothing was collected, so the fixture proves nothing"))
	c.Check(count.GetMetric()[0].GetGauge().GetValue(), Equals, float64(1),
		Commentf("an include list naming one database did not select exactly one"))

	// What the include list is for: which databases the database-local
	// collectors actually read. pg_extensions rather than a table family,
	// because a database with no user tables reports nothing either way.
	//
	// The control connection is a separate matter. It stays up and keeps
	// serving the cluster-wide collectors whether or not its database was
	// selected, which is why this looks at what was collected rather than at
	// which connections are up.
	extensions, ok := byName["pg_extensions"]
	c.Assert(ok, Equals, true, Commentf("no extensions were collected from any database"))

	var collected []string
	for _, m := range extensions.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "datname" && l.GetValue() != "" {
				collected = append(collected, l.GetValue())
			}
		}
	}
	sort.Strings(collected)
	collected = uniq(collected)

	c.Check(collected, DeepEquals, []string{chosen},
		Commentf("an include list naming %q collected from %v", chosen, collected))
}
