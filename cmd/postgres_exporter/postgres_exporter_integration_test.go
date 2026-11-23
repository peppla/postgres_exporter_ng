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
func (s *IntegrationSuite) TestApplicationQueryNameLookupFailureIsNotSuccess(c *C) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	c.Assert(dsn, Not(Equals), "")

	admin, err := NewServer(strings.Split(dsn, ",")[0])
	c.Assert(err, IsNil)
	defer admin.Close()

	const role = "noname"
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

	fixture := fmt.Sprintf(`
app_lookup:
  query: "SELECT 1::float8 AS value"
  databases:
    - %s
  metrics:
    - value: {usage: "GAUGE", description: "Constant one"}
`, control)
	dir := writeQueryFile(c, "app.yaml", fixture)

	// Discovery off, so the name is not supplied by the catalog and has to be
	// asked for.
	exporter := NewExporter(
		[]string{"postgresql://" + role + ":secret@" + hostPort(c, dsn) + "/" + control + "?sslmode=disable"},
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

	dir := writeQueryFile(c, "cached.yaml", `
cached_probe:
  query: "SELECT nextval('`+seq+`')::float8 AS value"
  cache_seconds: 300
  metrics:
    - value: {usage: "GAUGE", description: "a sequence, so a second run is visible"}
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
	c.Check(firstTimestamped, Equals, false,
		Commentf("a freshly collected sample carried a timestamp"))

	second := scrape()
	secondValue, secondTimestamped := sampleValue(c, second, "cached_probe_value")

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
