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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	_ "github.com/lib/pq"
	"github.com/percona/postgres_exporter/collector"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/versioncache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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
		err = s.e.checkMapVersions(rec.ch, server)
		c.Assert(err, IsNil)
		c.Assert(len(server.metricMap) > 0, Equals, true,
			Commentf("checkMapVersions produced no namespaces to query"))

		err = querySettings(rec.ch, server)
		if !c.Check(err, Equals, nil) {
			fmt.Println("## ERRORS FOUND")
			fmt.Println(err)
		}

		// This should never happen in our test cases.
		errMap := queryNamespaceMappings(rec.ch, server)
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
			map[string]ColumnMapping{},
			true,
			0,
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
