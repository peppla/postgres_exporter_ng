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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	_ "github.com/lib/pq"
	"github.com/percona/postgres_exporter/collector"
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
		// namespaces return at least one row on any reachable server, unlike
		// pg_stat_replication, pg_replication_slots and pg_lock_conflicts,
		// which are empty on an idle standalone primary.
		for _, want := range []string{
			"pg_stat_database_conflicts_confl_lock",
			"pg_stat_archiver_archived_count",
			"pg_stat_activity_count",
		} {
			c.Check(names[want] > 0, Equals, true,
				Commentf("%s was not produced, so the namespace queries did not reach the server", want))
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
	c.Check(len(success.GetMetric()), Equals, len(pc.Collectors))

	var failed []string
	for _, m := range success.GetMetric() {
		if m.GetGauge().GetValue() == 1 {
			continue
		}
		name := ""
		for _, l := range m.GetLabel() {
			if l.GetName() == "collector" {
				name = l.GetValue()
			}
		}
		failed = append(failed, name)
	}
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

	// The same set discovery is built from, asked for directly.
	names, err := queryDatabases(context.Background(), server)
	c.Assert(err, IsNil)

	dsns := exporter.discoverDatabaseDSNs()

	// Discovery keeps the DSN it was given and adds one per database it found.
	c.Assert(len(dsns), Equals, len(names)+1,
		Commentf("discovered %d DSNs from %d connectable databases: %v", len(dsns), len(names), names))
	c.Assert(len(names) > 0, Equals, true,
		Commentf("the fixture server has no second database, so discovery was never exercised"))
}
