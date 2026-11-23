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
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// cancelledEngine is an engine whose every connection permit request fails,
// which is what a scrape at its connection limit does.
//
// The collectors are the caller's, because what the engine reports depends on
// which scopes were selected. An engine with no collectors at all asked for no
// database-local work, so every case about a deferred database has to select
// a database-scoped one.
func cancelledEngine(t *testing.T, collectors map[string]Collector) PostgresCollector {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return PostgresCollector{
		Collectors: collectors,
		logger:     log.NewNopLogger(),
		ctx:        ctx,
		connSema:   semaphore.NewWeighted(1),
		instance:   &instance{dsn: "postgresql://probe@198.51.100.9:5432/app", name: "198.51.100.9:5432"},
	}
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
	p := cancelledEngine(t, map[string]Collector{})
	target := dbTarget{datname: "app", dsn: "postgresql://probe@198.51.100.9:5432/app"}

	var wasDeferred bool
	published := publishedByName(func(ch chan<- prometheus.Metric) {
		wasDeferred = p.collectDatabase(ch, target, map[string]Collector{}, "198.51.100.9:5432")
	})

	if !wasDeferred {
		t.Error("a database with no connection permit was not reported as deferred to its caller")
	}
	deferred, ok := published["pg_collector_database_deferred"]
	if !ok {
		t.Fatalf("no deferral was published: %v", published)
	}
	if deferred.labels["reason"] != dbselect.ReasonNoPermit {
		t.Errorf("the deferral reason is %q, want %q", deferred.labels["reason"], dbselect.ReasonNoPermit)
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
}

// TestADeferredControlConnectionIsNotAFailure covers the other path: the
// control connection itself never gets a permit, so the engine collects
// nothing at all.
//
// A database-scoped collector is selected, so this scrape did mean to cover a
// database and the counts say so.
func TestADeferredControlConnectionIsNotAFailure(t *testing.T) {
	withScopedCollector(t, "deferral_test_local", scopeDatabase)
	p := cancelledEngine(t, map[string]Collector{"deferral_test_local": quietCollector{}})

	published := publishedByName(func(ch chan<- prometheus.Metric) {
		p.Collect(ch)
	})

	if _, ok := published["pg_collector_database_deferred"]; !ok {
		t.Fatalf("no deferral was published: %v", published)
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
}

// TestAClusterOnlyDeferralSelectsNoDatabase is the same path with only a
// cluster-scoped collector selected.
//
// The connection was deferred, so the per-connection diagnostic names it. No
// database was meant to be covered, so the count beside it is zero and nothing
// is said about a set of databases. The two describe different things here,
// which the help text says.
func TestAClusterOnlyDeferralSelectsNoDatabase(t *testing.T) {
	withScopedCollector(t, "deferral_test_cluster", scopeCluster)
	p := cancelledEngine(t, map[string]Collector{"deferral_test_cluster": quietCollector{}})

	published := publishedByName(func(ch chan<- prometheus.Metric) {
		p.Collect(ch)
	})

	deferred, ok := published["pg_collector_database_deferred"]
	if !ok {
		t.Fatalf("the deferred connection was not named: %v", published)
	}
	if deferred.labels["reason"] != dbselect.ReasonNoPermit {
		t.Errorf("the deferral reason is %q", deferred.labels["reason"])
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
}

// TestDeferralReasonsAreAClosedSet keeps the label from becoming an error
// string, which is how a bounded label set becomes unbounded.
func TestDeferralReasonsAreAClosedSet(t *testing.T) {
	for _, reason := range []string{dbselect.ReasonNoPermit, dbselect.ReasonNotAttempted} {
		if strings.ContainsAny(reason, " .:\"") {
			t.Errorf("%q does not look like a closed-set value", reason)
		}
	}
}
