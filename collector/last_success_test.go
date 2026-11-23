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

// When each collector last worked on each database.
//
// pg_scrape_collector_success says what this scrape did. It cannot say how
// long a collector has been failing on one database, which is the question an
// operator asks next, and no single scrape can answer it.

package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// failingCollector reports an error without touching a database.
type failingCollector struct{}

func (failingCollector) Update(context.Context, *instance, chan<- prometheus.Metric) error {
	return errors.New("the collector could not read what it needed")
}

// runCollector runs one collector through execute and returns what it
// published, by metric name.
func runCollector(c Collector, name, datname, dsn string, store *lastsuccess.Store) map[string]MetricResult {
	ch := make(chan prometheus.Metric, 16)
	execute(context.Background(), name, c, &instance{dsn: dsn}, ch, log.NewNopLogger(), datname, store)
	close(ch)
	out := map[string]MetricResult{}
	for m := range ch {
		out[familyName(m)] = readMetric(m)
	}
	return out
}

const lastSuccessMetric = "pg_collector_last_success_timestamp_seconds"

// TestACollectorsLastSuccessSurvivesItsFailure is what the series exists for.
func TestACollectorsLastSuccessSurvivesItsFailure(t *testing.T) {
	store := lastsuccess.New()
	const dsn = "postgresql://probe@198.51.100.9:5432/app"

	before := time.Now()
	good := runCollector(quietCollector{}, "scope_test_collector", "app", dsn, store)
	recorded, ok := good[lastSuccessMetric]
	if !ok {
		t.Fatalf("a collector that succeeded published no last success: %v", good)
	}
	if recorded.value < float64(before.Add(-time.Minute).Unix()) {
		t.Errorf("the recorded time is %v, which is not about now", recorded.value)
	}
	if recorded.labels["collector"] != "scope_test_collector" || recorded.labels["datname"] != "app" {
		t.Errorf("the series is labelled %v", recorded.labels)
	}
	if recorded.labels["target"] == "" {
		t.Error("the series carries no target identity, so two connections to one database would share it")
	}

	bad := runCollector(failingCollector{}, "scope_test_collector", "app", dsn, store)
	if got := bad["pg_scrape_collector_success"]; got.value != 0 {
		t.Errorf("the failing run reports success %v", got.value)
	}
	kept, ok := bad[lastSuccessMetric]
	if !ok {
		t.Fatal("a collector that failed lost the last success it had")
	}
	if kept.value != recorded.value {
		t.Errorf("the last success reads %v after a failure, want the %v it recorded", kept.value, recorded.value)
	}
}

// TestACollectorThatNeverSucceededHasNoLastSuccess keeps the absent case
// absent: zero is a real time and it says 1970.
func TestACollectorThatNeverSucceededHasNoLastSuccess(t *testing.T) {
	published := runCollector(failingCollector{}, "scope_test_collector", "app",
		"postgresql://probe@198.51.100.9:5432/app", lastsuccess.New())
	if _, ok := published[lastSuccessMetric]; ok {
		t.Error("a collector that has never succeeded reports a last success")
	}
}

// TestCollectorStateIsSeparateFromTheQueryEngine keeps one engine's success
// from answering for the other's. They collect the same databases over the
// same connections and fail independently.
func TestCollectorStateIsSeparateFromTheQueryEngine(t *testing.T) {
	store := lastsuccess.New()
	const dsn = "postgresql://probe@198.51.100.9:5432/app"

	// The query engine's record for the same connection and database, under a
	// job name that happens to match a collector's. The identity has to be the
	// one the collector will use, or the keys differ for a reason this test is
	// not about and it passes without proving anything, which is what the
	// first version of it did. What it holds is that the two engines use
	// different Engine values: it fails when this engine is given the query
	// engine's, which is the copy-paste that would cause the collision.
	target, _ := targetid.ID(dsn)
	store.Record(lastsuccess.Key{
		Engine:    lastsuccess.EngineQuery,
		Target:    target,
		Database:  "app",
		Collector: "scope_test_collector",
	}, time.Now())

	published := runCollector(failingCollector{}, "scope_test_collector", "app", dsn, store)
	if _, ok := published[lastSuccessMetric]; ok {
		t.Error("a collector that has never succeeded inherited the query engine's timestamp")
	}
}

// TestTwoCollectorsOnOneDatabaseKeepSeparateTimes covers the per-collector
// half: one failing collector must not read as the whole database failing, and
// one succeeding must not answer for the other.
func TestTwoCollectorsOnOneDatabaseKeepSeparateTimes(t *testing.T) {
	store := lastsuccess.New()
	const dsn = "postgresql://probe@198.51.100.9:5432/app"

	worked := runCollector(quietCollector{}, "scope_test_worked", "app", dsn, store)
	if _, ok := worked[lastSuccessMetric]; !ok {
		t.Fatal("the collector that worked recorded nothing")
	}
	failed := runCollector(failingCollector{}, "scope_test_failed", "app", dsn, store)
	if _, ok := failed[lastSuccessMetric]; ok {
		t.Error("a collector that never succeeded read another collector's success on the same database")
	}
}

// TestLastSuccessSurvivesPathsThatRunNoCollector is ChatGPT's requirement for
// this component: recording the state correctly is not enough if the series
// disappears on the paths that never reach a collector.
//
// A connection that fails and work with no permit both run nothing, and both
// are exactly when an operator asks how long this has been going on.
func TestLastSuccessSurvivesPathsThatRunNoCollector(t *testing.T) {
	// A port nothing listens on, so the connection is refused at once. An
	// unroutable address waits for a TCP timeout instead, which took this case
	// from milliseconds to over a minute.
	const dsn = "postgresql://probe@127.0.0.1:1/app?sslmode=disable"
	target, _ := targetid.ID(dsn)
	local := map[string]Collector{"scope_test_local": quietCollector{}}

	for _, tc := range []struct {
		name  string
		drive func(p PostgresCollector, ch chan<- prometheus.Metric)
	}{
		{"a database with no connection permit", func(p PostgresCollector, ch chan<- prometheus.Metric) {
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			p.ctx = cancelled
			p.connSema = semaphore.NewWeighted(1)
			p.collectDatabase(ch, dbTarget{datname: "app", dsn: dsn}, local, "127.0.0.1:1")
		}},
		{"a database that cannot be connected to", func(p PostgresCollector, ch chan<- prometheus.Metric) {
			// No semaphore, so the permit is free and the connection itself is
			// what fails.
			p.collectDatabase(ch, dbTarget{datname: "app", dsn: dsn}, local, "127.0.0.1:1")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := lastsuccess.New()
			when := time.Now().Add(-time.Hour)
			store.Record(lastsuccess.Key{
				Engine:    lastsuccess.EngineCollector,
				Target:    target,
				Database:  "app",
				Collector: "scope_test_local",
			}, when)

			p := PostgresCollector{
				Collectors:  local,
				logger:      log.NewNopLogger(),
				ctx:         context.Background(),
				lastSuccess: store,
				instance:    &instance{dsn: dsn, name: "127.0.0.1:1"},
			}

			ch := make(chan prometheus.Metric, 16)
			tc.drive(p, ch)
			close(ch)

			published := map[string]MetricResult{}
			for m := range ch {
				published[familyName(m)] = readMetric(m)
			}

			got, ok := published[lastSuccessMetric]
			if !ok {
				t.Fatalf("the series disappeared on this path: %v", published)
			}
			if want := float64(when.UnixNano()) / 1e9; got.value != want {
				t.Errorf("the last success reads %v, want the recorded %v", got.value, want)
			}
			if got.labels["collector"] != "scope_test_local" || got.labels["datname"] != "app" {
				t.Errorf("the retained series is labelled %v", got.labels)
			}
		})
	}
}

// TestTheControlConnectionsCollectorsKeepTheirTimes covers the other half: a
// scrape that never opens the control connection runs neither the cluster-wide
// collectors nor the control database's own.
func TestTheControlConnectionsCollectorsKeepTheirTimes(t *testing.T) {
	const dsn = "postgresql://probe@198.51.100.9:5432/app"
	target, _ := targetid.ID(dsn)
	when := time.Now().Add(-time.Hour)

	store := lastsuccess.New()
	// A cluster-wide collector carries no database; a database-local one
	// carries the control database.
	store.Record(lastsuccess.Key{Engine: lastsuccess.EngineCollector, Target: target,
		Database: "", Collector: "scope_test_cluster"}, when)
	store.Record(lastsuccess.Key{Engine: lastsuccess.EngineCollector, Target: target,
		Database: "app", Collector: "scope_test_local"}, when)

	withScopedCollector(t, "scope_test_cluster", scopeCluster)
	withScopedCollector(t, "scope_test_local", scopeDatabase)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	p := PostgresCollector{
		Collectors: map[string]Collector{
			"scope_test_cluster": quietCollector{},
			"scope_test_local":   quietCollector{},
		},
		logger:      log.NewNopLogger(),
		ctx:         cancelled,
		connSema:    semaphore.NewWeighted(1),
		lastSuccess: store,
		instance:    &instance{dsn: dsn, name: "198.51.100.9:5432"},
	}

	ch := make(chan prometheus.Metric, 32)
	p.Collect(ch)
	close(ch)

	byCollector := map[string]MetricResult{}
	for m := range ch {
		if familyName(m) != lastSuccessMetric {
			continue
		}
		r := readMetric(m)
		byCollector[r.labels["collector"]] = r
	}

	for _, name := range []string{"scope_test_cluster", "scope_test_local"} {
		got, ok := byCollector[name]
		if !ok {
			t.Errorf("%s lost its last success when the control connection was never opened", name)
			continue
		}
		if want := float64(when.UnixNano()) / 1e9; got.value != want {
			t.Errorf("%s reads %v, want the recorded %v", name, got.value, want)
		}
	}
	if got := byCollector["scope_test_cluster"].labels["datname"]; got != "" {
		t.Errorf("a cluster-wide collector's retained series carries datname %q", got)
	}
	if got := byCollector["scope_test_local"].labels["datname"]; got != "app" {
		t.Errorf("the control database's collector carries datname %q", got)
	}
}

// TestAFilteredOutCollectorIsAbsentButRemembered covers the last of ChatGPT's
// requirements: a collector a request did not select is left out of the
// response without its state being touched.
func TestAFilteredOutCollectorIsAbsentButRemembered(t *testing.T) {
	const dsn = "postgresql://probe@198.51.100.9:5432/app"
	target, _ := targetid.ID(dsn)
	when := time.Now().Add(-time.Hour)

	store := lastsuccess.New()
	key := lastsuccess.Key{Engine: lastsuccess.EngineCollector, Target: target,
		Database: "app", Collector: "scope_test_absent"}
	store.Record(key, when)

	// A scrape that selected a different collector entirely.
	published := runCollector(quietCollector{}, "scope_test_present", "app", dsn, store)
	if got := published[lastSuccessMetric]; got.labels["collector"] == "scope_test_absent" {
		t.Error("a collector the request did not select was published")
	}
	if _, ok := store.At(key); !ok {
		t.Error("a collector the request did not select lost its remembered success")
	}
}
