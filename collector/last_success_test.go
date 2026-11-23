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
