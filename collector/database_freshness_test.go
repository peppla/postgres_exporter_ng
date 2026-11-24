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

// When a database was last covered completely.
//
// The per-collector times answer a narrower question than the contract asks.
// "This collector last worked here at T" does not say the database was covered,
// because a database is covered when all of its collectors worked, and an
// operator deciding whether to trust a dashboard is asking about the database.
//
// The trap is a narrow request. A scrape that asked for one collector and got
// it has shown nothing about the others, so it must not write a time that says
// the database was covered. These tests hold that line from both sides.

package collector

import (
	"context"
	"testing"
	"time"

	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const databaseFreshnessMetric = "pg_collector_database_last_success_timestamp_seconds"

// freshnessByDatabase returns the published database-level time per database.
func freshnessByDatabase(out map[string][]*dto.Metric) map[string]float64 {
	times := map[string]float64{}
	for _, m := range out[databaseFreshnessMetric] {
		times[labelOf(m, "datname")] = m.GetGauge().GetValue()
	}
	return times
}

// engineRemembering is engineOn with a store to remember successes in.
func engineRemembering(collectors map[string]Collector, store *lastsuccess.Store) PostgresCollector {
	p := engineOn(collectors, dbselect.Absent(), dbselect.Absent())
	p.lastSuccess = store
	return p
}

// TestACoveredDatabaseRecordsWhenItWasCovered is the baseline: two databases,
// every collector working, and both get a time.
func TestACoveredDatabaseRecordsWhenItWasCovered(t *testing.T) {
	stubServer(t, catalogueOf("app", "other"))
	withScopedCollector(t, "freshness_local", scopeDatabase)
	store := lastsuccess.New()

	before := time.Now()
	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local": quietCollector{}}, store))
	after := time.Now()

	times := freshnessByDatabase(out)
	if len(times) != 2 {
		t.Fatalf("%d database freshness series, want one per covered database: %v", len(times), times)
	}
	for _, datname := range []string{"app", "other"} {
		got, ok := times[datname]
		if !ok {
			t.Errorf("%s has no freshness series after being covered", datname)
			continue
		}
		if got < float64(before.UnixNano())/1e9 || got > float64(after.UnixNano())/1e9 {
			t.Errorf("%s was covered at %v, which is outside the scrape", datname, got)
		}
	}
}

// TestTheDatabaseTimeIsNotLaterThanItsCollectors keeps the summary honest.
//
// The database-level time stands for every collector in it, so a value later
// than one of them would claim freshness no collector reported. It is stamped
// from before the work for that reason.
func TestTheDatabaseTimeIsNotLaterThanItsCollectors(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local", scopeDatabase)
	store := lastsuccess.New()

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local": quietCollector{}}, store))

	database, ok := freshnessByDatabase(out)["app"]
	if !ok {
		t.Fatal("no database freshness series was published")
	}
	collectors := out["pg_collector_last_success_timestamp_seconds"]
	if len(collectors) == 0 {
		t.Fatal("no per-collector freshness series was published, so there is nothing to compare")
	}
	for _, m := range collectors {
		if got := m.GetGauge().GetValue(); database > got {
			t.Errorf("the database time %v is later than the %s collector's %v",
				database, labelOf(m, "collector"), got)
		}
	}
}

// TestANarrowRequestDoesNotRecordTheDatabase is the case the whole design
// turns on.
//
// Two database-local collectors are enabled and the request asked for one.
// That request has shown nothing about the other, so it cannot say the
// database was covered.
func TestANarrowRequestDoesNotRecordTheDatabase(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	// Only one of the two, which is what collect[]=postgres.collector.<name>
	// builds.
	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local_a": quietCollector{}}, store))

	if times := freshnessByDatabase(out); len(times) != 0 {
		t.Errorf("a request for one of two collectors recorded the database as covered: %v", times)
	}
	// The collector it did ask for still reports for itself. The narrow
	// request is not told to forget what it did.
	if len(out["pg_collector_last_success_timestamp_seconds"]) != 1 {
		t.Errorf("%d per-collector freshness series, want the one the request asked for",
			len(out["pg_collector_last_success_timestamp_seconds"]))
	}
}

// TestAFullRequestRecordsWhereANarrowOneDidNot is what makes the test above
// mean anything: the same two collectors, both asked for, and the database is
// recorded.
func TestAFullRequestRecordsWhereANarrowOneDidNot(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	out := collectEngine(t, engineRemembering(map[string]Collector{
		"freshness_local_a": quietCollector{},
		"freshness_local_b": quietCollector{},
	}, store))

	if _, ok := freshnessByDatabase(out)["app"]; !ok {
		t.Error("a request for every database-local collector did not record the database as covered")
	}
}

// TestAFailedCollectorLeavesTheDatabaseTimeAlone covers the other way a
// database fails to be covered.
//
// The time already held is published, because that is the question an operator
// has when something is failing, and it is not moved forward, because this
// scrape did not cover the database.
func TestAFailedCollectorLeavesTheDatabaseTimeAlone(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local", scopeDatabase)
	store := lastsuccess.New()

	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	earlier := time.Now().Add(-90 * time.Minute)
	store.Record(databaseKey(target, "app"), earlier)

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local": failsOnDatabase{datname: "app"}}, store))

	got, ok := freshnessByDatabase(out)["app"]
	if !ok {
		t.Fatal("a database whose collector failed stopped reporting when it last worked")
	}
	if want := float64(earlier.UnixNano()) / 1e9; got != want {
		t.Errorf("the database time is %v, want the %v it already held: a scrape that did not cover it moved it forward", got, want)
	}
}

// TestAFanOutDatabaseThatFailedKeepsItsTime is the same rule on the other
// path.
//
// The control database is collected over the connection the engine already
// holds, and the databases around it are opened by the fan-out. The two are
// separate code and the first test only reached the control one, which a
// mutation showed by surviving it.
func TestAFanOutDatabaseThatFailedKeepsItsTime(t *testing.T) {
	stubServer(t, catalogueOf("app", "other"))
	withScopedCollector(t, "freshness_local", scopeDatabase)
	store := lastsuccess.New()

	// Asked for rather than written out. The fan-out builds this DSN from the
	// control one by replacing the database, and the identity is per
	// connection string, so a string that differs by a character is a
	// different target. Writing it out by hand produced exactly that: the
	// rewrite adds an empty password, and the test looked for a record the
	// engine had never written.
	fanout, err := config.DSNWithDatabase("postgresql://probe@198.51.100.9:5432/app", "other")
	if err != nil {
		t.Fatalf("building the fan-out DSN: %s", err)
	}
	target, _ := targetid.ID(fanout)
	earlier := time.Now().Add(-45 * time.Minute)
	store.Record(databaseKey(target, "other"), earlier)

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local": failsOnDatabase{datname: "other"}}, store))

	times := freshnessByDatabase(out)
	got, ok := times["other"]
	if !ok {
		t.Fatalf("a fan-out database whose collector failed stopped reporting when it last worked: %v", times)
	}
	if want := float64(earlier.UnixNano()) / 1e9; got != want {
		t.Errorf("the database time is %v, want the %v it already held", got, want)
	}
	// The control database was covered in the same scrape, so its own time is
	// this scrape's. The two databases are reported separately, which is the
	// point of the label.
	if _, ok := times["app"]; !ok {
		t.Error("the control database was covered and has no time")
	}
}

// TestADatabaseThatCannotBeReachedKeepsItsTime covers the connection failure,
// where no collector runs at all.
func TestADatabaseThatCannotBeReachedKeepsItsTime(t *testing.T) {
	withScopedCollector(t, "freshness_local", scopeDatabase)
	store := lastsuccess.New()

	const dsn = "postgresql://probe@127.0.0.1:1/other?sslmode=disable"
	target, _ := targetid.ID(dsn)
	earlier := time.Now().Add(-30 * time.Minute)
	store.Record(databaseKey(target, "other"), earlier)

	p := engineRemembering(map[string]Collector{"freshness_local": quietCollector{}}, store)
	p.ctx = context.Background()
	published := publishedByName(func(ch chan<- prometheus.Metric) {
		p.collectDatabase(ch, dbTarget{datname: "other", dsn: dsn},
			map[string]Collector{"freshness_local": quietCollector{}}, "127.0.0.1:1", true)
	})

	got, ok := published[databaseFreshnessMetric]
	if !ok {
		t.Fatal("a database that could not be reached stopped reporting when it last worked")
	}
	if want := float64(earlier.UnixNano()) / 1e9; got.value != want {
		t.Errorf("the database time is %v, want the %v it already held", got.value, want)
	}
}

// TestClusterOnlyCollectionSaysNothingAboutDatabaseFreshness keeps the series
// to the question it answers.
//
// A request that selected no database-local collector asked nothing about
// database-local collection, and a time published for it would be an answer to
// a question nobody put.
func TestClusterOnlyCollectionSaysNothingAboutDatabaseFreshness(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_cluster", scopeCluster)
	store := lastsuccess.New()

	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	store.Record(databaseKey(target, "app"), time.Now().Add(-10*time.Minute))

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_cluster": quietCollector{}}, store))

	if times := freshnessByDatabase(out); len(times) != 0 {
		t.Errorf("a cluster-only request reported database-local freshness: %v", times)
	}
}
