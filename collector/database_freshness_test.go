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
// the whole set was covered. The first version of this answered that by
// recording only when a request selected every enabled collector, which breaks
// an installation that runs two collect[] profiles at different intervals: it
// never selects the whole set, so it never gets a time at all, and worse, a
// narrow request republished a wider one's older time, so the series looked
// answered while going stale.
//
// What is recorded is therefore the selection, and a request reads and writes
// its own. These tests hold that from both sides: a selection gets its own
// time, and it cannot borrow or move another's.

package collector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

// TestANarrowRequestRecordsItsOwnContext is the case the design turns on.
//
// Two collectors are enabled and the request asked for one, on a store that
// holds nothing. It gets a time, because what it claims is that its own
// selection completed, which it did.
func TestANarrowRequestRecordsItsOwnContext(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	// One of the two, which is what collect[]=postgres.collector.<name>
	// builds.
	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local_a": quietCollector{}}, store))

	if _, ok := freshnessByDatabase(out)["app"]; !ok {
		t.Error("a request that completed its own selection reported no time for it")
	}
	// Recorded under that selection and no other. A request for both
	// collectors has not been answered by this one.
	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	if _, ok := store.At(databaseKey(target, "app", "freshness_local_a")); !ok {
		t.Error("the narrow selection recorded nothing under its own context")
	}
	if _, ok := store.At(databaseKey(target, "app", "freshness_local_a,freshness_local_b")); ok {
		t.Error("a request for one collector wrote the record for both")
	}
}

// TestANarrowRequestDoesNotRepublishAWiderOnesTime is the stale case.
//
// A wider selection completed an hour ago and a narrow one completes now. The
// narrow request must report its own time, not the hour-old one, because the
// series under a narrow scrape job is that job's answer and an hour-old value
// there says collection is behind when it is not. The reverse matters more: a
// narrow request reporting a wider selection's recent time would hide a subset
// that has not run in an hour.
func TestANarrowRequestDoesNotRepublishAWiderOnesTime(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	earlier := time.Now().Add(-time.Hour)
	store.Record(databaseKey(target, "app", "freshness_local_a,freshness_local_b"), earlier)

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local_a": quietCollector{}}, store))

	got, ok := freshnessByDatabase(out)["app"]
	if !ok {
		t.Fatal("the narrow selection reported no time of its own")
	}
	if want := float64(earlier.UnixNano()) / 1e9; got == want {
		t.Error("the narrow request republished the wider selection's hour-old time as its own")
	}
	// And it left that record where it was.
	held, ok := store.At(databaseKey(target, "app", "freshness_local_a,freshness_local_b"))
	if !ok {
		t.Fatal("the wider selection's record was removed")
	}
	if !held.Equal(earlier) {
		t.Errorf("the wider selection's record moved to %v, which no request for it earned", held)
	}
}

// TestAWiderFailureDoesNotInheritANarrowSuccess is the other direction.
//
// The narrow selection worked. The wider one then fails, so it has no time of
// its own, and it must not borrow the narrow one's: most of what it asked for
// did not work.
func TestAWiderFailureDoesNotInheritANarrowSuccess(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	store.Record(databaseKey(target, "app", "freshness_local_a"), time.Now().Add(-time.Minute))

	// Both collectors asked for, one of them failing.
	out := collectEngine(t, engineRemembering(map[string]Collector{
		"freshness_local_a": quietCollector{},
		"freshness_local_b": failsOnDatabase{datname: "app"},
	}, store))

	if times := freshnessByDatabase(out); len(times) != 0 {
		t.Errorf("a selection that failed reported a time it never earned: %v", times)
	}
}

// TestTwoSelectionsAreRememberedSeparately is the installation ChatGPT named:
// two collect[] profiles at different intervals, neither of them the whole set.
func TestTwoSelectionsAreRememberedSeparately(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	withScopedCollector(t, "freshness_local_c", scopeDatabase)
	store := lastsuccess.New()

	first := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local_a": quietCollector{}}, store))
	second := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local_b": quietCollector{}}, store))

	if _, ok := freshnessByDatabase(first)["app"]; !ok {
		t.Error("the first profile reported no time")
	}
	if _, ok := freshnessByDatabase(second)["app"]; !ok {
		t.Error("the second profile reported no time")
	}
	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	for _, selection := range []string{"freshness_local_a", "freshness_local_b"} {
		if _, ok := store.At(databaseKey(target, "app", selection)); !ok {
			t.Errorf("the %s profile has no record of its own", selection)
		}
	}
	// Neither profile is the whole enabled set, which under the first version
	// of this meant neither ever got a time.
	if _, ok := store.At(databaseKey(target, "app", "freshness_local_a,freshness_local_b,freshness_local_c")); ok {
		t.Error("a record exists for the whole enabled set, which no request asked for")
	}
}

// TestTheSelectionIsTheSameWhateverOrderItArrivesIn keeps one profile from
// becoming two.
//
// The selection comes out of a map, so its order is not stable between
// requests. Without a canonical form the same profile would write a new record
// on most scrapes and never read its own.
func TestTheSelectionIsTheSameWhateverOrderItArrivesIn(t *testing.T) {
	first := selectionOf(map[string]Collector{
		"b": quietCollector{}, "a": quietCollector{}, "c": quietCollector{},
	})
	second := selectionOf(map[string]Collector{
		"c": quietCollector{}, "a": quietCollector{}, "b": quietCollector{},
	})
	if first != second {
		t.Errorf("the same selection canonicalised to %q and %q", first, second)
	}
	if first != "a,b,c" {
		t.Errorf("the canonical selection is %q, want the sorted names", first)
	}
	if got := selectionOf(map[string]Collector{}); got != "" {
		t.Errorf("a request that selected nothing has context %q, want none", got)
	}
}

// TestASelectionDoesNotCollideWithOneCollectorsRecord covers the key.
//
// A request for exactly one collector has a selection equal to that
// collector's name. Held under one field, the database-level record and that
// collector's own record would be the same key, and a collect[] naming one
// collector is the common case.
func TestASelectionDoesNotCollideWithOneCollectorsRecord(t *testing.T) {
	store := lastsuccess.New()
	const target, datname, name = "1", "app", "stat_user_tables"

	collectorTime := time.Now().Add(-2 * time.Hour)
	store.Record(lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: name,
	}, collectorTime)

	databaseTime := time.Now()
	store.Record(databaseKey(target, datname, name), databaseTime)

	held, ok := store.At(lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: name,
	})
	if !ok {
		t.Fatal("the collector's own record is gone")
	}
	if !held.Equal(collectorTime) {
		t.Errorf("the collector's record now reads %v, which is the database-level time", held)
	}
	if store.Len() != 2 {
		t.Errorf("the store holds %d records, want one for the collector and one for the selection", store.Len())
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
	store.Record(databaseKey(target, "app", "freshness_local"), earlier)

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
	store.Record(databaseKey(target, "other", "freshness_local"), earlier)

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
	store.Record(databaseKey(target, "other", "freshness_local"), earlier)

	p := engineRemembering(map[string]Collector{"freshness_local": quietCollector{}}, store)
	p.ctx = context.Background()
	published := publishedByName(func(ch chan<- prometheus.Metric) {
		p.collectDatabase(ch, dbTarget{datname: "other", dsn: dsn},
			map[string]Collector{"freshness_local": quietCollector{}}, "127.0.0.1:1", "freshness_local")
	})

	got, ok := published[databaseFreshnessMetric]
	if !ok {
		t.Fatal("a database that could not be reached stopped reporting when it last worked")
	}
	if want := float64(earlier.UnixNano()) / 1e9; got.value != want {
		t.Errorf("the database time is %v, want the %v it already held", got.value, want)
	}
}

// TestConcurrentSelectionsDoNotCrossRecords runs two profiles at once.
//
// Two scrape jobs on one exporter overlap routinely, and the store is shared
// process state. Under the race detector this says the two write their own
// records and neither waits on the other to finish to get it right.
func TestConcurrentSelectionsDoNotCrossRecords(t *testing.T) {
	stubServer(t, catalogueOf("app"))
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	var wg sync.WaitGroup
	for _, name := range []string{"freshness_local_a", "freshness_local_b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			ch := make(chan prometheus.Metric, 64)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range ch {
				}
			}()
			engineRemembering(map[string]Collector{name: quietCollector{}}, store).Collect(ch)
			close(ch)
			<-done
		}(name)
	}
	wg.Wait()

	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	for _, selection := range []string{"freshness_local_a", "freshness_local_b"} {
		if _, ok := store.At(databaseKey(target, "app", selection)); !ok {
			t.Errorf("the %s profile has no record after running beside the other", selection)
		}
	}
}

// TestAControlFailureRetainsOnlyTheMatchingContext covers the path that
// publishes without collecting.
//
// The connection never opened, so the engine reports what it already knew. It
// has to report what this request's selection knew, not what some other
// selection did.
func TestAControlFailureRetainsOnlyTheMatchingContext(t *testing.T) {
	stubServer(t, func(_ int, mock sqlmock.Sqlmock) {
		mock.ExpectPing().WillReturnError(errors.New("the server is not listening"))
	})
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	target, _ := targetid.ID("postgresql://probe@198.51.100.9:5432/app")
	mine := time.Now().Add(-5 * time.Minute)
	theirs := time.Now().Add(-10 * time.Minute)
	store.Record(databaseKey(target, "app", "freshness_local_a"), mine)
	store.Record(databaseKey(target, "app", "freshness_local_b"), theirs)

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_local_a": quietCollector{}}, store))

	times := freshnessByDatabase(out)
	if len(times) != 1 {
		t.Fatalf("%d freshness series on a failed connection, want this selection's alone: %v", len(times), times)
	}
	got := times["app"]
	if want := float64(mine.UnixNano()) / 1e9; got != want {
		t.Errorf("the retained time is %v, want this selection's %v rather than another's", got, want)
	}
}

// TestAnUnnamedControlDatabaseResolvesByItsOwnSelection reaches the branch the
// other control test does not.
//
// A connection string need not carry a database. When one does not and the
// connection fails, the engine has no name to report against, so it asks what
// this connection last covered. That question has to be asked of this
// selection, and of the database level rather than of a collector that happens
// to share the name.
//
// Both traps are live here: the selection is "freshness_local_a", which is
// also a collector's name, and a second selection holds a record on another
// database. Two mutations survived until this existed, one dropping the
// selection from the lookup and one dropping the level from the key
// comparison.
func TestAnUnnamedControlDatabaseResolvesByItsOwnSelection(t *testing.T) {
	stubServer(t, func(_ int, mock sqlmock.Sqlmock) {
		mock.ExpectPing().WillReturnError(errors.New("the server is not listening"))
	})
	withScopedCollector(t, "freshness_local_a", scopeDatabase)
	withScopedCollector(t, "freshness_local_b", scopeDatabase)
	store := lastsuccess.New()

	// No database in the path, so the engine cannot name one itself.
	const dsn = "postgresql://probe@198.51.100.9:5432/"
	target, _ := targetid.ID(dsn)

	// The database this selection last covered.
	store.Record(databaseKey(target, "mine", "freshness_local_a"), time.Now().Add(-5*time.Minute))
	// Another selection, on another database, which must not answer for it.
	store.Record(databaseKey(target, "theirs", "freshness_local_b"), time.Now().Add(-1*time.Minute))
	// And the collector of the same name as this selection, on a third
	// database. A lookup that ignores the level would find this one.
	store.Record(lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  "collector_only",
		Collector: "freshness_local_a",
	}, time.Now())

	p := engineRemembering(map[string]Collector{"freshness_local_a": quietCollector{}}, store)
	p.instance = &instance{dsn: dsn, name: "198.51.100.9:5432"}
	out := collectEngine(t, p)

	times := freshnessByDatabase(out)
	if len(times) != 1 {
		t.Fatalf("%d database freshness series, want the one this selection knows: %v", len(times), times)
	}
	if _, ok := times["mine"]; !ok {
		t.Errorf("the retained series names %v, want the database this selection last covered", times)
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
	store.Record(databaseKey(target, "app", "freshness_cluster"), time.Now().Add(-10*time.Minute))

	out := collectEngine(t, engineRemembering(
		map[string]Collector{"freshness_cluster": quietCollector{}}, store))

	if times := freshnessByDatabase(out); len(times) != 0 {
		t.Errorf("a cluster-only request reported database-local freshness: %v", times)
	}
}
