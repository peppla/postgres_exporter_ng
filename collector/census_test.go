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

package collector

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/targetid"
)

// catalogueThatLoses gives every connection the version lookup and a catalogue
// whose contents the caller can change between scrapes.
func catalogueThatLoses(current string, others *[]string, eligible map[string]bool) func(int, sqlmock.Sqlmock) {
	return func(_ int, mock sqlmock.Sqlmock) {
		mock.MatchExpectationsInOrder(false)
		mock.ExpectPing()
		mock.ExpectQuery("SELECT version").WillReturnRows(
			sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 18.0 on x86_64-pc-linux-gnu"))
		rows := sqlmock.NewRows([]string{"datname", "is_current", "eligible"}).AddRow(current, true, true)
		for _, name := range *others {
			ok := true
			if v, given := eligible[name]; given {
				ok = v
			}
			rows.AddRow(name, false, ok)
		}
		mock.ExpectQuery("FROM pg_database").WillReturnRows(rows)
	}
}

// connectionTo finds the connection a seed built to open one of its databases,
// as the store that holds the records knows it.
func connectionTo(store *lastsuccess.Store, seed, database string) (string, bool) {
	for id, name := range store.Owned(seed) {
		if name == database {
			return id, true
		}
	}
	return "", false
}

// TestBuiltInCollectionMaintainsItsOwnState is why this engine applies a
// census of its own.
//
// The query engine applies one, but only a request that runs application
// queries reaches that code. A deployment that collects nothing but built-in
// metrics would then create per-database state on every scrape and never have
// anything remove it, so the cleanup would exist and not run.
//
// Nothing in this test runs an application query.
func TestBuiltInCollectionMaintainsItsOwnState(t *testing.T) {
	targetid.Clear()
	t.Cleanup(targetid.Clear)

	others := []string{"other"}
	stubServer(t, catalogueThatLoses("app", &others, nil))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	store := lastsuccess.New()
	e := engineOn(map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Absent(), dbselect.Absent())
	e.lastSuccess = store

	collectEngine(t, e)

	seed, _ := targetid.ID(e.instance.dsn)
	target, built := connectionTo(store, seed, "other")
	if !built {
		t.Fatal("no connection was built for other, so it was never discovered")
	}
	if _, _, ok := store.Latest(lastsuccess.EngineCollector, lastsuccess.OneCollector, target, "outcome_local"); !ok {
		t.Fatal("other was never collected, so this says nothing about forgetting it")
	}

	// The server no longer has it.
	others = nil
	collectEngine(t, e)

	if _, _, ok := store.Latest(lastsuccess.EngineCollector, lastsuccess.OneCollector, target, "outcome_local"); ok {
		t.Error("a database the server no longer has kept its history, so this engine never reached the cleanup")
	}
}

// TestBuiltInCollectionKeepsADatabaseItCanNoLongerReach is the other half.
//
// A database that stops accepting connections leaves the eligible set and
// stays on the server. It is absent from what this engine collects and present
// in what the server has, and only the second of those decides whether its
// history goes.
func TestBuiltInCollectionKeepsADatabaseItCanNoLongerReach(t *testing.T) {
	targetid.Clear()
	t.Cleanup(targetid.Clear)

	others := []string{"other"}
	eligible := map[string]bool{}
	stubServer(t, catalogueThatLoses("app", &others, eligible))
	withScopedCollector(t, "outcome_local", scopeDatabase)

	store := lastsuccess.New()
	e := engineOn(map[string]Collector{"outcome_local": quietCollector{}},
		dbselect.Absent(), dbselect.Absent())
	e.lastSuccess = store

	collectEngine(t, e)

	seed, _ := targetid.ID(e.instance.dsn)
	target, built := connectionTo(store, seed, "other")
	if !built {
		t.Fatal("no connection was built for other, so it was never discovered")
	}
	if _, _, ok := store.Latest(lastsuccess.EngineCollector, lastsuccess.OneCollector, target, "outcome_local"); !ok {
		t.Fatal("other was never collected, so this says nothing about keeping it")
	}

	// Still on the server, and no longer connectable.
	eligible["other"] = false
	collectEngine(t, e)

	if _, _, ok := store.Latest(lastsuccess.EngineCollector, lastsuccess.OneCollector, target, "outcome_local"); !ok {
		t.Error("a database that is still there lost its history because it stopped accepting connections")
	}
}
