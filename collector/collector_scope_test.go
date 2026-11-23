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

import "testing"

// TestEveryCollectorDeclaresItsScope checks that the registry and the scope
// map describe the same set of collectors.
//
// A missing entry does not read as missing. collectorScopes returns the zero
// value for an absent key, which is scopeCluster, so a collector registered
// without a scope would be run once on the configured connection and would
// report nothing about any other database. That is the failure the fan-out
// exists to prevent, and it would look like a working collector.
func TestEveryCollectorDeclaresItsScope(t *testing.T) {
	if len(collectorState) == 0 {
		t.Fatal("no collectors are registered, so this test proves nothing")
	}
	for name := range collectorState {
		if _, ok := collectorScopes[name]; !ok {
			t.Errorf("%s is registered but has no scope, so it would default to running once per server", name)
		}
	}
	for name := range collectorScopes {
		if _, ok := collectorState[name]; !ok {
			t.Errorf("a scope is recorded for %q, which is not a registered collector", name)
		}
	}
}

// TestScopeSplitIsExhaustiveAndDisjoint checks that the two groups a scrape
// can ask for between them cover every collector, exactly once.
//
// scopedCollectors returns only enabled collectors, and a test binary has not
// parsed the command line, so both groups come back empty here. The
// classification is read straight from the registry instead, which init has
// filled.
func TestScopeSplitIsExhaustiveAndDisjoint(t *testing.T) {
	var cluster, database []string
	for name := range collectorState {
		switch collectorScopes[name] {
		case scopeCluster:
			cluster = append(cluster, name)
		case scopeDatabase:
			database = append(database, name)
		default:
			t.Errorf("%s has a scope that is neither cluster nor database", name)
		}
	}

	if got, want := len(cluster)+len(database), len(collectorState); got != want {
		t.Errorf("the split covers %d collectors, want %d", got, want)
	}
	if len(cluster) == 0 {
		t.Error("no collector is cluster-scoped, so the classification is not being read")
	}
	if len(database) == 0 {
		t.Error("no collector is database-scoped, so the per-database collectors have gone missing")
	}
}

// TestScopedCollectorsReportsOnlyEnabled pins the difference between asking
// for a group and naming a collector.
//
// A group asks for whatever of its kind the exporter has, so a disabled
// collector is left out quietly. Naming one that is turned off is a mistake
// worth reporting, which NewPostgresCollector does.
func TestScopedCollectorsReportsOnlyEnabled(t *testing.T) {
	enabled, disabled := false, true
	collectorState["scope_test_on"] = &disabled
	collectorScopes["scope_test_on"] = scopeCluster
	collectorState["scope_test_off"] = &enabled
	collectorScopes["scope_test_off"] = scopeCluster
	defer func() {
		delete(collectorState, "scope_test_on")
		delete(collectorScopes, "scope_test_on")
		delete(collectorState, "scope_test_off")
		delete(collectorScopes, "scope_test_off")
	}()

	var sawOn, sawOff bool
	for _, name := range scopedCollectors(scopeCluster) {
		switch name {
		case "scope_test_on":
			sawOn = true
		case "scope_test_off":
			sawOff = true
		}
	}
	if !sawOn {
		t.Error("an enabled collector was left out of its group")
	}
	if sawOff {
		t.Error("a disabled collector was put in a group")
	}
}
