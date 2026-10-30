// Copyright 2026 The Prometheus Authors
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
package collector

import "testing"

// clusterScopedCollectors is keyed by collector name, and a key that matches no
// collector is invisible: the collector it was meant to name silently falls
// through to the database-scoped side and gets collected once per database.
// This catches a typo in the map, and a collector renamed without the map being
// updated.
func TestClusterScopedCollectorsAreRegistered(t *testing.T) {
	for name := range clusterScopedCollectors {
		if _, ok := collectorState[name]; !ok {
			t.Errorf("clusterScopedCollectors names %q, which is not a registered collector", name)
		}
	}
}

// Every registered collector sits on exactly one side of the split, and the
// split is exhaustive: a collector missing from clusterScopedCollectors is
// database-scoped by default rather than unclassified.
func TestScopeSplitIsExhaustiveAndDisjoint(t *testing.T) {
	// scopedCollectors filters to enabled collectors, and kingpin has not parsed
	// flags in a test binary, so both sides come back empty here. Classify
	// directly from the registry instead, which init() has populated.
	var cluster, database []string
	for name := range collectorState {
		if clusterScopedCollectors[name] {
			cluster = append(cluster, name)
		} else {
			database = append(database, name)
		}
	}

	if len(collectorState) == 0 {
		t.Fatal("no collectors registered; this test cannot prove anything")
	}
	if got, want := len(cluster)+len(database), len(collectorState); got != want {
		t.Errorf("split covers %d collectors, want %d", got, want)
	}
	if len(cluster) == 0 {
		t.Error("no collector classified as cluster-scoped; the map is probably not being read")
	}
	if len(database) == 0 {
		t.Error("no collector classified as database-scoped; the per-database collectors have gone missing")
	}
}
