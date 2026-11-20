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

package main

import (
	"sort"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/percona/postgres_exporter/collector"
)

// withDefaultFlags parses the empty command line so the collector enablement
// flags hold their defaults.
//
// Collector flags are registered on the global kingpin command line at init,
// and a test binary never parses it, so every collector reads as disabled
// until this runs. The group expansions depend on that state, so a test of
// them that skipped this would be testing an exporter with no collectors.
func withDefaultFlags(t *testing.T) {
	t.Helper()
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatalf("parsing default flags: %s", err)
	}
}

func TestParseCollectFiltersSelection(t *testing.T) {
	withDefaultFlags(t)

	cluster := collector.ClusterScopedCollectors()
	database := collector.DatabaseScopedCollectors()
	if len(cluster) == 0 || len(database) == 0 {
		t.Fatalf("the scope groups are empty (%d cluster, %d database), so nothing below is tested",
			len(cluster), len(database))
	}

	for _, tc := range []struct {
		name    string
		filters []string
		wantErr bool
		// want is what reaches NewPostgresCollector. nil means every enabled
		// collector, which is not the same as the empty slice.
		want         []string
		wantPostgres bool
	}{
		{
			name: "no filters collects everything", filters: nil,
			want: nil, wantPostgres: false,
		},
		{
			name:    "a bare postgres collects every collector",
			filters: []string{"postgres"}, want: nil, wantPostgres: true,
		},
		{
			name:    "the cluster group expands to the cluster-scoped collectors",
			filters: []string{"postgres.cluster"}, want: cluster, wantPostgres: true,
		},
		{
			name:    "the database group expands to the database-scoped collectors",
			filters: []string{"postgres.database"}, want: database, wantPostgres: true,
		},
		{
			name:    "a collector is named at the third level",
			filters: []string{"postgres.collector.locks"}, want: []string{"locks"}, wantPostgres: true,
		},
		{
			name:    "several selections add up",
			filters: []string{"postgres.collector.locks", "postgres.collector.stat_user_tables"},
			want:    []string{"locks", "stat_user_tables"}, wantPostgres: true,
		},
		{
			// Both halves of the split in one request is every enabled
			// collector, reached by a different route than a bare postgres.
			name:    "both groups together cover the whole split",
			filters: []string{"postgres.cluster", "postgres.database"},
			want:    append(append([]string{}, cluster...), database...), wantPostgres: true,
		},
		{
			name:    "a bare postgres overrides a narrower selection sent with it",
			filters: []string{"postgres.collector.locks", "postgres"},
			want:    nil, wantPostgres: true,
		},
		{
			name:    "other groups are untouched",
			filters: []string{"custom_query.lr"}, want: nil, wantPostgres: false,
		},
		{
			// A PMM agent may send group names this exporter does not
			// implement, which has always been ignored rather than refused.
			name:    "an unknown value outside the postgres namespace is ignored",
			filters: []string{"mysql.something"}, want: nil, wantPostgres: false,
		},
		{
			name:    "an unknown value inside the postgres namespace is an error",
			filters: []string{"postgres.nonsense"}, wantErr: true,
		},
		{
			name:    "the old second-level collector form is an error",
			filters: []string{"postgres.stat_user_tables"}, wantErr: true,
		},
		{
			name:    "a collector prefix with no name is an error",
			filters: []string{"postgres.collector."}, wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parseCollectFilters(tc.filters)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseCollectFilters(%q) returned no error", tc.filters)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCollectFilters(%q): %s", tc.filters, err)
			}
			if f.EnablePostgresCollector != tc.wantPostgres {
				t.Errorf("EnablePostgresCollector = %v, want %v", f.EnablePostgresCollector, tc.wantPostgres)
			}
			got := f.postgresCollectors()
			if tc.want == nil {
				if got != nil {
					t.Errorf("postgresCollectors() = %v, want nothing, which means every enabled collector", got)
				}
				return
			}
			gotSorted, wantSorted := append([]string{}, got...), append([]string{}, tc.want...)
			sort.Strings(gotSorted)
			sort.Strings(wantSorted)
			if len(gotSorted) != len(wantSorted) {
				t.Fatalf("postgresCollectors() selected %d collectors, want %d\n got %v\nwant %v",
					len(gotSorted), len(wantSorted), gotSorted, wantSorted)
			}
			for i := range gotSorted {
				if gotSorted[i] != wantSorted[i] {
					t.Fatalf("postgresCollectors() = %v, want %v", gotSorted, wantSorted)
				}
			}
		})
	}
}

// TestCollectGroupsDoNotOverlap checks the property the split exists for. The
// two groups partition the enabled collectors, so scraping both on separate
// jobs collects everything exactly once and neither job does the other's work.
func TestCollectGroupsDoNotOverlap(t *testing.T) {
	withDefaultFlags(t)

	seen := map[string]int{}
	for _, name := range collector.ClusterScopedCollectors() {
		seen[name]++
	}
	for _, name := range collector.DatabaseScopedCollectors() {
		seen[name]++
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("%s appears in %d groups, want 1", name, n)
		}
	}

	// And a collector named explicitly is still the one collector, whichever
	// group it belongs to.
	f, err := parseCollectFilters([]string{"postgres.collector.database"})
	if err != nil {
		t.Fatalf("selecting the database collector by name: %s", err)
	}
	if got := f.postgresCollectors(); len(got) != 1 || got[0] != "database" {
		t.Errorf("postgres.collector.database selected %v, want just the database collector", got)
	}
}
