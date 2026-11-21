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

// Keeping the migration mapping and the shipped query files in step.
//
// docs/metric-migration.md says what replaces each namespace in the query
// files PMM ships. The files are data rather than code, so a namespace can be
// added to one without anything noticing, and the first sign would be a
// retirement that quietly dropped it.

package collector

import (
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

const migrationPath = "../docs/metric-migration.md"

// shippedQueryFiles are the query files this repository ships, relative to
// the collector package.
var shippedQueryFiles = []string{
	"../queries-hr.yml",
	"../queries-mr.yaml",
	"../queries-lr.yaml",
}

// queryFileNamespace is as much of a namespace as this test needs.
type queryFileNamespace struct {
	Metrics []map[string]struct {
		Usage string `yaml:"usage"`
	} `yaml:"metrics"`
}

// TestMigrationMappingCoversEveryShippedNamespace is the drift check.
//
// Every namespace in a shipped query file needs a row saying what replaces it,
// including "nothing". A namespace with no row is one nobody has decided
// about, and the decision that matters is whether retiring the file loses it.
func TestMigrationMappingCoversEveryShippedNamespace(t *testing.T) {
	mapping, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("reading the migration mapping: %s", err)
	}
	text := string(mapping)

	var found int
	for _, path := range shippedQueryFiles {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %s", path, err)
		}
		doc := map[string]queryFileNamespace{}
		if err := yaml.Unmarshal(content, &doc); err != nil {
			t.Fatalf("parsing %s: %s", path, err)
		}
		if len(doc) == 0 {
			t.Errorf("%s defines no namespaces, so this test proves nothing about it", path)
		}

		names := make([]string, 0, len(doc))
		for ns := range doc {
			names = append(names, ns)
		}
		sort.Strings(names)

		for _, ns := range names {
			found++
			if !strings.Contains(text, "`"+ns+"`") {
				t.Errorf("%s defines %s, which the migration mapping does not mention. Say what replaces it, or that nothing does",
					path, ns)
			}
		}
	}
	if found == 0 {
		t.Fatal("no namespaces were read from the query files")
	}

	// The file it maps into has to exist too, or the mapping points nowhere.
	if _, err := os.Stat(catalogPath); err != nil {
		t.Errorf("the migration mapping refers to the catalogue, which is not there: %s", err)
	}
}

// TestRenamedMetricsStillExistOnBothSides pins the renames the mapping lists.
//
// Each pair is one measurement under two names, and the mapping tells somebody
// to move from the first to the second. If the second stops being published
// the advice sends them to a metric that does not exist, and if the first
// stops being published the row is describing a migration that is already
// over.
func TestRenamedMetricsStillExistOnBothSides(t *testing.T) {
	renames := map[string]string{
		"pg_statio_user_tables_heap_blks_hit":   "pg_statio_user_tables_heap_blocks_hit",
		"pg_statio_user_tables_heap_blks_read":  "pg_statio_user_tables_heap_blocks_read",
		"pg_statio_user_tables_idx_blks_hit":    "pg_statio_user_tables_idx_blocks_hit",
		"pg_statio_user_tables_idx_blks_read":   "pg_statio_user_tables_idx_blocks_read",
		"pg_statio_user_tables_toast_blks_hit":  "pg_statio_user_tables_toast_blocks_hit",
		"pg_statio_user_tables_toast_blks_read": "pg_statio_user_tables_toast_blocks_read",
		"pg_statio_user_tables_tidx_blks_hit":   "pg_statio_user_tables_tidx_blocks_hit",
		"pg_statio_user_tables_tidx_blks_read":  "pg_statio_user_tables_tidx_blocks_read",
		"pg_replication_lag":                    "pg_replication_lag_seconds",
	}

	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}
	builtIn := map[string]bool{}
	for _, d := range defs {
		builtIn[d.Name] = true
	}

	produced := map[string]bool{}
	for _, path := range shippedQueryFiles {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %s", path, err)
		}
		doc := map[string]queryFileNamespace{}
		if err := yaml.Unmarshal(content, &doc); err != nil {
			t.Fatalf("parsing %s: %s", path, err)
		}
		for ns, body := range doc {
			for _, entry := range body.Metrics {
				for column, spec := range entry {
					if spec.Usage == "LABEL" || spec.Usage == "DISCARD" {
						continue
					}
					produced[ns+"_"+column] = true
				}
			}
		}
	}

	for from, to := range renames {
		if !produced[from] {
			t.Errorf("the mapping lists %s as a query-file metric, and no shipped file produces it any more", from)
		}
		if !builtIn[to] {
			t.Errorf("the mapping sends readers of %s to %s, which no collector publishes", from, to)
		}
	}
}
