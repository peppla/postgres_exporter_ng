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
	"regexp"
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

const breakingChangesPath = "../docs/breaking-changes.md"

// removalRow matches a row of the removal table: a metric that goes, and
// either the metric that replaces it or a note that nothing does yet.
var removalRow = regexp.MustCompile("(?m)^\\| `(pg_custom_[a-z0-9_]+)` \\| (?:`(pg_[a-z0-9_]+)`|([^|]+)) \\|")

// TestBreakingChangesListEveryCustomMetric is what stops a metric
// disappearing without anyone being told.
//
// Pep decided on 2026-11-21 that every pg_custom_ metric goes. Each one is
// something a dashboard or an alert can be using, so each one needs a row
// saying what replaces it. A metric still produced by a shipped query file and
// missing from that table is a removal nobody has written down.
func TestBreakingChangesListEveryCustomMetric(t *testing.T) {
	content, err := os.ReadFile(breakingChangesPath)
	if err != nil {
		t.Fatalf("reading the breaking changes: %s", err)
	}

	listed := map[string]bool{}
	for _, m := range removalRow.FindAllStringSubmatch(string(content), -1) {
		listed[m[1]] = true
	}
	if len(listed) == 0 {
		t.Fatal("no removals were found in the breaking changes, so this test proves nothing")
	}

	var produced int
	for _, path := range shippedQueryFiles {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %s", path, err)
		}
		doc := map[string]queryFileNamespace{}
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("parsing %s: %s", path, err)
		}
		for ns, body := range doc {
			if !strings.HasPrefix(ns, "pg_custom_") {
				continue
			}
			for _, entry := range body.Metrics {
				for column, spec := range entry {
					if spec.Usage == "LABEL" || spec.Usage == "DISCARD" {
						continue
					}
					name := ns + "_" + column
					produced++
					if !listed[name] {
						t.Errorf("%s produces %s, which the breaking changes do not list. Every pg_custom_ metric that goes needs a row saying what replaces it",
							path, name)
					}
				}
			}
		}
	}
	if produced == 0 {
		t.Error("no pg_custom_ metrics were found in the shipped query files. If they are all gone, this test and the table it checks can go too")
	}
}

// TestBreakingChangeReplacementsExist checks the other half: a row that names
// a replacement has to name one that is published.
//
// A table of renames pointing at metrics that do not exist is worse than no
// table, because someone follows it and finds nothing. A row may say the
// replacement is not written yet, which is a different and honest answer.
func TestBreakingChangeReplacementsExist(t *testing.T) {
	content, err := os.ReadFile(breakingChangesPath)
	if err != nil {
		t.Fatalf("reading the breaking changes: %s", err)
	}

	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}
	published := map[string]bool{}
	for _, d := range defs {
		published[d.Name] = true
	}

	var named, pending int
	for _, m := range removalRow.FindAllStringSubmatch(string(content), -1) {
		removed, replacement, note := m[1], m[2], strings.TrimSpace(m[3])
		if replacement == "" {
			pending++
			if note == "" {
				t.Errorf("%s has an empty replacement cell: name a metric or say that none is written yet", removed)
			}
			continue
		}
		named++
		if !published[replacement] {
			t.Errorf("%s is listed as replaced by %s, which no collector publishes", removed, replacement)
		}
	}
	if named == 0 {
		t.Error("no replacements were named at all")
	}
	t.Logf("%d removals with a replacement, %d still to write", named, pending)
}
