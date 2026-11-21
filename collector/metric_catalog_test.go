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

// Keeping the catalogue and the code in step.
//
// docs/metric-catalog.md says what every built-in metric measures, on which
// versions, with what labels and what happens when collection fails. A
// document like that is right on the day it is written and drifts afterwards,
// so these tests make the drift fail rather than accumulate.

package collector

import (
	"context"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const catalogPath = "../docs/metric-catalog.md"

// countersWithoutTotalSuffix are the metrics that break the Prometheus naming
// convention, as it stood when the catalogue was written.
//
// They are inherited names. Renaming them breaks every dashboard and alert
// that uses them, so they are recorded rather than fixed, and the decision to
// rename belongs with the compatibility mapping in stage 5 rather than here.
//
// The list exists so that a NEW counter cannot join them quietly. Adding one
// fails this test, and the fix is normally to name it correctly.
//
// There is one reason to extend it. A new member of a family whose existing
// members all break the convention follows its family, because a single
// correctly named metric among fourteen wrongly named siblings is harder for
// someone writing a query than a family that is wrong consistently. Say so
// against the entry, as n_tup_newpage_upd does.
var countersWithoutTotalSuffix = []string{
	"pg_stat_bgwriter_buffers_alloc",
	"pg_stat_bgwriter_buffers_backend",
	"pg_stat_bgwriter_buffers_backend_fsync",
	"pg_stat_bgwriter_buffers_checkpoint",
	"pg_stat_bgwriter_buffers_clean",
	"pg_stat_bgwriter_checkpoint_sync_time",
	"pg_stat_bgwriter_checkpoint_write_time",
	"pg_stat_bgwriter_checkpoints_done",
	"pg_stat_bgwriter_checkpoints_req",
	"pg_stat_bgwriter_checkpoints_timed",
	"pg_stat_bgwriter_maxwritten_clean",
	"pg_stat_bgwriter_slru_written",
	"pg_stat_bgwriter_stats_reset",
	"pg_stat_database_blk_read_time",
	"pg_stat_database_blk_write_time",
	"pg_stat_database_blks_hit",
	"pg_stat_database_blks_read",
	"pg_stat_database_conflicts",
	"pg_stat_database_deadlocks",
	"pg_stat_database_parallel_workers_launched",
	"pg_stat_database_parallel_workers_to_launch",
	"pg_stat_database_stats_reset",
	"pg_stat_database_temp_bytes",
	"pg_stat_database_temp_files",
	"pg_stat_database_tup_deleted",
	"pg_stat_database_tup_fetched",
	"pg_stat_database_tup_inserted",
	"pg_stat_database_tup_returned",
	"pg_stat_database_tup_updated",
	"pg_stat_database_xact_commit",
	"pg_stat_database_xact_rollback",
	"pg_stat_user_indexes_idx_scan",
	"pg_stat_user_indexes_idx_tup_fetch",
	"pg_stat_user_indexes_idx_tup_read",
	"pg_stat_user_tables_analyze_count",
	"pg_stat_user_tables_autoanalyze_count",
	"pg_stat_user_tables_autovacuum_count",
	"pg_stat_user_tables_idx_scan",
	"pg_stat_user_tables_idx_tup_fetch",
	"pg_stat_user_tables_n_tup_del",
	"pg_stat_user_tables_n_tup_hot_upd",
	"pg_stat_user_tables_n_tup_ins",
	// Added after this list was written. Its fourteen siblings in the family
	// all lack the suffix, and PostgreSQL's own column is n_tup_newpage_upd,
	// so it follows them.
	"pg_stat_user_tables_n_tup_newpage_upd",
	"pg_stat_user_tables_n_tup_upd",
	"pg_stat_user_tables_seq_scan",
	"pg_stat_user_tables_seq_tup_read",
	"pg_stat_user_tables_total_analyze_time",
	"pg_stat_user_tables_total_autoanalyze_time",
	"pg_stat_user_tables_total_autovacuum_time",
	"pg_stat_user_tables_total_vacuum_time",
	"pg_stat_user_tables_vacuum_count",
	"pg_stat_wal_receiver_flushed_lsn",
	"pg_stat_wal_receiver_last_msg_receipt_time",
	"pg_stat_wal_receiver_last_msg_send_time",
	"pg_stat_wal_receiver_latest_end_lsn",
	"pg_stat_wal_receiver_latest_end_time",
	"pg_stat_wal_receiver_receive_start_lsn",
	"pg_statio_user_tables_heap_blocks_hit",
	"pg_statio_user_tables_heap_blocks_read",
	"pg_statio_user_tables_idx_blocks_hit",
	"pg_statio_user_tables_idx_blocks_read",
	"pg_statio_user_tables_tidx_blocks_hit",
	"pg_statio_user_tables_tidx_blocks_read",
	"pg_statio_user_tables_toast_blocks_hit",
	"pg_statio_user_tables_toast_blocks_read",
}

// TestEveryMetricHasExactlyOneValueType checks the inventory can see how every
// descriptor is emitted.
//
// A metric with no value type is one the extractor could not follow, which
// means it is missing from everything checked against the inventory. A metric
// with two is a metric published as a counter in one place and a gauge in
// another, which no consumer can read correctly.
func TestEveryMetricHasExactlyOneValueType(t *testing.T) {
	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) == 0 {
		t.Fatal("the inventory is empty, so nothing below is checked")
	}
	for _, d := range defs {
		switch len(d.Types) {
		case 1:
		case 0:
			t.Errorf("%s (%s:%d) is defined but the inventory cannot see where it is emitted. Pass the descriptor as a named variable to MustNewConstMetric or to a local emit(desc, vtype, value), rather than through a struct field or a table of descriptors, which cannot be followed by reading the source",
				d.Name, d.File, d.Line)
		default:
			t.Errorf("%s is emitted as %v, so its type depends on which code path produced it", d.Name, d.Types)
		}
	}
}

// TestCatalogCoversEveryMetric is the drift check.
//
// Every metric the code defines needs an entry, and every entry needs a
// metric. The second direction matters as much as the first: an entry for a
// metric that no longer exists describes something an operator cannot find,
// and reads as a gap in their monitoring rather than a stale document.
func TestCatalogCoversEveryMetric(t *testing.T) {
	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("reading the catalogue: %s", err)
	}

	// A catalogue entry is a table row whose first cell is a metric name in
	// backticks.
	row := regexp.MustCompile("(?m)^\\| `(pg_[a-z0-9_]+)` \\|")
	documented := map[string]bool{}
	for _, m := range row.FindAllStringSubmatch(string(content), -1) {
		if documented[m[1]] {
			t.Errorf("%s has more than one entry in the catalogue", m[1])
		}
		documented[m[1]] = true
	}
	if len(documented) == 0 {
		t.Fatalf("no entries were found in %s, so this test proves nothing", catalogPath)
	}

	inCode := map[string]bool{}
	for _, d := range defs {
		inCode[d.Name] = true
	}

	var missing, extra []string
	for name := range inCode {
		if !documented[name] {
			missing = append(missing, name)
		}
	}
	for name := range documented {
		if !inCode[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)

	for _, name := range missing {
		t.Errorf("%s is published and has no catalogue entry", name)
	}
	for _, name := range extra {
		t.Errorf("%s has a catalogue entry and is not published", name)
	}
}

// TestCatalogRecordsTheValueType compares the type column against the type the
// code emits.
//
// The names and the labels were checked before this and the type was not, so
// an entry could call a counter a gauge and nothing noticed. That is the field
// a reader acts on first: it decides whether rate() is the right question to
// ask, and it is the field the known defects in this catalogue are about.
func TestCatalogRecordsTheValueType(t *testing.T) {
	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}
	emitted := map[string][]string{}
	for _, d := range defs {
		emitted[d.Name] = d.Types
	}

	content, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("reading the catalogue: %s", err)
	}

	// The first two cells of an entry: the name and the type.
	row := regexp.MustCompile("(?m)^\\| `(pg_[a-z0-9_]+)` \\| ([a-z]+) \\|")
	matches := row.FindAllStringSubmatch(string(content), -1)
	if len(matches) == 0 {
		t.Fatalf("no entries with a type were found in %s, so this test proves nothing", catalogPath)
	}

	// What the code's value type names mean in the type column.
	wanted := map[string]string{
		"Counter":   "counter",
		"Gauge":     "gauge",
		"Untyped":   "untyped",
		"Histogram": "histogram",
		"Summary":   "summary",
	}

	checked := 0
	for _, m := range matches {
		name, documented := m[1], m[2]
		types, ok := emitted[name]
		if !ok {
			// Covered by TestCatalogCoversEveryMetric, which names it.
			continue
		}
		if len(types) != 1 {
			// Covered by TestEveryMetricHasExactlyOneValueType.
			continue
		}
		want, known := wanted[types[0]]
		if !known {
			t.Errorf("%s is emitted as %q, which this test does not know how to compare with a catalogue type", name, types[0])
			continue
		}
		checked++
		if documented != want {
			t.Errorf("%s is documented as a %s and is emitted as a %s", name, documented, want)
		}
	}
	if checked == 0 {
		t.Fatal("no entry was compared against an emitted type, so this test proves nothing")
	}
	if checked < len(matches)/2 {
		t.Errorf("only %d of %d entries were compared, so the table's shape has changed and most rows are no longer being read",
			checked, len(matches))
	}
}

// TestCatalogRecordsTheLabels checks the one mechanical field the catalogue
// repeats, because labels are what a query joins on and a wrong list there
// sends someone writing a query that returns nothing.
func TestCatalogRecordsTheLabels(t *testing.T) {
	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("reading the catalogue: %s", err)
	}
	text := string(content)

	for _, d := range defs {
		if len(d.Labels) == 0 {
			continue
		}
		section := labelSectionFor(text, d.Name)
		if section == "" {
			continue // TestCatalogCoversEveryMetric reports a missing entry.
		}
		for _, label := range d.Labels {
			if !strings.Contains(section, "`"+label+"`") {
				t.Errorf("%s carries the label %q, which its catalogue section does not mention", d.Name, label)
			}
		}
	}
}

// labelSectionFor returns the catalogue section a metric is documented in, so
// a label can be looked for in the text that describes it.
func labelSectionFor(text, metric string) string {
	i := strings.Index(text, "| `"+metric+"` |")
	if i < 0 {
		return ""
	}
	start := strings.LastIndex(text[:i], "\n## ")
	if start < 0 {
		start = 0
	}
	end := strings.Index(text[i:], "\n## ")
	if end < 0 {
		return text[start:]
	}
	return text[start : i+end]
}

// TestNoNewCounterWithoutTotal holds the naming convention forward.
func TestNoNewCounterWithoutTotal(t *testing.T) {
	recorded := map[string]bool{}
	for _, name := range countersWithoutTotalSuffix {
		recorded[name] = true
	}

	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	for _, d := range defs {
		if d.Types[0] != "Counter" || strings.HasSuffix(d.Name, "_total") {
			continue
		}
		seen[d.Name] = true
		if !recorded[d.Name] {
			t.Errorf("%s is a counter and does not end in _total. Name it %s_total rather than adding it to countersWithoutTotalSuffix, which is a record of inherited names and not a list to grow",
				d.Name, d.Name)
		}
	}

	for name := range recorded {
		if !seen[name] {
			t.Errorf("countersWithoutTotalSuffix records %s, which is no longer a counter without the suffix: remove it", name)
		}
	}
}

// TestBgwriterTotalPairsAreDuplicates is evidence for a catalogue entry rather
// than a guard on behaviour.
//
// Thirteen bgwriter metrics exist twice, once with the Prometheus _total
// suffix and once without. The catalogue says they are the same measurement
// published twice, and this is what that claim rests on: the collector is run
// over one fixture and each pair is required to carry the same value and the
// same labels.
//
// It also stops them diverging. Two names for one number is a compatibility
// decision; two names for two different numbers would be a defect, and the
// difference would be invisible without this.
func TestBgwriterTotalPairsAreDuplicates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	columns := []string{
		"checkpoints_timed", "checkpoints_req", "checkpoint_write_time",
		"checkpoint_sync_time", "buffers_checkpoint", "buffers_clean",
		"maxwritten_clean", "buffers_backend", "buffers_backend_fsync",
		"buffers_alloc", "stats_reset",
	}
	reset, err := time.Parse("2006-01-02 15:04:05.00000-07", "2023-05-25 17:10:42.81132-07")
	if err != nil {
		t.Fatalf("parsing the fixture time: %s", err)
	}
	mock.ExpectQuery(sanitizeQuery(statBGWriterQueryPrePG17)).WillReturnRows(
		sqlmock.NewRows(columns).AddRow(
			354, 4945, 289097744, 1242257, int64(3275602074), 89320867,
			450139, 2034563757, 7, int64(2725688749), reset))

	ch := make(chan prometheus.Metric, 128)
	c := PGStatBGWriterCollector{}
	if err := c.Update(context.Background(),
		&instance{db: db, version: semver.MustParse("16.0.0")}, ch); err != nil {
		t.Fatalf("Update: %s", err)
	}
	close(ch)

	type sample struct {
		value  float64
		labels string
	}
	got := map[string]sample{}
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("writing a metric: %s", err)
		}
		var value float64
		switch {
		case pb.Counter != nil:
			value = pb.Counter.GetValue()
		case pb.Gauge != nil:
			value = pb.Gauge.GetValue()
		}
		pairs := []string{}
		for _, l := range pb.Label {
			pairs = append(pairs, l.GetName()+"="+l.GetValue())
		}
		sort.Strings(pairs)
		got[descName(m)] = sample{value: value, labels: strings.Join(pairs, ",")}
	}

	pairs := 0
	for name, withSuffix := range got {
		base, found := strings.CutSuffix(name, "_total")
		if !found {
			continue
		}
		withoutSuffix, ok := got[base]
		if !ok {
			continue
		}
		pairs++
		if withSuffix.value != withoutSuffix.value {
			t.Errorf("%s is %v and %s is %v, so they are no longer the same measurement",
				name, withSuffix.value, base, withoutSuffix.value)
		}
		if withSuffix.labels != withoutSuffix.labels {
			t.Errorf("%s carries %q and %s carries %q", name, withSuffix.labels, base, withoutSuffix.labels)
		}
	}
	// The pre-17 query does not produce checkpoints_done or slru_written, so
	// eleven of the thirteen pairs appear on this path. The count of pairs
	// that exist at all is checked from the inventory below, which does not
	// depend on a version.
	if pairs != 11 {
		t.Errorf("found %d duplicated pairs on the pre-17 path, expected 11", pairs)
	}
}

// TestThirteenMetricsAreDefinedTwice counts the duplication itself, from the
// descriptors rather than from one scrape, so the number in the catalogue does
// not depend on which version a test ran against.
func TestThirteenMetricsAreDefinedTwice(t *testing.T) {
	defs, err := readMetricInventory(".")
	if err != nil {
		t.Fatal(err)
	}
	defined := map[string]bool{}
	for _, d := range defs {
		defined[d.Name] = true
	}

	var twinned []string
	for name := range defined {
		if defined[name+"_total"] {
			twinned = append(twinned, name)
		}
	}
	sort.Strings(twinned)

	if len(twinned) != 13 {
		t.Errorf("%d metrics are defined both with and without _total, the catalogue records 13:\n  %s",
			len(twinned), strings.Join(twinned, "\n  "))
	}
	for _, name := range twinned {
		if !strings.HasPrefix(name, "pg_stat_bgwriter_") {
			t.Errorf("%s is defined twice and is outside the bgwriter family, which is the only place the catalogue records this", name)
		}
	}
}

// descName pulls the family name out of a descriptor's string form, which is
// the only place a written dto.Metric does not carry it.
func descName(m prometheus.Metric) string {
	desc := m.Desc().String()
	const marker = `fqName: "`
	start := strings.Index(desc, marker)
	if start < 0 {
		return desc
	}
	rest := desc[start+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return desc
	}
	return rest[:end]
}
