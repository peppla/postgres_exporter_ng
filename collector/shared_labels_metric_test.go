// Copyright 2023 The Prometheus Authors
// SPDX-License-Identifier: Apache-2.0
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
// Adapted for postgres_exporter_ng in 2026 from
// collector/pg_stat_user_tables_test.go, whose column fixture and sqlmock
// setup this reuses. The comparisons themselves are new, and no copyright is
// asserted over them here.

// Equivalence tests for the label pairs shared across one row's metrics.
//
// A benchmark can compare two live runs, but that only establishes that the
// same series IDENTITIES are published: it compares (family, labels) and leaves
// the values alone, and two runs of a live server legitimately differ in them.
// That is not a correctness argument. These tests are.
//
// They compare the complete written dto.Metric -- label names, label values,
// the value itself and which scalar field carries it -- plus the descriptor, so
// the metric name is compared too, between sharedLabelsMetric and the
// prometheus.MustNewConstMetric call it replaces, for the same inputs.

package collector

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// statUserTablesEmitted is every metric PGStatUserTablesCollector produces from
// one row, in the order it produces them, with the value type it uses.
//
// It is one list on purpose. The substitution test needs every descriptor, and
// the collector test needs the order and the types; keeping them apart lets
// them drift, and a descriptor missing from either list is a descriptor nothing
// checks. A metric added to the collector has to be added here, and both tests
// then cover it.
var statUserTablesEmitted = []struct {
	name  string
	desc  *prometheus.Desc
	vtype prometheus.ValueType
}{
	{"seq_scan", statUserTablesSeqScan, prometheus.CounterValue},
	{"seq_tup_read", statUserTablesSeqTupRead, prometheus.CounterValue},
	{"idx_scan", statUserTablesIdxScan, prometheus.CounterValue},
	{"idx_tup_fetch", statUserTablesIdxTupFetch, prometheus.CounterValue},
	{"n_tup_ins", statUserTablesNTupIns, prometheus.CounterValue},
	{"n_tup_upd", statUserTablesNTupUpd, prometheus.CounterValue},
	{"n_tup_del", statUserTablesNTupDel, prometheus.CounterValue},
	{"n_tup_hot_upd", statUserTablesNTupHotUpd, prometheus.CounterValue},
	{"n_live_tup", statUserTablesNLiveTup, prometheus.GaugeValue},
	{"n_dead_tup", statUserTablesNDeadTup, prometheus.GaugeValue},
	{"n_mod_since_analyze", statUserTablesNModSinceAnalyze, prometheus.GaugeValue},
	{"last_vacuum", statUserTablesLastVacuum, prometheus.GaugeValue},
	{"last_autovacuum", statUserTablesLastAutovacuum, prometheus.GaugeValue},
	{"last_analyze", statUserTablesLastAnalyze, prometheus.GaugeValue},
	{"last_autoanalyze", statUserTablesLastAutoanalyze, prometheus.GaugeValue},
	{"vacuum_count", statUserTablesVacuumCount, prometheus.CounterValue},
	{"autovacuum_count", statUserTablesAutovacuumCount, prometheus.CounterValue},
	{"analyze_count", statUserTablesAnalyzeCount, prometheus.CounterValue},
	{"autoanalyze_count", statUserTablesAutoanalyzeCount, prometheus.CounterValue},
	{"total_size", statUserTablesTotalSize, prometheus.GaugeValue},
	{"total_vacuum_time", statUserTablesTotalVacuumTime, prometheus.CounterValue},
	{"total_autovacuum_time", statUserTablesTotalAutovacuumTime, prometheus.CounterValue},
	{"total_analyze_time", statUserTablesTotalAnalyzeTime, prometheus.CounterValue},
	{"total_autoanalyze_time", statUserTablesTotalAutoanalyzeTime, prometheus.CounterValue},
}

func writeDTO(t *testing.T, m prometheus.Metric) *dto.Metric {
	t.Helper()
	pb := &dto.Metric{}
	if err := m.Write(pb); err != nil {
		t.Fatalf("Write: %s", err)
	}
	return pb
}

// sameDTO compares every field of a written metric that the exposition format
// can carry, rather than trusting proto.Equal to mean what we want here.
func sameDTO(a, b *dto.Metric) error {
	if len(a.Label) != len(b.Label) {
		return fmt.Errorf("label count: %d != %d", len(a.Label), len(b.Label))
	}
	for i := range a.Label {
		if a.Label[i].GetName() != b.Label[i].GetName() {
			return fmt.Errorf("label %d name: %q != %q", i, a.Label[i].GetName(), b.Label[i].GetName())
		}
		if a.Label[i].GetValue() != b.Label[i].GetValue() {
			return fmt.Errorf("label %d value: %q != %q", i, a.Label[i].GetValue(), b.Label[i].GetValue())
		}
	}
	type part struct {
		name  string
		set   func(*dto.Metric) bool
		value func(*dto.Metric) float64
	}
	parts := []part{
		{"counter", func(m *dto.Metric) bool { return m.Counter != nil }, func(m *dto.Metric) float64 { return m.GetCounter().GetValue() }},
		{"gauge", func(m *dto.Metric) bool { return m.Gauge != nil }, func(m *dto.Metric) float64 { return m.GetGauge().GetValue() }},
		{"untyped", func(m *dto.Metric) bool { return m.Untyped != nil }, func(m *dto.Metric) float64 { return m.GetUntyped().GetValue() }},
	}
	for _, p := range parts {
		if p.set(a) != p.set(b) {
			return fmt.Errorf("%s set: %v != %v", p.name, p.set(a), p.set(b))
		}
		if !p.set(a) {
			continue
		}
		x, y := p.value(a), p.value(b)
		if math.IsNaN(x) && math.IsNaN(y) {
			continue
		}
		if x != y {
			return fmt.Errorf("%s value: %v != %v", p.name, x, y)
		}
	}
	return nil
}

// TestSharedLabelsMetricMatchesConstMetric is the substitution property the
// change relies on: for the same descriptor, type, value and label values,
// sharedLabelsMetric must write exactly what MustNewConstMetric writes.
//
// The shared side is built ONCE per label set from statUserTablesLabelTemplate
// and then handed to every descriptor, which is what the collector does. That
// detail is the whole test. Building it from each descriptor in turn would
// compare every descriptor against itself and prove nothing about the template
// working for all of them -- which is the claim the change actually rests on.
func TestSharedLabelsMetricMatchesConstMetric(t *testing.T) {
	// Every descriptor in pg_stat_user_tables declares these three variable
	// labels in this order and no constant labels.
	labelSets := [][]string{
		{"postgres", "public", "a_table"},
		{"", "", ""},                             // empty values
		{"zzz", "aaa", "mmm"},                    // value order fights name order
		{"db-1", "schema with spaces", "táblë"},  // non-ASCII
		{"a\"quote", "back\\slash", "new\nline"}, // characters the text format escapes
		{strings.Repeat("x", 300), "s", "r"},     // long value
	}
	values := []float64{0, 1, -1, 1685664000, math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.NaN(), math.Inf(1), math.Inf(-1)}
	types := []prometheus.ValueType{prometheus.CounterValue, prometheus.GaugeValue, prometheus.UntypedValue}

	if len(statUserTablesEmitted) != 24 {
		t.Fatalf("expected 24 descriptors, listed %d", len(statUserTablesEmitted))
	}

	for _, lv := range labelSets {
		// One slice, from the template descriptor, for every family below --
		// exactly as the collector builds one per row.
		shared := prometheus.MakeLabelPairs(statUserTablesLabelTemplate, lv)
		for _, e := range statUserTablesEmitted {
			for _, vt := range types {
				for _, v := range values {
					want := writeDTO(t, prometheus.MustNewConstMetric(e.desc, vt, v, lv...))
					got := writeDTO(t, sharedLabelsMetric{
						desc:   e.desc,
						vtype:  vt,
						value:  v,
						labels: shared,
					})
					if err := sameDTO(want, got); err != nil {
						t.Errorf("%s labels=%q type=%v value=%v: %s", e.name, lv, vt, v, err)
					}
				}
			}
		}
	}
}

// TestSharedLabelsMetricDescriptorIsUnchanged checks the half sameDTO cannot
// see: dto.Metric carries no metric name, so the name travels in the
// descriptor and has to be compared separately.
func TestSharedLabelsMetricDescriptorIsUnchanged(t *testing.T) {
	lv := []string{"postgres", "public", "a_table"}
	for _, desc := range []*prometheus.Desc{statUserTablesSeqScan, statUserTablesNLiveTup, statUserTablesTotalSize} {
		m := sharedLabelsMetric{desc: desc, vtype: prometheus.GaugeValue, value: 1,
			labels: prometheus.MakeLabelPairs(desc, lv)}
		if m.Desc() != desc {
			t.Errorf("Desc() returned a different descriptor")
		}
		want := prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1, lv...)
		if m.Desc().String() != want.Desc().String() {
			t.Errorf("descriptor text differs:\n got %s\nwant %s", m.Desc().String(), want.Desc().String())
		}
	}
}

// TestSharedLabelsMetricUnsupportedType pins the error path, so a future value
// type cannot be silently dropped.
func TestSharedLabelsMetricUnsupportedType(t *testing.T) {
	m := sharedLabelsMetric{
		desc:   statUserTablesSeqScan,
		vtype:  prometheus.ValueType(99),
		value:  1,
		labels: prometheus.MakeLabelPairs(statUserTablesSeqScan, []string{"a", "b", "c"}),
	}
	if err := m.Write(&dto.Metric{}); err == nil {
		t.Fatal("expected an error for an unsupported value type, got nil")
	}
}

// TestSharedLabelsAreNotMutated is the assumption the whole change rests on:
// that nothing downstream writes to the slice every metric from a row points
// at. client_golang states the contract in registry.go -- "We cannot sort
// dtoMetric.Label in place as it is immutable by contract" -- and copies before
// sorting. This test holds it to that, through a real Gather.
func TestSharedLabelsAreNotMutated(t *testing.T) {
	lv := []string{"postgres", "public", "a_table"}
	shared := prometheus.MakeLabelPairs(statUserTablesSeqScan, lv)

	type snap struct{ name, value string }
	before := make([]snap, len(shared))
	for i, lp := range shared {
		before[i] = snap{lp.GetName(), lp.GetValue()}
	}
	pointers := make([]*dto.LabelPair, len(shared))
	copy(pointers, shared)

	descs := []*prometheus.Desc{
		statUserTablesSeqScan, statUserTablesSeqTupRead, statUserTablesNLiveTup,
		statUserTablesNDeadTup, statUserTablesTotalSize,
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.CollectorFunc(func(ch chan<- prometheus.Metric) {
		for i, d := range descs {
			vt := prometheus.CounterValue
			if i%2 == 1 {
				vt = prometheus.GaugeValue
			}
			ch <- sharedLabelsMetric{desc: d, vtype: vt, value: float64(i), labels: shared}
		}
	}))

	for i := 0; i < 2; i++ {
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatalf("Gather: %s", err)
		}
		if len(mfs) != len(descs) {
			t.Fatalf("expected %d families, got %d", len(descs), len(mfs))
		}
		for _, mf := range mfs {
			for _, m := range mf.Metric {
				if len(m.Label) != 3 {
					t.Fatalf("%s: expected 3 labels, got %d", mf.GetName(), len(m.Label))
				}
			}
		}
	}

	for i, lp := range shared {
		if lp != pointers[i] {
			t.Errorf("label pair %d was replaced in the shared slice", i)
		}
		if lp.GetName() != before[i].name || lp.GetValue() != before[i].value {
			t.Errorf("label pair %d was mutated: %s=%s became %s=%s",
				i, before[i].name, before[i].value, lp.GetName(), lp.GetValue())
		}
	}
}

// TestPGStatUserTablesCollectorDTOEquivalence runs the collector itself over
// fixture rows and checks that every metric it emits writes the same DTO as
// the MustNewConstMetric call it replaced. This is the test a benchmark cannot
// be: same input, both constructions, everything compared.
//
// It derives the comparison from what the collector emitted rather than from a
// list of expected metrics. The earlier version carried the metric names, the
// column set and the per-version counts by hand, so adding a column to the
// query broke the test for reasons that had nothing to do with the invariant
// it exists to hold. What matters is narrower than the metric list: for each
// emitted metric, a const metric built from the same descriptor, type, value
// and labels must write an identical DTO.
//
// Both version paths run, because the collector assembles a different query
// for each and every other test in this collector uses 18, which leaves the
// path most installations take unchecked. Two rows each, because a version
// that built the labels once and reused them for every row would pass a
// single-row test.
func TestPGStatUserTablesCollectorDTOEquivalence(t *testing.T) {
	for _, vc := range []struct {
		name    string
		version semver.Version
	}{
		{"pre-18", semver.MustParse("17.0.0")},
		{"18-and-later", pg18},
	} {
		t.Run(vc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("Error opening a stub db connection: %s", err)
			}
			defer db.Close()

			rows := sqlmock.NewRows(userTablesColumns).
				AddRow(userTablesRow("a_table")...).
				AddRow(userTablesRow("b_table")...)
			mock.ExpectQuery(sanitizeQuery(statUserTablesQuery(vc.version))).WillReturnRows(rows)

			ch := make(chan prometheus.Metric)
			go func() {
				defer close(ch)
				c := PGStatUserTablesCollector{}
				if err := c.Update(context.Background(), &instance{db: db, version: vc.version}, ch); err != nil {
					t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
				}
			}()

			seenRelations := map[string]bool{}
			n := 0
			for got := range ch {
				n++
				dto := writeDTO(t, got)

				// Rebuild the same metric the old code would have built, from
				// what this one actually wrote.
				//
				// A written DTO carries its label pairs sorted by name, while
				// MustNewConstMetric wants them in the descriptor's declared
				// order, so they are looked up by name rather than copied in
				// the order they arrive.
				byName := map[string]string{}
				for _, l := range dto.Label {
					byName[l.GetName()] = l.GetValue()
				}
				seenRelations[byName["relname"]] = true
				labels := []string{byName["datname"], byName["schemaname"], byName["relname"]}
				var vtype prometheus.ValueType
				var value float64
				switch {
				case dto.Counter != nil:
					vtype, value = prometheus.CounterValue, dto.Counter.GetValue()
				case dto.Gauge != nil:
					vtype, value = prometheus.GaugeValue, dto.Gauge.GetValue()
				default:
					t.Fatalf("metric %d (%s) is neither a counter nor a gauge", n, got.Desc())
				}

				constructed := prometheus.MustNewConstMetric(got.Desc(), vtype, value, labels...)
				if err := sameDTO(writeDTO(t, constructed), dto); err != nil {
					t.Errorf("metric %d (%s): %s", n, got.Desc(), err)
				}
			}

			if n == 0 {
				t.Fatal("the collector emitted nothing, so nothing was compared")
			}
			// Both rows have to appear, or the second row's labels were never
			// built and the reuse bug this test exists for would pass.
			for _, rel := range []string{"a_table", "b_table"} {
				if !seenRelations[rel] {
					t.Errorf("no metric carried relname=%q, so only one row was emitted", rel)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		})
	}
}

// TestSharedLabelsMetricWriteClearsInactiveFields pins the one place a naive
// implementation differs from the construction it replaces.
//
// constMetric.Write assigns Counter, Gauge AND Untyped on every call, so two of
// the three are set to nil each time. A Write that only set the active field
// would leave a stale value behind on a DTO that had been written to before.
// The registry hands out a fresh dto.Metric per Gather, so nothing observable
// depends on this today. It is tested so the two constructions cannot drift
// apart if that ever changes.
func TestSharedLabelsMetricWriteClearsInactiveFields(t *testing.T) {
	lv := []string{"postgres", "public", "a_table"}
	shared := prometheus.MakeLabelPairs(statUserTablesLabelTemplate, lv)
	types := []prometheus.ValueType{prometheus.CounterValue, prometheus.GaugeValue, prometheus.UntypedValue}

	for _, first := range types {
		for _, second := range types {
			want := &dto.Metric{}
			for i, vt := range []prometheus.ValueType{first, second} {
				m := prometheus.MustNewConstMetric(statUserTablesSeqScan, vt, float64(i), lv...)
				if err := m.Write(want); err != nil {
					t.Fatalf("const Write: %s", err)
				}
			}
			got := &dto.Metric{}
			for i, vt := range []prometheus.ValueType{first, second} {
				m := sharedLabelsMetric{desc: statUserTablesSeqScan, vtype: vt, value: float64(i), labels: shared}
				if err := m.Write(got); err != nil {
					t.Fatalf("shared Write: %s", err)
				}
			}
			if err := sameDTO(want, got); err != nil {
				t.Errorf("%v then %v into one DTO: %s", first, second, err)
			}
		}
	}
}

// TestSharedLabelsRenderIdenticalText is the end of the argument the DTO
// comparisons start. Those compare what Write produces; this compares what a
// scraper receives.
//
// Two registries collect the same families over the same rows, one building
// every metric with MustNewConstMetric and the other sharing one slice per row,
// and both are rendered through the exposition encoders -- text and
// OpenMetrics. The rendered bytes must be equal: same HELP and TYPE, same
// family grouping and order, same label order, same escaping, same value
// formatting.
//
// The label values here are chosen to make the encoder work: quotes,
// backslashes and newlines are escaped in the text format, and a value that
// sorts against its own label name would expose an ordering mistake.
func TestSharedLabelsRenderIdenticalText(t *testing.T) {
	rows := [][]string{
		{"postgres", "public", "a_table"},
		{"otherdb", "zzz_schema", "aaa_table"},
		{"db\"quoted", "back\\slash", "new\nline"},
		{"", "", ""},
		{"ünïcode", "schema with spaces", strings.Repeat("x", 300)},
	}
	values := []float64{0, 1, -1, 1685664000, math.Inf(1)}

	render := func(shared bool, format expfmt.Format) string {
		t.Helper()
		reg := prometheus.NewRegistry()
		reg.MustRegister(prometheus.CollectorFunc(func(ch chan<- prometheus.Metric) {
			for ri, lv := range rows {
				var labels []*dto.LabelPair
				if shared {
					labels = prometheus.MakeLabelPairs(statUserTablesSeqScan, lv)
				}
				for i, e := range statUserTablesEmitted {
					v := values[(ri+i)%len(values)]
					if shared {
						ch <- sharedLabelsMetric{desc: e.desc, vtype: e.vtype, value: v, labels: labels}
					} else {
						ch <- prometheus.MustNewConstMetric(e.desc, e.vtype, v, lv...)
					}
				}
			}
		}))
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatalf("Gather: %s", err)
		}
		var buf bytes.Buffer
		enc := expfmt.NewEncoder(&buf, format)
		for _, mf := range mfs {
			if err := enc.Encode(mf); err != nil {
				t.Fatalf("Encode: %s", err)
			}
		}
		if c, ok := enc.(expfmt.Closer); ok {
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %s", err)
			}
		}
		return buf.String()
	}

	// Both formats a scraper can ask this exporter for. OpenMetrics is the one
	// that would expose a difference in how the value field is set, since it
	// renders counters and gauges differently.
	for _, f := range []struct {
		name   string
		format expfmt.Format
	}{
		{"text", expfmt.NewFormat(expfmt.TypeTextPlain)},
		{"openmetrics", expfmt.NewFormat(expfmt.TypeOpenMetrics)},
	} {
		t.Run(f.name, func(t *testing.T) {
			want, got := render(false, f.format), render(true, f.format)
			if want == "" {
				t.Fatal("rendered nothing")
			}
			if got != want {
				wl, gl := strings.Split(want, "\n"), strings.Split(got, "\n")
				for i := 0; i < len(wl) && i < len(gl); i++ {
					if wl[i] != gl[i] {
						t.Fatalf("line %d differs:\n got %q\nwant %q", i+1, gl[i], wl[i])
					}
				}
				t.Fatalf("output lengths differ: %d lines against %d", len(gl), len(wl))
			}
		})
	}
}

// userTablesRow builds one fixture row for userTablesColumns, with a given
// relation name so two rows can be told apart.
//
// Values are arbitrary and distinct; what the equivalence test checks is that
// whatever comes out is written identically by both constructions, not what
// the numbers are.
func userTablesRow(relname string) []driver.Value {
	ts := func(s string) time.Time {
		v, _ := time.Parse("2006-01-02Z", s)
		return v
	}
	return []driver.Value{
		"postgres", "public", relname,
		1, 2, 3, 4, // seq_scan, seq_tup_read, idx_scan, idx_tup_fetch
		5, 6, 7, 8, // n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd
		9, 10, 0, 27, // n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum
		ts("2023-06-02Z"), ts("2023-06-03Z"), ts("2023-06-04Z"), ts("2023-06-05Z"),
		11, 12, 13, 14, // vacuum_count, autovacuum_count, analyze_count, autoanalyze_count
		15,             // total_size
		20,             // n_tup_newpage_upd
		21, 22, 23, 24, // relpages, reltuples, relallvisible, relallfrozen
		25, 26, // relfrozenxid_age, relminmxid_age
		16, 17, 18, 19, // the four total_*_time columns
	}
}
