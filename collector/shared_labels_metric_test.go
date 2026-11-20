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
// minMajor is the first PostgreSQL major version that produces the family. A
// zero means every supported version does. A gated family is omitted below its
// version rather than published as a zero, which is the distinction the
// equivalence test's expected sets rest on.
var statUserTablesEmitted = []struct {
	name     string
	desc     *prometheus.Desc
	vtype    prometheus.ValueType
	minMajor uint64
}{
	{"seq_scan", statUserTablesSeqScan, prometheus.CounterValue, 0},
	{"seq_tup_read", statUserTablesSeqTupRead, prometheus.CounterValue, 0},
	{"idx_scan", statUserTablesIdxScan, prometheus.CounterValue, 0},
	{"idx_tup_fetch", statUserTablesIdxTupFetch, prometheus.CounterValue, 0},
	{"n_tup_ins", statUserTablesNTupIns, prometheus.CounterValue, 0},
	{"n_tup_upd", statUserTablesNTupUpd, prometheus.CounterValue, 0},
	{"n_tup_del", statUserTablesNTupDel, prometheus.CounterValue, 0},
	{"n_tup_hot_upd", statUserTablesNTupHotUpd, prometheus.CounterValue, 0},
	{"n_live_tup", statUserTablesNLiveTup, prometheus.GaugeValue, 0},
	{"n_dead_tup", statUserTablesNDeadTup, prometheus.GaugeValue, 0},
	{"n_mod_since_analyze", statUserTablesNModSinceAnalyze, prometheus.GaugeValue, 0},
	{"n_ins_since_vacuum", statUserTablesNInsSinceVacuum, prometheus.GaugeValue, 0},
	{"last_vacuum", statUserTablesLastVacuum, prometheus.GaugeValue, 0},
	{"last_autovacuum", statUserTablesLastAutovacuum, prometheus.GaugeValue, 0},
	{"last_analyze", statUserTablesLastAnalyze, prometheus.GaugeValue, 0},
	{"last_autoanalyze", statUserTablesLastAutoanalyze, prometheus.GaugeValue, 0},
	{"vacuum_count", statUserTablesVacuumCount, prometheus.CounterValue, 0},
	{"autovacuum_count", statUserTablesAutovacuumCount, prometheus.CounterValue, 0},
	{"analyze_count", statUserTablesAnalyzeCount, prometheus.CounterValue, 0},
	{"autoanalyze_count", statUserTablesAutoanalyzeCount, prometheus.CounterValue, 0},
	{"total_size", statUserTablesTotalSize, prometheus.GaugeValue, 0},
	{"n_tup_newpage_upd", statUserTablesNTupNewpageUpd, prometheus.CounterValue, 16},
	{"relpages", statUserTablesRelpages, prometheus.GaugeValue, 0},
	{"reltuples", statUserTablesReltuples, prometheus.GaugeValue, 0},
	{"relallvisible", statUserTablesRelallvisible, prometheus.GaugeValue, 0},
	{"relallfrozen", statUserTablesRelallfrozen, prometheus.GaugeValue, 18},
	{"relfrozenxid_age", statUserTablesRelfrozenxidAge, prometheus.GaugeValue, 0},
	{"relminmxid_age", statUserTablesRelminmxidAge, prometheus.GaugeValue, 0},
	{"total_vacuum_time", statUserTablesTotalVacuumTime, prometheus.CounterValue, 18},
	{"total_autovacuum_time", statUserTablesTotalAutovacuumTime, prometheus.CounterValue, 18},
	{"total_analyze_time", statUserTablesTotalAnalyzeTime, prometheus.CounterValue, 18},
	{"total_autoanalyze_time", statUserTablesTotalAutoanalyzeTime, prometheus.CounterValue, 18},
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

	if len(statUserTablesEmitted) != 32 {
		t.Fatalf("expected 32 descriptors, listed %d", len(statUserTablesEmitted))
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
// fixture rows and checks what it emits against an expected result written
// here, not read back out of the collector.
//
// Three things are checked at once, because one DTO comparison covers all
// three: every family carries the column this test says it carries, with the
// value type this test says it has and the label values the fixture row went
// in with, and the metric the collector built writes the same DTO as the
// MustNewConstMetric call it replaced.
//
// The expected set is also checked both ways. A family that disappears from
// the collector is a missing entry, and a family that appears below its
// version gate is an unexpected one. Neither can be absorbed by a comparison
// that only looks at what arrived.
//
// Both version paths run, because the collector assembles a different query
// for each and every other test in this collector uses 18, which leaves the
// path most installations take unchecked. Two rows each, with no value in
// common, because a version that built one row's numbers or labels and reused
// them for the next would pass a single-row test.
func TestPGStatUserTablesCollectorDTOEquivalence(t *testing.T) {
	byDesc := map[*prometheus.Desc]int{}
	byFamily := map[string]int{}
	for i, e := range statUserTablesEmitted {
		byDesc[e.desc] = i
		byFamily[e.name] = i
	}

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

			rows := sqlmock.NewRows(userTablesColumns)
			for _, f := range userTablesFixtures {
				rows = rows.AddRow(f.row()...)
			}
			mock.ExpectQuery(sanitizeQuery(statUserTablesQuery(vc.version))).WillReturnRows(rows)

			ch := make(chan prometheus.Metric)
			go func() {
				defer close(ch)
				c := PGStatUserTablesCollector{}
				if err := c.Update(context.Background(), &instance{db: db, version: vc.version}, ch); err != nil {
					t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
				}
			}()

			// relname -> family name -> what the collector wrote.
			emitted := map[string]map[string]*dto.Metric{}
			total := 0
			for m := range ch {
				total++
				i, known := byDesc[m.Desc()]
				if !known {
					t.Fatalf("metric %d carries a descriptor no family in statUserTablesEmitted claims: %s", total, m.Desc())
				}
				written := writeDTO(t, m)

				// A written DTO carries its label pairs sorted by name, so the
				// row a metric belongs to is looked up by label name rather
				// than by position.
				relname := ""
				for _, l := range written.Label {
					if l.GetName() == "relname" {
						relname = l.GetValue()
					}
				}
				if emitted[relname] == nil {
					emitted[relname] = map[string]*dto.Metric{}
				}
				family := statUserTablesEmitted[i].name
				if _, repeated := emitted[relname][family]; repeated {
					t.Errorf("%s was emitted twice for relname=%q", family, relname)
				}
				emitted[relname][family] = written
			}

			wantTotal := 0
			for _, f := range userTablesFixtures {
				want := f.expected(vc.version.Major)
				wantTotal += len(want)

				rowEmitted, ok := emitted[f.relname]
				if !ok {
					t.Errorf("nothing was emitted for relname=%q", f.relname)
					continue
				}

				for family, wantValue := range want {
					written, ok := rowEmitted[family]
					if !ok {
						t.Errorf("relname=%q: %s was not emitted", f.relname, family)
						continue
					}
					e := statUserTablesEmitted[byFamily[family]]

					// The expected metric is built from this test's own
					// descriptor, type, value and label values, which is what
					// makes this a reference rather than a restatement.
					expected := prometheus.MustNewConstMetric(
						e.desc, e.vtype, wantValue,
						f.datname, f.schemaname, f.relname,
					)
					if err := sameDTO(writeDTO(t, expected), written); err != nil {
						t.Errorf("relname=%q %s: %s", f.relname, family, err)
					}
				}

				for family := range rowEmitted {
					if _, expected := want[family]; !expected {
						t.Errorf("relname=%q: %s was emitted on PostgreSQL %s, where it is not available",
							f.relname, family, vc.version)
					}
				}
			}

			if total != wantTotal {
				t.Errorf("the collector emitted %d metrics, expected %d", total, wantTotal)
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

// userTablesFixture is one input row for PGStatUserTablesCollector, written
// out field by field.
//
// Having the input as named fields is what lets the expected output be a
// separate statement about the same row. A fixture read back out of the
// collector's own output can only say that the output equals itself.
type userTablesFixture struct {
	datname, schemaname, relname string

	seqScan, seqTupRead, idxScan, idxTupFetch                    int64
	nTupIns, nTupUpd, nTupDel, nTupHotUpd                        int64
	nLiveTup, nDeadTup, nModSinceAnalyze, nInsSinceVacuum        int64
	lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze     time.Time
	vacuumCount, autovacuumCount, analyzeCount, autoanalyzeCount int64
	totalSize                                                    int64

	nTupNewpageUpd  int64
	relpages        int64
	reltuples       float64
	relallvisible   int64
	relallfrozen    int64
	relfrozenxidAge int64
	relminmxidAge   int64

	totalVacuumTime, totalAutovacuumTime, totalAnalyzeTime, totalAutoanalyzeTime float64
}

// row writes the fixture out in the 35-column order statUserTablesQuery
// selects and the collector scans.
func (f userTablesFixture) row() []driver.Value {
	return []driver.Value{
		f.datname, f.schemaname, f.relname,
		f.seqScan, f.seqTupRead, f.idxScan, f.idxTupFetch,
		f.nTupIns, f.nTupUpd, f.nTupDel, f.nTupHotUpd,
		f.nLiveTup, f.nDeadTup, f.nModSinceAnalyze, f.nInsSinceVacuum,
		f.lastVacuum, f.lastAutovacuum, f.lastAnalyze, f.lastAutoanalyze,
		f.vacuumCount, f.autovacuumCount, f.analyzeCount, f.autoanalyzeCount,
		f.totalSize,
		f.nTupNewpageUpd,
		f.relpages, f.reltuples, f.relallvisible, f.relallfrozen,
		f.relfrozenxidAge, f.relminmxidAge,
		f.totalVacuumTime, f.totalAutovacuumTime, f.totalAnalyzeTime, f.totalAutoanalyzeTime,
	}
}

// expected states which family carries which column, as a number, on a server
// of the given major version.
//
// This mapping is written by hand and belongs to the test. The timestamps
// become Unix-epoch seconds, which is the one conversion the metric contract
// makes between a column and its family.
//
// A family missing from this map on a version that produces it, or present on
// a version that does not, fails the test in one direction or the other.
func (f userTablesFixture) expected(major uint64) map[string]float64 {
	want := map[string]float64{
		"seq_scan":            float64(f.seqScan),
		"seq_tup_read":        float64(f.seqTupRead),
		"idx_scan":            float64(f.idxScan),
		"idx_tup_fetch":       float64(f.idxTupFetch),
		"n_tup_ins":           float64(f.nTupIns),
		"n_tup_upd":           float64(f.nTupUpd),
		"n_tup_del":           float64(f.nTupDel),
		"n_tup_hot_upd":       float64(f.nTupHotUpd),
		"n_live_tup":          float64(f.nLiveTup),
		"n_dead_tup":          float64(f.nDeadTup),
		"n_mod_since_analyze": float64(f.nModSinceAnalyze),
		"n_ins_since_vacuum":  float64(f.nInsSinceVacuum),
		"last_vacuum":         float64(f.lastVacuum.Unix()),
		"last_autovacuum":     float64(f.lastAutovacuum.Unix()),
		"last_analyze":        float64(f.lastAnalyze.Unix()),
		"last_autoanalyze":    float64(f.lastAutoanalyze.Unix()),
		"vacuum_count":        float64(f.vacuumCount),
		"autovacuum_count":    float64(f.autovacuumCount),
		"analyze_count":       float64(f.analyzeCount),
		"autoanalyze_count":   float64(f.autoanalyzeCount),
		"total_size":          float64(f.totalSize),
		"relpages":            float64(f.relpages),
		"reltuples":           f.reltuples,
		"relallvisible":       float64(f.relallvisible),
		"relfrozenxid_age":    float64(f.relfrozenxidAge),
		"relminmxid_age":      float64(f.relminmxidAge),
	}
	if major >= 16 {
		want["n_tup_newpage_upd"] = float64(f.nTupNewpageUpd)
	}
	if major >= 18 {
		want["relallfrozen"] = float64(f.relallfrozen)
		want["total_vacuum_time"] = f.totalVacuumTime
		want["total_autovacuum_time"] = f.totalAutovacuumTime
		want["total_analyze_time"] = f.totalAnalyzeTime
		want["total_autoanalyze_time"] = f.totalAutoanalyzeTime
	}
	return want
}

// userTablesFixtures are the two rows the equivalence test runs.
//
// No number, timestamp or label value appears in both rows, so a collector
// that built a value or a label set for the first row and reused it for the
// second fails rather than passes. The second row's reltuples is the -1 that
// PostgreSQL stores for a relation that has never been analyzed, which the
// collector passes through rather than flattening to zero.
var userTablesFixtures = []userTablesFixture{
	{
		datname: "postgres", schemaname: "public", relname: "a_table",
		seqScan: 1, seqTupRead: 2, idxScan: 3, idxTupFetch: 4,
		nTupIns: 5, nTupUpd: 6, nTupDel: 7, nTupHotUpd: 8,
		nLiveTup: 9, nDeadTup: 10, nModSinceAnalyze: 11, nInsSinceVacuum: 27,
		lastVacuum:      time.Unix(1685664000, 0).UTC(),
		lastAutovacuum:  time.Unix(1685750400, 0).UTC(),
		lastAnalyze:     time.Unix(1685836800, 0).UTC(),
		lastAutoanalyze: time.Unix(1685923200, 0).UTC(),
		vacuumCount:     12, autovacuumCount: 13, analyzeCount: 14, autoanalyzeCount: 15,
		totalSize:      16,
		nTupNewpageUpd: 20,
		relpages:       21, reltuples: 22.5, relallvisible: 23, relallfrozen: 24,
		relfrozenxidAge: 25, relminmxidAge: 26,
		totalVacuumTime: 17.5, totalAutovacuumTime: 18.25, totalAnalyzeTime: 19.75, totalAutoanalyzeTime: 28.125,
	},
	{
		datname: "otherdb", schemaname: "app", relname: "b_table",
		seqScan: 101, seqTupRead: 102, idxScan: 103, idxTupFetch: 104,
		nTupIns: 105, nTupUpd: 106, nTupDel: 107, nTupHotUpd: 108,
		nLiveTup: 109, nDeadTup: 110, nModSinceAnalyze: 111, nInsSinceVacuum: 127,
		lastVacuum:      time.Unix(1700000000, 0).UTC(),
		lastAutovacuum:  time.Unix(1700086400, 0).UTC(),
		lastAnalyze:     time.Unix(1700172800, 0).UTC(),
		lastAutoanalyze: time.Unix(1700259200, 0).UTC(),
		vacuumCount:     112, autovacuumCount: 113, analyzeCount: 114, autoanalyzeCount: 115,
		totalSize:      116,
		nTupNewpageUpd: 120,
		relpages:       121, reltuples: -1, relallvisible: 123, relallfrozen: 124,
		relfrozenxidAge: 125, relminmxidAge: 126,
		totalVacuumTime: 117.5, totalAutovacuumTime: 118.25, totalAnalyzeTime: 119.75, totalAutoanalyzeTime: 128.125,
	},
}
