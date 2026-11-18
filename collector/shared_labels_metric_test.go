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

// Equivalence tests for the shared-label prototype.
//
// The benchmark that motivated this change compares two live runs, which can
// only establish that the same series IDENTITIES are published: it compares
// (family, labels) and ignores values, and the two runs scrape separately
// seeded databases whose values legitimately differ. That is not a correctness
// argument. These tests are.
//
// They compare the complete written dto.Metric -- label names, label values,
// the value itself and which oneof field carries it -- plus the descriptor, so
// the metric name is compared too, between sharedLabelsMetric and the
// prometheus.MustNewConstMetric call it replaces, for the same inputs.

package collector

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

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
// prototype relies on: for the same descriptor, type, value and label values,
// sharedLabelsMetric must write exactly what MustNewConstMetric writes.
func TestSharedLabelsMetricMatchesConstMetric(t *testing.T) {
	// Every descriptor in pg_stat_user_tables declares these three variable
	// labels in this order and no constant labels, which is what makes one
	// shared slice correct for all 24 families.
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

	// All 24 descriptors, so a descriptor that quietly disagrees about label
	// names or order is caught here rather than in production.
	descs := map[string]*prometheus.Desc{
		"seq_scan": statUserTablesSeqScan, "seq_tup_read": statUserTablesSeqTupRead,
		"idx_scan": statUserTablesIdxScan, "idx_tup_fetch": statUserTablesIdxTupFetch,
		"n_tup_ins": statUserTablesNTupIns, "n_tup_upd": statUserTablesNTupUpd,
		"n_tup_del": statUserTablesNTupDel, "n_tup_hot_upd": statUserTablesNTupHotUpd,
		"n_live_tup": statUserTablesNLiveTup, "n_dead_tup": statUserTablesNDeadTup,
		"n_mod_since_analyze":    statUserTablesNModSinceAnalyze,
		"last_vacuum":            statUserTablesLastVacuum,
		"last_autovacuum":        statUserTablesLastAutovacuum,
		"last_analyze":           statUserTablesLastAnalyze,
		"last_autoanalyze":       statUserTablesLastAutoanalyze,
		"vacuum_count":           statUserTablesVacuumCount,
		"autovacuum_count":       statUserTablesAutovacuumCount,
		"analyze_count":          statUserTablesAnalyzeCount,
		"autoanalyze_count":      statUserTablesAutoanalyzeCount,
		"total_size":             statUserTablesTotalSize,
		"total_vacuum_time":      statUserTablesTotalVacuumTime,
		"total_autovacuum_time":  statUserTablesTotalAutovacuumTime,
		"total_analyze_time":     statUserTablesTotalAnalyzeTime,
		"total_autoanalyze_time": statUserTablesTotalAutoanalyzeTime,
	}
	if len(descs) != 24 {
		t.Fatalf("expected 24 descriptors, listed %d", len(descs))
	}

	for name, desc := range descs {
		for _, lv := range labelSets {
			for _, vt := range types {
				for _, v := range values {
					want := writeDTO(t, prometheus.MustNewConstMetric(desc, vt, v, lv...))
					got := writeDTO(t, sharedLabelsMetric{
						desc:   desc,
						vtype:  vt,
						value:  v,
						labels: prometheus.MakeLabelPairs(desc, lv),
					})
					if err := sameDTO(want, got); err != nil {
						t.Errorf("%s labels=%q type=%v value=%v: %s", name, lv, vt, v, err)
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

// TestSharedLabelsAreNotMutated is the assumption the whole prototype rests on:
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
// fixture rows and compares all 24 emitted metrics, DTO and descriptor, against
// the MustNewConstMetric calls the shipped collector makes for the same row.
// This is the test the benchmark cannot be: same input, both constructions,
// everything compared.
func TestPGStatUserTablesCollectorDTOEquivalence(t *testing.T) {
	ts := func(s string) time.Time {
		v, err := time.Parse("2006-01-02Z", s)
		if err != nil {
			t.Fatalf("parsing %s: %s", s, err)
		}
		return v
	}
	lastVacuum := ts("2023-06-02Z")
	lastAutovacuum := ts("2023-06-03Z")
	lastAnalyze := ts("2023-06-04Z")
	lastAutoanalyze := ts("2023-06-05Z")

	columns := []string{
		"datname", "schemaname", "relname",
		"seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
		"n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
		"n_live_tup", "n_dead_tup", "n_mod_since_analyze",
		"last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze",
		"vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count",
		"total_size",
		"total_vacuum_time", "total_autovacuum_time", "total_analyze_time", "total_autoanalyze_time",
	}

	// Two rows, so the test also covers the shared slice being rebuilt per row
	// rather than leaking the previous row's labels into the next one.
	rows := sqlmock.NewRows(columns).
		AddRow("postgres", "public", "a_table",
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0,
			lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze,
			11, 12, 13, 14, 15, 16, 17, 18, 19).
		AddRow("otherdb", "app", "b_table",
			21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
			lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze,
			32, 33, 34, 35, 36, 37, 38, 39, 40)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()
	mock.ExpectQuery(sanitizeQuery(statUserTablesQueryPostPG18)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserTablesCollector{}
		if err := c.Update(context.Background(), &instance{db: db, version: pg18}, ch); err != nil {
			t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
		}
	}()

	type want struct {
		desc  *prometheus.Desc
		vtype prometheus.ValueType
		value float64
	}
	order := []*prometheus.Desc{
		statUserTablesSeqScan, statUserTablesSeqTupRead, statUserTablesIdxScan,
		statUserTablesIdxTupFetch, statUserTablesNTupIns, statUserTablesNTupUpd,
		statUserTablesNTupDel, statUserTablesNTupHotUpd, statUserTablesNLiveTup,
		statUserTablesNDeadTup, statUserTablesNModSinceAnalyze,
		statUserTablesLastVacuum, statUserTablesLastAutovacuum,
		statUserTablesLastAnalyze, statUserTablesLastAutoanalyze,
		statUserTablesVacuumCount, statUserTablesAutovacuumCount,
		statUserTablesAnalyzeCount, statUserTablesAutoanalyzeCount,
		statUserTablesTotalSize, statUserTablesTotalVacuumTime,
		statUserTablesTotalAutovacuumTime, statUserTablesTotalAnalyzeTime,
		statUserTablesTotalAutoanalyzeTime,
	}
	types := []prometheus.ValueType{
		prometheus.CounterValue, prometheus.CounterValue, prometheus.CounterValue,
		prometheus.CounterValue, prometheus.CounterValue, prometheus.CounterValue,
		prometheus.CounterValue, prometheus.CounterValue, prometheus.GaugeValue,
		prometheus.GaugeValue, prometheus.GaugeValue,
		prometheus.GaugeValue, prometheus.GaugeValue, prometheus.GaugeValue, prometheus.GaugeValue,
		prometheus.CounterValue, prometheus.CounterValue, prometheus.CounterValue,
		prometheus.CounterValue, prometheus.GaugeValue,
		prometheus.CounterValue, prometheus.CounterValue, prometheus.CounterValue, prometheus.CounterValue,
	}

	rowCases := []struct {
		labels []string
		values []float64
	}{
		{
			labels: []string{"postgres", "public", "a_table"},
			values: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0,
				float64(lastVacuum.Unix()), float64(lastAutovacuum.Unix()),
				float64(lastAnalyze.Unix()), float64(lastAutoanalyze.Unix()),
				11, 12, 13, 14, 15, 16, 17, 18, 19},
		},
		{
			labels: []string{"otherdb", "app", "b_table"},
			values: []float64{21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
				float64(lastVacuum.Unix()), float64(lastAutovacuum.Unix()),
				float64(lastAnalyze.Unix()), float64(lastAutoanalyze.Unix()),
				32, 33, 34, 35, 36, 37, 38, 39, 40},
		},
	}

	n := 0
	for _, rc := range rowCases {
		for i, desc := range order {
			got, ok := <-ch
			if !ok {
				t.Fatalf("collector stopped after %d metrics, expected %d", n, len(order)*len(rowCases))
			}
			n++
			w := want{desc: desc, vtype: types[i], value: rc.values[i]}
			if got.Desc() != w.desc {
				t.Fatalf("metric %d: descriptor mismatch\n got %s\nwant %s", n, got.Desc(), w.desc)
			}
			expected := prometheus.MustNewConstMetric(w.desc, w.vtype, w.value, rc.labels...)
			if err := sameDTO(writeDTO(t, expected), writeDTO(t, got)); err != nil {
				t.Errorf("metric %d (%s): %s", n, w.desc, err)
			}
		}
	}
	for range ch {
		n++
	}
	if n != len(order)*len(rowCases) {
		t.Errorf("emitted %d metrics, expected %d", n, len(order)*len(rowCases))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
