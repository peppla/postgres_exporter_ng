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
// New in postgres_exporter_ng, 2026. No copyright line: this is newly written
// code and its holder is not settled, so none is asserted. Write follows the
// shape of client_golang's populateMetric and constMetric.Write, which are
// Apache 2.0 and copyright The Prometheus Authors.

// One set of label pairs shared by every metric built from the same database
// row.
//
// prometheus.MustNewConstMetric calls MakeLabelPairs for each metric, which
// allocates a slice, one dto.LabelPair per label, two string cells per pair,
// and sorts the result. A collector publishing many families from one row --
// pg_stat_user_tables publishes 24, all carrying the identical
// datname/schemaname/relname triple -- pays that per family to express one
// answer: 24 slices, 72 pairs and 144 string cells per row, where 1, 3 and 6
// would do. sharedLabelsMetric pays it once per row instead.
//
// Two conditions make that sound, and shared_labels_metric_test.go holds both
// to account rather than assuming them.
//
// The pairs must be read-only once built. The registry treats what a metric
// writes as immutable and copies before sorting, which client_golang states in
// registry.go: "We cannot sort dtoMetric.Label in place as it is immutable by
// contract." Nor is relying on that a trick played on the library --
// MakeLabelPairs returns desc.constLabelPairs directly for a descriptor with no
// variable labels, so one slice shared by many metrics is already how it works.
//
// And every descriptor involved must declare the same variable labels in the
// same order and the same constant labels, since one descriptor builds the
// pairs all of them carry. That is a per-collector property, not a general one.
// The type therefore stays unexported, and each caller establishes it: for
// pg_stat_user_tables, TestSharedLabelsMetricMatchesConstMetric runs every
// descriptor the collector emits against the construction being replaced.

package collector

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// sharedLabelsMetric is a prometheus.Metric that carries label pairs it does
// not own. It is a struct rather than a constructor because there is nothing to
// validate at construction: prometheus.Desc does not expose its label names, so
// the agreement between desc and labels cannot be checked here. The caller
// establishes it, and a test proves it. See the package comment above.
//
// It is deliberately not exported. Exporting it would invite callers whose
// descriptors do not agree, and the only thing standing between that and wrong
// labels on real metrics is a test that knows which descriptors are involved.
type sharedLabelsMetric struct {
	desc   *prometheus.Desc
	vtype  prometheus.ValueType
	value  float64
	labels []*dto.LabelPair // shared across one row's metrics; never mutated
}

var _ prometheus.Metric = sharedLabelsMetric{}

// Desc returns this metric's own descriptor, not the one whose label names
// built the shared slice. The descriptor carries the metric name.
func (m sharedLabelsMetric) Desc() *prometheus.Desc { return m.desc }

// Write assigns the shared labels, which MakeLabelPairs already sorted by name,
// and replaces ALL THREE scalar fields rather than only the active one.
// dto.Metric's Counter, Gauge and Untyped are independent optional fields, not
// a protobuf oneof, so setting one clears nothing; constMetric.Write assigns
// all three for that reason and this matches it, which only shows on a reused
// DTO. An unsupported type returns an error and leaves out untouched.
func (m sharedLabelsMetric) Write(out *dto.Metric) error {
	v := m.value // a copy, so the pointer the DTO carries belongs to this metric
	// Built before anything is assigned, so an unsupported type cannot leave a
	// half-filled DTO behind.
	var (
		counter *dto.Counter
		gauge   *dto.Gauge
		untyped *dto.Untyped
	)
	switch m.vtype {
	case prometheus.CounterValue:
		counter = &dto.Counter{Value: &v}
	case prometheus.GaugeValue:
		gauge = &dto.Gauge{Value: &v}
	case prometheus.UntypedValue:
		untyped = &dto.Untyped{Value: &v}
	default:
		// An error rather than the panic MustNewConstMetric would raise: the
		// registry reports a failed collection instead of taking the process
		// down. Histogram and Summary are left alone, as constMetric leaves
		// them.
		return fmt.Errorf("sharedLabelsMetric: unsupported value type %v", m.vtype)
	}
	out.Label = m.labels
	out.Counter, out.Gauge, out.Untyped = counter, gauge, untyped
	return nil
}
