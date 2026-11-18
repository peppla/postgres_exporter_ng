// Prototype: share one set of label pairs across every metric built from the
// same database row.
//
// prometheus.MustNewConstMetric calls MakeLabelPairs for each metric, which
// allocates a slice, one dto.LabelPair per label, two proto string cells per
// pair, and sorts the result. pg_stat_user_tables publishes 24 families from
// one row, all carrying the identical datname/schemaname/relname triple, so
// that work is done 24 times for one answer: 24 slices, 72 pairs and 144 string
// cells per row, where 1, 3 and 6 would do.
//
// sharedLabelsMetric does the label work once per row and lets every family
// from that row point at the result. The pairs are read-only after
// construction: Write only assigns the slice into the outgoing dto.Metric, and
// the registry sorts metrics within a family by reading label values, never by
// mutating them.

package collector

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

type sharedLabelsMetric struct {
	desc   *prometheus.Desc
	vtype  prometheus.ValueType
	value  float64
	labels []*dto.LabelPair // shared across one row's metrics; never mutated
}

func (m sharedLabelsMetric) Desc() *prometheus.Desc { return m.desc }

func (m sharedLabelsMetric) Write(out *dto.Metric) error {
	out.Label = m.labels
	switch m.vtype {
	case prometheus.CounterValue:
		out.Counter = &dto.Counter{Value: proto.Float64(m.value)}
	case prometheus.GaugeValue:
		out.Gauge = &dto.Gauge{Value: proto.Float64(m.value)}
	case prometheus.UntypedValue:
		out.Untyped = &dto.Untyped{Value: proto.Float64(m.value)}
	default:
		return fmt.Errorf("sharedLabelsMetric: unsupported value type %v", m.vtype)
	}
	return nil
}
