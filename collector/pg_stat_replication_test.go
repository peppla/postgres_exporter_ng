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

package collector

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var statReplicationColumns = []string{
	"pid", "usename", "application_name", "client_addr", "state", "sync_state",
	"write_lsn_bytes", "flush_lsn_bytes", "replay_lsn_bytes",
	"write_lag_seconds", "flush_lag_seconds", "replay_lag_seconds",
}

// collectReplication runs the collector over one row and returns what it
// emitted, by metric name.
func collectReplication(t *testing.T, row []driver.Value) map[string]*dto.Metric {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery(sanitizeQuery(statReplicationQuery)).WillReturnRows(
		sqlmock.NewRows(statReplicationColumns).AddRow(row...))

	ch := make(chan prometheus.Metric, 32)
	c := PGStatReplicationCollector{}
	if err := c.Update(context.Background(), &instance{db: db}, ch); err != nil {
		t.Fatalf("Update: %s", err)
	}
	close(ch)

	out := map[string]*dto.Metric{}
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("writing a metric: %s", err)
		}
		out[descName(m)] = pb
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	return out
}

// TestStatReplicationOmitsNullLags pins the difference between a lag of zero
// and no lag yet.
//
// write_lag, flush_lag and replay_lag are NULL until the standby has reported
// at least once. Publishing zero there says the standby is perfectly caught
// up, which is the opposite of what the server is saying, and the alert that
// would have fired does not.
func TestStatReplicationOmitsNullLags(t *testing.T) {
	got := collectReplication(t, []driver.Value{
		int64(42), "replicator", "standby1", "10.0.0.2", "streaming", "async",
		int64(1000), int64(900), int64(800),
		nil, nil, nil,
	})

	for _, name := range []string{
		"pg_stat_replication_write_lag_seconds",
		"pg_stat_replication_flush_lag_seconds",
		"pg_stat_replication_replay_lag_seconds",
	} {
		if m, present := got[name]; present {
			t.Errorf("%s was published as %v for a standby that has not reported a lag yet",
				name, m.GetGauge().GetValue())
		}
	}

	// The positions are not NULL, so those are published. Without this the
	// test would pass against a collector that emitted nothing at all.
	for name, want := range map[string]float64{
		"pg_stat_replication_write_lsn_bytes":  1000,
		"pg_stat_replication_flush_lsn_bytes":  900,
		"pg_stat_replication_replay_lsn_bytes": 800,
	} {
		m, present := got[name]
		if !present {
			t.Errorf("%s was not published", name)
			continue
		}
		if m.GetGauge().GetValue() != want {
			t.Errorf("%s = %v, want %v", name, m.GetGauge().GetValue(), want)
		}
	}
}

// TestStatReplicationLabelsAStandbyWithNoAddress covers a standby connected
// over a Unix socket, where client_addr is NULL.
//
// An empty label value is what Prometheus treats as absent, which is the
// honest rendering of "this connection has no address". The word "unknown"
// that the older collectors in this package use is a value an operator can
// also legitimately have.
func TestStatReplicationLabelsAStandbyWithNoAddress(t *testing.T) {
	got := collectReplication(t, []driver.Value{
		int64(42), "replicator", "standby1", nil, "streaming", "async",
		int64(1000), int64(900), int64(800), 0.1, 0.2, 0.3,
	})

	m, present := got["pg_stat_replication_replay_lsn_bytes"]
	if !present {
		t.Fatal("nothing was published")
	}
	labels := map[string]string{}
	for _, l := range m.GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	if labels["client_addr"] != "" {
		t.Errorf("client_addr is %q for a connection with no address", labels["client_addr"])
	}
	if labels["application_name"] != "standby1" {
		t.Errorf("application_name is %q", labels["application_name"])
	}
	if !strings.Contains(labels["pid"], "42") {
		t.Errorf("pid is %q", labels["pid"])
	}
}
