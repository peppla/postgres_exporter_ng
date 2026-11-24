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

// What a WAL receiver publishes when the server reports a NULL.
//
// Every column used to skip the whole row when it was NULL. Two of them are
// NULL on an ordinary standby: slot_name, when the standby streams without a
// replication slot, and upstream_node, which is parsed out of the slot name
// with a repmgr pattern and is therefore NULL for every slot name that is not
// repmgr's and for every standby with no slot. So an ordinary streaming
// standby published nothing at all, and reported no failure while doing it.

package collector

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
)

// walReceiverRow runs the collector against one row of pg_stat_wal_receiver
// and returns what it published, by metric name.
func walReceiverRow(t *testing.T, slotName, upstreamNode interface{}) map[string]MetricResult {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery(sanitizeQuery(pgStatWalColumnQuery)).WillReturnRows(
		sqlmock.NewRows([]string{"column_name"}).AddRow("flushed_lsn"))

	columns := []string{
		"upstream_host", "slot_name", "status",
		"receive_start_lsn", "receive_start_tli", "flushed_lsn", "received_tli",
		"last_msg_send_time", "last_msg_receipt_time",
		"latest_end_lsn", "latest_end_time", "upstream_node",
	}
	mock.ExpectQuery(sanitizeQuery(queryWithFlushedLSN)).WillReturnRows(
		sqlmock.NewRows(columns).AddRow(
			"primary.example", slotName, "streaming",
			int64(1200668684563608), 1687321285, int64(1200668684563609), 1687321280,
			1687321275, 1687321276,
			int64(1200668684563610), 1687321277, upstreamNode,
		))

	ch := make(chan prometheus.Metric, 32)
	c := PGStatWalReceiverCollector{}
	if err := c.Update(context.Background(), &instance{db: db}, ch); err != nil {
		t.Fatalf("the collector failed: %s", err)
	}
	close(ch)

	out := map[string]MetricResult{}
	for m := range ch {
		out[descName(m)] = readMetric(m)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
	return out
}

// The measurements that are present whatever the slot is, which is what used
// to be lost with it.
var walReceiverMeasurements = []string{
	"pg_stat_wal_receiver_receive_start_lsn",
	"pg_stat_wal_receiver_receive_start_tli",
	"pg_stat_wal_receiver_flushed_lsn",
	"pg_stat_wal_receiver_received_tli",
	"pg_stat_wal_receiver_last_msg_send_time",
	"pg_stat_wal_receiver_last_msg_receipt_time",
	"pg_stat_wal_receiver_latest_end_lsn",
	"pg_stat_wal_receiver_latest_end_time",
}

const upstreamNodeMetric = "pg_stat_wal_receiver_upstream_node"

// TestWalReceiverWithoutASlotStillReportsItsMeasurements is the case ChatGPT
// found on a live standby: streaming, replaying, and publishing nothing.
func TestWalReceiverWithoutASlotStillReportsItsMeasurements(t *testing.T) {
	published := walReceiverRow(t, nil, nil)

	for _, name := range walReceiverMeasurements {
		m, ok := published[name]
		if !ok {
			t.Errorf("%s is missing for a standby streaming without a slot", name)
			continue
		}
		if got := m.labels["slot_name"]; got != "" {
			t.Errorf("%s carries slot_name %q for a standby with no slot", name, got)
		}
		if got := m.labels["upstream_host"]; got != "primary.example" {
			t.Errorf("%s carries upstream_host %q", name, got)
		}
		if got := m.labels["status"]; got != "streaming" {
			t.Errorf("%s carries status %q", name, got)
		}
	}
	if _, ok := published[upstreamNodeMetric]; ok {
		t.Error("upstream_node was published for a standby with no slot, so a missing node reads as node zero")
	}
}

// TestWalReceiverWithAnOrdinarySlotReportsItsMeasurements covers the slot name
// that is not repmgr's, which is every slot name outside one tool.
func TestWalReceiverWithAnOrdinarySlotReportsItsMeasurements(t *testing.T) {
	published := walReceiverRow(t, "standby_one", nil)

	for _, name := range walReceiverMeasurements {
		m, ok := published[name]
		if !ok {
			t.Errorf("%s is missing for a standby with an ordinary slot name", name)
			continue
		}
		if got := m.labels["slot_name"]; got != "standby_one" {
			t.Errorf("%s carries slot_name %q, want standby_one", name, got)
		}
	}
	if _, ok := published[upstreamNodeMetric]; ok {
		t.Error("upstream_node was published for a slot name that carries no node number")
	}
}

// TestWalReceiverWithARepmgrSlotReportsTheUpstreamNode keeps the one case that
// worked before working.
func TestWalReceiverWithARepmgrSlotReportsTheUpstreamNode(t *testing.T) {
	published := walReceiverRow(t, "repmgr_slot_3", 3)

	for _, name := range walReceiverMeasurements {
		if _, ok := published[name]; !ok {
			t.Errorf("%s is missing for a repmgr slot", name)
		}
	}
	m, ok := published[upstreamNodeMetric]
	if !ok {
		t.Fatal("upstream_node is missing for a repmgr slot, which is the one slot name it can be read from")
	}
	if m.value != 3 {
		t.Errorf("upstream_node is %v, want 3", m.value)
	}
	if got := m.labels["slot_name"]; got != "repmgr_slot_3" {
		t.Errorf("upstream_node carries slot_name %q", got)
	}
}
