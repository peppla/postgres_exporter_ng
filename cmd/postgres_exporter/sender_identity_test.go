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

// What separates one WAL sender from another in the legacy replication map.
//
// The labels it declared were application_name, client_addr, state and
// slot_name. slot_name is not a column of pg_stat_replication, so it carried
// the first column's value, which is the sender's pid: the separation was an
// accident of a wrong label. Removing that label removed the separation, and
// two standbys using the same tool behind the same address while streaming
// then produced one label set for two senders.

package main

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestTwoSendersAlikeInEveryOtherLabelStaySeparate is the regression.
func TestTwoSendersAlikeInEveryOtherLabelStaySeparate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	// The columns the modern query returns, which is the view plus the three
	// it computes. Two senders alike in everything the map labels except the
	// pid.
	columns := []string{
		"pid", "usesysid", "usename", "application_name", "client_addr",
		"client_hostname", "client_port", "backend_start", "backend_xmin",
		"state", "sent_lsn", "write_lsn", "flush_lsn", "replay_lsn",
		"write_lag", "flush_lag", "replay_lag", "sync_priority", "sync_state",
		"reply_time",
		"pg_current_wal_lsn", "pg_current_wal_lsn_bytes", "pg_wal_lsn_diff",
	}
	row := func(pid int64) []driver.Value {
		return []driver.Value{
			pid, 10, "replicator", "walreceiver", "198.51.100.4",
			nil, 54321, "2026-11-23T10:00:00Z", nil,
			"streaming", "0/1000", "0/1000", "0/1000", "0/1000",
			nil, nil, nil, 0, "async",
			1795000000.5,
			"0/2000", float64(8192), float64(4096),
		}
	}
	mock.ExpectQuery("FROM pg_stat_replication").WillReturnRows(
		sqlmock.NewRows(columns).AddRow(row(4001)...).AddRow(row(4002)...))

	// makeDescMap renames each namespace with the metric-prefix flag, which is
	// empty until a command line is parsed, so without this the namespace is
	// _stat_replication and nothing below finds it.
	previousPrefix := *metricPrefix
	*metricPrefix = "pg"
	defer func() { *metricPrefix = previousPrefix }()

	server := &Server{
		db:     db,
		labels: prometheus.Labels{"server": "test"},
	}
	version := semver.MustParse("18.0.0")
	server.metricMap = makeDescMap(version, server.labels, builtinMetricMaps)
	server.queryOverrides = makeQueryOverrideMap(version, queryOverrides)

	mapping, ok := server.metricMap["pg_stat_replication"]
	if !ok {
		t.Fatal("the replication namespace is not in the built-in map")
	}

	metrics, _, err := queryNamespaceMapping(context.Background(), server,
		"pg_stat_replication", mapping)
	if err != nil {
		t.Fatalf("the namespace failed: %s", err)
	}
	if len(metrics) == 0 {
		t.Fatal("nothing was published, so this says nothing about separating senders")
	}

	// Every published series has to be distinct. A label set that repeats is
	// one the registry refuses, and one of the two senders is then dropped.
	seen := map[string]int{}
	pids := map[string]bool{}
	for _, m := range metrics {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("writing a metric: %s", err)
		}
		key := m.Desc().String()
		for _, l := range pb.GetLabel() {
			key += " " + l.GetName() + "=" + l.GetValue()
			if l.GetName() == "pid" {
				pids[l.GetValue()] = true
			}
		}
		seen[key]++
		if seen[key] > 1 {
			t.Errorf("two senders produced the same series: %s", key)
		}
	}
	if len(pids) != 2 {
		t.Errorf("the published series carry %d distinct pids, want 2: %v", len(pids), pids)
	}
}
