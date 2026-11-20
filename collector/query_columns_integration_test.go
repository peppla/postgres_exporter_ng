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

// These tests ask a running PostgreSQL server what a collector's query
// actually returns, which is the one thing a mocked connection cannot say.
// DATA_SOURCE_NAME must point at a server.
//go:build integration

package collector

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

// columnsOf asks the server for the output column names of a query, in order.
//
// The query runs, so the server's own parser decides the answer. The rows do
// not matter and an empty view is fine.
func columnsOf(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("running the query: %s\n%s", err, query)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("reading the column list: %s", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating: %s", err)
	}
	return cols
}

// TestWalReceiverQueryMatchesScanOrder pins the order rows.Scan depends on
// against the order the server returns.
//
// rows.Scan binds by position and both columns here are integers, so a swapped
// pair produces no error at all. Each metric simply carries the other's value.
// That is what happened: flushed_lsn was selected one column earlier than it
// was scanned, so on every server that has the column, and every supported
// version does, pg_stat_wal_receiver_flushed_lsn reported a timeline number
// and pg_stat_wal_receiver_receive_start_tli reported a log position.
//
// The unit tests could not see it. They build their fixture columns in the
// order Update scans them, and sqlmock returns what it was given without
// parsing any SQL, so the fixture agreed with the code and both disagreed with
// the server. Only the server settles it.
func TestWalReceiverQueryMatchesScanOrder(t *testing.T) {
	db, err := sql.Open("postgres", os.Getenv("DATA_SOURCE_NAME"))
	if err != nil {
		t.Fatalf("opening a connection: %s", err)
	}
	defer db.Close()

	// The order Update binds, read off its two rows.Scan calls.
	withFlushed := []string{
		"upstream_host", "slot_name", "status", "receive_start_lsn",
		"receive_start_tli", "flushed_lsn", "received_tli",
		"last_msg_send_time", "last_msg_receipt_time", "latest_end_lsn",
		"latest_end_time", "upstream_node",
	}
	withoutFlushed := []string{
		"upstream_host", "slot_name", "status", "receive_start_lsn",
		"receive_start_tli", "received_tli",
		"last_msg_send_time", "last_msg_receipt_time", "latest_end_lsn",
		"latest_end_time", "upstream_node",
	}

	for _, tc := range []struct {
		name     string
		flushed  string
		wantScan []string
	}{
		{
			name:     "with flushed_lsn",
			flushed:  "(flushed_lsn - '0/0') % (2^52)::bigint as flushed_lsn,\n",
			wantScan: withFlushed,
		},
		{
			name:     "without flushed_lsn",
			flushed:  "",
			wantScan: withoutFlushed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := columnsOf(t, db, fmt.Sprintf(pgStatWalReceiverQueryTemplate, tc.flushed))
			if len(got) != len(tc.wantScan) {
				t.Fatalf("the server returns %d columns, Scan binds %d\n got %v\nscan %v",
					len(got), len(tc.wantScan), got, tc.wantScan)
			}
			for i := range got {
				if got[i] != tc.wantScan[i] {
					t.Errorf("column %d: the server returns %q, Scan binds it into %q",
						i+1, got[i], tc.wantScan[i])
				}
			}
		})
	}
}

// TestWalReceiverColumnProbeFindsTheColumn checks the premise of the branch
// above: on a supported server the probe finds flushed_lsn, so the query with
// the column is the one that runs.
func TestWalReceiverColumnProbeFindsTheColumn(t *testing.T) {
	db, err := sql.Open("postgres", os.Getenv("DATA_SOURCE_NAME"))
	if err != nil {
		t.Fatalf("opening a connection: %s", err)
	}
	defer db.Close()

	var name string
	switch err := db.QueryRow(pgStatWalColumnQuery).Scan(&name); {
	case err == sql.ErrNoRows:
		t.Fatal("the probe found no flushed_lsn column on a supported server")
	case err != nil:
		t.Fatalf("running the probe: %s", err)
	case name != "flushed_lsn":
		t.Fatalf("the probe returned %q", name)
	}
}
