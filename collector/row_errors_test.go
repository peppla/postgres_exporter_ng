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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

// probeSeq numbers the hosts unseenHost hands out.
var probeSeq atomic.Int64

// unseenHost returns a connection string for a server nothing has probed.
//
// A collector that asks which distribution it is talking to caches the answer
// for the life of the process, by host and port. A test reusing a host is
// answered from that cache instead of asking, so the expectation standing
// ready for the probe is never reached and the collector's first real query is
// matched against it. The result is a row error reported as a query mismatch,
// on the second run and not the first.
//
// A counter rather than the test's name, because -count runs the same name
// several times in one process.
func unseenHost(role string) string {
	return fmt.Sprintf("postgres://u:p@%s-%d:5432/d", role, probeSeq.Add(1))
}

// errRowFailure stands in for a query that dies partway through its rows: a
// cancelled scrape, a dropped connection, a server shutting down.
var errRowFailure = errors.New("connection lost while reading rows")

// drainMetrics consumes a collector's output so Update is never blocked by a
// full channel, and returns a function that waits for the drain to finish.
func drainMetrics() (chan prometheus.Metric, func()) {
	ch := make(chan prometheus.Metric, 256)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	return ch, func() { close(ch); <-done }
}

// TestCollectorsReportRowErrors is the regression for a defect that made
// incomplete collection look complete.
//
// sql.Rows.Next returns false at the end of the rows and also when the query
// fails partway, and the two are told apart only by calling Err afterwards. A
// collector that returns nil after its loop reports success for a scrape it
// did not finish, so pg_scrape_collector_success says 1 for a database whose
// data is truncated. The successful-path tests cannot see this: every row they
// supply arrives.
//
// Each case supplies one good row and then fails, which is the shape that used
// to be discarded.
func TestCollectorsReportRowErrors(t *testing.T) {
	statDatabaseColumns := []string{
		"datid", "datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted",
		"tup_updated", "tup_deleted", "conflicts", "temp_files", "temp_bytes",
		"deadlocks", "blk_read_time", "blk_write_time", "stats_reset",
		"parallel_workers_to_launch", "parallel_workers_launched",
	}
	statDatabaseRow := []driver.Value{
		"oid", "postgres", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15.0, 16.0, nil, 17, 18,
	}

	statIOColumns := []string{
		"backend_type", "io_object", "io_context", "reads", "read_bytes",
		"read_time", "writes", "write_bytes", "write_time", "writebacks",
		"writeback_time", "extends", "extend_bytes", "extend_time", "hits",
		"evictions", "reueses", "fsyncs", "fsync_time",
	}
	statIORow := []driver.Value{
		"client backend", "relation", "normal", 100, 90, 50.5, 75, 80, 25.2,
		10, 12.0, 7, 30, 11.0, 1, 2, 3, 4, 8.0,
	}

	for _, tc := range []struct {
		name string
		// queries is one column set and row per query the collector runs, in
		// order. The last one is the one that fails.
		queries [][]driver.Value
		columns [][]string
		version semver.Version
		build   func() Collector
		// dsn is needed by a collector that asks which distribution it is
		// talking to. That answer is cached per host:port, so each such case
		// needs its own, or the query runs or does not depending on which
		// test ran first.
		dsn string
	}{
		{
			name:    "extensions, available",
			columns: [][]string{{"current_database", "name", "default_version", "installed_version"}},
			queries: [][]driver.Value{{"postgres", "pg_trgm", "1.6", nil}},
			build:   factory(NewExtensionsCollector),
		},
		{
			name: "extensions, installed",
			columns: [][]string{
				{"current_database", "name", "default_version", "installed_version"},
				{"current_database", "extname", "extrelocatable", "extversion"},
			},
			queries: [][]driver.Value{
				{"postgres", "pg_trgm", "1.6", nil},
				{"postgres", "plpgsql", false, "1.0"},
			},
			build: factory(NewExtensionsCollector),
		},
		{
			name:    "stat_database",
			columns: [][]string{statDatabaseColumns},
			queries: [][]driver.Value{statDatabaseRow},
			version: semver.Version{Major: 18},
			build:   factory(NewPGStatDatabaseCollector),
		},
		{
			name:    "stat_io",
			columns: [][]string{statIOColumns},
			queries: [][]driver.Value{statIORow},
			version: semver.Version{Major: 18},
			build:   factory(NewPGStatIOCollector),
		},
		{
			// Two queries: the column probe, then the view itself. The probe
			// has its own case below, because this harness fails the last
			// query and the probe is the first.
			name: "stat_wal_receiver",
			columns: [][]string{
				{"column_name"},
				{
					"upstream_host", "slot_name", "status", "receive_start_lsn",
					"receive_start_tli", "flushed_lsn", "received_tli",
					"last_msg_send_time", "last_msg_receipt_time",
					"latest_end_lsn", "latest_end_time", "upstream_node",
				},
			},
			queries: [][]driver.Value{
				{"flushed_lsn"},
				{
					"upstream", "slot", "streaming", int64(1200668684563608), 1,
					int64(1200668684563609), 1, 1687321275.0, 1687321276.0,
					int64(1200668684563610), 1687321277.0, 5,
				},
			},
			build: factory(NewPGStatWalReceiverCollector),
			dsn:   unseenHost("walreceiver-rows"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("opening a stub connection: %s", err)
			}
			defer db.Close()

			// A collector that checks the distribution asks first, and that
			// query is not one of this collector's own.
			if tc.dsn != "" {
				mock.ExpectQuery("aurora_version").
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
			}

			// Every query but the last returns cleanly. The last one returns a
			// row and then fails, so Next stops for a reason that is not the
			// end of the rows.
			for i, row := range tc.queries {
				rows := sqlmock.NewRows(tc.columns[i]).AddRow(row...)
				if i == len(tc.queries)-1 {
					rows = rows.AddRow(row...).RowError(1, errRowFailure)
				}
				mock.ExpectQuery(".*").WillReturnRows(rows)
			}

			ch, wait := drainMetrics()
			err = tc.build().Update(context.Background(),
				&instance{db: db, version: tc.version, dsn: tc.dsn}, ch)
			wait()

			if !errors.Is(err, errRowFailure) {
				t.Errorf("row error lost: got %v", err)
			}
		})
	}
}

// factory builds a collector the way the registry does, so it gets the logger
// and configuration its methods expect rather than a zero value.
func factory(create func(collectorConfig) (Collector, error)) func() Collector {
	return func() Collector {
		c, err := create(collectorConfig{logger: log.NewNopLogger()})
		if err != nil {
			panic(err)
		}
		return c
	}
}

// TestWalReceiverProbeReportsRowErrors covers the one iterator in this
// repository whose result is a decision rather than a metric.
//
// The probe asks whether pg_stat_wal_receiver has a flushed_lsn column, and
// reads the answer from whether Next returns a row. False means the column is
// absent and also means the lookup failed, so without the Err check a failed
// probe silently selects the query for a server that does not have the column
// and drops flushed_lsn from one that does.
func TestWalReceiverProbeReportsRowErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	// The distribution check runs before any of this collector's own queries.
	// Its answer is cached per host:port, so this test carries a dsn of its
	// own and the query runs here whatever else has run before.
	mock.ExpectQuery("aurora_version").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	// The probe returns no row and fails, which is what a cancelled or broken
	// lookup looks like from Next alone.
	mock.ExpectQuery(sanitizeQuery(pgStatWalColumnQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).
			AddRow("flushed_lsn").RowError(0, errRowFailure))

	ch, wait := drainMetrics()
	err = factory(NewPGStatWalReceiverCollector)().Update(context.Background(),
		&instance{db: db, dsn: unseenHost("walreceiver-probe")}, ch)
	wait()

	if !errors.Is(err, errRowFailure) {
		t.Errorf("the probe swallowed its row error and chose a query anyway: got %v", err)
	}
	// The second query must not have run. If it did, the collector decided
	// which columns the server has from a failed lookup.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
