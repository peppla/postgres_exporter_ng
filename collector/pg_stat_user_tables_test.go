// Copyright 2023 The Prometheus Authors
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
package collector

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

// expectQuery escapes the full query for sqlmock's regexp matcher. The shared
// sanitizeQuery helper does not escape '+', which appears in the idx_tup_fetch
// expression and would otherwise be interpreted as a regex quantifier and fail
// to match. regexp.QuoteMeta escapes every metacharacter; sqlmock normalizes
// whitespace on both sides, so this matches the built query exactly.
func expectQuery(q string) string {
	return regexp.QuoteMeta(q)
}

// Version fixtures for exercising each branch of statUserTablesQuery.
// pg18 is declared in pg_stat_database_test.go (same package).
var (
	pg13 = semver.MustParse("13.0.0")
	pg16 = semver.MustParse("16.0.0")
)

// userTablesColumns is the full 35-column result set that statUserTablesQuery
// produces on every version (older versions fill the version-specific columns
// with typed NULLs, but the column set/order is constant). The mock must always
// supply exactly these columns, in this order, to match the rows.Scan in Update.
var userTablesColumns = []string{
	"datname",                // 1
	"schemaname",             // 2
	"relname",                // 3
	"seq_scan",               // 4
	"seq_tup_read",           // 5
	"idx_scan",               // 6
	"idx_tup_fetch",          // 7
	"n_tup_ins",              // 8
	"n_tup_upd",              // 9
	"n_tup_del",              // 10
	"n_tup_hot_upd",          // 11
	"n_live_tup",             // 12
	"n_dead_tup",             // 13
	"n_mod_since_analyze",    // 14
	"n_ins_since_vacuum",     // 15
	"last_vacuum",            // 16
	"last_autovacuum",        // 17
	"last_analyze",           // 18
	"last_autoanalyze",       // 19
	"vacuum_count",           // 20
	"autovacuum_count",       // 21
	"analyze_count",          // 22
	"autoanalyze_count",      // 23
	"total_size",             // 24
	"n_tup_newpage_upd",      // 25
	"relpages",               // 26
	"reltuples",              // 27
	"relallvisible",          // 28
	"relallfrozen",           // 29
	"relfrozenxid_age",       // 30
	"relminmxid_age",         // 31
	"total_vacuum_time",      // 32
	"total_autovacuum_time",  // 33
	"total_analyze_time",     // 34
	"total_autoanalyze_time", // 35
}

// mustParseVacuumTimes returns the four last_* timestamps reused across the
// tests. Their Unix-epoch values are 1685664000, 1685750400, 1685836800 and
// 1685923200 respectively.
func mustParseVacuumTimes(t *testing.T) (vac, autovac, ana, autoana time.Time) {
	t.Helper()
	parse := func(s string) time.Time {
		ts, err := time.Parse("2006-01-02Z", s)
		if err != nil {
			t.Fatalf("Error parsing time %q: %s", s, err)
		}
		return ts
	}
	return parse("2023-06-02Z"), parse("2023-06-03Z"), parse("2023-06-04Z"), parse("2023-06-05Z")
}

// TestPGStatUserTablesCollector exercises the PostgreSQL 18+ path, where every
// column is populated (n_tup_newpage_upd, relallfrozen and the total_*_time
// columns all present).
func TestPGStatUserTablesCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: pg18}

	vac, autovac, ana, autoana := mustParseVacuumTimes(t)

	rows := sqlmock.NewRows(userTablesColumns).
		AddRow("postgres", "public", "a_table",
			1,  // seq_scan
			2,  // seq_tup_read
			3,  // idx_scan
			4,  // idx_tup_fetch
			5,  // n_tup_ins
			6,  // n_tup_upd
			7,  // n_tup_del
			8,  // n_tup_hot_upd
			9,  // n_live_tup
			10, // n_dead_tup
			0,  // n_mod_since_analyze
			27, // n_ins_since_vacuum
			vac, autovac, ana, autoana,
			11, // vacuum_count
			12, // autovacuum_count
			13, // analyze_count
			14, // autoanalyze_count
			15, // total_size
			20, // n_tup_newpage_upd
			21, // relpages
			22, // reltuples
			23, // relallvisible
			24, // relallfrozen
			25, // relfrozenxid_age
			26, // relminmxid_age
			16, // total_vacuum_time
			17, // total_autovacuum_time
			18, // total_analyze_time
			19, // total_autoanalyze_time
		)
	mock.ExpectQuery(expectQuery(statUserTablesQuery(pg18))).WillReturnRows(rows)
	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserTablesCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
		}
	}()

	l := labelMap{"datname": "postgres", "schemaname": "public", "relname": "a_table"}
	expected := []MetricResult{
		{labels: l, metricType: dto.MetricType_COUNTER, value: 1},        // seq_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 2},        // seq_tup_read
		{labels: l, metricType: dto.MetricType_COUNTER, value: 3},        // idx_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 4},        // idx_tup_fetch
		{labels: l, metricType: dto.MetricType_COUNTER, value: 5},        // n_tup_ins
		{labels: l, metricType: dto.MetricType_COUNTER, value: 6},        // n_tup_upd
		{labels: l, metricType: dto.MetricType_COUNTER, value: 7},        // n_tup_del
		{labels: l, metricType: dto.MetricType_COUNTER, value: 8},        // n_tup_hot_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 9},          // n_live_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 10},         // n_dead_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},          // n_mod_since_analyze
		{labels: l, metricType: dto.MetricType_GAUGE, value: 27},         // n_ins_since_vacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685664000}, // last_vacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685750400}, // last_autovacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685836800}, // last_analyze
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685923200}, // last_autoanalyze
		{labels: l, metricType: dto.MetricType_COUNTER, value: 11},       // vacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 12},       // autovacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 13},       // analyze_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 14},       // autoanalyze_count
		{labels: l, metricType: dto.MetricType_GAUGE, value: 15},         // size_bytes
		{labels: l, metricType: dto.MetricType_COUNTER, value: 20},       // n_tup_newpage_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 21},         // relpages
		{labels: l, metricType: dto.MetricType_GAUGE, value: 22},         // reltuples
		{labels: l, metricType: dto.MetricType_GAUGE, value: 23},         // relallvisible
		{labels: l, metricType: dto.MetricType_GAUGE, value: 24},         // relallfrozen
		{labels: l, metricType: dto.MetricType_GAUGE, value: 25},         // relfrozenxid_age
		{labels: l, metricType: dto.MetricType_GAUGE, value: 26},         // relminmxid_age
		{labels: l, metricType: dto.MetricType_COUNTER, value: 16},       // total_vacuum_time
		{labels: l, metricType: dto.MetricType_COUNTER, value: 17},       // total_autovacuum_time
		{labels: l, metricType: dto.MetricType_COUNTER, value: 18},       // total_analyze_time
		{labels: l, metricType: dto.MetricType_COUNTER, value: 19},       // total_autoanalyze_time
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

// TestPGStatUserTablesCollectorNullValues verifies that NULL columns become 0
// (or epoch 0 for timestamps) and that the "unknown" label fallbacks apply.
func TestPGStatUserTablesCollectorNullValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: pg18}

	rows := sqlmock.NewRows(userTablesColumns).
		AddRow("postgres",
			nil, nil, // schemaname, relname
			nil, nil, nil, nil, // seq_scan, seq_tup_read, idx_scan, idx_tup_fetch
			nil, nil, nil, nil, // n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd
			nil, nil, // n_live_tup, n_dead_tup
			nil, nil, // n_mod_since_analyze, n_ins_since_vacuum
			nil, nil, nil, nil, // last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
			nil, nil, nil, nil, // vacuum_count, autovacuum_count, analyze_count, autoanalyze_count
			nil,                // total_size
			nil,                // n_tup_newpage_upd
			nil, nil, nil, nil, // relpages, reltuples, relallvisible, relallfrozen
			nil, nil, // relfrozenxid_age, relminmxid_age
			nil, nil, nil, nil, // total_*_time
		)
	mock.ExpectQuery(expectQuery(statUserTablesQuery(pg18))).WillReturnRows(rows)
	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserTablesCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
		}
	}()

	l := labelMap{"datname": "postgres", "schemaname": "unknown", "relname": "unknown"}
	expected := []MetricResult{
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // seq_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // seq_tup_read
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // idx_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // idx_tup_fetch
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // n_tup_ins
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // n_tup_upd
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // n_tup_del
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // n_tup_hot_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // n_live_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // n_dead_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // n_mod_since_analyze
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // n_ins_since_vacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // last_vacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // last_autovacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // last_analyze
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // last_autoanalyze
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // vacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // autovacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // analyze_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // autoanalyze_count
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // size_bytes
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // n_tup_newpage_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // relpages
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // reltuples
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // relallvisible
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // relallfrozen
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // relfrozenxid_age
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // relminmxid_age
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // total_vacuum_time
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // total_autovacuum_time
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // total_analyze_time
		{labels: l, metricType: dto.MetricType_COUNTER, value: 0}, // total_autoanalyze_time
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

// TestPGStatUserTablesCollectorPrePG16 exercises the pre-PostgreSQL-16 path:
// n_tup_newpage_upd, relallfrozen and the total_*_time columns do not exist, so
// their metrics must NOT be emitted. n_ins_since_vacuum and the pg_class columns
// available on all versions (relpages, reltuples, relallvisible, freeze ages)
// must still be emitted.
func TestPGStatUserTablesCollectorPrePG16(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: pg13}

	vac, autovac, ana, autoana := mustParseVacuumTimes(t)

	rows := sqlmock.NewRows(userTablesColumns).
		AddRow("postgres", "public", "a_table",
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0,
			27, // n_ins_since_vacuum
			vac, autovac, ana, autoana,
			11, 12, 13, 14, 15,
			nil,                // n_tup_newpage_upd  (PG16+, absent here)
			21,                 // relpages
			22,                 // reltuples
			23,                 // relallvisible
			nil,                // relallfrozen       (PG18+, absent here)
			25,                 // relfrozenxid_age
			26,                 // relminmxid_age
			nil, nil, nil, nil, // total_*_time (PG18+, absent here)
		)
	mock.ExpectQuery(expectQuery(statUserTablesQuery(pg13))).WillReturnRows(rows)
	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserTablesCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
		}
	}()

	l := labelMap{"datname": "postgres", "schemaname": "public", "relname": "a_table"}
	expected := []MetricResult{
		{labels: l, metricType: dto.MetricType_COUNTER, value: 1}, // seq_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 2}, // seq_tup_read
		{labels: l, metricType: dto.MetricType_COUNTER, value: 3}, // idx_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 4}, // idx_tup_fetch
		{labels: l, metricType: dto.MetricType_COUNTER, value: 5}, // n_tup_ins
		{labels: l, metricType: dto.MetricType_COUNTER, value: 6}, // n_tup_upd
		{labels: l, metricType: dto.MetricType_COUNTER, value: 7}, // n_tup_del
		{labels: l, metricType: dto.MetricType_COUNTER, value: 8}, // n_tup_hot_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 9},   // n_live_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 10},  // n_dead_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // n_mod_since_analyze
		{labels: l, metricType: dto.MetricType_GAUGE, value: 27},  // n_ins_since_vacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685664000},
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685750400},
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685836800},
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685923200},
		{labels: l, metricType: dto.MetricType_COUNTER, value: 11}, // vacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 12}, // autovacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 13}, // analyze_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 14}, // autoanalyze_count
		{labels: l, metricType: dto.MetricType_GAUGE, value: 15},   // size_bytes
		// No n_tup_newpage_upd (PG16+). pg_class columns still emitted:
		{labels: l, metricType: dto.MetricType_GAUGE, value: 21}, // relpages
		{labels: l, metricType: dto.MetricType_GAUGE, value: 22}, // reltuples
		{labels: l, metricType: dto.MetricType_GAUGE, value: 23}, // relallvisible
		// No relallfrozen (PG18+).
		{labels: l, metricType: dto.MetricType_GAUGE, value: 25}, // relfrozenxid_age
		{labels: l, metricType: dto.MetricType_GAUGE, value: 26}, // relminmxid_age
		// No total_*_time (PG18+).
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

// TestPGStatUserTablesCollectorPG16to17 exercises the PostgreSQL 16/17 path:
// n_tup_newpage_upd is present and must be emitted, but relallfrozen and the
// total_*_time columns are still PostgreSQL 18+ and must NOT be emitted.
func TestPGStatUserTablesCollectorPG16to17(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: pg16}

	vac, autovac, ana, autoana := mustParseVacuumTimes(t)

	rows := sqlmock.NewRows(userTablesColumns).
		AddRow("postgres", "public", "a_table",
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0,
			27, // n_ins_since_vacuum
			vac, autovac, ana, autoana,
			11, 12, 13, 14, 15,
			20,                 // n_tup_newpage_upd  (PG16+, present)
			21,                 // relpages
			22,                 // reltuples
			23,                 // relallvisible
			nil,                // relallfrozen       (PG18+, absent here)
			25,                 // relfrozenxid_age
			26,                 // relminmxid_age
			nil, nil, nil, nil, // total_*_time (PG18+, absent here)
		)
	mock.ExpectQuery(expectQuery(statUserTablesQuery(pg16))).WillReturnRows(rows)
	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserTablesCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserTablesCollector.Update: %s", err)
		}
	}()

	l := labelMap{"datname": "postgres", "schemaname": "public", "relname": "a_table"}
	expected := []MetricResult{
		{labels: l, metricType: dto.MetricType_COUNTER, value: 1}, // seq_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 2}, // seq_tup_read
		{labels: l, metricType: dto.MetricType_COUNTER, value: 3}, // idx_scan
		{labels: l, metricType: dto.MetricType_COUNTER, value: 4}, // idx_tup_fetch
		{labels: l, metricType: dto.MetricType_COUNTER, value: 5}, // n_tup_ins
		{labels: l, metricType: dto.MetricType_COUNTER, value: 6}, // n_tup_upd
		{labels: l, metricType: dto.MetricType_COUNTER, value: 7}, // n_tup_del
		{labels: l, metricType: dto.MetricType_COUNTER, value: 8}, // n_tup_hot_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 9},   // n_live_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 10},  // n_dead_tup
		{labels: l, metricType: dto.MetricType_GAUGE, value: 0},   // n_mod_since_analyze
		{labels: l, metricType: dto.MetricType_GAUGE, value: 27},  // n_ins_since_vacuum
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685664000},
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685750400},
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685836800},
		{labels: l, metricType: dto.MetricType_GAUGE, value: 1685923200},
		{labels: l, metricType: dto.MetricType_COUNTER, value: 11}, // vacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 12}, // autovacuum_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 13}, // analyze_count
		{labels: l, metricType: dto.MetricType_COUNTER, value: 14}, // autoanalyze_count
		{labels: l, metricType: dto.MetricType_GAUGE, value: 15},   // size_bytes
		{labels: l, metricType: dto.MetricType_COUNTER, value: 20}, // n_tup_newpage_upd
		{labels: l, metricType: dto.MetricType_GAUGE, value: 21},   // relpages
		{labels: l, metricType: dto.MetricType_GAUGE, value: 22},   // reltuples
		{labels: l, metricType: dto.MetricType_GAUGE, value: 23},   // relallvisible
		// No relallfrozen (PG18+).
		{labels: l, metricType: dto.MetricType_GAUGE, value: 25}, // relfrozenxid_age
		{labels: l, metricType: dto.MetricType_GAUGE, value: 26}, // relminmxid_age
		// No total_*_time (PG18+).
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
