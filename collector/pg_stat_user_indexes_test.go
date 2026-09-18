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
package collector

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

// statUserIndexesColumns is the column set the collector scans, in order.
var statUserIndexesColumns = []string{
	"datname",
	"schemaname",
	"relname",
	"indexrelname",
	"idx_scan",
	"idx_tup_read",
	"idx_tup_fetch",
	"relpages",
	"reltuples",
}

// The query contains '^' (the pg_toast schema filter), which the shared
// sanitizeQuery does not escape — sqlmock's regexp matcher would read it as an
// anchor and fail to match. expectQuery (regexp.QuoteMeta, declared in
// pg_stat_user_tables_test.go) escapes the whole string instead.

func TestPGStatUserIndexesCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()
	inst := &instance{db: db}

	rows := sqlmock.NewRows(statUserIndexesColumns).
		AddRow("postgres", "public", "pgtest_accounts", "pgtest_accounts_pkey", 12, 34, 28, 5, 1000.0)

	mock.ExpectQuery(expectQuery(statUserIndexesQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserIndexesCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserIndexesCollector.Update: %s", err)
		}
	}()

	labels := labelMap{
		"datname":      "postgres",
		"schemaname":   "public",
		"relname":      "pgtest_accounts",
		"indexrelname": "pgtest_accounts_pkey",
	}
	expected := []MetricResult{
		{labels: labels, value: 12, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 34, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 28, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 5, metricType: dto.MetricType_GAUGE},
		{labels: labels, value: 1000, metricType: dto.MetricType_GAUGE},
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

func TestPGStatUserIndexesCollectorNullValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()
	inst := &instance{db: db}

	rows := sqlmock.NewRows(statUserIndexesColumns).
		AddRow(nil, nil, nil, nil, nil, nil, nil, nil, nil)

	mock.ExpectQuery(expectQuery(statUserIndexesQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserIndexesCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserIndexesCollector.Update: %s", err)
		}
	}()

	labels := labelMap{
		"datname":      "unknown",
		"schemaname":   "unknown",
		"relname":      "unknown",
		"indexrelname": "unknown",
	}
	expected := []MetricResult{
		{labels: labels, value: 0, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 0, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 0, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 0, metricType: dto.MetricType_GAUGE},
		{labels: labels, value: 0, metricType: dto.MetricType_GAUGE},
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

// A negative reltuples is emitted verbatim rather than clamped to 0, so that an
// "unknown row count" stays distinguishable from a genuinely empty index.
//
// Verified on live PostgreSQL 13-18 (2026-09-18): an index does NOT in practice
// carry the -1 that a never-analyzed *table* does, because CREATE INDEX sets
// reltuples at build time — a never-analyzed index, and even an invalid index
// left behind by a failed CREATE INDEX CONCURRENTLY, both report 0. This test
// therefore pins the pass-through behaviour, not an expected server value.
func TestPGStatUserIndexesCollectorNegativeReltuples(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()
	inst := &instance{db: db}

	rows := sqlmock.NewRows(statUserIndexesColumns).
		AddRow("postgres", "public", "pgtest_accounts", "pgtest_accounts_pkey", 0, 0, 0, 0, -1.0)

	mock.ExpectQuery(expectQuery(statUserIndexesQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGStatUserIndexesCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGStatUserIndexesCollector.Update: %s", err)
		}
	}()

	labels := labelMap{
		"datname":      "postgres",
		"schemaname":   "public",
		"relname":      "pgtest_accounts",
		"indexrelname": "pgtest_accounts_pkey",
	}
	expected := []MetricResult{
		{labels: labels, value: 0, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 0, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 0, metricType: dto.MetricType_COUNTER},
		{labels: labels, value: 0, metricType: dto.MetricType_GAUGE},
		{labels: labels, value: -1, metricType: dto.MetricType_GAUGE},
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
