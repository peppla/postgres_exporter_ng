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

// Row-error regressions for the legacy engine's two iterators.
//
// sql.Rows.Next returns false at the end of the rows and also when the
// iteration failed, and nothing but Err() tells the two apart. A loop without
// it returns the rows it managed to read and reports success, so a scrape that
// collected half a database says it collected all of it.
//
// The collector engine's four are in collector/row_errors_test.go.

package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var errLegacyRowFailure = errors.New("the connection went away mid-result")

// TestQueryNamespaceMappingReportsRowErrors covers the loop that feeds the
// per-database success series.
//
// This one mattered most of the five found in the audit. A namespace query
// that failed partway returned the rows it had read with no error, so the
// scrape published pg_exporter_databases_scraped for a database it had only
// partly collected. The metric that exists to report incomplete coverage was
// reporting the opposite.
func TestQueryNamespaceMappingReportsRowErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	const namespace = "a_namespace"
	const query = "SELECT value FROM somewhere"

	// Two rows, failing on the second: one row arrives, so the loop runs and
	// a version that ignored the error would return that row and no error.
	mock.ExpectQuery("FROM somewhere").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).
			AddRow(1.0).
			AddRow(2.0).
			RowError(1, errLegacyRowFailure))

	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{namespace: query},
	}

	metrics, _, err := queryNamespaceMapping(context.Background(), server, namespace, MetricMapNamespace{})
	if !errors.Is(err, errLegacyRowFailure) {
		t.Errorf("row error lost: got %v", err)
	}
	if len(metrics) != 0 {
		t.Errorf("a failed namespace query returned %d metrics, want none", len(metrics))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestQuerySettingsReportsRowErrors covers the third iterator in this engine.
//
// Partial emission is expected here: the settings already sent are sent, and
// the error says the rest were not read. What must not happen is the scrape
// reporting success, which is what a loop without Err() does. The error is
// formatted with %v, so it is matched on its text rather than with errors.Is.
func TestQuerySettingsReportsRowErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	// Two rows, failing on the second, so the first arrives and a version
	// that ignored the error would report one setting and no failure.
	mock.ExpectQuery("FROM pg_settings").WillReturnRows(
		sqlmock.NewRows([]string{"name", "setting", "unit", "short_desc", "vartype"}).
			AddRow("max_connections", "100", "", "Sets the maximum number of concurrent connections.", "integer").
			AddRow("work_mem", "4096", "kB", "Sets the maximum memory to be used for query workspaces.", "integer").
			RowError(1, errLegacyRowFailure))

	ch := make(chan prometheus.Metric, 8)
	err = querySettings(context.Background(), ch, &Server{db: db, labels: prometheus.Labels{"server": "test"}})
	close(ch)

	if err == nil {
		t.Fatal("expected settings iterator failure, got <nil>")
	}
	if !strings.Contains(err.Error(), errLegacyRowFailure.Error()) {
		t.Errorf("the row error is not in the reported error: got %v", err)
	}

	emitted := 0
	var first float64
	for metric := range ch {
		pb := &dto.Metric{}
		if err := metric.Write(pb); err != nil {
			t.Fatalf("writing a metric: %s", err)
		}
		if emitted == 0 {
			first = pb.GetGauge().GetValue()
		}
		emitted++
	}
	if emitted != 1 {
		t.Fatalf("a settings read that failed on its second row emitted %d metrics, want the one it managed to read", emitted)
	}
	if first != 100 {
		t.Errorf("the emitted setting reads %v, want the 100 of the row that arrived", first)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestQueryDatabasesReportsRowErrors covers the catalog read that decides
// which databases exist.
//
// A truncated read here is worse than a truncated metric. The databases that
// were never read do not look unread, they look absent, so the engine reports
// on the ones it happened to see and says nothing about the rest.
func TestQueryDatabasesReportsRowErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM pg_database").WillReturnRows(
		sqlmock.NewRows([]string{"datname", "is_current"}).
			AddRow("postgres", true).
			AddRow("app", false).
			RowError(1, errLegacyRowFailure))

	names, err := queryDatabases(context.Background(), &Server{db: db})
	if !errors.Is(err, errLegacyRowFailure) {
		t.Errorf("row error lost: got %v", err)
	}
	if names != nil {
		t.Errorf("a failed catalog read returned %v, want nothing", names)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
