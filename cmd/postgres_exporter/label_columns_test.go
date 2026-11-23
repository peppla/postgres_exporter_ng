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

// A label column that the query does not return.
//
// The engine looked a label's column up in a map of column names to indices,
// and a Go map returns zero for a key it does not hold. So a label named in a
// query file and missing from its SELECT took the first column's value, the
// scrape reported success, and the series carried a label that was wrong
// about what it labelled.

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
)

// TestAMissingLabelColumnFailsTheNamespace is the correction.
func TestAMissingLabelColumnFailsTheNamespace(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	const namespace = "tenant_metrics"
	const query = "SELECT value FROM tenant_metrics"

	// One column, and a label the query does not return.
	mock.ExpectQuery("FROM tenant_metrics").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(42.0))

	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{namespace: query},
	}
	mapping := MetricMapNamespace{
		labels:         []string{"tenant_id"},
		columnMappings: map[string]MetricMap{},
	}

	metrics, nonFatal, err := queryNamespaceMapping(context.Background(), server, namespace, mapping)
	if err == nil {
		t.Fatal("a query that does not return its label column reported success")
	}
	if !strings.Contains(err.Error(), "tenant_id") {
		t.Errorf("the error does not name the missing column: %s", err)
	}
	if len(metrics) != 0 {
		t.Errorf("%d metrics were published for a namespace whose labels cannot be read", len(metrics))
	}
	if len(nonFatal) != 0 {
		t.Errorf("the failure was reported as %d non-fatal errors as well", len(nonFatal))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// TestALabelColumnThatIsReturnedStillWorks keeps the check from being a
// refusal of everything.
//
// Without this, removing the lookup entirely and failing every namespace with
// a label would pass the test above.
func TestALabelColumnThatIsReturnedStillWorks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	const namespace = "tenant_metrics"
	const query = "SELECT tenant_id, value FROM tenant_metrics"

	mock.ExpectQuery("FROM tenant_metrics").WillReturnRows(
		sqlmock.NewRows([]string{"tenant_id", "value"}).AddRow("acme", 42.0))

	server := &Server{
		db:             db,
		labels:         prometheus.Labels{"server": "test"},
		queryOverrides: map[string]string{namespace: query},
	}
	mapping := MetricMapNamespace{
		labels:         []string{"tenant_id"},
		columnMappings: map[string]MetricMap{},
	}

	metrics, _, err := queryNamespaceMapping(context.Background(), server, namespace, mapping)
	if err != nil {
		t.Fatalf("a query that returns its label column failed: %s", err)
	}
	// The value column, as an untyped metric, carrying the label.
	if len(metrics) != 1 {
		t.Fatalf("%d metrics were published, want 1", len(metrics))
	}
	desc := metrics[0].Desc().String()
	if !strings.Contains(desc, "tenant_id") {
		t.Errorf("the published metric does not carry the label: %s", desc)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
