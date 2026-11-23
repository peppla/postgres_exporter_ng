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

// Label names a query file can write that the registry refuses.
//
// Both of these are ordinary editing mistakes, and both used to end the
// process. The metric constructors that panic do so inside a scrape worker,
// where nothing recovers them, so a scrape of a query file with one bad label
// exited the exporter rather than failing that namespace. Measured by ChatGPT
// against the merged binary: exit 2 with the registry's own message.

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
)

// namespaceWithLabel runs one namespace whose query returns a label column of
// the given name, and returns what came back.
func namespaceWithLabel(t *testing.T, label string, serverLabels prometheus.Labels) ([]prometheus.Metric, error) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening a stub connection: %s", err)
	}
	defer db.Close()

	const namespace = "label_error"
	query := "SELECT " + label + ", value FROM label_error"
	mock.ExpectQuery("FROM label_error").WillReturnRows(
		sqlmock.NewRows([]string{label, "value"}).AddRow("a_value", 42.0))

	server := &Server{
		db:             db,
		labels:         serverLabels,
		queryOverrides: map[string]string{namespace: query},
	}
	// The descriptor the engine would build for this file, with the label the
	// file names and the constant labels the exporter adds.
	mapping := MetricMapNamespace{
		labels: []string{label},
		columnMappings: map[string]MetricMap{
			"value": {
				vtype: prometheus.GaugeValue,
				desc: prometheus.NewDesc("label_error_value", "a value",
					[]string{label}, serverLabels),
			},
		},
	}

	metrics, _, err := queryNamespaceMapping(context.Background(), server, namespace, mapping)
	if mockErr := mock.ExpectationsWereMet(); mockErr != nil {
		t.Errorf("there were unfulfilled expectations: %s", mockErr)
	}
	return metrics, err
}

// TestAReservedLabelNameFailsTheNamespace covers the first of ChatGPT's two
// inputs: a label name the registry reserves.
func TestAReservedLabelNameFailsTheNamespace(t *testing.T) {
	metrics, err := namespaceWithLabel(t, "__tenant", prometheus.Labels{"server": "test"})

	if err == nil {
		t.Fatal("a reserved label name was accepted, so this says nothing about surviving it")
	}
	if !strings.Contains(err.Error(), "__tenant") {
		t.Errorf("the error does not name the label: %s", err)
	}
	if !strings.Contains(err.Error(), "label_error") {
		t.Errorf("the error does not name the namespace: %s", err)
	}
	if len(metrics) != 0 {
		t.Errorf("%d metrics were published by a namespace that cannot publish any", len(metrics))
	}
}

// TestALabelCollidingWithAConstantLabelFailsTheNamespace covers the second: a
// label a query file names that the exporter also adds as a constant.
func TestALabelCollidingWithAConstantLabelFailsTheNamespace(t *testing.T) {
	metrics, err := namespaceWithLabel(t, "server", prometheus.Labels{"server": "test"})

	if err == nil {
		t.Fatal("a label colliding with a constant label was accepted")
	}
	if !strings.Contains(err.Error(), "label_error") {
		t.Errorf("the error does not name the namespace: %s", err)
	}
	if len(metrics) != 0 {
		t.Errorf("%d metrics were published by a namespace that cannot publish any", len(metrics))
	}
}

// TestAValidLabelStillPublishes keeps the two above from being satisfied by a
// namespace that refuses every label.
func TestAValidLabelStillPublishes(t *testing.T) {
	metrics, err := namespaceWithLabel(t, "tenant", prometheus.Labels{"server": "test"})
	if err != nil {
		t.Fatalf("a valid label was refused: %s", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("%d metrics were published, want 1", len(metrics))
	}
	if desc := metrics[0].Desc().String(); !strings.Contains(desc, "tenant") {
		t.Errorf("the published metric does not carry the label: %s", desc)
	}
}
