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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestPgReplicationSlotCollectorActive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db}

	columns := []string{"slot_name", "plugin", "slot_type", "current_wal_lsn", "confirmed_flush_lsn", "restart_lsn", "active"}
	rows := sqlmock.NewRows(columns).
		AddRow("test_slot", "test_decoding", "physical", 5, 3, 2, true)
	mock.ExpectQuery(sanitizeQuery(pgReplicationSlotQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGReplicationSlotCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGPostmasterCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 5, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 2, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 3, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 1, metricType: dto.MetricType_GAUGE},
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

func TestPgReplicationSlotCollectorInActive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db}

	columns := []string{"slot_name", "plugin", "slot_type", "current_wal_lsn", "confirmed_flush_lsn", "restart_lsn", "active"}
	rows := sqlmock.NewRows(columns).
		AddRow("test_slot", "test_decoding", "physical", 6, 12, 4, false)
	mock.ExpectQuery(sanitizeQuery(pgReplicationSlotQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGReplicationSlotCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGReplicationSlotCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 6, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 4, metricType: dto.MetricType_GAUGE},
		// The confirmed flush position, now published for an inactive slot
		// too. That is the slot holding log nothing will confirm, which is
		// the case worth seeing.
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 12, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 0, metricType: dto.MetricType_GAUGE},
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

func TestPgReplicationSlotCollectorActiveNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db}

	columns := []string{"slot_name", "plugin", "slot_type", "current_wal_lsn", "confirmed_flush_lsn", "restart_lsn", "active"}
	rows := sqlmock.NewRows(columns).
		AddRow("test_slot", "test_decoding", "physical", 6, 12, 4, nil)
	mock.ExpectQuery(sanitizeQuery(pgReplicationSlotQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGReplicationSlotCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGReplicationSlotCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 6, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 4, metricType: dto.MetricType_GAUGE},
		// The confirmed flush position, now published for an inactive slot
		// too. That is the slot holding log nothing will confirm, which is
		// the case worth seeing.
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 12, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "test_slot", "plugin": "test_decoding", "slot_type": "physical"}, value: 0, metricType: dto.MetricType_GAUGE},
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

func TestPgReplicationSlotCollectorTestNilValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db}

	columns := []string{"slot_name", "plugin", "slot_type", "current_wal_lsn", "confirmed_flush_lsn", "restart_lsn", "active"}
	rows := sqlmock.NewRows(columns).
		AddRow(nil, nil, nil, nil, nil, nil, true)
	mock.ExpectQuery(sanitizeQuery(pgReplicationSlotQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGReplicationSlotCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGReplicationSlotCollector.Update: %s", err)
		}
	}()

	// Two, not four. A row of NULLs has no confirmed flush position and no
	// restart position, and a position that does not exist is left out rather
	// than published as zero: zero is a real position, the start of the log.
	// What remains is the server's current position, which does not come from
	// the slot, and whether the slot is active, which is never NULL in
	// practice and reads as false when it is.
	expected := []MetricResult{
		{labels: labelMap{"slot_name": "unknown", "plugin": "unknown", "slot_type": "unknown"}, value: 0, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"slot_name": "unknown", "plugin": "unknown", "slot_type": "unknown"}, value: 1, metricType: dto.MetricType_GAUGE},
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if extra, open := <-ch; open {
		t.Errorf("a row of NULLs published a further metric: %s", extra.Desc())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

// TestReplicationSlotConfirmedFlushAppliesOnlyWhereItExists covers the four
// slot shapes an operator actually has.
//
// A physical slot has no confirmed flush position: the column is NULL whether
// the slot is active or not. A logical slot has one once something has
// consumed from it, and an inactive logical slot keeps the last one it
// confirmed, which is the case an operator watching retention cares about.
//
// The query used to COALESCE that NULL to zero, so a physical slot reported a
// consumer that had confirmed nothing. Zero is a real position, the start of
// the log, so there was nothing in the output to tell the two apart.
func TestReplicationSlotConfirmedFlushAppliesOnlyWhereItExists(t *testing.T) {
	const flushMetric = "pg_replication_slot_slot_confirmed_flush_lsn"
	const restartMetric = "pg_replication_slot_slot_restart_lsn"

	for _, tc := range []struct {
		name          string
		slotType      string
		active        bool
		confirmed     interface{}
		restart       interface{}
		wantConfirmed bool
	}{
		{"an active physical slot", "physical", true, nil, int64(5000), false},
		{"an inactive physical slot", "physical", false, nil, int64(5000), false},
		{"an active logical slot", "logical", true, int64(6000), int64(5000), true},
		{"an inactive logical slot", "logical", false, int64(6000), int64(5000), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("opening a stub connection: %s", err)
			}
			defer db.Close()

			columns := []string{"slot_name", "plugin", "slot_type", "current_wal_lsn", "confirmed_flush_lsn", "restart_lsn", "active"}
			mock.ExpectQuery(sanitizeQuery(pgReplicationSlotQuery)).WillReturnRows(
				sqlmock.NewRows(columns).AddRow(
					"a_slot", "pgoutput", tc.slotType, int64(7000), tc.confirmed, tc.restart, tc.active))

			ch := make(chan prometheus.Metric, 16)
			c := PGReplicationSlotCollector{}
			if err := c.Update(context.Background(), &instance{db: db}, ch); err != nil {
				t.Fatalf("the collector failed: %s", err)
			}
			close(ch)

			published := map[string]MetricResult{}
			for m := range ch {
				published[descName(m)] = readMetric(m)
			}

			got, ok := published[flushMetric]
			if ok != tc.wantConfirmed {
				t.Errorf("confirmed flush published=%v, want %v", ok, tc.wantConfirmed)
			}
			if ok && tc.wantConfirmed && got.value != 6000 {
				t.Errorf("confirmed flush is %v, want 6000", got.value)
			}
			// The retention boundary is published for every shape, because it
			// is what the slot holds and it exists for all of them.
			restart, ok := published[restartMetric]
			if !ok {
				t.Fatalf("%s is missing", restartMetric)
			}
			if restart.value != 5000 {
				t.Errorf("restart position is %v, want 5000", restart.value)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		})
	}
}
