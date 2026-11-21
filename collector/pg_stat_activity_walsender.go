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

// The WAL sender processes on a primary.
//
// One row per connected standby or logical subscriber. What it adds over
// stat_replication is when the connection started, which is how a standby
// that keeps reconnecting is told apart from one that has been attached all
// along.

package collector

import (
	"context"
	"database/sql"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const statActivityWalsenderSubsystem = "stat_activity_walsender"

func init() {
	registerCollector(statActivityWalsenderSubsystem, defaultEnabled, scopeCluster, NewPGStatActivityWalsenderCollector)
}

type PGStatActivityWalsenderCollector struct {
	log log.Logger
}

func NewPGStatActivityWalsenderCollector(config collectorConfig) (Collector, error) {
	return &PGStatActivityWalsenderCollector{log: config.logger}, nil
}

var (
	statActivityWalsenderLabels = []string{"pid", "usename", "application_name", "client_addr", "state"}

	statActivityWalsenderBackendStart = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statActivityWalsenderSubsystem, "backend_start_seconds"),
		"When this WAL sender process started. A value that keeps moving is a client that keeps reconnecting.",
		statActivityWalsenderLabels,
		prometheus.Labels{},
	)

	// backend_type has named the walsender since PostgreSQL 10, so no version
	// gate is needed across the range this exporter supports.
	statActivityWalsenderQuery = `SELECT
		pid,
		usename,
		application_name,
		client_addr,
		state,
		EXTRACT(EPOCH FROM backend_start) AS backend_start_seconds
	FROM pg_catalog.pg_stat_activity
	WHERE backend_type = 'walsender';`
)

func (c *PGStatActivityWalsenderCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.getDB().QueryContext(ctx, statActivityWalsenderQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var pid sql.NullInt64
		var usename, applicationName, clientAddr, state sql.NullString
		var backendStart sql.NullFloat64

		if err := rows.Scan(&pid, &usename, &applicationName, &clientAddr, &state, &backendStart); err != nil {
			return err
		}
		if !backendStart.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			statActivityWalsenderBackendStart, prometheus.GaugeValue, backendStart.Float64,
			nullIntLabel(pid),
			nullStringLabel(usename),
			nullStringLabel(applicationName),
			nullStringLabel(clientAddr),
			nullStringLabel(state),
		)
	}

	return rows.Err()
}
