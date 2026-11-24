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

// What a primary knows about the standbys connected to it.
//
// This was a shipped query file rather than a collector, which meant built-in
// monitoring depended on configuration an operator could edit or remove. The
// contract puts built-in monitoring in Go and leaves the query engine for
// application queries, so it is a collector now.
//
// The view is empty on a standby and on a primary with nothing connected, and
// an empty view is no series rather than a zero.

package collector

import (
	"context"
	"database/sql"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const statReplicationSubsystem = "stat_replication"

func init() {
	registerCollector(statReplicationSubsystem, defaultEnabled, scopeCluster, NewPGStatReplicationCollector)
}

type PGStatReplicationCollector struct {
	log log.Logger
}

func NewPGStatReplicationCollector(config collectorConfig) (Collector, error) {
	return &PGStatReplicationCollector{log: config.logger}, nil
}

var (
	// One standby is one set of labels. pid changes when a standby
	// reconnects, so the set of series grows over time with reconnections
	// rather than with standbys. It is kept because dropping it would merge
	// two connections from the same host and application into one series,
	// which is worse for the thing this is read for.
	statReplicationLabels = []string{"pid", "usename", "application_name", "client_addr", "state", "sync_state"}

	statReplicationWriteLSN = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statReplicationSubsystem, "write_lsn_bytes"),
		"Write-ahead log position this standby has written, as a byte offset from the start of the log.",
		statReplicationLabels,
		prometheus.Labels{},
	)
	statReplicationFlushLSN = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statReplicationSubsystem, "flush_lsn_bytes"),
		"Write-ahead log position this standby has flushed to disk, as a byte offset from the start of the log.",
		statReplicationLabels,
		prometheus.Labels{},
	)
	statReplicationReplayLSN = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statReplicationSubsystem, "replay_lsn_bytes"),
		"Write-ahead log position this standby has replayed, as a byte offset from the start of the log.",
		statReplicationLabels,
		prometheus.Labels{},
	)
	statReplicationWriteLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statReplicationSubsystem, "write_lag_seconds"),
		"Time between a local commit and this standby reporting it written. Absent until the standby has reported once, which is not zero.",
		statReplicationLabels,
		prometheus.Labels{},
	)
	statReplicationFlushLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statReplicationSubsystem, "flush_lag_seconds"),
		"Time between a local commit and this standby reporting it flushed. Absent until the standby has reported once.",
		statReplicationLabels,
		prometheus.Labels{},
	)
	statReplicationReplayLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statReplicationSubsystem, "replay_lag_seconds"),
		"Time between a local commit and this standby reporting it replayed. Absent until the standby has reported once.",
		statReplicationLabels,
		prometheus.Labels{},
	)

	// The lag columns are intervals and the positions are pg_lsn, and neither
	// survives a scan into a number on its own. Converting them in SQL is what
	// the query file did too, and it is the only part of that file's approach
	// worth keeping.
	statReplicationQuery = `SELECT
		pid,
		usename,
		application_name,
		client_addr,
		state,
		sync_state,
		write_lsn - '0/0' AS write_lsn_bytes,
		flush_lsn - '0/0' AS flush_lsn_bytes,
		replay_lsn - '0/0' AS replay_lsn_bytes,
		EXTRACT(EPOCH FROM write_lag) AS write_lag_seconds,
		EXTRACT(EPOCH FROM flush_lag) AS flush_lag_seconds,
		EXTRACT(EPOCH FROM replay_lag) AS replay_lag_seconds
	FROM pg_catalog.pg_stat_replication;`
)

func (c *PGStatReplicationCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.getDB().QueryContext(ctx, statReplicationQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var pid sql.NullInt64
		var usename, applicationName, clientAddr, state, syncState sql.NullString
		var writeLSN, flushLSN, replayLSN sql.NullFloat64
		var writeLag, flushLag, replayLag sql.NullFloat64

		if err := rows.Scan(&pid, &usename, &applicationName, &clientAddr, &state, &syncState,
			&writeLSN, &flushLSN, &replayLSN, &writeLag, &flushLag, &replayLag); err != nil {
			return err
		}

		labels := []string{
			nullIntLabel(pid),
			nullStringLabel(usename),
			nullStringLabel(applicationName),
			// NULL for a standby connected over a Unix socket, which is a
			// local connection rather than an unknown one.
			nullStringLabel(clientAddr),
			nullStringLabel(state),
			nullStringLabel(syncState),
		}

		// A NULL position or lag is omitted rather than published as zero. A
		// standby that has not reported yet has no lag, and zero would read as
		// no lag at all.
		//
		// Named arguments rather than a table of descriptors, because the
		// catalogue reads this package statically and a descriptor reached
		// through a struct field is one it cannot follow.
		emit := func(desc *prometheus.Desc, vtype prometheus.ValueType, v sql.NullFloat64) {
			if !v.Valid {
				return
			}
			ch <- prometheus.MustNewConstMetric(desc, vtype, v.Float64, labels...)
		}
		emit(statReplicationWriteLSN, prometheus.GaugeValue, writeLSN)
		emit(statReplicationFlushLSN, prometheus.GaugeValue, flushLSN)
		emit(statReplicationReplayLSN, prometheus.GaugeValue, replayLSN)
		emit(statReplicationWriteLag, prometheus.GaugeValue, writeLag)
		emit(statReplicationFlushLag, prometheus.GaugeValue, flushLag)
		emit(statReplicationReplayLag, prometheus.GaugeValue, replayLag)
	}

	return rows.Err()
}
