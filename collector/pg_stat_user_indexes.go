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
	"database/sql"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	registerCollector(statUserIndexesSubsystem, defaultEnabled, scopeDatabase, NewPGStatUserIndexesCollector)
}

type PGStatUserIndexesCollector struct {
	log log.Logger
}

const statUserIndexesSubsystem = "stat_user_indexes"

func NewPGStatUserIndexesCollector(config collectorConfig) (Collector, error) {
	return &PGStatUserIndexesCollector{log: config.logger}, nil
}

var (
	statUserIndexesIdxScan = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statUserIndexesSubsystem, "idx_scan"),
		"Number of index scans initiated on this index",
		[]string{"datname", "schemaname", "relname", "indexrelname"},
		prometheus.Labels{},
	)
	statUserIndexesIdxTupRead = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statUserIndexesSubsystem, "idx_tup_read"),
		"Number of index entries returned by scans on this index",
		[]string{"datname", "schemaname", "relname", "indexrelname"},
		prometheus.Labels{},
	)
	statUserIndexesIdxTupFetch = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statUserIndexesSubsystem, "idx_tup_fetch"),
		"Number of live table rows fetched by simple index scans using this index",
		[]string{"datname", "schemaname", "relname", "indexrelname"},
		prometheus.Labels{},
	)
	statUserIndexesRelpages = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statUserIndexesSubsystem, "relpages"),
		"Size of the index's on-disk representation in pages, as estimated by the planner (pg_class.relpages)",
		[]string{"datname", "schemaname", "relname", "indexrelname"},
		prometheus.Labels{},
	)
	statUserIndexesReltuples = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statUserIndexesSubsystem, "reltuples"),
		"Planner's estimate of the number of entries in the index (pg_class.reltuples). Populated by the index build and maintained by VACUUM/ANALYZE, so unlike a table's reltuples it is not expected to carry the PostgreSQL 14+ \"never analyzed\" value of -1; whatever the catalog holds is emitted verbatim",
		[]string{"datname", "schemaname", "relname", "indexrelname"},
		prometheus.Labels{},
	)

	// This reproduces the body of the pg_stat_user_indexes view rather than
	// selecting from it, for the same reason as the stat_user_tables collector:
	// the view already joins pg_class twice (once for the table, once for the
	// index relation), so the index's catalog columns (relpages, reltuples) come
	// off the join that is already there. Selecting from the view and joining
	// pg_class back on indexrelid would be a third scan for those two columns.
	//
	// Unlike stat_user_tables this needs no version gating: every column below
	// exists on PostgreSQL 13 through 18, and the relkind filter of the
	// underlying view is {r,t,m} on every version (partitioned tables never
	// appear here, so there is no PostgreSQL 14 boundary to handle). PG16 added
	// last_idx_scan, which is deliberately not collected — adding it would make
	// this collector version-dependent for a timestamp that the scan counters
	// already cover in Prometheus.
	//
	// The "user" in pg_stat_user_indexes is purely the schema filter; the view
	// is otherwise identical to pg_stat_all_indexes.
	statUserIndexesQuery = `SELECT
			current_database() as datname,
			n.nspname as schemaname,
			c.relname,
			i.relname as indexrelname,
			pg_stat_get_numscans(i.oid) as idx_scan,
			pg_stat_get_tuples_returned(i.oid) as idx_tup_read,
			pg_stat_get_tuples_fetched(i.oid) as idx_tup_fetch,
			i.relpages,
			i.reltuples
		FROM pg_class c
			JOIN pg_index x ON c.oid = x.indrelid
			JOIN pg_class i ON i.oid = x.indexrelid
			LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = ANY (ARRAY['r', 't', 'm'])
			AND n.nspname <> ALL (ARRAY['pg_catalog', 'information_schema'])
			AND n.nspname !~ '^pg_toast';`
)

func (c *PGStatUserIndexesCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, statUserIndexesQuery)

	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var datname, schemaname, relname, indexrelname sql.NullString
		var idxScan, idxTupRead, idxTupFetch, relpages sql.NullInt64
		// reltuples is a float (real); negative values are passed through rather
		// than clamped (see the metric help).
		var reltuples sql.NullFloat64

		if err := rows.Scan(&datname, &schemaname, &relname, &indexrelname,
			&idxScan, &idxTupRead, &idxTupFetch, &relpages, &reltuples); err != nil {
			return err
		}

		datnameLabel := "unknown"
		if datname.Valid {
			datnameLabel = datname.String
		}
		schemanameLabel := "unknown"
		if schemaname.Valid {
			schemanameLabel = schemaname.String
		}
		relnameLabel := "unknown"
		if relname.Valid {
			relnameLabel = relname.String
		}
		indexrelnameLabel := "unknown"
		if indexrelname.Valid {
			indexrelnameLabel = indexrelname.String
		}
		labels := []string{datnameLabel, schemanameLabel, relnameLabel, indexrelnameLabel}

		idxScanMetric := 0.0
		if idxScan.Valid {
			idxScanMetric = float64(idxScan.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserIndexesIdxScan,
			prometheus.CounterValue,
			idxScanMetric,
			labels...,
		)

		idxTupReadMetric := 0.0
		if idxTupRead.Valid {
			idxTupReadMetric = float64(idxTupRead.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserIndexesIdxTupRead,
			prometheus.CounterValue,
			idxTupReadMetric,
			labels...,
		)

		idxTupFetchMetric := 0.0
		if idxTupFetch.Valid {
			idxTupFetchMetric = float64(idxTupFetch.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserIndexesIdxTupFetch,
			prometheus.CounterValue,
			idxTupFetchMetric,
			labels...,
		)

		relpagesMetric := 0.0
		if relpages.Valid {
			relpagesMetric = float64(relpages.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserIndexesRelpages,
			prometheus.GaugeValue,
			relpagesMetric,
			labels...,
		)

		reltuplesMetric := 0.0
		if reltuples.Valid {
			reltuplesMetric = reltuples.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statUserIndexesReltuples,
			prometheus.GaugeValue,
			reltuplesMetric,
			labels...,
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
