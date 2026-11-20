// Copyright 2023 The Prometheus Authors
// SPDX-License-Identifier: Apache-2.0
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
// Modified for postgres_exporter_ng in 2026: the label pairs for a row are
// built once and shared by every metric that row produces.

package collector

import (
	"context"
	"database/sql"

	"github.com/blang/semver/v4"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const userTableSubsystem = "stat_user_tables"

func init() {
	registerCollector(userTableSubsystem, defaultEnabled, NewPGStatUserTablesCollector)
}

type PGStatUserTablesCollector struct {
	log log.Logger
}

func NewPGStatUserTablesCollector(config collectorConfig) (Collector, error) {
	return &PGStatUserTablesCollector{log: config.logger}, nil
}

var (
	statUserTablesSeqScan = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "seq_scan"),
		"Number of sequential scans initiated on this table",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesSeqTupRead = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "seq_tup_read"),
		"Number of live rows fetched by sequential scans",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesIdxScan = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "idx_scan"),
		"Number of index scans initiated on this table",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesIdxTupFetch = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "idx_tup_fetch"),
		"Number of live rows fetched by index scans",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNTupIns = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_tup_ins"),
		"Number of rows inserted",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNTupUpd = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_tup_upd"),
		"Number of rows updated",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNTupDel = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_tup_del"),
		"Number of rows deleted",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNTupHotUpd = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_tup_hot_upd"),
		"Number of rows HOT updated (i.e., with no separate index update required)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNLiveTup = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_live_tup"),
		"Estimated number of live rows",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNDeadTup = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_dead_tup"),
		"Estimated number of dead rows",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesNModSinceAnalyze = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_mod_since_analyze"),
		"Estimated number of rows changed since last analyze",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesLastVacuum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "last_vacuum"),
		"Last time at which this table was manually vacuumed (not counting VACUUM FULL)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesLastAutovacuum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "last_autovacuum"),
		"Last time at which this table was vacuumed by the autovacuum daemon",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesLastAnalyze = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "last_analyze"),
		"Last time at which this table was manually analyzed",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesLastAutoanalyze = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "last_autoanalyze"),
		"Last time at which this table was analyzed by the autovacuum daemon",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesVacuumCount = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "vacuum_count"),
		"Number of times this table has been manually vacuumed (not counting VACUUM FULL)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesAutovacuumCount = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "autovacuum_count"),
		"Number of times this table has been vacuumed by the autovacuum daemon",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesAnalyzeCount = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "analyze_count"),
		"Number of times this table has been manually analyzed",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesAutoanalyzeCount = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "autoanalyze_count"),
		"Number of times this table has been analyzed by the autovacuum daemon",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesTotalSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "size_bytes"),
		"Total disk space used by this table, in bytes, including all indexes and TOAST data",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesTotalVacuumTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "total_vacuum_time"),
		"Time spent vacuuming this table, in milliseconds (PostgreSQL 18+)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesTotalAutovacuumTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "total_autovacuum_time"),
		"Time spent auto-vacuuming this table, in milliseconds (PostgreSQL 18+)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesTotalAnalyzeTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "total_analyze_time"),
		"Time spent analyzing this table, in milliseconds (PostgreSQL 18+)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesTotalAutoanalyzeTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "total_autoanalyze_time"),
		"Time spent auto-analyzing this table, in milliseconds (PostgreSQL 18+)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)

	// statUserTablesLabelTemplate supplies the label NAMES for the pairs every
	// family from one row shares. Any descriptor above would do: they all
	// declare datname/schemaname/relname in that order with no constant
	// labels, which is the property the shared slice depends on. Naming the
	// choice here rather than picking one at the call site keeps the tests
	// pointed at the same descriptor production uses.
	statUserTablesLabelTemplate = statUserTablesSeqScan

	statUserTablesQueryPrePG18 = `
		SELECT
			current_database() datname,
			schemaname,
			relname,
			seq_scan,
			seq_tup_read,
			idx_scan,
			idx_tup_fetch,
			n_tup_ins,
			n_tup_upd,
			n_tup_del,
			n_tup_hot_upd,
			n_live_tup,
			n_dead_tup,
			n_mod_since_analyze,
			COALESCE(last_vacuum, '1970-01-01Z') as last_vacuum,
			COALESCE(last_autovacuum, '1970-01-01Z') as last_autovacuum,
			COALESCE(last_analyze, '1970-01-01Z') as last_analyze,
			COALESCE(last_autoanalyze, '1970-01-01Z') as last_autoanalyze,
			vacuum_count,
			autovacuum_count,
			analyze_count,
			autoanalyze_count,
			pg_total_relation_size(relid) as total_size,
			NULL::double precision as total_vacuum_time,
			NULL::double precision as total_autovacuum_time,
			NULL::double precision as total_analyze_time,
			NULL::double precision as total_autoanalyze_time
		FROM pg_stat_user_tables;`

	statUserTablesQueryPostPG18 = `
			SELECT
				current_database() datname,
				schemaname,
				relname,
				seq_scan,
				seq_tup_read,
				idx_scan,
				idx_tup_fetch,
				n_tup_ins,
				n_tup_upd,
				n_tup_del,
				n_tup_hot_upd,
				n_live_tup,
				n_dead_tup,
				n_mod_since_analyze,
				COALESCE(last_vacuum, '1970-01-01Z') as last_vacuum,
				COALESCE(last_autovacuum, '1970-01-01Z') as last_autovacuum,
				COALESCE(last_analyze, '1970-01-01Z') as last_analyze,
				COALESCE(last_autoanalyze, '1970-01-01Z') as last_autoanalyze,
				vacuum_count,
				autovacuum_count,
				analyze_count,
				autoanalyze_count,
				pg_total_relation_size(relid) as total_size,
				total_vacuum_time,
				total_autovacuum_time,
				total_analyze_time,
				total_autoanalyze_time
			FROM pg_stat_user_tables;`
)

func (c *PGStatUserTablesCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()

	v18plus := instance.version.GTE(semver.Version{Major: 18})

	statUserTablesQuery := statUserTablesQueryPrePG18
	if v18plus {
		statUserTablesQuery = statUserTablesQueryPostPG18
	}

	rows, err := db.QueryContext(ctx, statUserTablesQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, schemaname, relname sql.NullString
		var seqScan, seqTupRead, idxScan, idxTupFetch, nTupIns, nTupUpd, nTupDel, nTupHotUpd, nLiveTup, nDeadTup,
			nModSinceAnalyze, vacuumCount, autovacuumCount, analyzeCount, autoanalyzeCount, totalSize sql.NullInt64
		var lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze sql.NullTime
		var totalVacuumTime, totalAutovacuumTime, totalAnalyzeTime, totalAutoanalyzeTime sql.NullFloat64

		if err := rows.Scan(&datname, &schemaname, &relname, &seqScan, &seqTupRead, &idxScan, &idxTupFetch, &nTupIns, &nTupUpd, &nTupDel, &nTupHotUpd, &nLiveTup, &nDeadTup, &nModSinceAnalyze, &lastVacuum, &lastAutovacuum, &lastAnalyze, &lastAutoanalyze, &vacuumCount, &autovacuumCount, &analyzeCount, &autoanalyzeCount, &totalSize, &totalVacuumTime, &totalAutovacuumTime, &totalAnalyzeTime, &totalAutoanalyzeTime); err != nil {
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

		// One set of label pairs for every family this row produces. Every
		// descriptor passed to emit must declare the template's variable labels
		// in the template's order and the same constant labels; build anything
		// that differs with MustNewConstMetric instead.
		// TestSharedLabelsMetricMatchesConstMetric holds this invariant, and
		// collector/shared_labels_metric.go explains why it is needed.
		rowLabels := prometheus.MakeLabelPairs(
			statUserTablesLabelTemplate,
			[]string{datnameLabel, schemanameLabel, relnameLabel},
		)
		// Does not escape, so costs nothing per row.
		emit := func(desc *prometheus.Desc, vtype prometheus.ValueType, value float64) {
			ch <- sharedLabelsMetric{desc: desc, vtype: vtype, value: value, labels: rowLabels}
		}

		seqScanMetric := 0.0
		if seqScan.Valid {
			seqScanMetric = float64(seqScan.Int64)
		}
		emit(statUserTablesSeqScan, prometheus.CounterValue, seqScanMetric)

		seqTupReadMetric := 0.0
		if seqTupRead.Valid {
			seqTupReadMetric = float64(seqTupRead.Int64)
		}
		emit(statUserTablesSeqTupRead, prometheus.CounterValue, seqTupReadMetric)

		idxScanMetric := 0.0
		if idxScan.Valid {
			idxScanMetric = float64(idxScan.Int64)
		}
		emit(statUserTablesIdxScan, prometheus.CounterValue, idxScanMetric)

		idxTupFetchMetric := 0.0
		if idxTupFetch.Valid {
			idxTupFetchMetric = float64(idxTupFetch.Int64)
		}
		emit(statUserTablesIdxTupFetch, prometheus.CounterValue, idxTupFetchMetric)

		nTupInsMetric := 0.0
		if nTupIns.Valid {
			nTupInsMetric = float64(nTupIns.Int64)
		}
		emit(statUserTablesNTupIns, prometheus.CounterValue, nTupInsMetric)

		nTupUpdMetric := 0.0
		if nTupUpd.Valid {
			nTupUpdMetric = float64(nTupUpd.Int64)
		}
		emit(statUserTablesNTupUpd, prometheus.CounterValue, nTupUpdMetric)

		nTupDelMetric := 0.0
		if nTupDel.Valid {
			nTupDelMetric = float64(nTupDel.Int64)
		}
		emit(statUserTablesNTupDel, prometheus.CounterValue, nTupDelMetric)

		nTupHotUpdMetric := 0.0
		if nTupHotUpd.Valid {
			nTupHotUpdMetric = float64(nTupHotUpd.Int64)
		}
		emit(statUserTablesNTupHotUpd, prometheus.CounterValue, nTupHotUpdMetric)

		nLiveTupMetric := 0.0
		if nLiveTup.Valid {
			nLiveTupMetric = float64(nLiveTup.Int64)
		}
		emit(statUserTablesNLiveTup, prometheus.GaugeValue, nLiveTupMetric)

		nDeadTupMetric := 0.0
		if nDeadTup.Valid {
			nDeadTupMetric = float64(nDeadTup.Int64)
		}
		emit(statUserTablesNDeadTup, prometheus.GaugeValue, nDeadTupMetric)

		nModSinceAnalyzeMetric := 0.0
		if nModSinceAnalyze.Valid {
			nModSinceAnalyzeMetric = float64(nModSinceAnalyze.Int64)
		}
		emit(statUserTablesNModSinceAnalyze, prometheus.GaugeValue, nModSinceAnalyzeMetric)

		lastVacuumMetric := 0.0
		if lastVacuum.Valid {
			lastVacuumMetric = float64(lastVacuum.Time.Unix())
		}
		emit(statUserTablesLastVacuum, prometheus.GaugeValue, lastVacuumMetric)

		lastAutovacuumMetric := 0.0
		if lastAutovacuum.Valid {
			lastAutovacuumMetric = float64(lastAutovacuum.Time.Unix())
		}
		emit(statUserTablesLastAutovacuum, prometheus.GaugeValue, lastAutovacuumMetric)

		lastAnalyzeMetric := 0.0
		if lastAnalyze.Valid {
			lastAnalyzeMetric = float64(lastAnalyze.Time.Unix())
		}
		emit(statUserTablesLastAnalyze, prometheus.GaugeValue, lastAnalyzeMetric)

		lastAutoanalyzeMetric := 0.0
		if lastAutoanalyze.Valid {
			lastAutoanalyzeMetric = float64(lastAutoanalyze.Time.Unix())
		}
		emit(statUserTablesLastAutoanalyze, prometheus.GaugeValue, lastAutoanalyzeMetric)

		vacuumCountMetric := 0.0
		if vacuumCount.Valid {
			vacuumCountMetric = float64(vacuumCount.Int64)
		}
		emit(statUserTablesVacuumCount, prometheus.CounterValue, vacuumCountMetric)

		autovacuumCountMetric := 0.0
		if autovacuumCount.Valid {
			autovacuumCountMetric = float64(autovacuumCount.Int64)
		}
		emit(statUserTablesAutovacuumCount, prometheus.CounterValue, autovacuumCountMetric)

		analyzeCountMetric := 0.0
		if analyzeCount.Valid {
			analyzeCountMetric = float64(analyzeCount.Int64)
		}
		emit(statUserTablesAnalyzeCount, prometheus.CounterValue, analyzeCountMetric)

		autoanalyzeCountMetric := 0.0
		if autoanalyzeCount.Valid {
			autoanalyzeCountMetric = float64(autoanalyzeCount.Int64)
		}
		emit(statUserTablesAutoanalyzeCount, prometheus.CounterValue, autoanalyzeCountMetric)

		totalSizeMetric := 0.0
		if totalSize.Valid {
			totalSizeMetric = float64(totalSize.Int64)
		}
		emit(statUserTablesTotalSize, prometheus.GaugeValue, totalSizeMetric)

		if v18plus {
			// PostgreSQL 18+ vacuum/analyze timing metrics
			totalVacuumTimeValue := 0.0
			if totalVacuumTime.Valid {
				totalVacuumTimeValue = totalVacuumTime.Float64
			}
			emit(statUserTablesTotalVacuumTime, prometheus.CounterValue, totalVacuumTimeValue)

			totalAutovacuumTimeValue := 0.0
			if totalAutovacuumTime.Valid {
				totalAutovacuumTimeValue = totalAutovacuumTime.Float64
			}
			emit(statUserTablesTotalAutovacuumTime, prometheus.CounterValue, totalAutovacuumTimeValue)

			totalAnalyzeTimeValue := 0.0
			if totalAnalyzeTime.Valid {
				totalAnalyzeTimeValue = totalAnalyzeTime.Float64
			}
			emit(statUserTablesTotalAnalyzeTime, prometheus.CounterValue, totalAnalyzeTimeValue)

			totalAutoanalyzeTimeValue := 0.0
			if totalAutoanalyzeTime.Valid {
				totalAutoanalyzeTimeValue = totalAutoanalyzeTime.Float64
			}
			emit(statUserTablesTotalAutoanalyzeTime, prometheus.CounterValue, totalAutoanalyzeTimeValue)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
