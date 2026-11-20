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
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const userTableSubsystem = "stat_user_tables"

func init() {
	registerCollector(userTableSubsystem, defaultEnabled, scopeDatabase, NewPGStatUserTablesCollector)
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
	statUserTablesNInsSinceVacuum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_ins_since_vacuum"),
		"Estimated number of rows inserted since this table was last vacuumed. Drives insert-triggered autovacuum (autovacuum_vacuum_insert_threshold)",
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

	// n_tup_newpage_upd was added to pg_stat_user_tables in PostgreSQL 16. It
	// complements n_tup_hot_upd: while HOT updates keep the new row version on
	// the same heap page, a "new page" update has to place it elsewhere, which
	// is more expensive and contributes to table fragmentation.
	statUserTablesNTupNewpageUpd = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "n_tup_newpage_upd"),
		"Number of rows updated where the successor row version is stored on a new heap page, leaving the original page (PostgreSQL 16+)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)

	// The following metrics come straight from pg_class. The query (see
	// statUserTablesQuery) reads pg_class directly rather than going through the
	// pg_stat_user_tables view, so these columns are obtained from the SAME scan
	// that produces the activity stats — no extra scan of pg_class. pg_class
	// holds the planner's cached, per-relation statistics and visibility-map
	// summaries. They are available on every supported version except
	// relallfrozen, which is PostgreSQL 18+.
	//
	// Note on the freeze ages: the query includes partitioned parent tables
	// (relkind 'p'), which have no storage and therefore a relfrozenxid/
	// relminmxid of 0 (InvalidTransactionId). age('0'::xid) would return a value
	// close to the entire XID space and falsely look like imminent wraparound,
	// so the query wraps these in NULLIF(..., '0'::xid); the resulting NULL is
	// emitted as 0 ("no freeze age").
	statUserTablesRelpages = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "relpages"),
		"Size of the table's on-disk representation in pages, as estimated by the planner (pg_class.relpages)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesReltuples = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "reltuples"),
		"Planner's estimate of the number of live rows in the table (pg_class.reltuples). PostgreSQL 14+ reports -1 when the relation has never been analyzed; the value is emitted verbatim",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesRelallvisible = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "relallvisible"),
		"Number of pages marked all-visible in the table's visibility map (pg_class.relallvisible). The ratio to relpages bounds how much of the table can be served by index-only scans",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesRelallfrozen = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "relallfrozen"),
		"Number of pages marked all-frozen in the table's visibility map (pg_class.relallfrozen, PostgreSQL 18+)",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesRelfrozenxidAge = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "relfrozenxid_age"),
		"Age, in transactions, of the table's relfrozenxid (age(pg_class.relfrozenxid)). As this approaches autovacuum_freeze_max_age PostgreSQL forces an anti-wraparound autovacuum",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
	statUserTablesRelminmxidAge = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "relminmxid_age"),
		"Age, in multixacts, of the table's relminmxid (mxid_age(pg_class.relminmxid)). The multixact analogue of relfrozenxid_age",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)
)

// statUserTablesQuery builds the collector's query for a given server version.
//
// Rather than selecting from the pg_stat_user_tables view and joining pg_class
// (which would scan pg_class twice — once inside the view, once for the join),
// it reads pg_class directly and reproduces the view's activity columns with
// the same pg_stat_get_*() functions the view itself uses. That yields the
// activity stats AND the catalog columns (relpages, reltuples, …) from a single
// pg_class scan. The table-level idx_scan/idx_tup_fetch are computed with
// correlated subqueries over pg_index (rather than the view's GROUP BY), so the
// outer query needs no GROUP BY — see the note below the column list.
//
// Why build the string per version instead of one static query: a static query
// cannot reference a function/column that does not exist on older servers —
// PostgreSQL resolves every name at parse time, even inside an unreachable CASE
// branch — so pg_stat_get_tuples_newpage_updated (PG16+), c.relallfrozen and the
// total_*_time functions (PG18+) would fail to parse pre-version. We therefore
// substitute typed NULL placeholders on older servers. Every version returns
// the SAME columns in the SAME order, so the rows.Scan in Update is shared.
//
// The relkind set is ['r','t','m','p'] on all versions. The official view omits
// partitioned parents ('p') on PG13 (added in 14); including them uniformly is a
// deliberate, benign deviation — on PG13 it surfaces a few extra storage-less
// parent rows whose freeze ages are guarded by NULLIF and whose stats are 0.
func statUserTablesQuery(version semver.Version) string {
	v16plus := version.GTE(semver.Version{Major: 16})
	v18plus := version.GTE(semver.Version{Major: 18})

	// n_tup_newpage_upd: function added in PostgreSQL 16.
	nTupNewpageUpd := "NULL::bigint as n_tup_newpage_upd"
	if v16plus {
		nTupNewpageUpd = "pg_stat_get_tuples_newpage_updated(c.oid) as n_tup_newpage_upd"
	}

	// relallfrozen: pg_class column added in PostgreSQL 18.
	relallfrozen := "NULL::integer as relallfrozen"
	if v18plus {
		relallfrozen = "c.relallfrozen"
	}

	// total_*_time: functions added in PostgreSQL 18.
	totalTimes := `NULL::double precision as total_vacuum_time,
			NULL::double precision as total_autovacuum_time,
			NULL::double precision as total_analyze_time,
			NULL::double precision as total_autoanalyze_time`
	if v18plus {
		totalTimes = `pg_stat_get_total_vacuum_time(c.oid) as total_vacuum_time,
			pg_stat_get_total_autovacuum_time(c.oid) as total_autovacuum_time,
			pg_stat_get_total_analyze_time(c.oid) as total_analyze_time,
			pg_stat_get_total_autoanalyze_time(c.oid) as total_autoanalyze_time`
	}

	// The column list is fixed across versions (placeholders fill the gaps), so
	// the rows.Scan order in Update must match this exactly.
	//
	// idx_scan/idx_tup_fetch are correlated subqueries over pg_index instead of
	// the view's "LEFT JOIN pg_index ... GROUP BY". This is deliberate: with a
	// GROUP BY we'd have to select the bare pg_class columns (relpages, …) via
	// functional dependency on the grouped primary key, and PostgreSQL 13 does
	// NOT accept that for system catalogs ("column must appear in the GROUP BY")
	// — it only works from PG14 on. Dropping the GROUP BY sidesteps the version
	// difference entirely; pg_index is small and indexed on indrelid, and this
	// still touches pg_class only once. (idx_tup_fetch is NULL for a table with
	// no indexes — NULL + heap_fetched — exactly as the view returns it.)
	return fmt.Sprintf(`
		SELECT
			current_database() as datname,
			n.nspname as schemaname,
			c.relname,
			pg_stat_get_numscans(c.oid) as seq_scan,
			pg_stat_get_tuples_returned(c.oid) as seq_tup_read,
			(SELECT sum(pg_stat_get_numscans(x.indexrelid)) FROM pg_index x WHERE x.indrelid = c.oid)::bigint as idx_scan,
			(SELECT sum(pg_stat_get_tuples_fetched(x.indexrelid)) FROM pg_index x WHERE x.indrelid = c.oid)::bigint + pg_stat_get_tuples_fetched(c.oid) as idx_tup_fetch,
			pg_stat_get_tuples_inserted(c.oid) as n_tup_ins,
			pg_stat_get_tuples_updated(c.oid) as n_tup_upd,
			pg_stat_get_tuples_deleted(c.oid) as n_tup_del,
			pg_stat_get_tuples_hot_updated(c.oid) as n_tup_hot_upd,
			pg_stat_get_live_tuples(c.oid) as n_live_tup,
			pg_stat_get_dead_tuples(c.oid) as n_dead_tup,
			pg_stat_get_mod_since_analyze(c.oid) as n_mod_since_analyze,
			pg_stat_get_ins_since_vacuum(c.oid) as n_ins_since_vacuum,
			COALESCE(pg_stat_get_last_vacuum_time(c.oid), '1970-01-01Z') as last_vacuum,
			COALESCE(pg_stat_get_last_autovacuum_time(c.oid), '1970-01-01Z') as last_autovacuum,
			COALESCE(pg_stat_get_last_analyze_time(c.oid), '1970-01-01Z') as last_analyze,
			COALESCE(pg_stat_get_last_autoanalyze_time(c.oid), '1970-01-01Z') as last_autoanalyze,
			pg_stat_get_vacuum_count(c.oid) as vacuum_count,
			pg_stat_get_autovacuum_count(c.oid) as autovacuum_count,
			pg_stat_get_analyze_count(c.oid) as analyze_count,
			pg_stat_get_autoanalyze_count(c.oid) as autoanalyze_count,
			pg_total_relation_size(c.oid) as total_size,
			%s,
			c.relpages,
			c.reltuples,
			c.relallvisible,
			%s,
			age(NULLIF(c.relfrozenxid, '0'::xid)) as relfrozenxid_age,
			mxid_age(NULLIF(c.relminmxid, '0'::xid)) as relminmxid_age,
			%s
		FROM pg_class c
			LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = ANY (ARRAY['r', 't', 'm', 'p'])
			AND n.nspname <> ALL (ARRAY['pg_catalog', 'information_schema'])
			AND n.nspname !~ '^pg_toast';`, nTupNewpageUpd, relallfrozen, totalTimes)
}

func (c *PGStatUserTablesCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()

	// The query is built per server version (see statUserTablesQuery): two
	// column-set boundaries matter — PostgreSQL 16 added n_tup_newpage_upd, and
	// PostgreSQL 18 added relallfrozen and the total_*_time columns. The built
	// query always returns the same 35 columns in the same order (older versions
	// fill the version-specific ones with typed NULLs), so the rows.Scan below is
	// shared, and we reuse the same version flags to decide what to emit.
	v16plus := instance.version.GTE(semver.Version{Major: 16})
	v18plus := instance.version.GTE(semver.Version{Major: 18})

	rows, err := db.QueryContext(ctx, statUserTablesQuery(instance.version))

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, schemaname, relname sql.NullString
		var seqScan, seqTupRead, idxScan, idxTupFetch, nTupIns, nTupUpd, nTupDel, nTupHotUpd, nLiveTup, nDeadTup,
			nModSinceAnalyze, nInsSinceVacuum, vacuumCount, autovacuumCount, analyzeCount, autoanalyzeCount, totalSize sql.NullInt64
		var lastVacuum, lastAutovacuum, lastAnalyze, lastAutoanalyze sql.NullTime
		// pg_class-derived integer columns and the PG16 n_tup_newpage_upd.
		// relpages/relallvisible/relallfrozen and the freeze ages are integers;
		// reltuples is a float (real) and may be -1 ("never analyzed").
		var nTupNewpageUpd, relpages, relallvisible, relallfrozen, relfrozenxidAge, relminmxidAge sql.NullInt64
		var reltuples sql.NullFloat64
		var totalVacuumTime, totalAutovacuumTime, totalAnalyzeTime, totalAutoanalyzeTime sql.NullFloat64

		// The scan order must match the SELECT column order built by
		// statUserTablesQuery (columns 1..35).
		if err := rows.Scan(&datname, &schemaname, &relname, &seqScan, &seqTupRead, &idxScan, &idxTupFetch, &nTupIns, &nTupUpd, &nTupDel, &nTupHotUpd, &nLiveTup, &nDeadTup, &nModSinceAnalyze, &nInsSinceVacuum, &lastVacuum, &lastAutovacuum, &lastAnalyze, &lastAutoanalyze, &vacuumCount, &autovacuumCount, &analyzeCount, &autoanalyzeCount, &totalSize, &nTupNewpageUpd, &relpages, &reltuples, &relallvisible, &relallfrozen, &relfrozenxidAge, &relminmxidAge, &totalVacuumTime, &totalAutovacuumTime, &totalAnalyzeTime, &totalAutoanalyzeTime); err != nil {
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

		// n_ins_since_vacuum is available on all supported versions. Like
		// n_mod_since_analyze it resets to 0 on vacuum, so it is a gauge.
		nInsSinceVacuumMetric := 0.0
		if nInsSinceVacuum.Valid {
			nInsSinceVacuumMetric = float64(nInsSinceVacuum.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesNInsSinceVacuum,
			prometheus.GaugeValue,
			nInsSinceVacuumMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)

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

		// n_tup_newpage_upd is only present on PostgreSQL 16+. On older
		// versions the column is a NULL placeholder and the metric is skipped
		// entirely (rather than emitted as 0) so it does not appear at all.
		if v16plus {
			nTupNewpageUpdMetric := 0.0
			if nTupNewpageUpd.Valid {
				nTupNewpageUpdMetric = float64(nTupNewpageUpd.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statUserTablesNTupNewpageUpd,
				prometheus.CounterValue,
				nTupNewpageUpdMetric,
				datnameLabel, schemanameLabel, relnameLabel,
			)
		}

		// pg_class statistics. relpages, reltuples and relallvisible are
		// available on every supported version and are always emitted.
		relpagesMetric := 0.0
		if relpages.Valid {
			relpagesMetric = float64(relpages.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesRelpages,
			prometheus.GaugeValue,
			relpagesMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)

		// reltuples may legitimately be -1 (PostgreSQL 14+: "never analyzed").
		// We emit the raw value so that state is observable rather than masked.
		reltuplesMetric := 0.0
		if reltuples.Valid {
			reltuplesMetric = reltuples.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesReltuples,
			prometheus.GaugeValue,
			reltuplesMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)

		relallvisibleMetric := 0.0
		if relallvisible.Valid {
			relallvisibleMetric = float64(relallvisible.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesRelallvisible,
			prometheus.GaugeValue,
			relallvisibleMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)

		// relallfrozen is PostgreSQL 18+ only; skip on older versions.
		if v18plus {
			relallfrozenMetric := 0.0
			if relallfrozen.Valid {
				relallfrozenMetric = float64(relallfrozen.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statUserTablesRelallfrozen,
				prometheus.GaugeValue,
				relallfrozenMetric,
				datnameLabel, schemanameLabel, relnameLabel,
			)
		}

		// Freeze-deadline ages (available on all supported versions).
		relfrozenxidAgeMetric := 0.0
		if relfrozenxidAge.Valid {
			relfrozenxidAgeMetric = float64(relfrozenxidAge.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesRelfrozenxidAge,
			prometheus.GaugeValue,
			relfrozenxidAgeMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)

		relminmxidAgeMetric := 0.0
		if relminmxidAge.Valid {
			relminmxidAgeMetric = float64(relminmxidAge.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesRelminmxidAge,
			prometheus.GaugeValue,
			relminmxidAgeMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)

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
