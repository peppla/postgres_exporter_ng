// Copyright 2021 The Prometheus Authors
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

package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blang/semver/v4"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/internal/admission"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/fanout"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/rotation"
	"github.com/percona/postgres_exporter/internal/versioncache"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// ColumnUsage should be one of several enum values which describe how a
// queried row is to be converted to a Prometheus metric.
type ColumnUsage int

const (
	// DISCARD ignores a column
	DISCARD ColumnUsage = iota
	// LABEL identifies a column as a label
	LABEL ColumnUsage = iota
	// COUNTER identifies a column as a counter
	COUNTER ColumnUsage = iota
	// GAUGE identifies a column as a gauge
	GAUGE ColumnUsage = iota
	// MAPPEDMETRIC identifies a column as a mapping of text values
	MAPPEDMETRIC ColumnUsage = iota
	// DURATION identifies a column as a text duration (and converted to milliseconds)
	DURATION ColumnUsage = iota
	// HISTOGRAM identifies a column as a histogram
	HISTOGRAM ColumnUsage = iota
)

// UnmarshalYAML implements the yaml.Unmarshaller interface.
func (cu *ColumnUsage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	columnUsage, err := stringToColumnUsage(value)
	if err != nil {
		return err
	}

	*cu = columnUsage
	return nil
}

// MappingOptions is a copy of ColumnMapping used only for parsing
type MappingOptions struct {
	Usage             string             `yaml:"usage"`
	Description       string             `yaml:"description"`
	Mapping           map[string]float64 `yaml:"metric_mapping"` // Optional column mapping for MAPPEDMETRIC
	SupportedVersions semver.Range       `yaml:"pg_version"`     // Semantic version ranges which are supported. Unsupported columns are not queried (internally converted to DISCARD).
}

// Mapping represents a set of MappingOptions
type Mapping map[string]MappingOptions

// Regex used to get the "short-version" from the postgres version field.
var versionRegex = regexp.MustCompile(`^\w+ ((\d+)(\.\d+)?(\.\d+)?)`)
var lowestSupportedVersion = semver.MustParse("10.0.0")

// Parses the version of postgres into the short version string we can use to
// match behaviors.
func parseVersion(versionString string) (semver.Version, error) {
	submatches := versionRegex.FindStringSubmatch(versionString)
	if len(submatches) > 1 {
		return semver.ParseTolerant(submatches[1])
	}
	return semver.Version{},
		errors.New(fmt.Sprintln("Could not find a postgres version in string:", versionString))
}

// ColumnMapping is the user-friendly representation of a prometheus descriptor map
type ColumnMapping struct {
	usage             ColumnUsage        `yaml:"usage"`
	description       string             `yaml:"description"`
	mapping           map[string]float64 `yaml:"metric_mapping"` // Optional column mapping for MAPPEDMETRIC
	supportedVersions semver.Range       `yaml:"pg_version"`     // Semantic version ranges which are supported. Unsupported columns are not queried (internally converted to DISCARD).
}

// UnmarshalYAML implements yaml.Unmarshaller
func (cm *ColumnMapping) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain ColumnMapping
	return unmarshal((*plain)(cm))
}

// intermediateMetricMap holds the partially loaded metric map parsing.
// This is mainly so we can parse cacheSeconds around.
type intermediateMetricMap struct {
	columnMappings map[string]ColumnMapping
	master         bool
	// databases is the per-query selection for an application query. It is
	// absent for built-in namespaces, which means they run wherever the
	// engine runs them.
	databases queryDatabaseList
	// runOnServer restricts a query to a range of PostgreSQL versions. Nil
	// means no restriction. It is parsed once, at load, so an unparseable
	// range is an error rather than a query that silently never runs.
	runOnServer    semver.Range
	runOnServerRaw string
	cacheSeconds   uint64
}

// MetricMapNamespace groups metric maps under a shared set of labels.
type MetricMapNamespace struct {
	labels         []string             // Label names for this namespace
	columnMappings map[string]MetricMap // Column mappings in this namespace
	master         bool                 // Call query only for master database
	databases      queryDatabaseList    // Application-query selection; absent means every database
	runOnServer    semver.Range         // Version restriction; nil means no restriction
	runOnServerRaw string               // The restriction as written, for logging
	cacheSeconds   uint64               // Number of seconds this metric namespace can be cached. 0 disables.
	// definition fingerprints the query and its column mapping, so a cached
	// result is not served after the query file that produced it changed. The
	// cache key carries it rather than comparing definitions, because the
	// thing being asked is whether two results describe the same question.
	definition string
}

// MetricMap stores the prometheus metric description which a given column will
// be mapped to by the collector
type MetricMap struct {
	discard    bool                              // Should metric be discarded during mapping?
	histogram  bool                              // Should metric be treated as a histogram?
	vtype      prometheus.ValueType              // Prometheus valuetype
	desc       *prometheus.Desc                  // Prometheus descriptor
	conversion func(interface{}) (float64, bool) // Conversion function to turn PG result into float64
}

// ErrorConnectToServer is a connection to PgSQL server error
type ErrorConnectToServer struct {
	Msg string
}

// Error returns error
func (e *ErrorConnectToServer) Error() string {
	return e.Msg
}

// TODO: revisit this with the semver system
func dumpMaps() {
	// TODO: make this function part of the exporter
	for name, cmap := range builtinMetricMaps {
		query, ok := queryOverrides[name]
		if !ok {
			fmt.Println(name)
		} else {
			for _, queryOverride := range query {
				fmt.Println(name, queryOverride.versionRange, queryOverride.query)
			}
		}

		for column, details := range cmap.columnMappings {
			fmt.Printf("  %-40s %v\n", column, details)
		}
		fmt.Println()
	}
}

var builtinMetricMaps = map[string]intermediateMetricMap{
	"pg_stat_database_conflicts": {
		columnMappings: map[string]ColumnMapping{
			"datid":            {LABEL, "OID of a database", nil, nil},
			"datname":          {LABEL, "Name of this database", nil, nil},
			"confl_tablespace": {COUNTER, "Number of queries in this database that have been canceled due to dropped tablespaces", nil, nil},
			"confl_lock":       {COUNTER, "Number of queries in this database that have been canceled due to lock timeouts", nil, nil},
			"confl_snapshot":   {COUNTER, "Number of queries in this database that have been canceled due to old snapshots", nil, nil},
			"confl_bufferpin":  {COUNTER, "Number of queries in this database that have been canceled due to pinned buffers", nil, nil},
			"confl_deadlock":   {COUNTER, "Number of queries in this database that have been canceled due to deadlocks", nil, nil},
		},
		master:       true,
		cacheSeconds: 0,
	},
	"pg_lock_conflicts": {
		columnMappings: map[string]ColumnMapping{
			"blocking_pid": {LABEL, "PID of blocking session", nil, nil},
			"count":        {GAUGE, "Number of blocked sessions", nil, nil},
		},
		master:       true,
		cacheSeconds: 0,
	},
	"pg_stat_replication": {
		columnMappings: map[string]ColumnMapping{
			"procpid": {DISCARD, "Process ID of a WAL sender process", nil, semver.MustParseRange("<9.2.0")},
			// The only label that identifies one sender. Two standbys can
			// share application_name, client_addr and state.
			"pid":              {LABEL, "Process ID of a WAL sender process", nil, semver.MustParseRange(">=9.2.0")},
			"usesysid":         {DISCARD, "OID of the user logged into this WAL sender process", nil, nil},
			"usename":          {DISCARD, "Name of the user logged into this WAL sender process", nil, nil},
			"application_name": {LABEL, "Name of the application that is connected to this WAL sender", nil, nil},
			"client_addr":      {LABEL, "IP address of the client connected to this WAL sender. If this field is null, it indicates that the client is connected via a Unix socket on the server machine.", nil, nil},
			"client_hostname":  {DISCARD, "Host name of the connected client, as reported by a reverse DNS lookup of client_addr. This field will only be non-null for IP connections, and only when log_hostname is enabled.", nil, nil},
			"client_port":      {DISCARD, "TCP port number that the client is using for communication with this WAL sender, or -1 if a Unix socket is used", nil, nil},
			"backend_start":    {DISCARD, "with time zone	Time when this process was started, i.e., when the client connected to this WAL sender", nil, nil},
			"backend_xmin":     {DISCARD, "The current backend's xmin horizon.", nil, nil},
			"state":            {LABEL, "Current WAL sender state", nil, nil},
			"sent_location":    {DISCARD, "Last transaction log position sent on this connection", nil, semver.MustParseRange("<10.0.0")},
			"write_location":   {DISCARD, "Last transaction log position written to disk by this standby server", nil, semver.MustParseRange("<10.0.0")},
			"flush_location":   {DISCARD, "Last transaction log position flushed to disk by this standby server", nil, semver.MustParseRange("<10.0.0")},
			"replay_location":  {DISCARD, "Last transaction log position replayed into the database on this standby server", nil, semver.MustParseRange("<10.0.0")},
			"sent_lsn":         {DISCARD, "Last transaction log position sent on this connection", nil, semver.MustParseRange(">=10.0.0")},
			"write_lsn":        {DISCARD, "Last transaction log position written to disk by this standby server", nil, semver.MustParseRange(">=10.0.0")},
			"flush_lsn":        {DISCARD, "Last transaction log position flushed to disk by this standby server", nil, semver.MustParseRange(">=10.0.0")},
			"replay_lsn":       {DISCARD, "Last transaction log position replayed into the database on this standby server", nil, semver.MustParseRange(">=10.0.0")},
			"sync_priority":    {DISCARD, "Priority of this standby server for being chosen as the synchronous standby", nil, nil},
			"sync_state":       {DISCARD, "Synchronous state of this standby server", nil, nil},
			// slot_name is not a column of pg_stat_replication. It belongs to
			// pg_replication_slots, which the namespace below reads, and
			// declaring it here gave every sample of this namespace a
			// slot_name holding the first column's value, which is the
			// backend pid. Declared with DISCARD so that the mapping still
			// records the name, and so that a column of that name appearing
			// in a future version is not published by accident.
			"slot_name":                {DISCARD, "Not a column of this view. Its slot is identified through pg_replication_slots.active_pid.", nil, nil},
			"plugin":                   {DISCARD, "The base name of the shared object containing the output plugin this logical slot is using, or null for physical slots", nil, nil},
			"slot_type":                {DISCARD, "The slot type - physical or logical", nil, nil},
			"datoid":                   {DISCARD, "The OID of the database this slot is associated with, or null. Only logical slots have an associated database", nil, nil},
			"database":                 {DISCARD, "The name of the database this slot is associated with, or null. Only logical slots have an associated database", nil, nil},
			"active":                   {DISCARD, "True if this slot is currently actively being used", nil, nil},
			"active_pid":               {DISCARD, "Process ID of a WAL sender process", nil, nil},
			"xmin":                     {DISCARD, "The oldest transaction that this slot needs the database to retain. VACUUM cannot remove tuples deleted by any later transaction", nil, nil},
			"catalog_xmin":             {DISCARD, "The oldest transaction affecting the system catalogs that this slot needs the database to retain. VACUUM cannot remove catalog tuples deleted by any later transaction", nil, nil},
			"restart_lsn":              {DISCARD, "The address (LSN) of oldest WAL which still might be required by the consumer of this slot and thus won't be automatically removed during checkpoints", nil, nil},
			"pg_current_xlog_location": {DISCARD, "pg_current_xlog_location", nil, nil},
			"pg_current_wal_lsn":       {DISCARD, "pg_current_xlog_location", nil, semver.MustParseRange(">=10.0.0")},
			"pg_current_wal_lsn_bytes": {GAUGE, "WAL position in bytes", nil, semver.MustParseRange(">=10.0.0")},
			"pg_xlog_location_diff":    {GAUGE, "Lag in bytes between master and slave", nil, semver.MustParseRange(">=9.2.0 <10.0.0")},
			"pg_wal_lsn_diff":          {GAUGE, "Lag in bytes between master and slave", nil, semver.MustParseRange(">=10.0.0")},
			"confirmed_flush_lsn":      {DISCARD, "LSN position a consumer of a slot has confirmed flushing the data received", nil, nil},
			"write_lag":                {DISCARD, "Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written it (but not yet flushed it or applied it). This can be used to gauge the delay that synchronous_commit level remote_write incurred while committing if this server was configured as a synchronous standby.", nil, semver.MustParseRange(">=10.0.0")},
			"flush_lag":                {DISCARD, "Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written and flushed it (but not yet applied it). This can be used to gauge the delay that synchronous_commit level remote_flush incurred while committing if this server was configured as a synchronous standby.", nil, semver.MustParseRange(">=10.0.0")},
			"replay_lag":               {DISCARD, "Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written, flushed and applied it. This can be used to gauge the delay that synchronous_commit level remote_apply incurred while committing if this server was configured as a synchronous standby.", nil, semver.MustParseRange(">=10.0.0")},
		},
		master:       true,
		cacheSeconds: 0,
	},
	"pg_replication_slots": {
		columnMappings: map[string]ColumnMapping{
			"slot_name":       {LABEL, "Name of the replication slot", nil, nil},
			"database":        {LABEL, "Name of the database", nil, nil},
			"active":          {GAUGE, "Flag indicating if the slot is active", nil, nil},
			"pg_wal_lsn_diff": {GAUGE, "Replication lag in bytes", nil, nil},
		},
		master:       true,
		cacheSeconds: 0,
	},
	"pg_stat_archiver": {
		columnMappings: map[string]ColumnMapping{
			"archived_count":     {COUNTER, "Number of WAL files that have been successfully archived", nil, nil},
			"last_archived_wal":  {DISCARD, "Name of the last WAL file successfully archived", nil, nil},
			"last_archived_time": {DISCARD, "Time of the last successful archive operation", nil, nil},
			"failed_count":       {COUNTER, "Number of failed attempts for archiving WAL files", nil, nil},
			"last_failed_wal":    {DISCARD, "Name of the WAL file of the last failed archival operation", nil, nil},
			"last_failed_time":   {DISCARD, "Time of the last failed archival operation", nil, nil},
			"stats_reset":        {DISCARD, "Time at which these statistics were last reset", nil, nil},
			"last_archive_age":   {GAUGE, "Time in seconds since last WAL segment was successfully archived", nil, nil},
		},
		master:       true,
		cacheSeconds: 0,
	},
	"pg_stat_activity": {
		columnMappings: map[string]ColumnMapping{
			"datname":            {LABEL, "Name of this database", nil, nil},
			"state":              {LABEL, "connection state", nil, semver.MustParseRange(">=9.2.0")},
			"usename":            {LABEL, "Name of the user logged into this backend", nil, nil},
			"application_name":   {LABEL, "Name of the application that is connected to this backend", nil, nil},
			"count":              {GAUGE, "number of connections in this state", nil, nil},
			"max_tx_duration":    {GAUGE, "max duration in seconds any active transaction has been running", nil, nil},
			"max_state_duration": {GAUGE, "max state change duration in seconds any active transaction has been", nil, nil},
		},
		master:       true,
		cacheSeconds: 0,
	},
}

// Turn the MetricMap column mapping into a prometheus descriptor mapping.
func makeDescMap(pgVersion semver.Version, serverLabels prometheus.Labels, metricMaps map[string]intermediateMetricMap) map[string]MetricMapNamespace {
	var metricMap = make(map[string]MetricMapNamespace)

	for namespace, intermediateMappings := range metricMaps {
		thisMap := make(map[string]MetricMap)

		namespace = strings.Replace(namespace, "pg", *metricPrefix, 1)

		// Get the constant labels
		var variableLabels []string
		for columnName, columnMapping := range intermediateMappings.columnMappings {
			if columnMapping.usage == LABEL {
				variableLabels = append(variableLabels, columnName)
			}
		}

		for columnName, columnMapping := range intermediateMappings.columnMappings {
			// Check column version compatibility for the current map
			// Force to discard if not compatible.
			if columnMapping.supportedVersions != nil {
				if !columnMapping.supportedVersions(pgVersion) {
					// It's very useful to be able to see what columns are being
					// rejected.
					level.Debug(logger).Log("msg", "Column is being forced to discard due to version incompatibility", "column", columnName)
					thisMap[columnName] = MetricMap{
						discard: true,
						conversion: func(_ interface{}) (float64, bool) {
							return math.NaN(), true
						},
					}
					continue
				}
			}

			// Determine how to convert the column based on its usage.
			// nolint: dupl
			switch columnMapping.usage {
			case DISCARD, LABEL:
				thisMap[columnName] = MetricMap{
					discard: true,
					conversion: func(_ interface{}) (float64, bool) {
						return math.NaN(), true
					},
				}
			case COUNTER:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.CounterValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, variableLabels, serverLabels),
					conversion: func(in interface{}) (float64, bool) {
						return dbToFloat64(in)
					},
				}
			case GAUGE:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.GaugeValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, variableLabels, serverLabels),
					conversion: func(in interface{}) (float64, bool) {
						return dbToFloat64(in)
					},
				}
			case HISTOGRAM:
				thisMap[columnName] = MetricMap{
					histogram: true,
					vtype:     prometheus.UntypedValue,
					desc:      prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, variableLabels, serverLabels),
					conversion: func(in interface{}) (float64, bool) {
						return dbToFloat64(in)
					},
				}
				thisMap[columnName+"_bucket"] = MetricMap{
					histogram: true,
					discard:   true,
				}
				thisMap[columnName+"_sum"] = MetricMap{
					histogram: true,
					discard:   true,
				}
				thisMap[columnName+"_count"] = MetricMap{
					histogram: true,
					discard:   true,
				}
			case MAPPEDMETRIC:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.GaugeValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, variableLabels, serverLabels),
					conversion: func(in interface{}) (float64, bool) {
						text, ok := in.(string)
						if !ok {
							return math.NaN(), false
						}

						val, ok := columnMapping.mapping[text]
						if !ok {
							return math.NaN(), false
						}
						return val, true
					},
				}
			case DURATION:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.GaugeValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s_milliseconds", namespace, columnName), columnMapping.description, variableLabels, serverLabels),
					conversion: func(in interface{}) (float64, bool) {
						var durationString string
						switch t := in.(type) {
						case []byte:
							durationString = string(t)
						case string:
							durationString = t
						default:
							level.Error(logger).Log("msg", "Duration conversion metric was not a string")
							return math.NaN(), false
						}

						if durationString == "-1" {
							return math.NaN(), false
						}

						d, err := time.ParseDuration(durationString)
						if err != nil {
							level.Error(logger).Log("msg", "Failed converting result to metric", "column", columnName, "in", in, "err", err)
							return math.NaN(), false
						}
						return float64(d / time.Millisecond), true
					},
				}
			}
		}

		metricMap[namespace] = MetricMapNamespace{
			labels:         variableLabels,
			columnMappings: thisMap,
			master:         intermediateMappings.master,
			databases:      intermediateMappings.databases,
			runOnServer:    intermediateMappings.runOnServer,
			runOnServerRaw: intermediateMappings.runOnServerRaw,
			cacheSeconds:   intermediateMappings.cacheSeconds,
			definition:     namespaceDefinition(namespace, intermediateMappings),
		}
	}

	return metricMap
}

// Exporter collects Postgres metrics. It implements prometheus.Collector.
type Exporter struct {
	collectorName string

	// queryCache outlives this exporter, which a request creates and
	// discards. Nil means no caching at all, which is what every test that
	// does not pass one gets.
	queryCache *querycache.Cache
	// lastSuccess outlives this exporter too, for the same reason: how long a
	// database has been failing cannot be answered from one scrape.
	lastSuccess *lastsuccess.Store
	// rotation outlives this exporter for the same kind of reason: where the
	// last scrape of this profile started is the one thing a new one cannot
	// work out for itself. Nil means the sorted order every scrape.
	rotation          *rotation.Store
	userQueriesPath   map[MetricResolution]string
	resolutionEnabled MetricResolution
	enabled           bool

	// Holds a reference to the build in column mappings. Currently this is for testing purposes
	// only, since it just points to the global.
	builtinMetricMaps map[string]intermediateMetricMap

	disableDefaultMetrics, disableSettingsMetrics, autoDiscoverDatabases bool

	excludeDatabases dbselect.List
	includeDatabases dbselect.List
	dsn              []string
	constantLabels   prometheus.Labels
	duration         prometheus.Gauge
	error            prometheus.Gauge
	psqlUp           prometheus.Gauge
	userQueriesError *prometheus.GaugeVec
	totalScrapes     prometheus.Counter
	coverage         *coverageMetrics

	// servers are used to allow re-using the DB connection between scrapes.
	// servers contains metrics map and query overrides.
	// servers *Servers

	connSema *semaphore.Weighted
	// dbWorkers is how many databases the fan-out works on at once. It is the
	// connection limit, because a worker's first act is to ask for a permit
	// and there is nothing for a worker beyond that limit to do but queue.
	dbWorkers int
	ctx       context.Context
	masterDSN string
}

// ExporterOpt configures Exporter.
type ExporterOpt func(*Exporter)

// DisableDefaultMetrics configures default metrics export.
func DisableDefaultMetrics(b bool) ExporterOpt {
	return func(e *Exporter) {
		e.disableDefaultMetrics = b
	}
}

// CollectorName configures collector name.
func CollectorName(name string) ExporterOpt {
	return func(e *Exporter) {
		e.collectorName = name
	}
}

// WithUserQueriesEnabled enables user's queries.
func WithUserQueriesEnabled(p MetricResolution) ExporterOpt {
	return func(e *Exporter) {
		e.resolutionEnabled = p
	}
}

// WithUserQueriesEnabled enables user's queries.
func WithEnabled(p bool) ExporterOpt {
	return func(e *Exporter) {
		e.enabled = p
	}
}

// WithContext sets context for the exporter.
func WithContext(ctx context.Context) ExporterOpt {
	return func(e *Exporter) {
		e.ctx = ctx
	}
}

// WithConnectionsSemaphore sets the limit on open connections to the server,
// as the semaphore that issues the permits and the number of them there are.
//
// The count is taken here rather than configured separately, so that the width
// of the fan-out and the number of permits cannot drift apart.
//
// A count below one means one database at a time. Configuration validation
// refuses a connection limit below one, so that is reachable only through this
// API, and a caller who passes nothing gets slow collection rather than silent
// unbounded growth.
func WithConnectionsSemaphore(sem *semaphore.Weighted, connections int64) ExporterOpt {
	return func(e *Exporter) {
		e.connSema = sem
		e.dbWorkers = int(connections)
	}
}

// DisableSettingsMetrics configures pg_settings export.
func DisableSettingsMetrics(b bool) ExporterOpt {
	return func(e *Exporter) {
		e.disableSettingsMetrics = b
	}
}

// AutoDiscoverDatabases allows scraping all databases on a database server.
func AutoDiscoverDatabases(b bool) ExporterOpt {
	return func(e *Exporter) {
		e.autoDiscoverDatabases = b
	}
}

// ExcludeDatabases allows to filter out result from AutoDiscoverDatabases.
//
// The caller passes the inherited comma-separated flag split on commas, which
// is a slice holding one empty string when the flag is unset. listFromFlag
// reads that as absent rather than as a list containing a database with no
// name.
func ExcludeDatabases(s []string) ExporterOpt {
	return func(e *Exporter) {
		e.excludeDatabases = dbselect.FromFlag(s)
	}
}

// IncludeDatabases allows to filter result from AutoDiscoverDatabases.
//
// An empty string is an absent list here, not an explicit empty one. The flag
// surface cannot express the difference, and until it can, absent is the safe
// reading: an explicit empty list selects no databases at all.
func IncludeDatabases(s string) ExporterOpt {
	return func(e *Exporter) {
		if len(s) > 0 {
			e.includeDatabases = dbselect.Of(strings.Split(s, ","))
		}
	}
}

// WithQueryCache gives the exporter somewhere to keep results that outlive a
// request.
//
// The cache belongs to the process and is passed in rather than created here,
// because an exporter is built per request and anything it owns is discarded
// with it. That is exactly what cache_seconds was doing: every scrape built a
// server with an empty cache and ran the query again.
func WithQueryCache(cache *querycache.Cache) ExporterOpt {
	return func(e *Exporter) {
		e.queryCache = cache
	}
}

// WithRotation sets the process-owned schedule of which selected database is
// offered the first attempt.
//
// Without one the order is the sorted order every scrape, which is what left a
// selected set that does not fit the budget giving the same prefix every
// opportunity and the rest none.
func WithRotation(store *rotation.Store) ExporterOpt {
	return func(e *Exporter) {
		e.rotation = store
	}
}

// WithLastSuccess configures the process-owned record of when each database
// was last collected without error.
func WithLastSuccess(store *lastsuccess.Store) ExporterOpt {
	return func(e *Exporter) {
		e.lastSuccess = store
	}
}

// WithUserQueriesPath configures user's queries path.
func WithUserQueriesPath(p map[MetricResolution]string) ExporterOpt {
	return func(e *Exporter) {
		e.userQueriesPath = p
	}
}

// WithConstantLabels configures constant labels.
func WithConstantLabels(s string) ExporterOpt {
	return func(e *Exporter) {
		e.constantLabels = parseConstLabels(s)
		if e.collectorName != "" {
			e.constantLabels["collector"] = e.collectorName
		} else {
			e.constantLabels["collector"] = "exporter"
		}
	}
}

func parseConstLabels(s string) prometheus.Labels {
	labels := make(prometheus.Labels)

	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return labels
	}

	parts := strings.Split(s, ",")
	for _, p := range parts {
		keyValue := strings.Split(strings.TrimSpace(p), "=")
		if len(keyValue) != 2 {
			level.Error(logger).Log(`Wrong constant labels format, should be "key=value"`, "input", p)
			continue
		}
		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])
		if key == "" || value == "" {
			continue
		}
		labels[key] = value
	}

	return labels
}

// NewExporter returns a new PostgreSQL exporter for the provided DSN.
func NewExporter(dsn []string, opts ...ExporterOpt) *Exporter {
	e := &Exporter{
		dsn:               dsn,
		builtinMetricMaps: builtinMetricMaps,
		enabled:           true,
		ctx:               context.Background(),
	}

	for _, opt := range opts {
		opt(e)
	}

	e.setupInternalMetrics()
	// e.servers = NewServers(ServerWithLabels(e.constantLabels))

	return e
}

// GetServer returns a new Server instance for the provided DSN.
func (e *Exporter) GetServer(ctx context.Context, dsn string, opts ...ServerOpt) (*Server, error) {
	var err error
	errCount := 0 // start at zero because we increment before doing work
	retries := 1
	var server *Server
	for {
		if errCount++; errCount > retries {
			return nil, err
		}

		server, err = NewServer(dsn, opts...)
		if err != nil {
			if lastAttempt(errCount, retries) {
				return nil, err
			}
			if werr := waitBeforeRetry(ctx, errCount); werr != nil {
				return nil, err
			}
			continue
		}

		if err = server.Ping(ctx); err != nil {
			server.Close()
			if lastAttempt(errCount, retries) {
				return nil, err
			}
			if werr := waitBeforeRetry(ctx, errCount); werr != nil {
				return nil, err
			}
			continue
		}
		break
	}
	return server, nil
}

// lastAttempt says whether the attempt that just failed was the final one.
//
// The loop returns on its next turn when the count passes the limit, so waiting
// after the last attempt buys nothing: no further attempt follows it. The wait
// is a second, cut short only by the request ending, and discovery holds a
// connection permit throughout it. A login refused immediately could therefore
// spend the rest of a short budget doing nothing, leaving the whole fan-out
// deferred.
func lastAttempt(errCount, retries int) bool {
	return errCount >= retries
}

func (e *Exporter) setupInternalMetrics() {
	e.duration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   namespace,
		Subsystem:   exporter,
		Name:        "last_scrape_duration_seconds",
		Help:        "Duration of the last scrape of metrics from PostgreSQL.",
		ConstLabels: e.constantLabels,
	})
	e.totalScrapes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   exporter,
		Name:        "scrapes_total",
		Help:        "Total number of times PostgreSQL was scraped for metrics.",
		ConstLabels: e.constantLabels,
	})
	e.error = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   namespace,
		Subsystem:   exporter,
		Name:        "last_scrape_error",
		Help:        "Whether the last scrape of metrics from PostgreSQL resulted in an error (1 for error, 0 for success).",
		ConstLabels: e.constantLabels,
	})
	e.psqlUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "up",
		Help:        "Whether the last scrape of metrics from PostgreSQL was able to connect to the server (1 for yes, 0 for no).",
		ConstLabels: e.constantLabels,
	})
	e.userQueriesError = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   namespace,
		Subsystem:   exporter,
		Name:        "user_queries_load_error",
		Help:        "Whether the user queries file was loaded and parsed successfully (1 for error, 0 for success).",
		ConstLabels: e.constantLabels,
	}, []string{"filename", "hashsum"})
	e.coverage = newCoverageMetrics(e.constantLabels, e.lastSuccess, e.collectorName)
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if !e.enabled {
		return
	}
	e.scrape(ch)

	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.error
	ch <- e.psqlUp
	e.userQueriesError.Collect(ch)
	e.coverage.collect(ch)
}

func newDesc(subsystem, name, help string, labels prometheus.Labels) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help, nil, labels,
	)
}

func checkPostgresVersion(ctx context.Context, db *sql.DB, server string) (semver.Version, string, error) {
	level.Debug(logger).Log("msg", "Querying PostgreSQL version", "server", server)
	versionRow := db.QueryRowContext(ctx, "SELECT version();")
	var versionString string
	err := versionRow.Scan(&versionString)
	if err != nil {
		return semver.Version{}, "", fmt.Errorf("Error scanning version string on %q: %v", server, err)
	}
	semanticVersion, err := parseVersion(versionString)
	if err != nil {
		return semver.Version{}, "", fmt.Errorf("Error parsing version string on %q: %v", server, err)
	}

	return semanticVersion, versionString, nil
}

// Check and update the exporters query maps if the version has changed.
func (e *Exporter) checkMapVersions(ctx context.Context, ch chan<- prometheus.Metric, server *Server) error {
	semanticVersion, versionString, err := versioncache.GetVersion(ctx, server.db, server.String(), checkPostgresVersion)
	if err != nil {
		return fmt.Errorf("Error fetching version string on %q: %v", server, err)
	}

	if !e.disableDefaultMetrics && semanticVersion.LT(lowestSupportedVersion) {
		level.Warn(logger).Log("msg", "PostgreSQL version is lower than our lowest supported version", "server", server, "version", semanticVersion, "lowest_supported_version", lowestSupportedVersion)
	}

	// Check if semantic version changed and recalculate maps if needed.
	// if semanticVersion.NE(server.lastMapVersion[e.resolutionEnabled]) || server.metricMap == nil {
	//	level.Info(logger).Log("msg", "Semantic version changed", "server", server, "from", server.lastMapVersion[e.resolutionEnabled], "to", semanticVersion)
	server.mappingMtx.Lock()

	// Get Default Metrics only for master database
	if !e.disableDefaultMetrics && server.master {
		server.metricMap = makeDescMap(semanticVersion, server.labels, e.builtinMetricMaps)
		server.queryOverrides = makeQueryOverrideMap(semanticVersion, queryOverrides)
	} else {
		server.metricMap = make(map[string]MetricMapNamespace)
		server.queryOverrides = make(map[string]string)
	}

	// The version every per-query version restriction is tested against.
	// It was never assigned, so runonserver compared against the zero version
	// and a restricted query either always ran or never did.
	server.lastMapVersion = semanticVersion

	if e.userQueriesPath[HR] != "" || e.userQueriesPath[MR] != "" || e.userQueriesPath[LR] != "" {
		// Clear the metric while reload
		e.userQueriesError.Reset()
	}

	e.loadCustomQueries(ctx, e.resolutionEnabled, semanticVersion, server)

	server.mappingMtx.Unlock()

	// Output the version as a special metric only for master database
	versionDesc := prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, staticLabelName),
		"Version string as reported by postgres", []string{"version", "short_version"}, server.labels)

	if !e.disableDefaultMetrics && server.master {
		ch <- prometheus.MustNewConstMetric(versionDesc,
			prometheus.UntypedValue, 1, versionString, semanticVersion.String())
	}
	return nil
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) {
	defer func(begun time.Time) {
		e.duration.Set(time.Since(begun).Seconds())
	}(time.Now())

	e.totalScrapes.Inc()

	d := discovery{ok: true, discovered: len(e.dsn)}
	if e.autoDiscoverDatabases {
		d = e.discoverDatabaseDSNs()
	} else {
		// No discovery, so no catalog and no names. The targets are the
		// configured DSNs as given, identified by server only.
		for _, dsn := range e.dsn {
			fingerprint, err := parseFingerprint(dsn)
			if err != nil {
				level.Error(logger).Log("msg", "Unable to identify the server for a DSN", "dsn", loggableDSN(dsn), "err", err)
			}
			d.targets = append(d.targets, scrapeTarget{
				dsn:         dsn,
				databaseRef: databaseRef{server: fingerprint, identity: identityOf(dsn)},
			})
		}
	}
	// Published before the fan-out, not after. If this scrape is cut short, the
	// intent still reaches the response; a shortfall with no record of what was
	// intended is indistinguishable from no shortfall.
	e.coverage.setSelection(e.autoDiscoverDatabases, d.ok, d.discovered, d.targets, d.excluded, d.notIncluded)

	errorsCount, connectionErrorsCount, deferredCount := e.scrapeTargets(ch, d.targets)

	e.coverage.setOutcome(len(d.targets), errorsCount, deferredCount)

	up, failed := health(len(d.targets), deferredCount, connectionErrorsCount, errorsCount, d)
	e.psqlUp.Set(up)
	e.error.Set(failed)
}

// health is what this scrape says about the server and about itself.
//
// Split out because it has to answer for work the fan-out never started as well
// as work it did, and those two came from different places: the fan-out's own
// counts, and what discovery saw before the fan-out began. Reading only the
// first is how a scrape that could not log in at all reported the server as up.
//
// up is 1 when something connected, 0 when something tried to connect and
// nothing succeeded, and 1 when nothing tried, because work that never started
// says nothing about the server either way. failed is 1 when anything went
// wrong, discovery included.
//
// Discovery's successful connection counts only where the fan-out attempted
// nothing. Where the fan-out did attempt and every attempt failed at the
// connection, that is what the scrape observed of the databases and it is not
// overridden by the catalogue read having worked.
func health(targets, deferred, connectionErrors, errors int, d discovery) (up, failed float64) {
	// A target that was attempted and did not fail at the connection is one
	// that connected.
	attempted := targets - deferred
	fanOutConnected := attempted - connectionErrors

	switch {
	case fanOutConnected > 0:
		up = 1
	case attempted == 0 && d.connectSuccesses > 0:
		// Nothing was attempted, so the only connection this scrape made is
		// discovery's, and it worked. Without this a seed that connected and
		// listed its databases, followed by a second seed that was refused and
		// a fan-out with no budget left, reported the server as down.
		up = 1
	case connectionErrors > 0 || d.connectFailures > 0:
		up = 0
	default:
		up = 1
	}

	if errors > 0 || !d.ok {
		failed = 1
	}
	return up, failed
}

// rotateTargets puts this request's targets in the order its schedule says.
//
// One schedule over everything the request selected, not one per server. Two
// earlier shapes were wrong and both starve a target rather than drop one.
//
// Grouping by server and keeping the groups in the order they first appear
// leaves the same server ahead of every other on every request. Rotating inside
// a group moves nothing between groups, so when the first server's attempt
// consumes the budget a later server is never attempted, however often its own
// single-member schedule advances. Measured on one target each at two hosts:
// twelve calls, the same host first every time.
//
// Naming the members by database, and keying the schedule by the host and port,
// collapses two configured connections that differ only in role, credentials or
// search_path and happen to reach the same database. They supply the same member
// name, and the first index matching the chosen name is always the same target,
// so the other never gets a turn. Measured on role_a and role_b over one
// endpoint and database: twelve calls, role_a first every time. The public
// server label is a host and port by construction and cannot tell those apart.
//
// So the members are the per-target identities this process assigns per
// connection string, which differ for two roles over one database and for two
// databases over one connection, and the schedule belongs to the request's
// configured set and its profile.
//
// A rotation and not a selection: every target is still returned, so a database
// that gets no attempt is still reported as deferred by name.
func (e *Exporter) rotateTargets(targets []scrapeTarget) []scrapeTarget {
	if len(targets) < 2 {
		return targets
	}

	members := make([]string, 0, len(targets))
	for _, t := range targets {
		members = append(members, t.identity)
	}

	key := rotation.Key{Engine: "query", Target: e.scheduleTarget(), Profile: e.collectorName}
	out := make([]scrapeTarget, 0, len(targets))
	for _, i := range rotation.Order(e.rotation.Next(key, members), len(targets)) {
		out = append(out, targets[i])
	}
	return out
}

// scheduleTarget identifies the configured set this request works over.
//
// The seed connection strings, as the identities this process assigns them, so
// two exporters configured with different sets keep their own schedules and the
// same set keeps one across requests. Identities rather than the strings
// themselves, because a connection string carries a password.
func (e *Exporter) scheduleTarget() string {
	ids := make([]string, 0, len(e.dsn))
	for _, dsn := range e.dsn {
		ids = append(ids, identityOf(dsn))
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// scrapeTargets runs the per-database fan-out and returns how many targets
// failed, how many of those failed to connect, and how many were deferred
// without being started.
//
// Split out of scrape so the per-database outcome reporting can be tested with
// targets that carry names. scrape builds its targets from discovery, and
// discovery needs a server, so without this the only reachable test case was
// the one where no name is known and the per-database gauges are empty.
func (e *Exporter) scrapeTargets(ch chan<- prometheus.Metric, targets []scrapeTarget) (failed, connectionFailures, deferred int) {
	var errorsCount atomic.Int32
	var connectionErrorsCount atomic.Int32
	var deferredCount atomic.Int32

	// Rotated, so the database offered the first attempt is not the same one
	// every scrape. Per server, because one call can carry targets discovered
	// from several seed DSNs and a schedule is about one server's selected set.
	targets = e.rotateTargets(targets)

	// A fixed number of workers, each taking the next database as it finishes
	// the last. Reaching that number is not a reason to leave a database out:
	// every target is visited, and what stops one short is its own admission
	// check against the scrape's deadline.
	fanout.Run(len(targets), e.dbWorkers, func(i int) {
		{
			target := targets[i]

			// Admission is checked whether or not a connection limit is
			// configured, and the decision says which of the two ran out. The
			// limiter answers an expired deadline and a full limit with the
			// same error, so reading that error alone reported every expired
			// scrape as a connection limit.
			permit, decision := admission.Admit(e.ctx, e.connSema)
			if decision != admission.Admitted {
				// This database is not scraped, so it must not be counted as
				// one that was: returning silently here would publish a
				// database that was never attempted as successfully covered,
				// which is the false claim these metrics exist to prevent.
				//
				// Deferred rather than failed. Nothing was tried, so nothing
				// is known to be wrong with this database, and the two call
				// for different responses: a failure points at the database
				// and a deferral points at the budget. It is not a connection
				// error either, because nothing was connected to, so pg_up
				// keeps the meaning it had.
				reason := deferralReason(decision)
				level.Warn(logger).Log("msg", "Deferred a database",
					"datname", target.datname, "reason", reason)
				deferredCount.Add(1)
				e.coverage.setDatabaseDeferred(target.databaseRef, reason)
				return
			}
			defer permit.Release()
			if err := e.scrapeDSN(ch, &target); err != nil {
				errorsCount.Add(1)

				level.Error(logger).Log("err", err)

				reason := reasonScrape
				if _, ok := err.(*ErrorConnectToServer); ok {
					connectionErrorsCount.Add(1)
					reason = reasonConnect
				}
				e.coverage.setDatabaseOutcome(target.databaseRef, reason)
				return
			}
			e.coverage.setDatabaseOutcome(target.databaseRef, "")
		}
	})

	return int(errorsCount.Load()), int(connectionErrorsCount.Load()), int(deferredCount.Load())
}

// namespaceDefinition fingerprints what a namespace asks for, so a cached
// result from before a query file was edited is not served after it.
//
// The column mapping is included as well as the query, because changing what a
// column means changes the answer without changing the SQL. The map is walked
// in sorted order, since Go's iteration order is not stable and a fingerprint
// that changes on its own would throw the cache away on every scrape.
func namespaceDefinition(namespace string, mapping intermediateMetricMap) string {
	columns := make([]string, 0, len(mapping.columnMappings))
	for name := range mapping.columnMappings {
		columns = append(columns, name)
	}
	sort.Strings(columns)

	h := sha256.New()
	fmt.Fprintf(h, "namespace=%s\n", namespace)
	fmt.Fprintf(h, "master=%v cache=%d runOnServer=%s\n",
		mapping.master, mapping.cacheSeconds, mapping.runOnServerRaw)
	for _, name := range columns {
		column := mapping.columnMappings[name]
		fmt.Fprintf(h, "column=%s usage=%v description=%s\n",
			name, column.usage, column.description)
	}
	return hex.EncodeToString(h.Sum(nil))
}
