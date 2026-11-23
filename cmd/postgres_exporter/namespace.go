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
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/lib/pq"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/prometheus/client_golang/prometheus"
)

// Query within a namespace mapping and emit metrics. Returns fatal errors if
// the scrape fails, and a slice of errors if they were non-fatal.
//
// Every metric here is built with a constructor that returns an error rather
// than one that panics. A query file decides these names and labels, so the
// exporter is handed invalid ones by ordinary editing: a label the registry
// reserves, or one that collides with a constant label the exporter adds. The
// panicking constructors turned both into an exit, from a scrape worker where
// nothing recovers it, so one bad line in a query file stopped the process
// rather than failing its namespace.
func queryNamespaceMapping(ctx context.Context, server *Server, namespace string, mapping MetricMapNamespace) ([]prometheus.Metric, []error, error) {
	// Check for a query override for this namespace
	query, found := server.queryOverrides[namespace]

	// Was this query disabled (i.e. nothing sensible can be queried on cu
	// version of PostgreSQL?
	if query == "" && found {
		// Return success (no pertinent data)
		return []prometheus.Metric{}, []error{}, nil
	}

	// Don't fail on a bad scrape of one metric
	var rows *sql.Rows
	var err error

	if !found {
		// I've no idea how to avoid this properly at the moment, but this is
		// an admin tool so you're not injecting SQL right?
		rows, err = server.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s;", namespace)) // nolint: gas
	} else {
		rows, err = server.db.QueryContext(ctx, query)
	}
	if err != nil {
		return []prometheus.Metric{}, []error{}, fmt.Errorf("Error running query on database %q: %s %v", server, namespace, err)
	}
	defer rows.Close() // nolint: errcheck

	var columnNames []string
	columnNames, err = rows.Columns()
	if err != nil {
		return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving column list for: ", namespace, err))
	}

	// Make a lookup map for the column indices
	var columnIdx = make(map[string]int, len(columnNames))
	for i, n := range columnNames {
		columnIdx[n] = i
	}

	// Every column the mapping names as a label has to be in the result, and
	// the namespace fails here if one is not.
	//
	// A name that is absent reads as index zero, because that is what a map
	// lookup returns for a key it does not hold, so the label would carry the
	// first column's value. That is not a partial answer: the series cannot be
	// aggregated by the label it is wrong about, and the file naming a column
	// it does not select is a mistake only the operator can fix.
	var missing []string
	for _, label := range mapping.labels {
		if _, ok := columnIdx[label]; !ok {
			missing = append(missing, label)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return []prometheus.Metric{}, []error{}, fmt.Errorf(
			"%s declares the label column %s, which its query does not return, so every sample would carry another column's value",
			namespace, strings.Join(missing, ", "))
	}

	var columnData = make([]interface{}, len(columnNames))
	var scanArgs = make([]interface{}, len(columnNames))
	for i := range columnData {
		scanArgs[i] = &columnData[i]
	}

	nonfatalErrors := []error{}

	metrics := make([]prometheus.Metric, 0)

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving rows:", namespace, err))
		}

		// Get the label values for this row.
		labels := make([]string, len(mapping.labels))
		for idx, label := range mapping.labels {
			labels[idx], _ = dbToString(columnData[columnIdx[label]])
		}

		// Loop over column names, and match to scan data. Unknown columns
		// will be filled with an untyped metric number *if* they can be
		// converted to float64s. NULLs are allowed and treated as NaN.
		for idx, columnName := range columnNames {
			var metric prometheus.Metric
			if metricMapping, ok := mapping.columnMappings[columnName]; ok {
				// Is this a metricy metric?
				if metricMapping.discard {
					continue
				}

				if metricMapping.histogram {
					var keys []float64
					err = pq.Array(&keys).Scan(columnData[idx])
					if err != nil {
						return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving", columnName, "buckets:", namespace, err))
					}

					var values []int64
					valuesIdx, ok := columnIdx[columnName+"_bucket"]
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Missing column: ", namespace, columnName+"_bucket")))
						continue
					}
					err = pq.Array(&values).Scan(columnData[valuesIdx])
					if err != nil {
						return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving", columnName, "bucket values:", namespace, err))
					}

					buckets := make(map[float64]uint64, len(keys))
					for i, key := range keys {
						if i >= len(values) {
							break
						}
						buckets[key] = uint64(values[i])
					}

					idx, ok = columnIdx[columnName+"_sum"]
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Missing column: ", namespace, columnName+"_sum")))
						continue
					}
					sum, ok := dbToFloat64(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName+"_sum", columnData[idx])))
						continue
					}

					idx, ok = columnIdx[columnName+"_count"]
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Missing column: ", namespace, columnName+"_count")))
						continue
					}
					count, ok := dbToUint64(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName+"_count", columnData[idx])))
						continue
					}

					metric, err = prometheus.NewConstHistogram(
						metricMapping.desc,
						count, sum, buckets,
						labels...,
					)
					if err != nil {
						return []prometheus.Metric{}, nonfatalErrors, fmt.Errorf(
							"%s cannot publish %s: %w", namespace, columnName, err)
					}
				} else {
					value, ok := dbToFloat64(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName, columnData[idx])))
						continue
					}
					// Generate the metric
					metric, err = prometheus.NewConstMetric(metricMapping.desc, metricMapping.vtype, value, labels...)
					if err != nil {
						return []prometheus.Metric{}, nonfatalErrors, fmt.Errorf(
							"%s cannot publish %s: %w", namespace, columnName, err)
					}
				}
			} else {
				// Unknown metric. Report as untyped if scan to float64 works, else note an error too.
				metricLabel := fmt.Sprintf("%s_%s", namespace, columnName)
				desc := prometheus.NewDesc(metricLabel, fmt.Sprintf("Unknown metric from %s", namespace), mapping.labels, server.labels)

				// Its not an error to fail here, since the values are
				// unexpected anyway.
				value, ok := dbToFloat64(columnData[idx])
				if !ok {
					nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unparseable column type - discarding: ", namespace, columnName, err)))
					continue
				}
				metric, err = prometheus.NewConstMetric(desc, prometheus.UntypedValue, value, labels...)
				if err != nil {
					return []prometheus.Metric{}, nonfatalErrors, fmt.Errorf(
						"%s cannot publish %s: %w", namespace, columnName, err)
				}
			}
			metrics = append(metrics, metric)
		}
	}
	// Next stops on error exactly as it stops at the end of the rows. Without
	// this, a namespace query that failed or was cancelled partway returns the
	// rows it managed to read with no error, and the scrape reports success
	// for a database it only partly collected.
	if err := rows.Err(); err != nil {
		return []prometheus.Metric{}, nonfatalErrors, fmt.Errorf("error retrieving rows: %s %w", namespace, err)
	}
	return metrics, nonfatalErrors, nil
}

// Iterate through all the namespace mappings in the exporter and run their
// queries.
func queryNamespaceMappings(ctx context.Context, ch chan<- prometheus.Metric, server *Server, cache *querycache.Cache, collector string) map[string]error {
	// Return a map of namespace -> errors
	namespaceErrors := make(map[string]error)

	for namespace, mapping := range server.metricMap {
		level.Debug(logger).Log("msg", "Querying namespace", "namespace", namespace)

		// Where this query runs. master: true means the configured connection
		// database and only it, whatever the databases list says. An absent
		// list means every database the engine reaches, which is why built-in
		// namespaces, whose list is always absent, behave exactly as before.
		if !mapping.databases.runsOn(server.datname, mapping.master, server.master) {
			level.Debug(logger).Log("msg", "Query skipped for this database",
				"namespace", namespace, "datname", server.datname)
			continue
		}

		// A query can restrict itself to a range of PostgreSQL versions. The
		// restriction belongs to the query, not to the server: a file holding
		// one query for 16 and later and another for everything must run each
		// where it applies. This used to read a server field that nothing
		// assigned, so every query in a file shared one restriction, which was
		// always empty.
		if mapping.runOnServer != nil && !mapping.runOnServer(server.lastMapVersion) {
			level.Debug(logger).Log("msg", "Query skipped for this database version",
				"namespace", namespace, "version", server.lastMapVersion.String(),
				"target_version", mapping.runOnServerRaw)
			continue
		}

		// The cache is the process's, not this request's. A per-request one
		// cannot hold anything: the handler builds a fresh exporter, which
		// builds a fresh server, so every scrape found an empty cache and ran
		// the query again however long cache_seconds was.
		key := querycache.Key{
			// The connection this came from, not the host and port it
			// reached, which targets share.
			Target:    server.cacheIdentity(),
			Database:  server.datname,
			Collector: collector,
			Namespace: namespace,
			// The SQL as well as the mapping. The query text is not part of
			// the mapping, it lives in the overrides, and leaving it out
			// meant editing a query file changed nothing: the old answer was
			// served for as long as cache_seconds said, and it answers a
			// question nobody is asking any more.
			Definition: definitionWithQuery(mapping.definition, server.queryOverrides[namespace]),
		}

		var metrics []prometheus.Metric
		var nonFatalErrors []error
		var err error

		// This namespace's collection time, read where its query runs rather
		// than once per scrape: a scrape collects one namespace after another,
		// and the time also orders two overlapping collections of the same
		// namespace. Read from the cache's clock, so the age below is a
		// difference between two readings of one clock.
		now := cache.Now()
		collectedAt := now

		cached, cachedAt, found := cache.Get(key)
		scrapeMetric := !found
		if found {
			metrics = cached
			collectedAt = cachedAt
		} else {
			metrics, nonFatalErrors, err = queryNamespaceMapping(ctx, server, namespace, mapping)
		}

		// Serious error - a namespace disappeared
		if err != nil {
			namespaceErrors[namespace] = err
			level.Error(logger).Log("err", err)
		}
		// Non-serious errors - likely version or parsing problems.
		if len(nonFatalErrors) > 0 {
			for _, err := range nonFatalErrors {
				level.Info(logger).Log("err", err)
			}
		}

		// Cacheable samples retain their collection timestamp on every
		// response. Uncached samples use the scraper's timestamp.
		cacheable := mapping.cacheSeconds > 0
		age := now.Sub(collectedAt)
		if age < 0 {
			age = 0
		}
		for _, metric := range metrics {
			if cacheable {
				metric = prometheus.NewMetricWithTimestamp(collectedAt, metric)
			}
			ch <- metric
		}

		// How old what this namespace served is. Published for a cacheable
		// namespace that produced an answer.
		//
		// Not for the others: without cache_seconds the age can only read
		// zero, and a series per namespace per database saying so is the one
		// thing here that grows with both the query files and the databases.
		// Not for a failed collection either, which has no samples to be the
		// age of, and reported zero: freshly collected, which is how a
		// namespace nothing could be read from looked like one read in full.
		if cacheable && err == nil {
			ch <- prometheus.MustNewConstMetric(
				cacheAgeDesc(server), prometheus.GaugeValue, age.Seconds(),
				namespace, server.datname, server.role, server.cacheIdentity(), collector)
		}

		// A failed query is not an answer, so it is not cached. Caching it
		// would publish the failure as an empty namespace for the whole cache
		// lifetime, and the retry would not happen until it expired: one slow
		// moment on the server blanked a namespace for a minute and reported
		// no error after the first scrape.
		if scrapeMetric && err == nil {
			cache.Put(key, metrics, collectedAt,
				time.Duration(mapping.cacheSeconds)*time.Second)
		}
	}

	return namespaceErrors
}

// cacheAgeDesc describes the age of the samples a namespace served.
//
// Built per server because the constant labels are the server's. The legacy
// engine builds descriptors per scrape throughout, so this is in keeping with
// its surroundings rather than a new cost.
func cacheAgeDesc(server *Server) *prometheus.Desc {
	return prometheus.NewDesc(
		"pg_exporter_namespace_cache_age_seconds",
		"Age in seconds of the samples this namespace served. Published for the namespaces that set cache_seconds, and not for a collection that failed. Zero means they were collected during this scrape. A positive value means they came from the cache, and those samples carry the time they were collected rather than the time of this scrape. The target label is this process's identity for the connection the samples were collected over, and it is not stable across restarts.",
		// One series per collection, so the labels have to say which
		// collection, and they are the parts of the cache key that a reader
		// can act on. The host and port in the constant labels cannot do it:
		// targets share those. datname and usename name what an operator
		// recognises. target separates collections that agree on all of it,
		// such as two connections to one database as one role with different
		// search_path values. collector separates the resolutions, which can
		// each define the same namespace with their own interval and their own
		// columns.
		//
		// datname and usename are empty when the connection string does not
		// name them and nothing has resolved them, which Prometheus reads as
		// absent. The engine must not also carry collector as a constant
		// label, which would make this descriptor invalid.
		[]string{"namespace", "datname", "usename", "target", "collector"},
		server.labels,
	)
}

// definitionWithQuery combines a namespace's mapping fingerprint with the SQL
// it runs, so either changing makes a cached result a different result.
func definitionWithQuery(definition, query string) string {
	h := sha256.New()
	fmt.Fprintf(h, "mapping=%s\nquery=%s", definition, query)
	return hex.EncodeToString(h.Sum(nil))
}
