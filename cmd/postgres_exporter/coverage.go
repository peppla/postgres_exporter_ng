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

// Coverage reporting: how many databases a scrape meant to cover, and how many
// it did.
//
// Before this, that question had no answer. `pg_up` goes to 0 only when EVERY
// database fails, so nine of ten unreachable still reported a healthy server.
// Discovery failure was silent: an error from the discovery query left the DSN
// list holding only the configured database, and the next scrape simply covered
// one database instead of fifty. A silent collapse from fifty to one looked
// exactly like a healthy single-database instance.
//
// These metrics are deliberately counts, not a scheduler. They answer "is the
// coverage what I asked for?" and nothing else.
//
// # The vocabulary
//
//	discovered  when discovery ran: databases the server reported as connectable,
//	            plus the one named in the DSN. The discovery query excludes
//	            current_database(), so the seed has to be added back or this
//	            undercounts by one -- the kind of off-by-one that makes a
//	            coverage metric lie. When discovery is disabled or failed it is
//	            the configured DSNs instead, which is a fallback rather than an
//	            inventory, and with several DSNs it is a sum across them.
//	selected    databases this scrape intends to cover, after exclusion and
//	            inclusion. Always at least 1: the configured DSN is in the set
//	            before any filter is applied, and cannot be filtered out.
//	scraped     selected databases whose scrape returned no error.
//	failed      selected databases the scrape did not cover, whether because it
//	            returned an error or because it was never started -- work
//	            skipped for want of a connection permit lands here, because the
//	            alternative is reporting it as covered.
//
// The invariant is `scraped + failed == selected`, and it is tested. It is what
// makes the set usable in an alert: anything else means the exporter lost track
// of its own work.
//
// `scraped` is NOT a claim of complete coverage for that database. It means the
// scrape reported no error. A collector that returned fewer rows than it should
// have still counts as scraped, and distinguishing that is a later change with
// its own metric -- per-collector status -- rather than a different meaning for
// this one.
//
// # Per-database status
//
// The counts answer "did we lose a database?". They cannot answer "which one?",
// and averaging a shortfall across a set is how one database going dark stays
// invisible. So every target the scrape knows a name for also reports its own
// outcome:
//
//	database_selected{server,datname}        1 for each database this scrape meant to cover
//	database_scrape_success{server,datname}  1 if it was covered, 0 if it was not
//	database_scrape_error{server,datname,reason}  1, with a reason from a closed set
//	database_excluded{server,datname,reason}      1, for configuration, not failure
//
// The server label is the host:port fingerprint the exporter already uses for
// its other per-server metrics. Without it, two servers that each hold a
// database called postgres share one series, and a failure on one is erased by
// a success on the other.
//
// The name was the obstacle. The DSN list is a list of connection strings, and
// the catalog query used to exclude current_database(), so the one name that
// could not be recovered was the configured database's own. The query returns
// it now, which removes the off-by-one at its source and names every target
// without a second round trip and without a connection-string parser.
//
// Names are therefore known whenever discovery ran. With discovery disabled
// there is a single target, the configured DSN, and no name for it; the counts
// are still published and pg_up still answers the question. That is a gap in
// the gauges rather than a silent claim, and closing it means parsing DSNs in
// both URI and keyword forms, which is its own change.
//
// # Cardinality
//
// One series per database per family, so a 50-database server costs a few
// hundred. That is the cost of being able to name the database that went dark.
//
// Exclusion is the asymmetric one. A database the operator named in an exclude
// list gets its own series, because that list is bounded by the configuration
// they wrote. Databases left out by a non-empty include list are counted and
// not named, because that complement can be the entire server: an include list
// of three on a server of five hundred would otherwise mint 497 series to say
// nothing the operator does not already know.
//
// # The vectors are reset every scrape
//
// A database that is dropped from the server, or removed from the selection,
// must stop being reported rather than freeze at its last value. The
// alternative is a gauge that still says a deleted database is healthy.
//
// # Scope
//
// These describe the legacy engine's fan-out, which is the only thing in this
// exporter that covers more than one database. The namespaced collectors read
// the DSN database alone and have nothing to report here yet.

package main

import (
	"time"

	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/prometheus/client_golang/prometheus"
)

type coverageMetrics struct {
	// lastSuccess is the process's, not this scrape's: how long a database has
	// been failing cannot be answered from one scrape. job names the collection
	// this set of metrics belongs to, because one store serves them all and
	// they succeed and fail independently.
	lastSuccess *lastsuccess.Store
	job         string

	discoveryEnabled prometheus.Gauge
	discoverySuccess prometheus.Gauge
	discovered       prometheus.Gauge
	selected         prometheus.Gauge
	attempted        prometheus.Gauge
	scraped          prometheus.Gauge
	failed           prometheus.Gauge
	deferred         prometheus.Gauge
	excluded         prometheus.Gauge

	databaseLastSuccess *prometheus.GaugeVec

	databaseSelected *prometheus.GaugeVec
	databaseSuccess  *prometheus.GaugeVec
	databaseError    *prometheus.GaugeVec
	databaseDeferred *prometheus.GaugeVec
	databaseExcluded *prometheus.GaugeVec
}

func newCoverageMetrics(constLabels prometheus.Labels, lastSuccess *lastsuccess.Store, job string) *coverageMetrics {
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   exporter,
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		})
	}
	vec := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   exporter,
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		}, labels)
	}
	return &coverageMetrics{
		lastSuccess: lastSuccess,
		job:         job,

		discoveryEnabled: gauge("database_discovery_enabled",
			"Whether database auto-discovery is configured (1 for yes, 0 for no)."),
		discoverySuccess: gauge("database_discovery_success",
			"Whether the last attempt to determine the set of databases succeeded (1 for yes, 0 for no). 1 when discovery is disabled, because the set is then the configured DSN and nothing can fail."),
		discovered: gauge("databases_discovered",
			"Databases known to the last scrape. When discovery ran, this is what the server reported plus the database named in the DSN, which the discovery query omits. When discovery is disabled or failed, it is the configured DSNs alone, which is a fallback and not a server inventory -- read database_discovery_enabled and database_discovery_success before reading this as one. With several DSNs it is a sum across them, not a distinct total."),
		selected: gauge("databases_selected",
			"Databases the last scrape intended to cover, after exclusion and inclusion."),
		attempted: gauge("databases_attempted",
			"Selected databases the last scrape started work on. Selected minus deferred."),
		scraped: gauge("databases_scraped",
			"Databases whose scrape returned no error at the last scrape. This is the completed count. Not a claim of complete coverage for that database."),
		failed: gauge("databases_failed",
			"Databases the last scrape started work on and did not cover, because the connection or a query failed. Work that was never started is counted in databases_deferred instead, which is a different thing: a failure says something is wrong with that database and a deferral says the exporter ran out of room."),
		deferred: gauge("databases_deferred",
			"Selected databases the last scrape did not start work on, because there was no room within its budget. Not covered and not failed. A number here that does not fall is a selected set the exporter cannot keep up with."),
		excluded: gauge("databases_excluded",
			"Databases the server offered that configuration left out of the selected set, by an exclude list or by a non-empty include list. These are decisions, not failures, and are never counted in databases_failed."),

		databaseLastSuccess: vec("database_last_success_timestamp_seconds",
			"When this database was last collected without error, as seconds since the epoch. Absent for a database this process has never collected, which is not the same as one it last collected at the epoch. Published for every selected database whose name is known, so a database that is failing keeps reporting when it last worked. The target label is this process's identity for the connection, because several connections share a host and port and each is collected separately; it is not stable across restarts.", "server", "datname", "target"),

		databaseSelected: vec("database_selected",
			"1 for each database the last scrape intended to cover, by server and database name. Absent for a database that was not selected.", "server", "datname"),
		databaseSuccess: vec("database_scrape_success",
			"Whether the last scrape of this database returned no error (1 for yes, 0 for no). Present for every selected database whose name is known, so a database going dark is visible rather than averaged into a count. Not a claim of complete coverage for that database.", "server", "datname"),
		databaseError: vec("database_scrape_error",
			"1 for each selected database the last scrape started work on and did not cover, labelled with why: connect for a connection that failed, scrape for a query or collector that failed. Work that never started is in database_deferred instead. Reasons are a closed set, never an error message.", "server", "datname", "reason"),
		databaseDeferred: vec("database_deferred",
			"1 for each selected database the last scrape did not start work on, labelled with why: no_connection_permit for work that waited for a connection and did not get one, and scrape_deadline for work the scrape ran out of time to start. Reasons are a closed set, never an error message.", "server", "datname", "reason"),
		databaseExcluded: vec("database_excluded",
			"1 for each database an exclude list named and kept out of the selected set. Databases left out by a non-empty include list are counted in databases_excluded and not named here, because that set can be the whole server.", "server", "datname", "reason"),
	}
}

// setSelection records what the exporter decided to cover, before it tries.
//
// This is ordering inside the scrape, not a delivery guarantee, and an earlier
// revision of this file claimed otherwise. Collect runs scrape synchronously
// and emits these gauges only after it returns, so a client whose HTTP request
// times out receives nothing at all -- intent included. Setting the values
// first keeps them correct with respect to the work, and nothing more.
func (c *coverageMetrics) setSelection(discoveryEnabled, discoverySuccess bool, discovered int, targets []scrapeTarget, excluded []databaseRef, notIncluded int) {
	c.discoveryEnabled.Set(boolToFloat(discoveryEnabled))
	c.discoverySuccess.Set(boolToFloat(discoverySuccess))
	c.discovered.Set(float64(discovered))
	c.selected.Set(float64(len(targets)))
	c.excluded.Set(float64(len(excluded) + notIncluded))

	// Reset before setting, so a database that is gone stops being reported
	// instead of keeping the value it had when it was last seen.
	c.databaseLastSuccess.Reset()
	c.databaseSelected.Reset()
	c.databaseSuccess.Reset()
	c.databaseError.Reset()
	c.databaseDeferred.Reset()
	c.databaseExcluded.Reset()

	for _, t := range targets {
		if t.datname == "" {
			// Not known before the connection is open, and a connection that
			// fails never opens. What this connection last succeeded on is the
			// database the freshness diagnostic belongs to, and dropping the
			// series exactly when the connection breaks drops it when it
			// matters most.
			if database, when, ok := c.lastSuccess.Latest(lastsuccess.EngineQuery, lastsuccess.OneCollector, t.identity, c.job); ok {
				c.databaseLastSuccess.WithLabelValues(t.server, database, t.identity).
					Set(float64(when.UnixNano()) / 1e9)
			}
			continue
		}
		c.databaseSelected.WithLabelValues(t.server, t.datname).Set(1)
		// What this process knows from before this scrape. A database that
		// fails today keeps reporting when it last worked, which is the
		// question the counts cannot answer.
		if when, ok := c.lastSuccess.At(c.keyFor(t.databaseRef)); ok {
			c.databaseLastSuccess.WithLabelValues(t.server, t.datname, t.identity).Set(float64(when.UnixNano()) / 1e9)
		}
		// Assume nothing until the outcome is known. A scrape cut short
		// leaves this at 0, which is the honest reading: it was not covered.
		c.databaseSuccess.WithLabelValues(t.server, t.datname).Set(0)
	}
	for _, ref := range excluded {
		c.databaseExcluded.WithLabelValues(ref.server, ref.datname, reasonExcludeList).Set(1)
	}
}

// setDatabaseOutcome records one database's result. Called once per target,
// from the goroutine that handled it.
func (c *coverageMetrics) setDatabaseOutcome(ref databaseRef, reason string) {
	if ref.datname == "" {
		return
	}
	if reason == "" {
		c.databaseSuccess.WithLabelValues(ref.server, ref.datname).Set(1)
		// Recorded against the connection rather than the host and port,
		// because several targets share those and a success on one of them is
		// not a success on the others.
		now := time.Now()
		c.lastSuccess.Record(c.keyFor(ref), now)
		c.databaseLastSuccess.WithLabelValues(ref.server, ref.datname, ref.identity).Set(float64(now.UnixNano()) / 1e9)
		return
	}
	c.databaseSuccess.WithLabelValues(ref.server, ref.datname).Set(0)
	c.databaseError.WithLabelValues(ref.server, ref.datname, reason).Set(1)
}

// keyFor is what this set of metrics remembers a success under: the
// connection, the database, and the job collecting them.
func (c *coverageMetrics) keyFor(ref databaseRef) lastsuccess.Key {
	return lastsuccess.Key{
		Engine:    lastsuccess.EngineQuery,
		Target:    ref.identity,
		Database:  ref.datname,
		Collector: c.job,
	}
}

// setDatabaseDeferred records one database the scrape never started work on.
//
// Success is 0, as it is for a failure: the database was not covered either
// way. What differs is that no error series is published for it, because
// nothing was tried and nothing went wrong with the database.
func (c *coverageMetrics) setDatabaseDeferred(ref databaseRef, reason string) {
	if ref.datname == "" {
		return
	}
	c.databaseSuccess.WithLabelValues(ref.server, ref.datname).Set(0)
	c.databaseDeferred.WithLabelValues(ref.server, ref.datname, reason).Set(1)
}

// setOutcome records what actually happened.
//
// attempted and scraped are derived rather than counted separately, so the two
// invariants cannot drift: attempted plus deferred is selected, and scraped
// plus failed is attempted. An exporter that loses track of its own work is
// the thing these metrics exist to make visible, so they must not be able to
// disagree with each other.
func (c *coverageMetrics) setOutcome(selected, failed, deferred int) {
	attempted := selected - deferred
	c.attempted.Set(float64(attempted))
	c.scraped.Set(float64(attempted - failed))
	c.failed.Set(float64(failed))
	c.deferred.Set(float64(deferred))
}

func (c *coverageMetrics) collect(ch chan<- prometheus.Metric) {
	ch <- c.discoveryEnabled
	ch <- c.discoverySuccess
	ch <- c.discovered
	ch <- c.selected
	ch <- c.attempted
	ch <- c.scraped
	ch <- c.failed
	ch <- c.deferred
	ch <- c.excluded
	c.databaseLastSuccess.Collect(ch)
	c.databaseSelected.Collect(ch)
	c.databaseSuccess.Collect(ch)
	c.databaseError.Collect(ch)
	c.databaseDeferred.Collect(ch)
	c.databaseExcluded.Collect(ch)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
