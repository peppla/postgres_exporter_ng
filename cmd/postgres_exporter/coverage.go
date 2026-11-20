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
// # What is deliberately not here yet
//
// Per-database status, labelled by database name. It is wanted, and the
// contract asks for it, but the DSN list is a list of connection strings: the
// name is known for every discovered database and NOT for the configured one,
// because the discovery query excludes it. Getting it costs either an extra
// round trip on every scrape -- a cost paid by the monitored server, which the
// contract says to account for rather than assume -- or a DSN parser handling
// both URI and keyword forms. Either is its own change with its own tests.
//
// Counts first. They answer "did we lose a database?", which is the question
// that currently has no answer at all.
//
// # Scope
//
// These describe the legacy engine's fan-out, which is the only thing in this
// exporter that covers more than one database. The namespaced collectors read
// the DSN database alone and have nothing to report here yet.

package main

import "github.com/prometheus/client_golang/prometheus"

type coverageMetrics struct {
	discoveryEnabled prometheus.Gauge
	discoverySuccess prometheus.Gauge
	discovered       prometheus.Gauge
	selected         prometheus.Gauge
	scraped          prometheus.Gauge
	failed           prometheus.Gauge
}

func newCoverageMetrics(constLabels prometheus.Labels) *coverageMetrics {
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   exporter,
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		})
	}
	return &coverageMetrics{
		discoveryEnabled: gauge("database_discovery_enabled",
			"Whether database auto-discovery is configured (1 for yes, 0 for no)."),
		discoverySuccess: gauge("database_discovery_success",
			"Whether the last attempt to determine the set of databases succeeded (1 for yes, 0 for no). 1 when discovery is disabled, because the set is then the configured DSN and nothing can fail."),
		discovered: gauge("databases_discovered",
			"Databases known to the last scrape. When discovery ran, this is what the server reported plus the database named in the DSN, which the discovery query omits. When discovery is disabled or failed, it is the configured DSNs alone, which is a fallback and not a server inventory -- read database_discovery_enabled and database_discovery_success before reading this as one. With several DSNs it is a sum across them, not a distinct total."),
		selected: gauge("databases_selected",
			"Databases the last scrape intended to cover, after exclusion and inclusion."),
		scraped: gauge("databases_scraped",
			"Databases whose scrape returned no error at the last scrape. Not a claim of complete coverage for that database."),
		failed: gauge("databases_failed",
			"Databases the last scrape did not cover: the scrape returned an error, or it could not be started. Work that was skipped is counted here rather than left out, so it cannot be mistaken for success."),
	}
}

// setSelection records what the exporter decided to cover, before it tries.
//
// This is ordering inside the scrape, not a delivery guarantee, and an earlier
// revision of this file claimed otherwise. Collect runs scrape synchronously
// and emits these gauges only after it returns, so a client whose HTTP request
// times out receives nothing at all -- intent included. Setting the values
// first keeps them correct with respect to the work, and nothing more.
func (c *coverageMetrics) setSelection(discoveryEnabled, discoverySuccess bool, discovered, selected int) {
	c.discoveryEnabled.Set(boolToFloat(discoveryEnabled))
	c.discoverySuccess.Set(boolToFloat(discoverySuccess))
	c.discovered.Set(float64(discovered))
	c.selected.Set(float64(selected))
}

// setOutcome records what actually happened. scraped is derived from selected
// and failed rather than counted separately, so the invariant cannot drift.
func (c *coverageMetrics) setOutcome(selected, failed int) {
	c.scraped.Set(float64(selected - failed))
	c.failed.Set(float64(failed))
}

func (c *coverageMetrics) collect(ch chan<- prometheus.Metric) {
	ch <- c.discoveryEnabled
	ch <- c.discoverySuccess
	ch <- c.discovered
	ch <- c.selected
	ch <- c.scraped
	ch <- c.failed
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
