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

// Logical replication publications, from the publisher's side.
//
// pg_publication is a per-database catalogue, so a publication created in one
// database is invisible from another. The query file this replaces ran with
// master: true, which meant it only ever described the database the connection
// string named. As a database-scoped collector it covers every selected
// database and says which one each publication is in.

package collector

import (
	"context"
	"database/sql"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const publicationSubsystem = "publication"

func init() {
	registerCollector(publicationSubsystem, defaultEnabled, scopeDatabase, NewPGPublicationCollector)
}

type PGPublicationCollector struct {
	log log.Logger
}

func NewPGPublicationCollector(config collectorConfig) (Collector, error) {
	return &PGPublicationCollector{log: config.logger}, nil
}

var (
	publicationLabels = []string{"datname", "pubname"}

	publicationAllTables = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, publicationSubsystem, "puballtables"),
		"Whether this publication covers every table in the database (1 for yes, 0 for no).",
		publicationLabels,
		prometheus.Labels{},
	)
	publicationInsert = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, publicationSubsystem, "pubinsert"),
		"Whether this publication replicates inserts (1 for yes, 0 for no).",
		publicationLabels,
		prometheus.Labels{},
	)
	publicationUpdate = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, publicationSubsystem, "pubupdate"),
		"Whether this publication replicates updates (1 for yes, 0 for no).",
		publicationLabels,
		prometheus.Labels{},
	)
	publicationDelete = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, publicationSubsystem, "pubdelete"),
		"Whether this publication replicates deletes (1 for yes, 0 for no).",
		publicationLabels,
		prometheus.Labels{},
	)
	publicationTruncate = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, publicationSubsystem, "pubtruncate"),
		"Whether this publication replicates truncates (1 for yes, 0 for no).",
		publicationLabels,
		prometheus.Labels{},
	)

	publicationQuery = `SELECT
		current_database() AS datname,
		pubname,
		puballtables,
		pubinsert,
		pubupdate,
		pubdelete,
		pubtruncate
	FROM pg_catalog.pg_publication;`
)

func (c *PGPublicationCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.getDB().QueryContext(ctx, publicationQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname, pubname sql.NullString
		var allTables, insert, update, del, truncate sql.NullBool

		if err := rows.Scan(&datname, &pubname, &allTables, &insert, &update, &del, &truncate); err != nil {
			return err
		}

		labels := []string{nullStringLabel(datname), nullStringLabel(pubname)}

		emit := func(desc *prometheus.Desc, vtype prometheus.ValueType, v sql.NullBool) {
			if !v.Valid {
				return
			}
			ch <- prometheus.MustNewConstMetric(desc, vtype, boolToFloat(v.Bool), labels...)
		}
		emit(publicationAllTables, prometheus.GaugeValue, allTables)
		emit(publicationInsert, prometheus.GaugeValue, insert)
		emit(publicationUpdate, prometheus.GaugeValue, update)
		emit(publicationDelete, prometheus.GaugeValue, del)
		emit(publicationTruncate, prometheus.GaugeValue, truncate)
	}

	return rows.Err()
}
