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

// Logical replication subscriptions, from the subscriber's side.
//
// This was a shipped query file, and one of its metrics never worked. It
// selected received_lsn without converting it, and a pg_lsn reaches the query
// engine as the string "0/1FDF5FA0", which ParseFloat refuses. The engine
// logged one line at info level and published nothing, so the metric has never
// existed. Measured against PostgreSQL 18 rather than read off the code.
//
// Converting in SQL is the fix, and doing it in a collector means a value that
// cannot be read is a scrape error rather than a line in a log nobody reads.

package collector

import (
	"context"
	"database/sql"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const statSubscriptionSubsystem = "stat_subscription"

func init() {
	registerCollector(statSubscriptionSubsystem, defaultEnabled, scopeCluster, NewPGStatSubscriptionCollector)
}

type PGStatSubscriptionCollector struct {
	log log.Logger
}

func NewPGStatSubscriptionCollector(config collectorConfig) (Collector, error) {
	return &PGStatSubscriptionCollector{log: config.logger}, nil
}

var (
	statSubscriptionLabels = []string{"subid", "subname", "pid"}

	statSubscriptionReceivedLSN = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statSubscriptionSubsystem, "received_lsn_bytes"),
		"Write-ahead log position last received from the publisher, as a byte offset from the start of the log.",
		statSubscriptionLabels,
		prometheus.Labels{},
	)
	statSubscriptionLatestEndLSN = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statSubscriptionSubsystem, "latest_end_lsn_bytes"),
		"Write-ahead log position last reported back to the publisher, as a byte offset from the start of the log.",
		statSubscriptionLabels,
		prometheus.Labels{},
	)
	statSubscriptionLastMsgSendTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statSubscriptionSubsystem, "last_msg_send_time_seconds"),
		"When the publisher sent the last message this subscription received.",
		statSubscriptionLabels,
		prometheus.Labels{},
	)
	statSubscriptionLastMsgReceiptTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statSubscriptionSubsystem, "last_msg_receipt_time_seconds"),
		"When this subscription received that message. The difference from the send time is the one-way delay, if the clocks agree.",
		statSubscriptionLabels,
		prometheus.Labels{},
	)
	statSubscriptionLatestEndTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statSubscriptionSubsystem, "latest_end_time_seconds"),
		"When the last position was reported back to the publisher.",
		statSubscriptionLabels,
		prometheus.Labels{},
	)

	statSubscriptionQuery = `SELECT
		subid,
		subname,
		pid,
		received_lsn - '0/0' AS received_lsn_bytes,
		latest_end_lsn - '0/0' AS latest_end_lsn_bytes,
		EXTRACT(EPOCH FROM last_msg_send_time) AS last_msg_send_time_seconds,
		EXTRACT(EPOCH FROM last_msg_receipt_time) AS last_msg_receipt_time_seconds,
		EXTRACT(EPOCH FROM latest_end_time) AS latest_end_time_seconds
	FROM pg_catalog.pg_stat_subscription;`
)

func (c *PGStatSubscriptionCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.getDB().QueryContext(ctx, statSubscriptionQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var subid, pid sql.NullInt64
		var subname sql.NullString
		var receivedLSN, latestEndLSN sql.NullFloat64
		var lastMsgSend, lastMsgReceipt, latestEndTime sql.NullFloat64

		if err := rows.Scan(&subid, &subname, &pid, &receivedLSN, &latestEndLSN,
			&lastMsgSend, &lastMsgReceipt, &latestEndTime); err != nil {
			return err
		}

		// pid is NULL for a subscription whose worker is not running, which
		// is a state worth seeing rather than a reason to skip the row.
		labels := []string{nullIntLabel(subid), nullStringLabel(subname), nullIntLabel(pid)}

		emit := func(desc *prometheus.Desc, vtype prometheus.ValueType, v sql.NullFloat64) {
			if !v.Valid {
				return
			}
			ch <- prometheus.MustNewConstMetric(desc, vtype, v.Float64, labels...)
		}
		emit(statSubscriptionReceivedLSN, prometheus.GaugeValue, receivedLSN)
		emit(statSubscriptionLatestEndLSN, prometheus.GaugeValue, latestEndLSN)
		emit(statSubscriptionLastMsgSendTime, prometheus.GaugeValue, lastMsgSend)
		emit(statSubscriptionLastMsgReceiptTime, prometheus.GaugeValue, lastMsgReceipt)
		emit(statSubscriptionLatestEndTime, prometheus.GaugeValue, latestEndTime)
	}

	return rows.Err()
}
