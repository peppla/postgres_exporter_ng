// Copyright 2023 The Prometheus Authors
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
	"fmt"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/distribution"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	registerCollector(statWalReceiverSubsystem, defaultEnabled, scopeCluster, NewPGStatWalReceiverCollector)
}

type PGStatWalReceiverCollector struct {
	log log.Logger
}

const statWalReceiverSubsystem = "stat_wal_receiver"

func NewPGStatWalReceiverCollector(config collectorConfig) (Collector, error) {
	return &PGStatWalReceiverCollector{log: config.logger}, nil
}

var (
	labelCats                      = []string{"upstream_host", "slot_name", "status"}
	statWalReceiverReceiveStartLsn = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "receive_start_lsn"),
		"First write-ahead log location used when WAL receiver is started represented as a decimal",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverReceiveStartTli = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "receive_start_tli"),
		"First timeline number used when WAL receiver is started",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverFlushedLSN = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "flushed_lsn"),
		"Last write-ahead log location already received and flushed to disk, the initial value of this field being the first log location used when WAL receiver is started represented as a decimal",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverReceivedTli = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "received_tli"),
		"Timeline number of last write-ahead log location received and flushed to disk",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverLastMsgSendTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "last_msg_send_time"),
		"Send time of last message received from origin WAL sender",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverLastMsgReceiptTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "last_msg_receipt_time"),
		"Send time of last message received from origin WAL sender",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverLatestEndLsn = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "latest_end_lsn"),
		"Last write-ahead log location reported to origin WAL sender as integer",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverLatestEndTime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "latest_end_time"),
		"Time of last write-ahead log location reported to origin WAL sender",
		labelCats,
		prometheus.Labels{},
	)
	statWalReceiverUpstreamNode = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statWalReceiverSubsystem, "upstream_node"),
		"Node ID of the upstream node",
		labelCats,
		prometheus.Labels{},
	)

	pgStatWalColumnQuery = `
	SELECT
		column_name
	FROM information_schema.columns
	WHERE
		table_name = 'pg_stat_wal_receiver' and
		column_name = 'flushed_lsn'
	`

	// The %s is flushed_lsn, which is selected only when the column exists.
	// Its position has to match where rows.Scan binds it in Update, which is
	// between receive_start_tli and received_tli. It used to sit one column
	// earlier, so on every server that has the column, and all supported
	// versions do, flushed_lsn was scanned into receive_start_tli and
	// receive_start_tli into flushed_lsn. Each metric carried the other's
	// value: a timeline number reported as a log position and a log position
	// reported as a timeline.
	//
	// The unit tests could not see it. They build their fixture columns in the
	// order Update scans, and sqlmock hands those back as given without
	// parsing the SQL, so the fixture agreed with the code and both differed
	// from the server. TestWalReceiverQueryMatchesScanOrder asks a real server
	// for the column order instead.
	pgStatWalReceiverQueryTemplate = `
	SELECT
		trim(both '''' from substring(conninfo from 'host=([^ ]*)')) as upstream_host,
		slot_name,
		status,
		(receive_start_lsn- '0/0') %% (2^52)::bigint as receive_start_lsn,
		receive_start_tli,
		%sreceived_tli,
		extract(epoch from last_msg_send_time) as last_msg_send_time,
		extract(epoch from last_msg_receipt_time) as last_msg_receipt_time,
		(latest_end_lsn - '0/0') %% (2^52)::bigint as latest_end_lsn,
		extract(epoch from latest_end_time) as latest_end_time,
		substring(slot_name from 'repmgr_slot_([0-9]*)') as upstream_node
	FROM pg_catalog.pg_stat_wal_receiver
	`
)

func (c *PGStatWalReceiverCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	// Skip Aurora instances (not supported pg_stat_get_wal_receiver)
	if distribution.IsAurora(ctx, instance.dsn, db) {
		return nil
	}

	hasFlushedLSNRows, err := db.QueryContext(ctx, pgStatWalColumnQuery)
	if err != nil {
		return err
	}

	hasFlushedLSN := hasFlushedLSNRows.Next()
	// This Next decides which query runs next, so an error here picks the
	// wrong one silently.
	if err := hasFlushedLSNRows.Err(); err != nil {
		hasFlushedLSNRows.Close()
		return err
	}
	var query string
	if hasFlushedLSN {
		query = fmt.Sprintf(pgStatWalReceiverQueryTemplate, "(flushed_lsn - '0/0') % (2^52)::bigint as flushed_lsn,\n")
	} else {
		query = fmt.Sprintf(pgStatWalReceiverQueryTemplate, "")
	}

	hasFlushedLSNRows.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var upstreamHost, slotName, status sql.NullString
		var receiveStartLsn, receiveStartTli, flushedLsn, receivedTli, latestEndLsn, upstreamNode sql.NullInt64
		var lastMsgSendTime, lastMsgReceiptTime, latestEndTime sql.NullFloat64

		if hasFlushedLSN {
			if err := rows.Scan(&upstreamHost, &slotName, &status, &receiveStartLsn, &receiveStartTli, &flushedLsn, &receivedTli, &lastMsgSendTime, &lastMsgReceiptTime, &latestEndLsn, &latestEndTime, &upstreamNode); err != nil {
				return err
			}
		} else {
			if err := rows.Scan(&upstreamHost, &slotName, &status, &receiveStartLsn, &receiveStartTli, &receivedTli, &lastMsgSendTime, &lastMsgReceiptTime, &latestEndLsn, &latestEndTime, &upstreamNode); err != nil {
				return err
			}
		}
		// A label the server did not report is published empty, which
		// Prometheus reads as absent. It used to skip the row: an ordinary
		// standby streaming without a replication slot reports slot_name
		// NULL, and the collector published nothing at all for it. There is
		// at most one receiver per standby, so an empty label cannot collide
		// with another row.
		labels := []string{upstreamHost.String, slotName.String, status.String}

		// Each measurement is published when the server reported it and
		// omitted when it did not, rather than one NULL column costing all of
		// them. upstream_node is the clearest case: it is parsed out of the
		// slot name with a repmgr pattern, so every slot name that is not
		// repmgr_slot_<n>, and every standby with no slot, left it NULL and
		// lost the receiver's positions, timelines and message times with it.
		// One shape, with the descriptor and its value type at the call, so
		// that the catalogue's extractor can still see what each descriptor is
		// emitted as. Closures it does not know about leave a descriptor with
		// no value type, which drops it out of every check that reads the
		// inventory.
		emit := func(desc *prometheus.Desc, vtype prometheus.ValueType, value float64, present bool) {
			if present {
				ch <- prometheus.MustNewConstMetric(desc, vtype, value, labels...)
			}
		}

		emit(statWalReceiverReceiveStartLsn, prometheus.CounterValue,
			float64(receiveStartLsn.Int64), receiveStartLsn.Valid)
		emit(statWalReceiverReceiveStartTli, prometheus.GaugeValue,
			float64(receiveStartTli.Int64), receiveStartTli.Valid)
		emit(statWalReceiverFlushedLSN, prometheus.CounterValue,
			float64(flushedLsn.Int64), hasFlushedLSN && flushedLsn.Valid)
		emit(statWalReceiverReceivedTli, prometheus.GaugeValue,
			float64(receivedTli.Int64), receivedTli.Valid)
		emit(statWalReceiverLastMsgSendTime, prometheus.CounterValue,
			lastMsgSendTime.Float64, lastMsgSendTime.Valid)
		emit(statWalReceiverLastMsgReceiptTime, prometheus.CounterValue,
			lastMsgReceiptTime.Float64, lastMsgReceiptTime.Valid)
		emit(statWalReceiverLatestEndLsn, prometheus.CounterValue,
			float64(latestEndLsn.Int64), latestEndLsn.Valid)
		emit(statWalReceiverLatestEndTime, prometheus.CounterValue,
			latestEndTime.Float64, latestEndTime.Valid)
		emit(statWalReceiverUpstreamNode, prometheus.GaugeValue,
			float64(upstreamNode.Int64), upstreamNode.Valid)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
