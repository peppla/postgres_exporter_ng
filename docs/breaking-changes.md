# Breaking changes

What stops working when you move to this exporter, and what to do about it.

Every entry here changes something a dashboard, an alert or a configuration
file can be relying on. Nothing in this file is a silent change.

## The pg_custom_ metrics are removed

**Decided by Pep on 2026-11-21.** Every metric whose name begins with
`pg_custom_` disappears. They were produced by query files the exporter
shipped, which made built-in monitoring depend on configuration an operator
could edit or delete. Built-in monitoring is the Go collectors, and the query
engine is for application queries an operator writes.

The measurements are not lost. Each one is published by a collector under a
name that says what it is rather than that it came from a file.

| removed | replacement |
| --- | --- |
| `pg_custom_stat_replication_write_lsn_bytes` | `pg_stat_replication_write_lsn_bytes` |
| `pg_custom_stat_replication_flush_lsn_bytes` | `pg_stat_replication_flush_lsn_bytes` |
| `pg_custom_stat_replication_replay_lsn_bytes` | `pg_stat_replication_replay_lsn_bytes` |
| `pg_custom_stat_replication_write_lag_seconds` | `pg_stat_replication_write_lag_seconds` |
| `pg_custom_stat_replication_flush_lag_seconds` | `pg_stat_replication_flush_lag_seconds` |
| `pg_custom_stat_replication_replay_lag_seconds` | `pg_stat_replication_replay_lag_seconds` |
| `pg_custom_stat_subscription_received_lsn` | `pg_stat_subscription_received_lsn_bytes` |
| `pg_custom_stat_subscription_last_msg_send_time` | `pg_stat_subscription_last_msg_send_time_seconds` |
| `pg_custom_stat_subscription_last_msg_receipt_time` | `pg_stat_subscription_last_msg_receipt_time_seconds` |
| `pg_custom_publication_puballtables` | `pg_publication_puballtables` |
| `pg_custom_publication_pubinsert` | `pg_publication_pubinsert` |
| `pg_custom_publication_pubupdate` | `pg_publication_pubupdate` |
| `pg_custom_publication_pubdelete` | `pg_publication_pubdelete` |
| `pg_custom_stat_activity_walsender_backend_start_unix` | `pg_stat_activity_walsender_backend_start_seconds` |
| `pg_custom_replication_slots_active` | `pg_replication_slot_slot_is_active` |
| `pg_custom_replication_slots_restart_lsn_bytes` | `pg_replication_slot_slot_restart_lsn` |
| `pg_custom_replication_slots_confirmed_flush_lsn_bytes` | `pg_replication_slot_slot_confirmed_flush_lsn` |
| `pg_custom_database_size_custom_bytes` | `pg_database_size_bytes` |
| `pg_custom_stat_wal_receiver_latest_end_lsn_bytes` | `pg_stat_wal_receiver_latest_end_lsn` |
| `pg_custom_stat_wal_receiver_latest_end_time_seconds` | `pg_stat_wal_receiver_latest_end_time` |
| `pg_custom_stat_wal_receiver_last_msg_send_time_seconds` | `pg_stat_wal_receiver_last_msg_send_time` |
| `pg_custom_stat_wal_receiver_last_msg_receipt_time_seconds` | `pg_stat_wal_receiver_last_msg_receipt_time` |
| `pg_custom_stat_wal_receiver_written_lsn_bytes` | not written yet |
| `pg_custom_stat_wal_receiver_lag_bytes` | not written yet |
| `pg_custom_stat_wal_receiver_lag_time_seconds` | not written yet |

The three at the end are the reason `queries-hr.yml` is still shipped. The wal
receiver collector publishes positions without differencing them, so the two
lag figures an operator actually reads have no replacement, and neither does
`written_lsn`. The file goes when they exist.

One of the removed metrics published nothing a reader could use.
`pg_custom_stat_subscription_received_lsn` selected a `pg_lsn` without
converting it, and a `pg_lsn` reaches the query engine as the string
`0/1FDF5FA0`, which it cannot parse into a number. Every real position was
dropped with one line at info level.

It was not silent in every case. A NULL in a column the file maps as a gauge
reaches `dbToFloat64(nil)`, which returns NaN rather than failing, so a
subscription with no received position published a NaN sample. A NaN is not a
number a dashboard can read either, and it is worse than a missing series,
because it looks like a measurement. Its replacement omits the series when
there is no position.

## Metrics lose the server label

The query engine stamps `server="host:port"` on everything it publishes. The
collectors do not, because the scraper already records which target a sample
came from.

So a query written as

```
pg_custom_publication_puballtables{server="db1:5432"}
```

matches nothing after the move, and one written as

```
sum by (server) (pg_stat_user_tables_n_live_tup)
```

groups by a label that is no longer there. Group by the scraper's own target
label instead, which is `instance` in a stock Prometheus.

## Two families are renamed on retirement

These are published by `queries-lr.yaml` and `queries-mr.yaml` today, under
names that differ from the collectors publishing the same measurement. When
those files are retired the old names go.

```
pg_statio_user_tables_heap_blks_hit    ->  pg_statio_user_tables_heap_blocks_hit
pg_statio_user_tables_heap_blks_read   ->  pg_statio_user_tables_heap_blocks_read
pg_statio_user_tables_idx_blks_hit     ->  pg_statio_user_tables_idx_blocks_hit
pg_statio_user_tables_idx_blks_read    ->  pg_statio_user_tables_idx_blocks_read
pg_statio_user_tables_toast_blks_hit   ->  pg_statio_user_tables_toast_blocks_hit
pg_statio_user_tables_toast_blks_read  ->  pg_statio_user_tables_toast_blocks_read
pg_statio_user_tables_tidx_blks_hit    ->  pg_statio_user_tables_tidx_blocks_hit
pg_statio_user_tables_tidx_blks_read   ->  pg_statio_user_tables_tidx_blocks_read
pg_replication_lag                     ->  pg_replication_lag_seconds
```

## Twenty families stop being published twice

Twenty metric families are published by both engines today, under one name,
separated only by the `server` label. The measured detail is in
[metric-migration.md](metric-migration.md).

A query that pins `server` to the query engine's value stops matching. A query
that does not pin it stops returning two series per thing and starts returning
one, so any `sum` over such a family halves. It was double before.

## Cached custom-query samples carry the time they were collected

A query file with `cache_seconds` set used to republish its last result on
every scrape with no timestamp, so Prometheus recorded each as measured at
that scrape. With `cache_seconds: 60` and a five-second scrape, one
measurement became twelve samples at twelve different moments.

A cached sample now carries the time it was collected. Prometheus stores one
sample per series per timestamp, so a cached series gets a point per
collection rather than a point per scrape. A graph of one will have gaps where
it previously had a flat line, and that line was not data.

## Configuration that used to be accepted now stops the exporter

A configuration file named with `--config.file` that cannot be read or parsed
is now fatal. It used to log a warning and start on defaults, which with
database selection or limits in the file means an operator gets the opposite
of what they configured and one warning line to say so.

A key written with nothing after it is refused rather than treated as absent.
`monitoring.databases.include:` and `monitoring.databases.include: []` mean
opposite things, and the first is now an error rather than a guess.

Setting the same thing in both the configuration file and a deprecated flag is
refused, with a message naming both. That covers
`monitoring.databases.exclude` against `--exclude-databases`,
`monitoring.databases.include` against `--include-databases`, and
`limits.connections` against `--max-connections`.

## Two collectors are now on by default

`postmaster` and `stat_wal_receiver` shipped disabled. They are the built-in
replacements for query file namespaces, and a replacement that is off is not a
replacement, so both are enabled.

New series appear: one for `pg_postmaster_start_time_seconds`, and the wal
receiver family on a standby. On a primary the wal receiver view is empty and
the collector publishes nothing.

## The confirmed flush position is published for inactive slots

`pg_replication_slot_slot_confirmed_flush_lsn` was published only for an
active slot. An inactive slot is the case an operator cares about, because it
is the one holding write-ahead log that nothing will confirm, so it is
published for every slot now.

More series appear, one per inactive slot. A query that assumed the presence
of this metric meant the slot was active has to read
`pg_replication_slot_slot_is_active` instead.

## collect[] selects collectors at a different level

`collect[]=postgres.<name>` for a single collector becomes
`collect[]=postgres.collector.<name>`. The second level is now two group
names, `postgres.cluster` and `postgres.database`, and at the old level
`postgres.database` was both a group and the collector that reads
`pg_database`.

A `collect[]` value in the `postgres` namespace that selects nothing now
returns 400 with the reason. It used to return 200 with no metrics, which
Prometheus records as a healthy target that has nothing to report.

## Where each of these lands

The metric removals and renames are in the branches that write the collectors.
The configuration and `collect[]` changes are in separate branches under
review. Nothing in this file has shipped, because nothing has been released.
