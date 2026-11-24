# Migration mapping

Which built-in collector replaces which shipped query file, and what is left to
write before each file can go.

The target is that the query engine collects no built-in metric at all. Built-in
monitoring is the Go collectors catalogued in
[metric-catalog.md](metric-catalog.md), and the query engine exists for
application queries an operator writes. A shipped query file that produces
monitoring is built-in monitoring depending on configuration that can be edited
or deleted, which is what the contract's §4.0 settled against.

So nothing below is a verdict that a file should stay. Where a file cannot be
retired, this says what has to be written first.

`collector/metric_migration_test.go` checks this file against those query
files, so a namespace added to one of them cannot stay undocumented here.

## What the overlap costs, measured

One exporter, both engines, the shipped query files, one scrape of a server
with five relations. The rig is `rig/producer-overlap.sh` in the workspace and
the output is kept beside it.

Twenty families are published **twice under one name**. The two series differ
only by the `server` label the query engine adds, so they are two series rather
than a collision the registry would have refused. Every pair carried the same
value.

```
pg_database_size_bytes                      3 series, matching
pg_stat_user_tables_*  (19 families)        5 series each, matching
```

That is per relation per database. On a database with a thousand relations the
nineteen table families are nineteen thousand duplicated series, and a query
written as `sum by (datname) (pg_stat_user_tables_n_live_tup)` returns twice
the truth unless it pins `server`.

Nine more families are published **twice under two names**. A query cannot
match both at once, so a dashboard uses one and silently ignores the other.

The collectors written to replace the high-resolution file use canonical names
rather than the `pg_custom_` ones, because a built-in metric should not be
named custom. That is a rename for anything reading them, listed below with
the others. If dashboard continuity matters more than the names, this is the
decision to overturn, and it is cheaper to overturn now than later.

```
pg_custom_stat_replication_write_lsn_bytes      ->  pg_stat_replication_write_lsn_bytes
pg_custom_stat_replication_flush_lsn_bytes      ->  pg_stat_replication_flush_lsn_bytes
pg_custom_stat_replication_replay_lsn_bytes     ->  pg_stat_replication_replay_lsn_bytes
pg_custom_stat_replication_write_lag_seconds    ->  pg_stat_replication_write_lag_seconds
pg_custom_stat_replication_flush_lag_seconds    ->  pg_stat_replication_flush_lag_seconds
pg_custom_stat_replication_replay_lag_seconds   ->  pg_stat_replication_replay_lag_seconds
pg_custom_stat_subscription_last_msg_send_time  ->  pg_stat_subscription_last_msg_send_time_seconds
pg_custom_stat_subscription_last_msg_receipt_time -> pg_stat_subscription_last_msg_receipt_time_seconds
pg_custom_publication_puballtables              ->  pg_publication_puballtables
pg_custom_publication_pubinsert                 ->  pg_publication_pubinsert
pg_custom_publication_pubupdate                 ->  pg_publication_pubupdate
pg_custom_publication_pubdelete                 ->  pg_publication_pubdelete
pg_custom_stat_activity_walsender_backend_start_unix -> pg_stat_activity_walsender_backend_start_seconds
pg_custom_replication_slots_restart_lsn_bytes   ->  pg_replication_slot_slot_restart_lsn
pg_custom_replication_slots_active              ->  pg_replication_slot_slot_is_active
pg_custom_replication_slots_confirmed_flush_lsn_bytes -> pg_replication_slot_slot_confirmed_flush_lsn
```

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

## queries-lr.yaml

| namespace | replacement | status |
| --- | --- | --- |
| `pg_stat_user_tables` | the `stat_user_tables` collector | fully covered, and duplicated today |
| `pg_statio_user_tables` | the `statio_user_tables` collector | fully covered, under renamed metrics |
| `pg_custom_database_size_custom` | the `database` collector, as `pg_database_size_bytes` | covered under a different name |

Everything this file produces has a built-in replacement, and nineteen of its
metrics are published alongside identical built-in ones right now. It is the
first candidate for retirement, and retiring it is a rename for anything
reading `_blks_` or `pg_custom_database_size_custom_bytes`.

The built-in collectors also do something this file cannot. The query engine
runs a per-query `databases:` list; the Go collectors fan out across the
selected set and label every sample with `datname` from `current_database()`,
so adding a database needs no configuration change.

## queries-mr.yaml

| namespace | replacement | status |
| --- | --- | --- |
| `pg_database` | the `database` collector | fully covered, and duplicated today |
| `pg_postmaster` | the `postmaster` collector | covered, and the collector is enabled now |
| `pg_replication` | the `replication` collector, as `pg_replication_lag_seconds` | covered under a different name, and calculated differently |

The replication lag is not only a rename. The query file computes the lag from
the timestamp of the last replayed transaction whenever the server is in
recovery. The collector reports zero when the standby has caught up, which the
file does not: on a caught-up standby with received and replayed positions
equal, the file reported 0.95 seconds and the collector reported 0. Both
describe the same standby. The file is reporting the age of the last
transaction it replayed, which on an idle primary grows without anything being
behind, and the collector is reporting that there is nothing left to replay.

The collector's answer is the one an operator wants from a metric called lag,
so this is a correction rather than a regression, and it is a change in the
number rather than in the name. A threshold set against the file's value will
behave differently against the collector's, and an alert on "lag above a few
seconds" will stop firing on idle standbys that were never behind.

The file also carries an Aurora-specific query, selected by distribution. The
collector returns without metrics on Aurora. Aurora support is not settled in
the supported-distribution contract, and that has to be resolved before this
file can be called fully replaced: today, retiring it would take replication
lag away from Aurora entirely. No Aurora instance was tested here, and this
is read from the query file and the collector rather than measured.

Everything else this file produces has a replacement that is switched on.

## queries-hr.yml

| namespace | replacement | status |
| --- | --- | --- |
| `pg_custom_replication_slots` | the `replication_slot` collector | covered. `restart_lsn` is published, and the confirmed-flush position is published for inactive slots too |
| `pg_custom_stat_wal_receiver` | the `stat_wal_receiver` collector | partly covered. The query file also publishes `written_lsn`, the lag in bytes and the lag in seconds, which the built-in does not compute |
| `pg_custom_stat_replication` | the `stat_replication` collector | covered |
| `pg_custom_stat_activity_walsender` | the `stat_activity_walsender` collector | covered |
| `pg_custom_stat_subscription` | the `stat_subscription` collector | covered, and one of its metrics works for the first time |
| `pg_custom_publication` | the `publication` collector | covered, and across every selected database rather than the connected one alone |

Five of the six are covered by collectors written for this. One thing is left
before the file can go.

The `stat_wal_receiver` collector needs `written_lsn`, the lag in bytes and
the lag in seconds. The lag figures are the ones that matter, since they are
what an operator reads, and the collector publishes positions without
differencing them. That is an open coverage item with nothing in front of it:
the column-order correction its query also needed merged in PR #14 and is in
this base.

`stat_wal_receiver` and `postmaster` shipped disabled and are enabled now. A
replacement that is off is not a replacement.

`pg_custom_stat_subscription_received_lsn` has never published a usable
position. The file selects `received_lsn` without converting it, and a `pg_lsn`
reaches the query engine as the string `0/1FDF5FA0`, which it cannot parse into
a number, so every real position is dropped with one line at info level.
Measured against PostgreSQL 18 rather than read off the code.

The family does reach the wire, though, in one case: a NULL received position
reaches `dbToFloat64(nil)`, which returns NaN, so a subscription whose worker
has not received anything publishes a NaN sample. A NaN is not a position
either, and it is worse than an absent series because it looks like a
measurement. The collector converts the position in SQL and omits the sample
where there is none.

## The legacy engine's own built-in maps

The query files above are not everything the legacy engine publishes. It also
carries six fixed metric maps in `cmd/postgres_exporter/postgres_exporter.go`,
plus the settings metrics from `querySettings` and its own framework output.
The exporter ships all of it, so none of it is operator-defined and none of it
can be dropped by deleting a query file. The metric catalogue does not cover
them, because what matters about them is what replaces them.

| built-in map | what it publishes | replacement | status |
| --- | --- | --- | --- |
| `pg_stat_database_conflicts` | recovery conflicts per database, by cause: tablespace, lock, snapshot, bufferpin, deadlock | none | not covered. The `stat_database` collector reads `pg_stat_database`, which carries only the `conflicts` total, not the breakdown by cause |
| `pg_lock_conflicts` | count of locks per blocking backend | none | not covered. The `locks` collector counts locks by mode, not by who is blocking whom |
| `pg_stat_replication` | two numbers per standby: the server's current WAL position in bytes, and the byte distance from it to that standby's replay position. The positions, the three lag intervals, the sync state and the priority are all read and discarded by the mapping, and reach no metric. `application_name`, `client_addr`, `state` and `pid` are labels | the collector this change adds | partly covered. The collector publishes each standby's write, flush and replay positions, which is more than the single replay distance this map computes, and it does not publish the primary's current position, which the map does. A server with no replication slots has no other producer for that position, so the distance cannot be reconstructed there. That gap is open |
| `pg_replication_slots` | two numbers per slot: whether it is active, and the byte distance from the current position to `restart_lsn`. `slot_name` and `database` are labels | the `replication_slot` collector | covered. That collector publishes whether a slot is active and the current and confirmed-flush positions, and this change adds `slot_restart_lsn` beside them, so the distance is a subtraction a query can do. The `database` label has no equivalent |
| `pg_stat_archiver` | three numbers: archived and failed counts, and the seconds since the last successful archive. The file names, the three timestamps and the reset time are read and discarded by the mapping, and reach no metric | none | not covered. No Go collector reads `pg_stat_archiver` |
| `pg_stat_activity` | connection counts and the longest transaction and state durations, by database, state, user and application | none | not covered. The `long_running_transactions` and `process_idle` collectors read the same view for different things, and neither reports connection counts by state |
| `querySettings` | every `pg_settings` row of type bool, integer or real, as `pg_settings_<name>` | a collector, to be written | not covered yet. The direction is settled: this output and its enable and disable behaviour are preserved and moved to a collector, with no period where both publish it. It is one series per setting per server, a few hundred of them, and what it reports is the monitored connection's effective settings, including anything a role or session override changes, rather than the server's configured defaults |

A column the mapping does not mention at all is not discarded: the engine
publishes it as an untyped metric named after the namespace and the column,
whenever its value converts to a float, and a timestamp converts to its unix
seconds. Only two of these namespaces query with `SELECT *`, so only two can
produce one: `pg_stat_database_conflicts`, which has no query override, and
`pg_stat_replication`. The rest name their columns.

These are published today, on supported versions, and were measured rather
than predicted:

| family | versions | what it is | replacement |
| --- | --- | --- | --- |
| `pg_stat_database_conflicts_confl_active_logicalslot` | 16-19 | Recovery conflicts with active logical slots, per database, labelled `datid` and `datname` | none. No Go collector reads `pg_stat_database_conflicts` at all |
| `pg_stat_database_conflicts_stats_reset` | 19 | When those conflict counters were last reset, as unix seconds | none |
| `pg_stat_replication_reply_time` | 15-19 | When the standby last replied, as unix seconds | none. The `stat_replication` collector publishes positions and lags and not this |

They arrived by accident, because nobody mapped the column, and they are
untyped and named after a view's column rather than after a measurement. None
of that makes them unpublished. Retiring the fixed maps removes them, so each
one is either replaced first or removed as a documented break with a reason,
and neither has been decided.

Four of the seven have no replacement written. Retiring the built-in path
means writing collectors for the recovery-conflict breakdown, lock blocking,
the archiver and connection counts by state, and deciding what happens to the
settings metrics, which are a different kind of thing from the rest.

The settings metrics are preserved, which is settled: existing monitoring is
kept until its removal is agreed, and a collector can carry this output and its
enable and disable behaviour unchanged. What they report is worth stating
precisely, because the name invites the wrong reading: `pg_settings` is the
effective configuration of the connection the exporter is using, so a role or
session override is what gets published, not the server's configured default.

Whether hundreds of series per server for values that do not change between
scrapes is worth its cost is a separate question, and a proposal to reduce it
can be considered on its own.

## What retirement requires

Stage 4 of the project plan says not to retire a query file until its intended
coverage has a tested replacement. For each namespace above that means the
replacement publishes the same measurement, on the same versions, with labels
that identify the same thing, and has a test that fails when it does not.

The renames are the part that cannot be done silently. `_blks_` to `_blocks_`,
`pg_replication_lag` to `pg_replication_lag_seconds` and
`pg_custom_database_size_custom_bytes` to `pg_database_size_bytes` each break
every dashboard and alert that uses the old name, and nothing in the exposition
tells a reader the new one exists.

The duplication is the part that can be ended first, because both names are
already published with the same values. Turning off `pg_stat_user_tables` and
`pg_database` in the query file removes twenty duplicated families, and that
is a visible change rather than a silent one.

An earlier version of this paragraph said it changes nothing for a query that
does not pin `server`. That is wrong, and this document's own measurements
show it: the two producers carry different `server` label values, so a query
that does not pin `server` sums both and reads double. Removing one producer
halves that sum, halves the series count for those families, and leaves the
remaining series under one label set instead of two. A dashboard reading the
doubled value sees its numbers halve on the day of the cutover.

So the cutover has to be announced rather than slipped in: say which families
lose a producer, say that any query summing across `server` will fall by the
duplicated share, and give the date. The measurement behind it is in
[what the overlap costs](#what-the-overlap-costs-measured), which lists the twenty families
and the label sets each producer uses.
