# Migration mapping

Which built-in collector replaces which shipped query file, and what is lost or
doubled on the way.

The exporter has two metric engines behind one `/metrics`. Built-in monitoring
is the Go collectors catalogued in [metric-catalog.md](metric-catalog.md). The
other engine runs query files, and the files PMM ships cover ground the Go
collectors also cover.

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
| `pg_postmaster` | the `postmaster` collector | covered, but the built-in collector ships disabled, so today only the query file publishes it |
| `pg_replication` | the `replication` collector, as `pg_replication_lag_seconds` | covered under a different name |

Retiring this file means enabling the `postmaster` collector in the same
change. Nothing else publishes the server start time, and losing it silently
would be the retirement doing exactly what stage 4 says not to do.

## queries-hr.yml

| namespace | replacement | status |
| --- | --- | --- |
| `pg_custom_replication_slots` | the `replication_slot` collector | partly covered. The built-in publishes the current and confirmed-flush positions and whether the slot is active; the query file also publishes `restart_lsn` |
| `pg_custom_stat_wal_receiver` | the `stat_wal_receiver` collector | partly covered, and the built-in ships disabled. The query file also publishes the receiver's lag in bytes and in seconds, which the built-in does not compute |
| `pg_custom_stat_replication` | none | not covered. No Go collector reads `pg_stat_replication`, so a primary's view of its standbys exists only here |
| `pg_custom_stat_activity_walsender` | none | not covered |
| `pg_custom_stat_subscription` | none | not covered. Logical replication subscriptions exist only here |
| `pg_custom_publication` | none | not covered |

This file must not be retired. Four of its six namespaces have no built-in
equivalent at all, and the two that do are covered only in part. Replacing it
means writing collectors for `pg_stat_replication`, `pg_stat_subscription` and
`pg_publication` first, and adding `restart_lsn` and the receiver lag
calculations to the collectors that exist.

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
| `pg_stat_replication` | a primary's view of each standby: positions sent, written, flushed and replayed, the three lag intervals, sync state and priority | the collector written for PR #20 | implemented, not merged. Until it is, this is the only producer |
| `pg_replication_slots` | per slot: active, and the distance from the current position | the `replication_slot` collector | covered for those columns, and that collector also publishes the confirmed flush position. Neither publishes `restart_lsn` |
| `pg_stat_archiver` | archived and failed WAL counts, the last archived and failed files and times, and the age of the last archive | none | not covered. No Go collector reads `pg_stat_archiver` |
| `pg_stat_activity` | connection counts and the longest transaction and state durations, by database, state, user and application | none | not covered. The `long_running_transactions` and `process_idle` collectors read the same view for different things, and neither reports connection counts by state |
| `querySettings` | every `pg_settings` row of type bool, integer or real, as `pg_settings_<name>` | none | not covered, and nothing proposes to cover it. It is one series per setting per server, a few hundred of them |

Four of the seven have no replacement written. Retiring the built-in path
means writing collectors for the recovery-conflict breakdown, lock blocking,
the archiver and connection counts by state, and deciding what happens to the
settings metrics, which are a different kind of thing from the rest.

The settings metrics deserve their own decision rather than a collector by
default. They describe configuration, not activity, they do not change between
scrapes, and there are hundreds of them per server. Publishing the whole of
`pg_settings` every scrape is the kind of cost the operational budget work
exists to bound.

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
