# Built-in metric catalogue

What every metric the built-in collectors publish measures, on which versions,
with what labels, and what you see when collection fails.

`collector/metric_catalog_test.go` checks this file against the code. A metric
without an entry fails the build, and so does an entry for a metric that is no
longer published. The labels listed in each section are checked too.

## What this covers, and what it does not

The namespaced collectors in `collector/`, which is what
`collector/metric_catalog_test.go` extracts and checks.

Not the custom-query engine's operator-written query files, which have nothing
fixed to catalogue.

Not the built-in query maps the legacy engine still carries in
`cmd/postgres_exporter/postgres_exporter.go`. Those are not operator-defined:
the exporter ships them, and they publish `pg_stat_database_conflicts`,
`pg_lock_conflicts`, `pg_stat_replication`, `pg_replication_slots`,
`pg_stat_archiver` and `pg_stat_activity`, plus the settings metrics from
`querySettings` and the engine's own framework output. They are inventoried in
[the migration mapping](metric-migration.md) rather than here, because what
matters about them is what replaces them, and none of that built-in path is
retired until each of its measurements is accounted for.

## How to read an entry

Scope says where a collector's queries run. A cluster collector runs once per
server on the configured connection and its answer describes the whole server.
A database collector runs once per selected database, on a connection to that
database, and everything it publishes carries `datname`.

Units are stated even when the name carries them, because several names do not
and two carry the wrong one. Where PostgreSQL reports milliseconds and the
metric publishes milliseconds, the entry says milliseconds. Prometheus
convention is seconds, and the gap between the two is recorded in [what is
wrong with this catalogue](#what-is-wrong-with-this-catalogue) rather than
quietly corrected here.

Versions are the range this exporter supports, 15 to 19. A narrower range means
the metric is omitted outside it rather than published as zero, which is the
distinction that lets a missing series mean "not available" instead of "none".

Four bgwriter metrics break that rule and say so in their rows: they are
published as zero on the versions that no longer have the column. That is a
defect, not a convention, and it is in the correction queue.

Reset says what makes a counter go backwards, and it is not one function for
everything. PostgreSQL keeps these counters in several places and resets them
separately:

- `pg_stat_reset()` resets the statistics of the database it is run in. That
  covers the table, index and function counters and that database's row in
  `pg_stat_database`. Run in one database it leaves every other database's
  counters alone.
- `pg_stat_reset_shared(target)` resets one set of server-wide statistics.
  Which targets a version accepts was measured on each supported version:
  `bgwriter`, `archiver` and `wal` on all of 15 to 19, `io` from 16,
  `checkpointer` and `slru` from 17.
- `pg_stat_reset_single_table_counters(oid)` and
  `pg_stat_reset_single_function_counters(oid)` reset one relation or one
  function, so a single series can go backwards while its neighbours do not.
- `pg_stat_reset_replication_slot(name)` and
  `pg_stat_reset_subscription_stats(oid)` reset one slot's or one
  subscription's counters.
- `pg_stat_statements_reset()` resets the extension's own statistics, which no
  core reset function touches.
- A crash that loses the statistics file resets everything, and so does a
  restore from a base backup.

Each section below names the reset that applies to it. A dashboard that does
not know which one is wrong at the moment it matters most: a reset of one
database's tables and a reset of the server's checkpointer counters look the
same in a graph and come from different commands.

Partial failure says what an operator sees when collection does not finish.
The unit of failure is the collector on one database, and
`pg_scrape_collector_success` says which collectors finished.

A collector that failed partway keeps what it already published. Collectors
write to a channel as they read rows, and the framework does not hold a
collector's output until it finishes, so a query that returns some rows and
then fails leaves those rows in the response alongside
`pg_scrape_collector_success 0`. Measured on the extensions collector with a
stub that returns one row and then a row error: one sample published, the
error returned, the sample kept.

So a success of 0 means some or all of that collector's rows are missing, and
nothing in the output says which. Buffering each collector's rows to make the
unit atomic was declined in the monitoring contract, along with what it would
cost, and this document does not promise it.

A relation that was never collected emits no series at all, which is not the
same as a relation with a zero.

## exporter framework

The collectors' own account of what they did. These come from the namespaced
collector framework rather than from PostgreSQL, so they are present whether or
not any collector succeeded.

| | |
| --- | --- |
| Scope | cluster, with per-database rows where named |
| Labels | `server`, with `collector`, `datname`, `usename`, `target` and `reason` where listed |
| Cardinality | a handful per server, plus one per selected database for the per-database families, and one per cacheable namespace per collection context for the cache age |
| Reset | none, these describe the last scrape |
| Availability | always |
| Partial failure | this is the report of partial failure |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_collector_databases_discovered` | gauge | databases | 15-19 | Connectable databases the server reported, including the connected one. Absent when the catalogue could not be read, because the only number it could invent is 1, which is what a one-database server looks like. |
| `pg_collector_databases_selected` | gauge | databases | 15-19 | Databases the scrape intended to run database-local collectors against, after exclusion and inclusion. |
| `pg_collector_databases_excluded` | gauge | databases | 15-19 | Databases configuration left out. A decision, never a failure. |
| `pg_collector_database_discovery_success` | gauge | boolean | 15-19 | Whether the set of databases could be determined. Zero means the set has collapsed to the connected database. |
| `pg_collector_database_up` | gauge | boolean | 15-19 | Whether this database could be connected to. Present for every selected database and for the connected one even when selection left it out, so it can carry one more database than `databases_selected` counts. |
| `pg_collector_database_error` | gauge | boolean | 15-19 | One per database this scrape did not cover, labelled `reason` with `connect` for a connection that failed, `scrape` for a database-local collector that failed on a connection that worked, or `scrape_deadline` and `scrape_cancelled` for a collection that started and then had collectors it never got to. The last three keep `database_up` 1, because the connection is not what went wrong, and the two budget reasons assert nothing about the database: they say the exporter ran out of room part way through. A database where both a collector failed and another never started carries one series for each, and is still one database in `databases_failed`. A database where nothing at all started is in `database_deferred` instead. A closed set, never an error message. |
| `pg_collector_database_excluded` | gauge | boolean | 15-19 | One per database an exclude list named. Databases left out by an include list are counted, not named, because that set can be the whole server. |
| `pg_collector_databases_attempted` | gauge | databases | 15-19 | Selected databases this scrape started database-local work on. Selected minus deferred. |
| `pg_collector_databases_scraped` | gauge | databases | 15-19 | Databases where every selected database-local collector ran without error. All of them, not the ones a scheduling decision happened to start. Stricter than the query engine's `pg_exporter_databases_scraped`, which counts one scrape that returned no error and says nothing about the collectors inside it. |
| `pg_collector_databases_failed` | gauge | databases | 15-19 | Databases this scrape started database-local work on and did not finish, because the connection failed, a collector failed, or the scrape ran out of room before every selected collector had its turn. A database whose remaining collectors were deferred is an attempt that did not finish, counted here once and not in `databases_deferred`. A database where nothing at all started is counted in `databases_deferred` and not here. Why it did not finish is in `pg_collector_database_error`. |
| `pg_collector_databases_deferred` | gauge | databases | 15-19 | Selected databases this scrape did not start work on, for want of room within its budget. Not covered and not failed: nothing was tried, so nothing is known to be wrong with them. |
| `pg_collector_database_deferred` | gauge | boolean | 15-19 | One per database this scrape did not start work on, labelled `reason` with `no_connection_permit` for work that waited for a connection and did not get one, `scrape_deadline` for work the scrape ran out of time to start, or `scrape_cancelled` for work the scrape was cancelled before starting. Also published for the configured connection database when that connection itself was deferred, whether or not selection kept it, so it can name one more database than `databases_deferred` counts, in the same way `database_up` can. A deferral is not an error and is never counted in `database_error`. |
| `pg_scrape_collector_success` | gauge | boolean | 15-19 | Whether one collector finished on one database. `datname` is empty for a cluster collector, which Prometheus treats as absent. Absent for a collector this scrape never started, which is in `pg_scrape_collector_deferred` instead. |
| `pg_scrape_collector_deferred` | gauge | boolean | 15-19 | One per collector this scrape did not start on this database, labelled `reason` with `scrape_deadline` for a turn that came after the scrape ran out of time, or `scrape_cancelled` for a scrape that was cancelled. No SQL ran, so this is not a collector that failed and there is no `pg_scrape_collector_success` beside it. The connection was already open and already held its permit, so `no_connection_permit` cannot appear here. `datname` is empty for a cluster collector. |
| `pg_scrape_collector_duration_seconds` | gauge | seconds | 15-19 | How long one collector took on one database. |
| `pg_collector_database_last_success_timestamp_seconds` | gauge | seconds since the epoch | 15-19 | When the database-local collectors this request selected last all ran on this database without error in one scrape. The selection is remembered rather than labelled: a request for a subset gets the time that subset last completed and cannot read or move another selection's time, and two scrape jobs with different `collect[]` sets are separated by their own target labels. Published on the paths that collected nothing too, so a database that is failing keeps the time it was last covered. One record per selection per database is held in the process, which is bounded by how many `collect[]` profiles are scraped. Nothing removes a record while the process runs: the store records and reads, and bounded cleanup is not implemented yet. The `target` label is this process's identity for the connection; it is not stable across restarts. |
| `pg_collector_last_success_timestamp_seconds` | gauge | seconds since the epoch | 15-19 | When this collector last ran on this database without error. Absent for a collector this process has never run successfully there, which is not the same as one that last succeeded at the epoch. Published on the paths that run no collector too, so a database that cannot be connected to keeps the time it last worked, and omitted for a database the configuration no longer selects while that state is kept for a request that selects it again. The `target` label is this process's identity for the connection; it is not stable across restarts. |

### When the response is missing something the collection produced

`promhttp_metric_handler_errors_total` with `cause="gathering"` counts the
scrapes of this endpoint whose metrics could not all be turned into a response.
A collector that returns an invalid metric, a duplicate sample or an
inconsistent label set has the affected metrics or samples rejected, and the
request still answers 200.

It is separate from everything else on this page. `pg_up` and
`pg_exporter_last_scrape_error` describe reaching the databases and running the
SQL, and both are right to report success here: the collection worked and the
exposition did not. Without this counter a sample asked for is simply absent,
and nothing in the response says why.

Two things to know before alerting on it. The count rises on the response after
the failure, because the handler increments once gathering has finished, so a
failure is seen on the next scrape rather than in the body it spoiled. And one
failed gather is one increment whatever it rejected: duplicate samples across
three databases advance it once. How much goes missing depends on what was
wrong. Duplicates were observed leaving the family in place with the samples
that survived, while an invalid metric takes its family with it. Which of two
conflicting samples survives is not something to depend on.

## database

Database sizes, from `pg_database`.

| | |
| --- | --- |
| Scope | cluster, `pg_database` is shared |
| Labels | `datname` |
| Cardinality | one series per database on the server |
| Reset | none, this is a current size |
| Availability | all versions; needs `CONNECT` on the database or the size is skipped |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_database_size_bytes` | gauge | bytes | 15-19 | Disk space the database occupies, from `pg_database_size()`. Includes every fork and the TOAST relations. |

## database_wraparound

How close each database is to transaction ID wraparound.

| | |
| --- | --- |
| Scope | cluster, `pg_database` is shared |
| Labels | `datname` |
| Cardinality | one series per database on the server |
| Reset | none, these fall when a freeze advances the oldest unfrozen ID |
| Availability | all versions |
| Partial failure | the collector fails as a unit; a row whose age is NULL is skipped and emits nothing |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_database_wraparound_age_datfrozenxid_seconds` | gauge | transactions | 15-19 | Transactions since the database's oldest unfrozen transaction ID, from `age(datfrozenxid)`. The name says seconds and the value is a count of transactions. Compare it with `autovacuum_freeze_max_age`, which is 200 million by default. |
| `pg_database_wraparound_age_datminmxid_seconds` | gauge | multixacts | 15-19 | Multixacts since the database's oldest unreplaced multixact ID, from `mxid_age(datminmxid)`. The name says seconds and the value is a count. Compare it with `autovacuum_multixact_freeze_max_age`. |

## extensions

Which extensions a database has, and which it could have.

| | |
| --- | --- |
| Scope | database, `pg_extension` is local to one database |
| Labels | `datname`, `name`, and `default_version` and `installed_version` or `relocatable` and `version` |
| Cardinality | one series per extension per selected database, bounded by what is installed |
| Reset | none |
| Availability | all versions; reads catalogues only, so no extension is needed to collect it |
| Partial failure | the collector fails as a unit, per database |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_available_extensions` | gauge | boolean | 15-19 | Always 1. The information is in the labels: an extension available for installation, with the version installed in this database if any. |
| `pg_extensions` | gauge | boolean | 15-19 | Always 1. An extension installed in this database, with its version as a label. |

## locks

Lock counts by mode.

| | |
| --- | --- |
| Scope | cluster, `pg_locks` reports every database from one connection |
| Labels | `datname`, `mode` |
| Cardinality | one series per database per lock mode, so up to eight per database |
| Reset | none, this is a current count |
| Availability | all versions; `pg_monitor` or superuser to see other users' locks |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_locks_count` | gauge | locks | 15-19 | Locks currently held or awaited, counted per database and lock mode. |

## long_running_transactions

Named for transactions that have been open a long time. Neither metric
measures that, and both are in the correction queue in [what is wrong with
this catalogue](#what-is-wrong-with-this-catalogue). What they publish today
is described in the rows below and nowhere else, so that a reader is not
misled by the names.

| | |
| --- | --- |
| Scope | cluster, `pg_stat_activity` reports every database |
| Labels | none |
| Cardinality | two series per server |
| Reset | none, these describe the present |
| Availability | all versions; `pg_monitor` to see other users' sessions, otherwise the counts are of your own |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_long_running_transactions` | gauge | backends | 15-19 | Rows of `pg_stat_activity` whose state is not `idle` and whose query does not start with `autovacuum:`. There is no age threshold in the query and no requirement that the backend is in a transaction at all, so this counts active backends rather than long-running transactions. |
| `pg_long_running_transactions_oldest_timestamp_seconds` | gauge | seconds since the epoch | 15-19 | `MAX(EXTRACT(EPOCH FROM clock_timestamp()))` over those rows, which is the time the query ran and not an age or a start time. It is the same number for every server and every transaction, and it advances with the clock whether or not anything is open. |

## postmaster

When the server started.

| | |
| --- | --- |
| Scope | cluster |
| Labels | none |
| Cardinality | one series per server |
| Reset | the value changes when the server restarts, which is the point of it. Normally it advances, because the new start is later than the old one. |
| Availability | all versions |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_postmaster_start_time_seconds` | gauge | seconds since the epoch | 15-19 | When the postmaster started, from `pg_postmaster_start_time()`. Any change identifies a restart, and the usual direction is upwards: a server that restarts now starts later than it did before. A fall happens too, when a scrape reaches a different server behind one address or the clock moves, so an alert written for a fall alone misses most restarts. |

## process_idle

How long sessions have been idle.

| | |
| --- | --- |
| Scope | cluster, `pg_stat_activity` reports every database |
| Labels | `state`, `application_name` |
| Cardinality | unbounded. `application_name` is whatever clients set, so a client that puts a request id in it mints a series per request |
| Reset | none, this describes the present |
| Availability | all versions; `pg_monitor` to see other users' sessions |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_process_idle_seconds` | histogram | seconds | 15-19 | Idle time of server processes, bucketed. The only histogram the built-in collectors publish. |

## replication

Whether this server is a standby, and how far behind it is.

| | |
| --- | --- |
| Scope | cluster |
| Labels | none |
| Cardinality | two series per server |
| Reset | none |
| Availability | all versions; the lag is meaningful only on a standby and reads zero on a primary |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_replication_is_replica` | gauge | boolean | 15-19 | 1 on a standby, 0 on a primary, from `pg_is_in_recovery()`. |
| `pg_replication_lag_seconds` | gauge | seconds | 15-19 | How far behind the primary this standby is, by replay timestamp. Zero on a primary and on an idle standby, which are different situations the value does not distinguish. |

## replication_slot

Replication slot positions.

| | |
| --- | --- |
| Scope | cluster, `pg_replication_slots` is shared |
| Labels | `slot_name`, `plugin`, `slot_type` |
| Cardinality | one series per slot, bounded by `max_replication_slots` |
| Reset | none, LSNs advance and are reset only by recreating a slot |
| Availability | all versions; `pg_monitor` or superuser |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_replication_slot_slot_current_wal_lsn` | gauge | bytes of WAL | 15-19 | The server's current write-ahead log position, as a number. |
| `pg_replication_slot_slot_confirmed_flush_lsn` | gauge | bytes of WAL | 15-19 | The last position the consumer confirmed. The difference from the current position is how far behind the consumer is, which is not the same as how much WAL the slot holds. Omitted for a slot that has no confirmed position, which is every physical slot and any logical slot nothing has consumed from yet. |
| `pg_replication_slot_slot_restart_lsn` | gauge | bytes of WAL | 15-19 | The oldest position the slot still needs, which is what retention is measured from. The distance from the current position is how much log the slot is holding on disk, and it is published whether or not the slot is active, because an inactive slot is where it matters. Slots that overlap hold the same segments once, so summing this across slots is not a disk figure. |
| `pg_replication_slot_slot_is_active` | gauge | boolean | 15-19 | Whether anything is connected to the slot. An inactive slot still holds WAL. |

## stat_activity_autovacuum

When each running autovacuum started.

| | |
| --- | --- |
| Scope | cluster, `pg_stat_activity` reports every database |
| Labels | `relname` |
| Cardinality | one series per relation currently being autovacuumed, so bounded by `autovacuum_max_workers` at any moment, but the set of relation names over time is not bounded |
| Reset | none, a series disappears when the vacuum finishes |
| Availability | all versions; `pg_monitor` to see the autovacuum workers' queries |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_activity_autovacuum_timestamp_seconds` | gauge | seconds since the epoch | 15-19 | When the autovacuum running on this relation started. Absent when nothing is being vacuumed, which is not zero. |

## stat_bgwriter

Background writer and checkpointer activity.

Thirteen of these metrics exist twice, once with the Prometheus `_total`
suffix and once without. The pair carries the same number with the same
labels, verified by `TestBgwriterTotalPairsAreDuplicates`. It is duplicated
data kept for compatibility, and consolidating it is a decision for the
migration mapping rather than something to do quietly.

| | |
| --- | --- |
| Scope | cluster |
| Labels | `collector`, `server` |
| Cardinality | 26 series per server, of which 13 duplicate the other 13 |
| Reset | `pg_stat_reset_shared('bgwriter')` for the writer counters. From 17 the checkpoint counters come from `pg_stat_checkpointer` and are reset by `pg_stat_reset_shared('checkpointer')`, so this one section has two reset sources there and one below it. A crash that loses the statistics file resets both. |
| Availability | PostgreSQL 17 moved checkpoint counters from `pg_stat_bgwriter` to `pg_stat_checkpointer`; the collector reads whichever exists. `checkpoints_done` and `slru_written` need 18 |
| Partial failure | the collector fails as a unit |

Times here are milliseconds, which the names do not say.

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_bgwriter_checkpoints_timed_total` | counter | checkpoints | 15-19 | Scheduled checkpoints performed. |
| `pg_stat_bgwriter_checkpoints_timed` | counter | checkpoints | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_checkpoints_req_total` | counter | checkpoints | 15-19 | Requested checkpoints performed. A high ratio against timed ones means `max_wal_size` is too small. |
| `pg_stat_bgwriter_checkpoints_req` | counter | checkpoints | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_checkpoints_done_total` | counter | checkpoints | 18-19 | Checkpoints completed. |
| `pg_stat_bgwriter_checkpoints_done` | counter | checkpoints | 18-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_checkpoint_write_time_total` | counter | milliseconds | 15-19 | Time spent writing files during checkpoints. |
| `pg_stat_bgwriter_checkpoint_write_time` | counter | milliseconds | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_checkpoint_sync_time_total` | counter | milliseconds | 15-19 | Time spent syncing files during checkpoints. |
| `pg_stat_bgwriter_checkpoint_sync_time` | counter | milliseconds | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_buffers_checkpoint_total` | counter | buffers | 15-19 | Buffers written during checkpoints. |
| `pg_stat_bgwriter_buffers_checkpoint` | counter | buffers | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_buffers_clean_total` | counter | buffers | 15-19 | Buffers written by the background writer. |
| `pg_stat_bgwriter_buffers_clean` | counter | buffers | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_maxwritten_clean_total` | counter | occurrences | 15-19 | Times the background writer stopped a scan for having written too many buffers. |
| `pg_stat_bgwriter_maxwritten_clean` | counter | occurrences | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_buffers_backend_total` | counter | buffers | measured 15-16, published as zero 17-19 | Buffers written by a backend rather than by the writer or a checkpoint. `pg_stat_bgwriter` lost the column in 17 and the series is published as zero there, which reads as no backend writes rather than as no measurement. `pg_stat_io` carries the accounting from 17. |
| `pg_stat_bgwriter_buffers_backend` | counter | buffers | measured 15-16, published as zero 17-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_buffers_backend_fsync_total` | counter | occurrences | measured 15-16, published as zero 17-19 | Times a backend had to run its own fsync. Zero from 17 for the same reason. |
| `pg_stat_bgwriter_buffers_backend_fsync` | counter | occurrences | measured 15-16, published as zero 17-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_buffers_alloc_total` | counter | buffers | 15-19 | Buffers allocated. |
| `pg_stat_bgwriter_buffers_alloc` | counter | buffers | 15-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_slru_written_total` | counter | buffers | 18-19 | SLRU buffers written during checkpoints and restartpoints. |
| `pg_stat_bgwriter_slru_written` | counter | buffers | 18-19 | The same measurement under the older name. |
| `pg_stat_bgwriter_stats_reset_total` | counter | seconds since the epoch | 15-19 | When these statistics were last reset. A timestamp published as a counter, so a reset reads as a counter going backwards and `rate()` over it means nothing. |
| `pg_stat_bgwriter_stats_reset` | counter | seconds since the epoch | 15-19 | The same measurement under the older name, with the same problem. |

## stat_database

Per-database activity, from `pg_stat_database`.

| | |
| --- | --- |
| Scope | cluster, the view reports every database from one connection |
| Labels | `datid`, `datname` |
| Cardinality | one series per database on the server, per metric |
| Reset | `pg_stat_reset()` run in that database, and a crash that loses the statistics file |
| Availability | all versions; the parallel worker counters need 18 |
| Partial failure | the collector fails as a unit |

Times here are milliseconds, which the names do not say.

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_database_numbackends` | gauge | connections | 15-19 | Backends connected to this database now. The only current-state column in the view. |
| `pg_stat_database_xact_commit` | counter | transactions | 15-19 | Transactions committed. |
| `pg_stat_database_xact_rollback` | counter | transactions | 15-19 | Transactions rolled back. |
| `pg_stat_database_blks_read` | counter | blocks | 15-19 | Disk blocks read. Counts reads from the operating system, which may themselves be cache hits. |
| `pg_stat_database_blks_hit` | counter | blocks | 15-19 | Blocks found in PostgreSQL's own buffer cache. |
| `pg_stat_database_tup_returned` | counter | rows | 15-19 | Rows returned by queries, which for a sequential scan is every row examined. |
| `pg_stat_database_tup_fetched` | counter | rows | 15-19 | Rows fetched by index lookups. |
| `pg_stat_database_tup_inserted` | counter | rows | 15-19 | Rows inserted. |
| `pg_stat_database_tup_updated` | counter | rows | 15-19 | Rows updated. |
| `pg_stat_database_tup_deleted` | counter | rows | 15-19 | Rows deleted. |
| `pg_stat_database_conflicts` | counter | queries | 15-19 | Queries cancelled by recovery conflicts. Non-zero only on a standby. |
| `pg_stat_database_temp_files` | counter | files | 15-19 | Temporary files created, whatever `log_temp_files` is set to. |
| `pg_stat_database_temp_bytes` | counter | bytes | 15-19 | Bytes written to temporary files. |
| `pg_stat_database_deadlocks` | counter | deadlocks | 15-19 | Deadlocks detected. |
| `pg_stat_database_blk_read_time` | counter | milliseconds | 15-19 | Time backends spent reading blocks. Zero unless `track_io_timing` is on, which is not the same as no time spent. |
| `pg_stat_database_blk_write_time` | counter | milliseconds | 15-19 | Time backends spent writing blocks. Zero unless `track_io_timing` is on. |
| `pg_stat_database_parallel_workers_to_launch` | counter | workers | 18-19 | Parallel workers planned. |
| `pg_stat_database_parallel_workers_launched` | counter | workers | 18-19 | Parallel workers actually started. The gap from the planned count is parallelism the server could not provide. |
| `pg_stat_database_stats_reset` | counter | seconds since the epoch | 15-19 | When these statistics were last reset. A timestamp published as a counter, so a reset reads as a counter going backwards. |

## stat_io

I/O by backend type, object and context, from `pg_stat_io`.

| | |
| --- | --- |
| Scope | cluster |
| Labels | `backend_type`, `io_object`, `io_context` |
| Cardinality | one series per combination the server reports, which is bounded by PostgreSQL's own enumerations and is around a hundred per metric |
| Reset | `pg_stat_reset_shared('io')`, and a crash that loses the statistics file |
| Availability | `pg_stat_io` was added in PostgreSQL 16, so nothing here is published on 15 |
| Partial failure | the collector fails as a unit |

Times here are milliseconds, which the names do not say, and they are zero
unless `track_io_timing` is on.

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_io_reads_total` | counter | operations | 16-19 | Read operations. |
| `pg_stat_io_read_bytes_total` | counter | bytes | 16-19 | Bytes read. |
| `pg_stat_io_read_time_total` | counter | milliseconds | 16-19 | Time spent reading. |
| `pg_stat_io_writes_total` | counter | operations | 16-19 | Write operations. |
| `pg_stat_io_write_bytes_total` | counter | bytes | 16-19 | Bytes written. |
| `pg_stat_io_write_time_total` | counter | milliseconds | 16-19 | Time spent writing. |
| `pg_stat_io_writebacks_total` | counter | blocks | 16-19 | Blocks the process asked the kernel to write out. |
| `pg_stat_io_writeback_time_total` | counter | milliseconds | 16-19 | Time spent asking for writeback. |
| `pg_stat_io_extends_total` | counter | operations | 16-19 | Relation extend operations. |
| `pg_stat_io_extend_bytes_total` | counter | bytes | 16-19 | Bytes added by extends. |
| `pg_stat_io_extend_time_total` | counter | milliseconds | 16-19 | Time spent extending. |
| `pg_stat_io_hits_total` | counter | operations | 16-19 | Times a wanted block was already in a shared buffer. |
| `pg_stat_io_evictions_total` | counter | operations | 16-19 | Times a block was written out of a buffer to free it. |
| `pg_stat_io_reueses_total` | counter | operations | 16-19 | Times a buffer in a size-limited ring was reused. The name is misspelled: it should be `reuses`. |
| `pg_stat_io_fsyncs_total` | counter | operations | 16-19 | fsync calls. |
| `pg_stat_io_fsync_time_total` | counter | milliseconds | 16-19 | Time spent in fsync. |

## stat_statements

Per-statement execution totals, from the `pg_stat_statements` extension.

| | |
| --- | --- |
| Scope | cluster, the view carries `dbid` and reports every database from one connection |
| Labels | `user`, `datname`, `queryid` |
| Cardinality | unbounded in practice. One series per distinct statement shape per user per database, up to `pg_stat_statements.max`, which defaults to 5000 |
| Reset | `pg_stat_statements_reset()`, and eviction when the tracked set is full, which drops a statement's history without resetting anything else |
| Availability | needs the `pg_stat_statements` extension created and in `shared_preload_libraries`. Disabled by default in this exporter |
| Partial failure | the collector fails as a unit |

This family converts PostgreSQL's milliseconds to seconds, which the rest of
the exporter does not.

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_statements_calls_total` | counter | calls | 15-19 | Times the statement was executed. |
| `pg_stat_statements_rows_total` | counter | rows | 15-19 | Rows the statement returned or affected. |
| `pg_stat_statements_seconds_total` | counter | seconds | 15-19 | Total execution time. |
| `pg_stat_statements_block_read_seconds_total` | counter | seconds | 15-19 | Time spent reading blocks. Zero unless `track_io_timing` is on. |
| `pg_stat_statements_block_write_seconds_total` | counter | seconds | 15-19 | Time spent writing blocks. Zero unless `track_io_timing` is on. |

## publication

Logical replication publications, from the publisher's side.

`pg_publication` is a per-database catalogue, so a publication created in one
database is invisible from another. The query file this replaces ran against
the connected database alone and so described only that one.

| | |
| --- | --- |
| Scope | database, `pg_publication` describes one database |
| Labels | `datname`, `pubname` |
| Cardinality | one series per publication per selected database, bounded by what is defined |
| Reset | none, these are definitions rather than counters |
| Availability | all versions |
| Partial failure | the collector fails as a unit, per database |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_publication_puballtables` | gauge | boolean | 15-19 | Whether the publication covers every table in the database. |
| `pg_publication_pubinsert` | gauge | boolean | 15-19 | Whether it replicates inserts. |
| `pg_publication_pubupdate` | gauge | boolean | 15-19 | Whether it replicates updates. |
| `pg_publication_pubdelete` | gauge | boolean | 15-19 | Whether it replicates deletes. |
| `pg_publication_pubtruncate` | gauge | boolean | 15-19 | Whether it replicates truncates. |

## stat_activity_walsender

The WAL sender processes on a primary, one per connected standby or logical
subscriber.

| | |
| --- | --- |
| Scope | cluster, `pg_stat_activity` reports every database |
| Labels | `pid`, `usename`, `application_name`, `client_addr`, `state` |
| Cardinality | one series per connected client, and `pid` changes on reconnection, so the set grows with reconnections rather than with clients |
| Reset | none, a series disappears when the client disconnects |
| Availability | all versions; `pg_monitor` to see other users' sessions. Empty on a server with no replication clients, which is no series rather than zero |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_activity_walsender_backend_start_seconds` | gauge | seconds since the epoch | 15-19 | When this sender process started. A value that keeps moving is a client that keeps reconnecting. |

## stat_replication

What a primary knows about the standbys connected to it.

| | |
| --- | --- |
| Scope | cluster |
| Labels | `pid`, `usename`, `application_name`, `client_addr`, `state`, `sync_state` |
| Cardinality | one series per connected standby per metric, and `pid` changes on reconnection |
| Reset | none, positions advance and lags are current measurements |
| Availability | all versions. Empty on a standby and on a primary with nothing connected, which is no series rather than zero. `client_addr` is empty for a standby connected over a Unix socket |
| Partial failure | the collector fails as a unit. A NULL position or lag is omitted rather than published as zero, because a standby that has not reported yet has no lag and zero would read as none |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_replication_write_lsn_bytes` | gauge | bytes of WAL | 15-19 | Position this standby has written. |
| `pg_stat_replication_flush_lsn_bytes` | gauge | bytes of WAL | 15-19 | Position this standby has flushed to disk. |
| `pg_stat_replication_replay_lsn_bytes` | gauge | bytes of WAL | 15-19 | Position this standby has replayed. The difference from the primary's current position is how far behind it is. |
| `pg_stat_replication_write_lag_seconds` | gauge | seconds | 15-19 | Time between a local commit and this standby reporting it written. |
| `pg_stat_replication_flush_lag_seconds` | gauge | seconds | 15-19 | Time between a local commit and this standby reporting it flushed. |
| `pg_stat_replication_replay_lag_seconds` | gauge | seconds | 15-19 | Time between a local commit and this standby reporting it replayed. |

## stat_subscription

Logical replication subscriptions, from the subscriber's side.

The query file this replaces selected `received_lsn` without converting it,
and a `pg_lsn` reaches the query engine as a string such as `0/1FDF5FA0`,
which the engine cannot parse into a number, so no real position was ever
published. A NULL position did reach the wire, as a NaN, because the engine
turns a NULL in a column mapped as a gauge into one. Measured against
PostgreSQL 18 rather than read off the code.

| | |
| --- | --- |
| Scope | cluster |
| Labels | `subid`, `subname`, `pid` |
| Cardinality | one series per subscription per metric, for the subscriptions that have something to report. `pid` is empty when the worker is not running, and a subscription with no worker has no positions or times either, so it has no series at all: there is no state indicator here saying that a subscription exists and is idle |
| Reset | none |
| Availability | all versions. Empty where nothing subscribes, which is no series rather than zero |
| Partial failure | the collector fails as a unit. A NULL position or time is omitted rather than published as zero |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_subscription_received_lsn_bytes` | gauge | bytes of WAL | 15-19 | Position last received from the publisher. |
| `pg_stat_subscription_latest_end_lsn_bytes` | gauge | bytes of WAL | 15-19 | Position last reported back to the publisher. |
| `pg_stat_subscription_last_msg_send_time_seconds` | gauge | seconds since the epoch | 15-19 | When the publisher sent the last message received. |
| `pg_stat_subscription_last_msg_receipt_time_seconds` | gauge | seconds since the epoch | 15-19 | When this subscriber received it. The difference from the send time is the one-way delay, if the clocks agree. |
| `pg_stat_subscription_latest_end_time_seconds` | gauge | seconds since the epoch | 15-19 | When the last position was reported back. |

## stat_user_indexes

Per-index usage and size estimates.

| | |
| --- | --- |
| Scope | database, these catalogues describe one database |
| Labels | `datname`, `schemaname`, `relname`, `indexrelname` |
| Cardinality | one series per index per selected database, per metric. The largest family here on a schema with many indexes |
| Reset | `pg_stat_reset()` in that database for the scan counters; the page and tuple estimates are maintained by the index build and by vacuum and analyze |
| Availability | all versions. Covers indexes of ordinary, TOAST and materialised-view relations, outside `pg_catalog`, `information_schema` and the TOAST schemas |
| Partial failure | the collector fails as a unit, per database. An index that was not collected emits no series |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_user_indexes_idx_scan` | counter | scans | 15-19 | Index scans started on this index. Zero over a long window is the usual sign of an unused index. |
| `pg_stat_user_indexes_idx_tup_read` | counter | entries | 15-19 | Index entries returned by scans. |
| `pg_stat_user_indexes_idx_tup_fetch` | counter | rows | 15-19 | Live table rows fetched by simple index scans on this index. |
| `pg_stat_user_indexes_relpages` | gauge | pages | 15-19 | The planner's estimate of the index's size in pages, from `pg_class`. An estimate, not a measurement. |
| `pg_stat_user_indexes_reltuples` | gauge | entries | 15-19 | The planner's estimate of the number of index entries. Unlike a table's, it is not expected to carry the -1 that means never analyzed. |

## stat_user_tables

Per-table activity, maintenance history and catalogue estimates.

Reads `pg_class` and the per-relation statistics functions directly rather
than the `pg_stat_user_tables` view, which is what lets the catalogue columns
below be published alongside the activity counters from one scan.

| | |
| --- | --- |
| Scope | database, these catalogues describe one database |
| Labels | `datname`, `schemaname`, `relname` |
| Cardinality | one series per relation per selected database, per metric |
| Reset | `pg_stat_reset()` in that database for the counters and the last-run timestamps; the `pg_class` estimates are updated by vacuum and analyze and do not reset |
| Availability | all versions. Covers ordinary, TOAST, materialised-view and partitioned relations outside the system schemas. `n_tup_newpage_upd` needs 16, `relallfrozen` and the four `total_*_time` metrics need 18 |
| Partial failure | the collector fails as a unit, per database. A relation that was not collected emits no series, which is not a zero |

Times here are milliseconds, which the names do not say.

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_user_tables_seq_scan` | counter | scans | 15-19 | Sequential scans started on this table. |
| `pg_stat_user_tables_seq_tup_read` | counter | rows | 15-19 | Live rows fetched by sequential scans. |
| `pg_stat_user_tables_idx_scan` | counter | scans | 15-19 | Index scans started on this table, summed over its indexes. Not interchangeable with the per-index family, which has a different scope and an extra label. |
| `pg_stat_user_tables_idx_tup_fetch` | counter | rows | 15-19 | Live rows fetched by index scans on this table. |
| `pg_stat_user_tables_n_tup_ins` | counter | rows | 15-19 | Rows inserted. |
| `pg_stat_user_tables_n_tup_upd` | counter | rows | 15-19 | Rows updated. |
| `pg_stat_user_tables_n_tup_del` | counter | rows | 15-19 | Rows deleted. |
| `pg_stat_user_tables_n_tup_hot_upd` | counter | rows | 15-19 | Rows updated without needing an index update. |
| `pg_stat_user_tables_n_tup_newpage_upd` | counter | rows | 16-19 | Rows whose update put the new version on a different heap page. |
| `pg_stat_user_tables_n_live_tup` | gauge | rows | 15-19 | Estimated live rows. An estimate from the statistics collector, not a count. |
| `pg_stat_user_tables_n_dead_tup` | gauge | rows | 15-19 | Estimated dead rows awaiting vacuum. |
| `pg_stat_user_tables_n_mod_since_analyze` | gauge | rows | 15-19 | Estimated rows changed since the last analyze. What autovacuum compares against its analyze threshold. |
| `pg_stat_user_tables_n_ins_since_vacuum` | gauge | rows | 15-19 | Estimated rows inserted since the last vacuum. A gauge because vacuum moves its reference point, not an insertion counter. |
| `pg_stat_user_tables_last_vacuum` | gauge | seconds since the epoch | 15-19 | When the table was last vacuumed by hand. Zero when it never has been, which is also a valid timestamp in 1970. |
| `pg_stat_user_tables_last_autovacuum` | gauge | seconds since the epoch | 15-19 | When autovacuum last vacuumed it. |
| `pg_stat_user_tables_last_analyze` | gauge | seconds since the epoch | 15-19 | When the table was last analyzed by hand. |
| `pg_stat_user_tables_last_autoanalyze` | gauge | seconds since the epoch | 15-19 | When autovacuum last analyzed it. |
| `pg_stat_user_tables_vacuum_count` | counter | runs | 15-19 | Manual vacuums. |
| `pg_stat_user_tables_autovacuum_count` | counter | runs | 15-19 | Autovacuum vacuums. |
| `pg_stat_user_tables_analyze_count` | counter | runs | 15-19 | Manual analyzes. |
| `pg_stat_user_tables_autoanalyze_count` | counter | runs | 15-19 | Autovacuum analyzes. |
| `pg_stat_user_tables_total_vacuum_time` | counter | milliseconds | 18-19 | Time spent vacuuming this table by hand. |
| `pg_stat_user_tables_total_autovacuum_time` | counter | milliseconds | 18-19 | Time autovacuum spent vacuuming it. |
| `pg_stat_user_tables_total_analyze_time` | counter | milliseconds | 18-19 | Time spent analyzing it by hand. |
| `pg_stat_user_tables_total_autoanalyze_time` | counter | milliseconds | 18-19 | Time autovacuum spent analyzing it. |
| `pg_stat_user_tables_size_bytes` | gauge | bytes | 15-19 | Total disk space for the table including indexes and TOAST, from `pg_total_relation_size()`. |
| `pg_stat_user_tables_relpages` | gauge | pages | 15-19 | The planner's estimate of the table's size in pages. An estimate, not a measurement. |
| `pg_stat_user_tables_reltuples` | gauge | rows | 15-19 | The planner's estimate of live rows. -1 means the table has never been analyzed, and that value is published rather than flattened to zero. |
| `pg_stat_user_tables_relallvisible` | gauge | pages | 15-19 | Pages marked all-visible. The gap from `relpages` is what an index-only scan cannot skip. |
| `pg_stat_user_tables_relallfrozen` | gauge | pages | 18-19 | Pages marked all-frozen. |
| `pg_stat_user_tables_relfrozenxid_age` | gauge | transactions | 15-19 | Transactions since this relation's oldest unfrozen transaction ID. An invalid zero ID becomes zero here, including for partitioned parents, and a zero therefore does not mean a freshly frozen relation. |
| `pg_stat_user_tables_relminmxid_age` | gauge | multixacts | 15-19 | Multixacts since this relation's oldest multixact ID, with the same zero caveat. |

## stat_wal_receiver

The WAL receiver on a standby.

| | |
| --- | --- |
| Scope | cluster |
| Labels | `upstream_host`, `slot_name`, `status` |
| Cardinality | one series per metric on a standby, none on a primary |
| Reset | the LSNs are taken modulo 2^52 before publishing, so they wrap, and a wrap looks like a counter reset |
| Availability | all versions. Publishes nothing on a primary, where `pg_stat_wal_receiver` is empty, and nothing on Aurora. Disabled by default in this exporter. A row with any NULL in it is skipped entirely |
| Partial failure | the collector fails as a unit |

The LSN and time metrics are published as counters. A timestamp is not a
counter, and an LSN taken modulo 2^52 is not one either.

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_stat_wal_receiver_receive_start_lsn` | counter | bytes of WAL, modulo 2^52 | 15-19 | The first log position used when the receiver started. |
| `pg_stat_wal_receiver_receive_start_tli` | gauge | timeline | 15-19 | The first timeline used when the receiver started. |
| `pg_stat_wal_receiver_flushed_lsn` | counter | bytes of WAL, modulo 2^52 | 15-19 | The last position received and flushed to disk. |
| `pg_stat_wal_receiver_received_tli` | gauge | timeline | 15-19 | The timeline of the last position received. |
| `pg_stat_wal_receiver_last_msg_send_time` | counter | seconds since the epoch | 15-19 | When the upstream sender sent its last message. |
| `pg_stat_wal_receiver_last_msg_receipt_time` | counter | seconds since the epoch | 15-19 | When this server received it. The difference from the send time is the one-way delay, assuming the clocks agree. |
| `pg_stat_wal_receiver_latest_end_lsn` | counter | bytes of WAL, modulo 2^52 | 15-19 | The last position reported back to the sender. |
| `pg_stat_wal_receiver_latest_end_time` | counter | seconds since the epoch | 15-19 | When that was reported. |
| `pg_stat_wal_receiver_upstream_node` | gauge | node id | 15-19 | The upstream node id, parsed out of a `repmgr_slot_N` slot name. Absent unless the slot follows that convention. |

## statio_user_indexes

Buffer and disk reads per index.

| | |
| --- | --- |
| Scope | database, these catalogues describe one database |
| Labels | `datname`, `schemaname`, `relname`, `indexrelname` |
| Cardinality | one series per index per selected database, per metric |
| Reset | `pg_stat_reset()` in that database |
| Availability | all versions |
| Partial failure | the collector fails as a unit, per database |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_statio_user_indexes_idx_blks_read_total` | counter | blocks | 15-19 | Blocks read from disk for this index. |
| `pg_statio_user_indexes_idx_blks_hit_total` | counter | blocks | 15-19 | Blocks found in the buffer cache for this index. |

## statio_user_tables

Buffer and disk reads per table, including its TOAST relations.

| | |
| --- | --- |
| Scope | database, these catalogues describe one database |
| Labels | `datname`, `schemaname`, `relname` |
| Cardinality | one series per relation per selected database, per metric |
| Reset | `pg_stat_reset()` in that database |
| Availability | all versions. The TOAST metrics are zero for a table without a TOAST relation, which is not the same as absent |
| Partial failure | the collector fails as a unit, per database |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_statio_user_tables_heap_blocks_read` | counter | blocks | 15-19 | Blocks read from disk for the table itself. |
| `pg_statio_user_tables_heap_blocks_hit` | counter | blocks | 15-19 | Blocks found in cache for the table itself. |
| `pg_statio_user_tables_idx_blocks_read` | counter | blocks | 15-19 | Blocks read from disk across all the table's indexes. Not interchangeable with the per-index family. |
| `pg_statio_user_tables_idx_blocks_hit` | counter | blocks | 15-19 | Blocks found in cache across all the table's indexes. |
| `pg_statio_user_tables_toast_blocks_read` | counter | blocks | 15-19 | Blocks read from disk for the table's TOAST relation. |
| `pg_statio_user_tables_toast_blocks_hit` | counter | blocks | 15-19 | Blocks found in cache for the TOAST relation. |
| `pg_statio_user_tables_tidx_blocks_read` | counter | blocks | 15-19 | Blocks read from disk for the TOAST relation's indexes. |
| `pg_statio_user_tables_tidx_blocks_hit` | counter | blocks | 15-19 | Blocks found in cache for the TOAST relation's indexes. |

## wal

Write-ahead log on disk.

| | |
| --- | --- |
| Scope | cluster |
| Labels | none |
| Cardinality | two series per server |
| Reset | none, these are current sizes |
| Availability | all versions; needs `pg_monitor` or superuser to list the WAL directory |
| Partial failure | the collector fails as a unit |

| metric | type | unit | versions | what it measures |
| --- | --- | --- | --- | --- |
| `pg_wal_segments` | gauge | segments | 15-19 | Files in `pg_wal`, from `pg_ls_waldir()`. |
| `pg_wal_size_bytes` | gauge | bytes | 15-19 | Total size of those files. Growth here with a lagging or inactive replication slot is the usual way a disk fills. |

## What is wrong with this catalogue

Writing it down surfaced these. None is fixed here, because every one of them
changes a metric name, type or value that something is already reading, and
that belongs with the compatibility mapping rather than with the description.

Two metrics are named in seconds and carry transaction counts.
`pg_database_wraparound_age_datfrozenxid_seconds` and
`pg_database_wraparound_age_datminmxid_seconds` publish `age(datfrozenxid)`
and `mxid_age(datminmxid)` unchanged, which PostgreSQL returns as integers.
An alert written as though the value were a duration compares a transaction
count against a number of seconds, and the two have no fixed conversion. These
are the ones most likely to be wrong in somebody's alerting today.

Thirteen bgwriter metrics are published twice under two names with identical
values, which is 13 wasted series per server and two names a query could pick
from with no way to tell they are the same.

Sixty-four counters do not end in `_total`. The convention is not decoration:
tooling that distinguishes counters from gauges by name gets these wrong.
`collector/metric_catalog_test.go` records all sixty-four so that no new one
joins them.

Seven timestamps are published as counters, in the bgwriter, stat_database and
wal receiver families. A timestamp goes backwards when something is reset, and
a counter that goes backwards is read as a counter reset, so `rate()` over one
of these produces a number that means nothing.

Three WAL positions are published as counters after being taken modulo 2^52.
They wrap, and a wrap is indistinguishable from a reset.

Time is published in two units. `pg_stat_statements` divides PostgreSQL's
milliseconds into seconds; bgwriter, stat_database, stat_io and
stat_user_tables publish milliseconds under names that say nothing about the
unit. Both are defensible on their own and together they mean a dashboard
author has to check each family.

`pg_stat_io_reueses_total` is a misspelling of `reuses`.

`pg_replication_slot_slot_confirmed_flush_lsn` is zero for a physical slot,
which has no confirmed flush position. The query coalesces the NULL away, so a
column that does not apply reads as a consumer that has confirmed nothing. The
contract asks for missing and zero to stay distinguishable and here they do
not.

Three label sets are unbounded, and nothing in the exposition says so.
`pg_stat_statements` mints a series per statement shape per user per database,
`pg_process_idle_seconds` mints one per `application_name` that any client
chooses to send, and `pg_stat_activity_autovacuum_timestamp_seconds` mints one
per relation that has ever been autovacuumed while a scrape was running.

Both long-running transaction metrics measure something other than their
names. The query counts rows of `pg_stat_activity` that are not idle and not
autovacuum, with no age threshold and no requirement that a transaction is
open, and its second column is `MAX(EXTRACT(EPOCH FROM clock_timestamp()))`,
which is the time the query ran. Measured on PostgreSQL 18 against a
transaction about half a second old: the metrics reported 10 and
1795247634.153131, while the age of the oldest transaction was 0.476791
seconds. The timestamp is correctable on its own: an oldest start taken from
`xact_start`, in the same unit, which is what the name already promises. The
count is the part that needs a threshold the operator sets, and that is a
compatibility decision, so the two are not waiting on each other.

Two bgwriter metrics are published as zero where the server does not have
them. `buffers_backend` and `buffers_backend_fsync`, with their `_total`
aliases, come from columns that `pg_stat_bgwriter` lost in 17. The query for
17 and later does not select them, the collector leaves their
`sql.NullInt64` unset, and it publishes zero rather than omitting the series.
A zero there reads as a measured absence of backend writes, which is the
false-zero shape the version gating exists to avoid, and it is why the
availability column says 15-19 for metrics that are only measured below 17.
PostgreSQL moved that accounting into `pg_stat_io`, which this exporter
already collects.

`pg_replication_slot_slot_restart_lsn` was added rather than corrected, and it
is the position retention is measured from. Before it, the positions published
were the server's current one and the consumer's confirmed flush, and the
distance between those two is how far behind the consumer is rather than how
much log the slot is holding. Summing restart distances across slots is still
not a disk figure, because slots that overlap hold the same segments once.

The confirmed flush position is published for an inactive slot now, which is
the case an operator cares about: an inactive slot is the one holding log that
nothing will confirm. It is omitted where the server reports no position at
all rather than published as zero.
