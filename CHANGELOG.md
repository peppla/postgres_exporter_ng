## [Unreleased]

* [ENHANCEMENT] Add PostgreSQL 18 support:
  * Add parallel worker activity metrics
  * Add vacuum/analyze timing metrics
  * Add enhanced checkpointer metrics
  * Add `pg_stat_io` collector with byte statistics and WAL I/O activity tracking
* [ENHANCEMENT] Update CI tested PostgreSQL versions to include PostgreSQL 18
* [CHANGE] Database-local collectors run against every selected database
  instead of only the one the exporter connects to. `pg_stat_user_tables`,
  `pg_statio_user_tables`, `pg_stat_user_indexes`, `pg_statio_user_indexes` and
  `pg_extensions` described the connection's own database and said nothing
  about the rest of the server. Every series from them now carries `datname`,
  so a query that aggregated across them without that label returns a sum over
  more databases than before.
* [CHANGE] A scrape is bounded in time and in concurrency. Each request gets a
  deadline, with time reserved for writing the response; every connection
  carries a connect timeout; every query runs under the request's deadline and
  is cancelled when it passes; and a request beyond the concurrency limit is
  refused with HTTP 503 before any SQL runs. Previously a scrape ran until it
  finished, however long that took. Bounded admission of each piece of
  per-database work, a bounded worker pool and fairness between repeated
  attempts are not implemented yet.
* [CHANGE] Every connection the exporter opens carries a `statement_timeout`,
  a `lock_timeout` and a `connect_timeout`. They are applied downwards only, so
  a stricter setting on the role or the server is never raised, and a scrape can
  no longer wait indefinitely on a server that accepts a connection and then
  stops answering.
* [CHANGE] `cache_seconds` in a custom query file now suppresses the query for
  as long as it says. It had no effect at all before, because the cache lived
  on an object the exporter rebuilt for every request, so the query ran on
  every scrape. Samples served from the cache carry the collection time on the
  wire instead of being presented as fresh, so a reader can tell a value
  measured a moment ago from one measured a minute ago. What a backend then
  stores depends on its own ingestion settings, which this exporter does not
  control.
* [CHANGE] `collect[]` selects collectors individually and by scope:
  `postgres.cluster` for the collectors that describe a whole server,
  `postgres.database` for the per-database ones, and
  `postgres.collector.<name>` for one by name. A request naming an individual
  collector that does not exist or that ships disabled is refused with HTTP 400
  instead of producing an empty but successful scrape. A group that expands to
  nothing is not refused, because an empty group is a configuration an operator
  can reasonably ask for.
* [FEATURE] Report what a scrape meant to cover and what it did. The
  custom-query engine publishes `pg_exporter_databases_discovered`,
  `_selected`, `_attempted`, `_scraped`, `_failed`, `_deferred` and
  `_excluded`, and per database `pg_exporter_database_selected`,
  `_scrape_success`, `_scrape_error`, `_deferred` and `_excluded`. The
  namespaced collectors publish their own set:
  `pg_collector_databases_discovered`, `_selected`, `_attempted`, `_scraped`,
  `_failed`, `_deferred` and `_excluded`,
  `pg_collector_database_discovery_success`, and per database
  `pg_collector_database_up`, `_error` and `_deferred`. On that engine a
  database counts as covered only when every database-local collector selected
  for it ran without error, which is stricter than the custom-query engine's
  count of the same name: that one reports a scrape that returned no error and
  knows nothing about the collectors inside it. A collector that fails on an
  open connection is now reported as `pg_collector_database_error` with
  `reason="scrape"`, beside a `pg_collector_database_up` that stays 1, because
  the connection is not what went wrong. Each engine also reports when a
  database was last collected without error, in
  `pg_exporter_database_last_success_timestamp_seconds` and
  `pg_collector_database_last_success_timestamp_seconds`, and the namespaced
  collectors report per collector as well in
  `pg_collector_last_success_timestamp_seconds`. The collector engine's
  database-level time belongs to the selection the request made: a scrape that
  asks for a subset of collectors gets the time that subset last completed, and
  cannot read or move the time another selection completed. Two scrape jobs
  with different `collect[]` sets therefore each see their own, separated by
  their own target labels rather than by a label on the metric.

  Read these rather than `pg_up` to tell whether a scrape covered what it
  intended. `pg_up` reports whether connections worked, not whether collection
  succeeded: it goes to zero only when every database fails to connect, so
  nine of ten unreachable databases read as a healthy server and a database
  whose queries all failed does not move it at all.
* [FEATURE] A custom query may name the databases it runs on, with `databases:`
  in the query file, and `master: true` keeps a query on the configured
  connection database alone. An omitted list means every database the engine
  reaches, and an explicit empty list means none.
* [FEATURE] A `limits` section in the configuration file bounds what the
  exporter may spend. Two kinds of limit: process-wide caps on database
  connections and on requests collecting at once, and per-request time budgets
  made of the request timeout, its ceiling, the reserve kept for writing the
  response, and the statement, lock and connect timeouts each connection
  carries. `--max-connections` sets the same number as `limits.connections` and
  is deprecated; setting both is refused at startup.
* [FEATURE] Table and index catalog metrics from `pg_class`: `relpages`,
  `reltuples`, `relallvisible`, version-gated `relallfrozen`, and
  `n_ins_since_vacuum` and version-gated `n_tup_newpage_upd` from
  `pg_stat_user_tables`.
* [BUGFIX] A query that failed partway through its rows reported success. Six
  iterators across both engines now check for the error that ends a row scan,
  so a database that was half collected is no longer reported as covered.
* [BUGFIX] `pg_stat_wal_receiver` reported a timeline number as a log position
  and a log position as a timeline, because two columns were scanned in the
  wrong order.
* [BUGFIX] A version restriction in a query file is evaluated against the
  server version the exporter detected. It was compared against a field nothing
  assigned, which left every restriction reading the zero version, so a query
  restricted to newer servers never ran and one restricted to older servers ran
  everywhere.
* [ENHANCEMENT] Table metrics share one label set across each row rather than
  building one per metric. On the recorded 5000-table benchmark this cut
  allocation from 127 to 81 MiB per scrape. Other schemas and collection
  profiles will differ.
* [CHANGE] A database the exporter had no room to collect from is reported as
  deferred rather than as a failure of that database. `databases_failed` no
  longer counts work that never started, and `databases_attempted` and
  `databases_deferred` say how much did and did not. An alert on failures no
  longer fires because the exporter was busy.
* [BUGFIX] A database the scrape ran out of time to start work on is reported
  as deferred with `reason="scrape_deadline"`, rather than as a database the
  exporter could not connect to. With no connection limit configured the
  exporter did not check the scrape's deadline before starting work, so after
  the deadline passed every remaining database was attempted, each connection
  failed on the expired deadline, and the result named the server as
  unreachable. The new reason separates the two ways a scrape runs out of room:
  `no_connection_permit` for work that waited for a connection and did not get
  one, and `scrape_deadline` for work there was no time left to start. The two
  call for different responses, so they are no longer reported as one.
* [CHANGE] Database selection written in both the configuration file and a
  deprecated flag is refused at startup instead of being resolved at scrape
  time. The exporter exits rather than starting and serving a response with no
  metrics in it, which is what it used to do.
* [CHANGE] A custom query that names a label column its SQL does not select now
  fails that namespace, and the exporter reports an error for it. Before this,
  the label took the value of the query's first selected column: a file naming
  `tenant_id` while selecting only `value` published `tenant_id="42"`. A query
  file in that state has to name the column in its SELECT or drop it from its
  metrics, and until it does, that namespace publishes nothing.
* [CHANGE] The built-in `pg_stat_replication` metrics no longer carry a
  `slot_name` label. `pg_stat_replication` has no column of that name on any
  supported PostgreSQL version, so the label carried the view's first column,
  which is the backend process id. Affected series are
  `pg_stat_replication_pg_current_wal_lsn_bytes`,
  `pg_stat_replication_pg_wal_lsn_diff` and any untyped series this namespace
  publishes for columns the mapping does not name. A query or dashboard that
  selects on `slot_name` for these metrics matches nothing after the upgrade,
  and what it matched before was a process id. Those metrics carry `pid` now,
  which is that process id under its own name, so a consumer that was using
  `slot_name` to tell one standby from another should use `pid`. The slot name
  itself is in `pg_replication_slots`, which is a different namespace, and is
  correlated through `active_pid`.

## 0.15.0 / 2023-10-27

* [ENHANCEMENT] Add 1kB and 2kB units #915
* [BUGFIX] Add error log when probe collector creation fails #918
* [BUGFIX] Fix test build failures on 32-bit arch #919
* [BUGFIX] Adjust collector to use separate connection per scrape #936

## 0.14.0 / 2023-09-11

* [CHANGE] Add `state` label to pg_process_idle_seconds #862
* [CHANGE] Change database connections to one per scrape #882 #902
* [ENHANCEMENT] Add wal collector #858
* [ENHANCEMENT] Add database_wraparound collector #834
* [ENHANCEMENT] Add stat_activity_autovacuum collector #840
* [ENHANCEMENT] Add stat_wal_receiver collector #844
* [ENHANCEMENT] Add xlog_location collector #849
* [ENHANCEMENT] Add statio_user_indexes collector #845
* [ENHANCEMENT] Add long_running_transactions collector #836
* [ENHANCEMENT] Add pg_stat_user_tables_size_bytes metric #904
* [BUGFIX] Fix tests on 32-bit systems #857
* [BUGFIX] Fix pg_stat_statements metrics on Postgres 13+ #874 #876
* [BUGFIX] Fix pg_stat_database metrics for NULL stats_reset #877
* [BUGFIX] Fix pg_replication_lag_seconds on Postgres 10+ when master is idle #895

## 0.13.2 / 2023-07-21

* [BUGFIX] Fix type issues on pg_postmaster metrics #828
* [BUGFIX] Fix pg_replication collector instantiation #854
* [BUGFIX] Fix pg_process_idle metrics #855

## 0.13.1 / 2023-06-27

* [BUGFIX] Make collectors not fail on null values #823

## 0.13.0 / 2023-06-21

BREAKING CHANGES:

Please note, the following features are deprecated and may be removed in a future release:
- `auto-discover-databases`
- `extend.query-path`
- `constantLabels`
- `exclude-databases`
- `include-databases`

This exporter is meant to monitor PostgresSQL servers, not the user data/databases. If
you need a generic SQL report exporter https://github.com/burningalchemist/sql_exporter
is recommended.

* [CHANGE] Adjust log level for collector startup #784
* [CHANGE] Move queries from queries.yaml to collectors #801
* [CHANGE] Deprecate extend queries feature #811
* [CHANGE] Deprecate additional database features #815
* [CHANGE] Convert pg_stat_database to new collector #685
* [ENHANCEMENT] Supports alternate postgres:// prefix in URLs #787
* [BUGFIX] Fix pg_setting different help values #771
* [BUGFIX] Fix column type for pg_replication_slots #777
* [BUGFIX] Fix pg_stat_database collector #809

## 0.12.1 / 2023-06-12
* [BUGFIX] Fix column type for pg_replication_slots #777

## 0.12.0 / 2023-03-21

BREAKING CHANGES:

This release changes support for multiple postgres servers to use the
multi-target exporter pattern. This makes it much easier to monitor multiple
PostgreSQL servers from a single exporter by passing the target via URL
params. See the Multi-Target Support section of the README.

* [CHANGE] Add multi-target support #618
* [CHANGE] Add usename and application_name to pg_stat_activity metrics #673
* [FEATURE] Add replication metrics from pg_replication_slots #747
* [BUGFIX] Add dsn type for handling datasources #678
* [BUGFIX] Add 64kB unit for postgres 15 #740
* [BUGFIX] Add 4kB unit for postgres compiled with small blocks #699

## 0.11.1 / 2022-08-01

* [BUGFIX] Fix checkpoint_write_time value type #666
* [BUGFIX] Fix checkpoint_sync_time value type #667

## 0.11.1 / 2022-08-01

* [BUGFIX] Fix checkpoint_write_time value type #666
* [BUGFIX] Fix checkpoint_sync_time value type #667

## 0.11.0 / 2022-07-28

NOTE: pg_stat_bgwriter counter metrics had the `_total` suffix added #556

* [CHANGE] refactor pg_stat_bgwriter metrics into standalone collector #556
* [FEATURE] Add pg_database collector #613
* [ENHANCEMENT] Add pg_database_size_bytes metric #613
* [BUGFIX] Avoid parsing error from bogus Azure Flexible Server custom GUC #587
* [BUGFIX] Fix pg_stat_archiver error in 9.4 and earlier. #599
* [BUGFIX] Sanitize setting values because of Aurora irregularity #620

## 0.10.1 / 2022-01-14

* [BUGFIX] Fix broken log-level for values other than debug. #560

## 0.10.0 / 2021-07-08

* [ENHANCEMENT] Add ability to set included databases when autoDiscoverDatabases is enabled #499
* [BUGFIX] fix pg_replication_slots on postgresql versions 9.4 <> 10.0 #537

## 0.9.0 / 2021-03-01

First release under the Prometheus Community organisation.

* [CHANGE] Update build to use standard Prometheus promu/Dockerfile
* [ENHANCEMENT] Remove duplicate column in queries.yml #433
* [ENHANCEMENT] Add query for 'pg_replication_slots' #465
* [ENHANCEMENT] Allow a custom prefix for metric namespace #387
* [ENHANCEMENT] Improve PostgreSQL replication lag detection #395
* [ENHANCEMENT] Support connstring syntax when discovering databases #473
* [ENHANCEMENT] Detect SIReadLock locks in the pg_locks metric #421
* [BUGFIX] Fix pg_database_size_bytes metric in queries.yaml #357
* [BUGFIX] Don't ignore errors in parseUserQueries #362
* [BUGFIX] Fix queries.yaml for AWS RDS #370
* [BUGFIX] Recover when connection cannot be established at startup #415
* [BUGFIX] Don't retry if an error occurs #426
* [BUGFIX] Do not panic on incorrect env #457

## 0.8.0 / 2019-11-25

* Add a build info metric (#323)
* Re-add pg_stat_bgwriter metrics which were accidentally removed in the previous version. (resolves #336)
* Export pg_stat_archiver metrics (#324)
* Add support for 'DATA_SOURCE_URI_FILE' envvar.
* Resolve #329
* Added new field "master" to queries.yaml. (credit to @sfalkon)
  - If "master" is true, query will be call only on once database in instance
* Change queries.yaml for work with autoDiscoveryDatabases options (credit to @sfalkon)
  - added current database name to metrics because any database in cluster maybe have the same table names
  - added "master" field for query instance metrics.

## 0.7.0 / 2019-11-01

Introduces some more significant changes, hence the minor version bump in
such a short time frame.

* Rename pg_database_size to pg_database_size_bytes in queries.yml.
* Add pg_stat_statements to sample queries.yml file.
* Add support for optional namespace caching. (#319)
* Fix some autodiscovery problems (#314) (resolves #308)
* Yaml parsing refactor (#299)
* Don't stop generating fingerprint while encountering value with "=" sign (#318)
  (may resolve problems with passwords and special characters).

## 0.6.0 / 2019-10-30

* Add SQL for grant connect (#303)
* Expose pg_current_wal_lsn_bytes (#307)
* [minor] fix landing page content-type (#305)
* Updated lib/pg driver to 1.2.0 in order to support stronger SCRAM-SHA-256 authentication. This drops support for Go < 1.11 and PostgreSQL < 9.4. (#304)
* Provide more helpful default values for tables that have never been vacuumed (#310)
* Add retries to getServer() (#316)
* Fix pg_up metric returns last calculated value without explicit resetting (#291)
* Discover only databases that are not templates and allow connections (#297)
* Add --exclude-databases option (#298)

## 0.5.1 / 2019-07-09

* Add application_name as a label for pg_stat_replication metrics (#285).

## 0.5.0 / 2019-07-03

It's been far too long since I've done a release and we have a lot of accumulated changes.

* Docker image now runs as a non-root user named "postgres_exporter"
* Add `--auto-discover-databases` option, which automatically discovers and scrapes all databases.
* Add support for boolean data types as metrics
* Replication lag is now expressed as a float and not truncated to an integer.
* When default metrics are disabled, no version metrics are collected anymore either.
* BUGFIX: Fix exporter panic when postgres server goes down.
* Add support for collecting metrics from multiple servers.
* PostgreSQL 11 is now supported in the integration tests.

## 0.4.7 / 2018-10-02

* Added a query for v9.1 pg_stat_activity.
* Add `--constantLabels` flag to allow applying fixed constant labels to metrics.
* queries.yml: dd pg_statio_user_tables.
* Support 'B' suffix in units.

## 0.4.6 / 2018-04-15

* Fix issue #173 - 32 and 64mb unit sizes were not supported in pg_settings.

## 0.4.5 / 2018-02-27

* Add commandline flag to disable default metrics (thanks @hsun-cnnxty)

## 0.4.4 / 2018-03-21

* Bugfix for 0.4.3 which broke pg_up (it would always be 0).
* pg_up is now refreshed based on database Ping() every scrape.
* Re-release of 0.4.4 to fix version numbering.

## 0.4.2 / 2018-02-19

* Adds the following environment variables for overriding defaults:
    * `PG_EXPORTER_WEB_LISTEN_ADDRESS`
    * `PG_EXPORTER_WEB_TELEMETRY_PATH`
    * `PG_EXPORTER_EXTEND_QUERY_PATH`

* Add Content-Type to HTTP landing page.
* Fix Makefile to produce .exe binaries for Windows.

## 0.4.1 / 2017-11-30

* No code changes to v0.4.0 for the exporter.
* First release switching to tar-file based distribution.
* First release with Windows and Darwin cross-builds.\\

## 0.4.0 / 2017-11-29

* Fix panic due to inconsistent label cardinality when using queries.yaml with
  queries which return extra columns.
* Add metric for whether the user queries YAML file parsed correctly. This also
  includes the filename and SHA256 sum allowing tracking of updates.
* Add pg_up metric to indicate whether the exporter was able to connect and
  Ping() the PG instance before a scrape.
* Fix broken link in landing page for `/metrics`

## 0.3.0 / 2017-10-23

* Add support for PostgreSQL 10.

## 0.2.3 / 2017-09-07

* Add support for the 16kB unit when decoding pg_settings. (#101)

## 0.2.2 / 2017-08-04

* Fix DSN logging. The exporter previously never actually logged the DSN when
  database connections failed. This was also masking a logic error which could
  potentially lead to a crash when DSN was unparseable, though no actual
  crash could be produced in testing.

## 0.2.1 / 2017-06-07

* Ignore functions that cannot be executed during replication recovery (#52)
* Add a `-version` flag finally.
* Add confirmed_flush_lsn to pg_stat_replication.

## 0.2.0 / 2017-04-18

* Major change - use pg_settings to retrieve runtime variables. Adds >180
  new metrics and descriptions (big thanks to Matt Bostock for this work).

  Removes the following metrics:
  ```
  pg_runtime_variable_max_connections
  pg_runtime_variable_max_files_per_process
  pg_runtime_variable_max_function_args
  pg_runtime_variable_max_identifier_length
  pg_runtime_variable_max_index_keys
  pg_runtime_variable_max_locks_per_transaction
  pg_runtime_variable_max_pred_locks_per_transaction
  pg_runtime_variable_max_prepared_transactions
  pg_runtime_variable_max_standby_archive_delay_milliseconds
  pg_runtime_variable_max_standby_streaming_delay_milliseconds
  pg_runtime_variable_max_wal_senders
  ```

  They are replaced by equivalent names under `pg_settings` with the exception of
  ```
  pg_runtime_variable_max_standby_archive_delay_milliseconds
  pg_runtime_variable_max_standby_streaming_delay_milliseconds
  ```
  which are replaced with
  ```
  pg_settings_max_standby_archive_delay_seconds
  pg_settings_max_standby_streaming_delay_seconds
  ```

## 0.1.3 / 2017-02-21

* Update the Go build to 1.7.5 to include a fix for NAT handling.
* Fix passwords leaking in DB url error message on connection failure.

## 0.1.2 / 2017-02-07

* Use a connection pool of size 1 to reduce memory churn on target database.

## 0.1.1 / 2016-11-29

* Fix pg_stat_replication metrics not being collected due to semantic version
  filter problem.

## 0.1.0 / 2016-11-21

* Change default port to 9187.
* Fix regressions with pg_stat_replication on older versions of Postgres.
* Add pg_static metric to store version strings as labels.
* Much more thorough testing structure.
* Move to semantic versioning for releases and docker image publications.

## 0.0.1 / 2016-06-03

Initial release for publication.
