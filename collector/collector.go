// Copyright 2022 The Prometheus Authors
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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/admission"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/fanout"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/rotation"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

var (
	factories              = make(map[string]func(collectorConfig) (Collector, error))
	initiatedCollectorsMtx = sync.Mutex{}
	initiatedCollectors    = make(map[string]Collector)
	collectorState         = make(map[string]*bool)
	collectorScopes        = make(map[string]collectorScope)
	forcedCollectors       = map[string]bool{} // collectors which have been explicitly enabled or disabled
)

const (
	// Namespace for all metrics.
	namespace = "pg"

	defaultEnabled  = true
	defaultDisabled = false
)

var (
	// The datname label is empty for a cluster-wide collector, because there
	// is no one database its result belongs to. Prometheus treats an
	// empty-valued label as absent, so those series keep the identity they
	// had before the label existed, and an alert on
	// pg_scrape_collector_success == 0 keeps matching them.
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"postgres_exporter: Duration of a collector scrape. datname is the database a database-local collector ran against, and is empty for a cluster-wide collector.",
		[]string{"collector", "datname"},
		nil,
	)
	collectorLastSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "last_success_timestamp_seconds"),
		"When this collector last ran on this database without error, as seconds since the epoch. Absent for a collector this process has never run successfully there, which is not the same as one that last succeeded at the epoch. The target label is this process's identity for the connection; it is not stable across restarts. datname is empty for a cluster-wide collector.",
		[]string{"collector", "datname", "target"},
		nil,
	)
	collectorDatabaseLastSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "database_last_success_timestamp_seconds"),
		"When the database-local collectors this request selected last all ran on this database without error in one scrape, as seconds since the epoch. The selection is part of what is remembered, not a label: a request that asks for a subset gets the time that subset last completed, and cannot read or move the time a different selection completed. So two scrape jobs with different collect[] sets each see their own, separated by their own target labels. Absent for a selection this process has never completed here, which is not the same as one completed at the epoch. The target label is this process's identity for the connection; it is not stable across restarts.",
		[]string{"datname", "target"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"postgres_exporter: Whether a collector succeeded. datname is the database a database-local collector ran against, and is empty for a cluster-wide collector. Absent for a collector this scrape never started, which is in scrape_collector_deferred instead.",
		[]string{"collector", "datname"},
		nil,
	)
	scrapeDeferredDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_deferred"),
		"1 for each collector this scrape did not start on this database, labelled with why: scrape_deadline for a turn that came after the scrape ran out of time, scrape_cancelled for a scrape that was cancelled. No SQL ran, so this is not a collector that failed and carries no scrape_collector_success beside it. The connection was already open and already held its permit, so no_connection_permit is not among the reasons here. datname is empty for a cluster-wide collector.",
		[]string{"collector", "datname", "reason"},
		nil,
	)

	// Coverage for this engine, reported against its own intentions. The
	// legacy engine publishes its own counts under pg_exporter_, and one count
	// spanning both would merge two different sets and could suggest either a
	// coverage problem or full coverage where neither is true.
	collectorDatabaseUpDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "database_up"),
		"Whether the collectors could connect to this database (1 for yes, 0 for no). Published for every database the namespaced collectors selected, and also for the configured connection database even when selection left it out, because its connection is what cluster-wide collection runs on. So this can carry one more database than databases_selected counts.",
		[]string{"server", "datname"},
		nil,
	)
	collectorDiscoveredDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_discovered"),
		"Connectable databases the server reported to the namespaced collectors, including the configured connection database.",
		[]string{"server"},
		nil,
	)
	collectorSelectedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_selected"),
		"Databases the namespaced collectors intended to run database-local collectors against.",
		[]string{"server"},
		nil,
	)
	collectorExcludedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_excluded"),
		"Databases the server offered that configuration left out of the namespaced collectors' set. These are decisions, not failures.",
		[]string{"server"},
		nil,
	)
	collectorDiscoverySuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "database_discovery_success"),
		"Whether the namespaced collectors could determine the set of databases (1 for yes, 0 for no). When this is 0 the set has collapsed to the configured connection database, which otherwise looks like a one-database server.",
		[]string{"server"},
		nil,
	)
	collectorDatabaseErrorDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "database_error"),
		"1 for each database this scrape did not cover, labelled with why: connect for a connection that failed, scrape for a database-local collector that failed on a connection that worked, and scrape_deadline or scrape_cancelled for a collection that started and then had collectors it never got to. The last three keep database_up 1, because the connection is not what went wrong, and the budget reasons assert nothing about the database itself: they say the exporter ran out of room part way through. A database where both happened carries one series for each, so a real collector failure is not replaced by the budget reason, and it is still one database in databases_failed. A database where nothing at all started is in database_deferred instead. Reasons are a closed set, never an error message.",
		[]string{"server", "datname", "reason"},
		nil,
	)
	// What the database-local work came to. attempted and scraped are derived
	// from the other three rather than counted beside them, so the two
	// invariants cannot drift: attempted plus deferred is selected, and
	// scraped plus failed is attempted. An exporter that has lost track of its
	// own work is what these series exist to show, so they must not be able to
	// disagree with each other.
	collectorAttemptedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_attempted"),
		"Selected databases this scrape started database-local work on. Selected minus deferred.",
		[]string{"server"},
		nil,
	)
	collectorScrapedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_scraped"),
		"Databases where every selected database-local collector ran without error. All of them, not the ones a scheduling decision happened to start, so a database whose collection was partly left out is not counted here. Not a claim that each collector read everything there was to read.",
		[]string{"server"},
		nil,
	)
	collectorFailedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_failed"),
		"Databases this scrape started database-local work on and did not finish, because the connection failed, a collector failed, or the scrape ran out of room before every selected collector had its turn. Why it did not finish is in database_error. A database where nothing at all started is counted in databases_deferred instead, which is a different thing: an attempt that did not finish says work was done here, and a deferral says none was.",
		[]string{"server"},
		nil,
	)
	collectorDeferredDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "databases_deferred"),
		"Selected databases this scrape did not start work on, because there was no room within its budget. Not covered and not failed: nothing was tried, so nothing is known to be wrong with them. A number here that does not fall is a selected set the exporter cannot keep up with.",
		[]string{"server"},
		nil,
	)
	collectorDeferredDatabaseDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "database_deferred"),
		"1 for each selected database this scrape did not start work on, labelled with why: no_connection_permit for work that waited for a connection and did not get one, scrape_deadline for work the scrape ran out of time to start, and scrape_cancelled for work the scrape was cancelled before starting. Also published for the configured connection database when that connection itself was deferred, whether or not selection kept it for database-local work, because that connection is what cluster-wide collection runs on. So this can name one more database than databases_deferred counts, in the same way database_up can. Reasons are a closed set, never an error message. A deferral is not an error and is not counted in database_error.",
		[]string{"server", "datname", "reason"},
		nil,
	)
	collectorExcludedDatabaseDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "collector", "database_excluded"),
		"1 for each database an exclude list named and kept out of the namespaced collectors' set. Databases left out by a given include list are counted in databases_excluded and not named here, because that set can be the whole server.",
		[]string{"server", "datname", "reason"},
		nil,
	)
)

// collectorScope says where a collector's queries have to run.
//
// PostgreSQL splits its catalogs. pg_database, pg_locks, pg_stat_activity,
// pg_stat_database and the replication views are shared, so one connection
// anywhere on the server sees the whole cluster. pg_stat_user_tables,
// pg_statio_user_tables, pg_statio_user_indexes and pg_extension are not: they
// show the objects of the database the connection is attached to and nothing
// else.
//
// Before this distinction existed the exporter ran every collector on one
// connection, so the four database-local ones reported the configured
// database's tables and silently nothing about any other database on the
// server. The scope is declared at registration because it is a property of
// the collector's SQL, and a collector added without thinking about it should
// not quietly inherit whichever behaviour is cheaper.
type collectorScope int

const (
	// scopeCluster runs once per server, on the configured connection.
	scopeCluster collectorScope = iota
	// scopeDatabase runs once per selected database, on a connection to it.
	//
	// Every metric such a collector produces has to carry a datname label.
	// Without one, two databases produce series identical in name and labels,
	// which the registry rejects as duplicates and which an operator could not
	// tell apart anyway. None of the underlying views supply the database as a
	// column, so the label is synthesized with current_database().
	scopeDatabase
)

func (s collectorScope) String() string {
	if s == scopeDatabase {
		return "database"
	}
	return "cluster"
}

type Collector interface {
	Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error
}

type collectorConfig struct {
	logger           log.Logger
	excludeDatabases []string
}

func registerCollector(name string, isDefaultEnabled bool, scope collectorScope, createFunc func(collectorConfig) (Collector, error)) {
	var helpDefaultState string
	if isDefaultEnabled {
		helpDefaultState = "enabled"
	} else {
		helpDefaultState = "disabled"
	}

	// Create flag for this collector
	flagName := fmt.Sprintf("collector.%s", name)
	flagHelp := fmt.Sprintf("Enable the %s collector (default: %s).", name, helpDefaultState)
	defaultValue := fmt.Sprintf("%v", isDefaultEnabled)

	flag := kingpin.Flag(flagName, flagHelp).Default(defaultValue).Action(collectorFlagAction(name)).Bool()
	collectorState[name] = flag
	collectorScopes[name] = scope

	// Register the create function for this collector
	factories[name] = createFunc
}

// PostgresCollector implements the prometheus.Collector interface.
type PostgresCollector struct {
	Collectors map[string]Collector
	logger     log.Logger

	instance *instance

	// Which databases the database-local collectors run against. This engine's
	// own set: the legacy engine builds its own from its own configuration and
	// the two are never intersected.
	excludeDatabases dbselect.List
	includeDatabases dbselect.List

	connSema *semaphore.Weighted
	// dbWorkers is how many databases the fan-out works on at once. It is the
	// connection limit, because a worker's first act is to ask for a permit
	// and there is nothing for a worker beyond that limit to do but queue.
	// Travelling with the semaphore rather than beside it keeps the two from
	// disagreeing, which would put the goroutines back.
	dbWorkers int
	ctx       context.Context

	// lastSuccess outlives one scrape: how long a collector has been failing
	// on a database cannot be answered from the scrape that is failing.
	lastSuccess *lastsuccess.Store

	// rotation outlives one scrape too, and for the same kind of reason: where
	// the last scrape started is the one thing a new one cannot work out for
	// itself. Nil means no rotation, so the order is the sorted one.
	rotation *rotation.Store
}

type Option func(*PostgresCollector) error

// scopedCollectors returns the enabled collectors that registered with the
// given scope.
//
// The classification is read from the registration, not from a second list of
// names kept beside it. A list like that is only correct while someone
// maintains it: a collector renamed, or added without a thought for it, falls
// to whichever side the lookup happens to miss and nothing says so. The scope
// is already a required argument to registerCollector, so there is one answer
// and it is the one the fan-out uses.
//
// Disabled collectors are left out rather than reported as an error. A group
// asks for whatever of its kind the exporter has, which is a different request
// from naming a collector, where being turned off is worth saying.
func scopedCollectors(scope collectorScope) []string {
	var names []string
	for name, enabled := range collectorState {
		if enabled == nil || !*enabled {
			continue
		}
		if collectorScopes[name] == scope {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// ClusterScopedCollectors returns the enabled collectors whose answer is the
// same from any database on the server, so one scrape of them describes the
// whole cluster.
func ClusterScopedCollectors() []string { return scopedCollectors(scopeCluster) }

// DatabaseScopedCollectors returns the enabled collectors that read catalogs
// belonging to one database, and which therefore run once per selected
// database and carry a datname label.
func DatabaseScopedCollectors() []string { return scopedCollectors(scopeDatabase) }

// NewPostgresCollector creates a new PostgresCollector.
func NewPostgresCollector(logger log.Logger, excludeDatabases []string, dsn string, filters []string, options ...Option) (*PostgresCollector, error) {
	p := &PostgresCollector{
		logger: logger,
	}
	// Apply options to customize the collector
	for _, o := range options {
		err := o(p)
		if err != nil {
			return nil, err
		}
	}

	f := make(map[string]bool)
	for _, filter := range filters {
		enabled, exist := collectorState[filter]
		if !exist {
			return nil, fmt.Errorf("missing collector: %s", filter)
		}
		if !*enabled {
			return nil, fmt.Errorf("disabled collector: %s", filter)
		}
		f[filter] = true
	}
	collectors := make(map[string]Collector)
	initiatedCollectorsMtx.Lock()
	defer initiatedCollectorsMtx.Unlock()
	for key, enabled := range collectorState {
		if !*enabled || (len(f) > 0 && !f[key]) {
			continue
		}
		if collector, ok := initiatedCollectors[key]; ok {
			collectors[key] = collector
		} else {
			collector, err := factories[key](collectorConfig{
				logger:           log.With(logger, "collector", key),
				excludeDatabases: excludeDatabases,
			})
			if err != nil {
				return nil, err
			}
			collectors[key] = collector
			initiatedCollectors[key] = collector
		}
	}

	p.Collectors = collectors

	if dsn == "" {
		return nil, errors.New("empty dsn")
	}

	instance, err := newInstance(dsn)
	if err != nil {
		return nil, err
	}
	p.instance = instance

	return p, nil
}

// WithContext sets context for the collector.
func WithContext(ctx context.Context) Option {
	return func(c *PostgresCollector) error {
		c.ctx = ctx
		return nil
	}
}

// WithConnectionsSemaphore sets the limit on open connections to the server,
// as the semaphore that issues the permits and the number of them there are.
//
// The count is taken here rather than configured separately. A second key would
// be a second thing to get wrong, and a fan-out wider than the connection limit
// has nothing to do with the extra width but wait.
//
// A count below one means one database at a time. Configuration validation
// refuses a connection limit below one, so that is reachable only through this
// API, and a caller who passes nothing gets slow collection rather than silent
// unbounded growth.
func WithConnectionsSemaphore(sem *semaphore.Weighted, connections int64) Option {
	return func(c *PostgresCollector) error {
		c.connSema = sem
		c.dbWorkers = int(connections)
		return nil
	}
}

// WithRotation sets the process-owned schedule of which selected database is
// offered the first attempt.
//
// Without one the order is the sorted order every scrape, which is what left a
// selected set that does not fit the budget giving the same prefix every
// opportunity and the rest none.
func WithRotation(store *rotation.Store) Option {
	return func(c *PostgresCollector) error {
		c.rotation = store
		return nil
	}
}

// WithLastSuccess sets the process-owned record of when each collector last
// ran without error on each database.
func WithLastSuccess(store *lastsuccess.Store) Option {
	return func(c *PostgresCollector) error {
		c.lastSuccess = store
		return nil
	}
}

// WithDatabaseSelection sets which databases the database-local collectors run
// against. Both lists remember whether they were configured, because an
// explicitly empty include list selects no databases and an omitted one means
// no restriction.
func WithDatabaseSelection(exclude, include dbselect.List) Option {
	return func(c *PostgresCollector) error {
		c.excludeDatabases = exclude
		c.includeDatabases = include
		return nil
	}
}

// Describe implements the prometheus.Collector interface.
func (p PostgresCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
	ch <- scrapeDeferredDesc
	ch <- collectorDatabaseUpDesc
	ch <- collectorDiscoveredDesc
	ch <- collectorSelectedDesc
	ch <- collectorExcludedDesc
	ch <- collectorDiscoverySuccessDesc
	ch <- collectorExcludedDatabaseDesc
	ch <- collectorDatabaseErrorDesc
	ch <- collectorAttemptedDesc
	ch <- collectorScrapedDesc
	ch <- collectorFailedDesc
	ch <- collectorDeferredDesc
	ch <- collectorDeferredDatabaseDesc
	ch <- collectorLastSuccessDesc
	ch <- collectorDatabaseLastSuccessDesc
}

// Collect implements the prometheus.Collector interface.
//
// Cluster-wide collectors run once, on the configured connection.
// Database-local collectors run once per selected database, each on its own
// connection, because the views they read show only the database their
// connection is attached to. Before the fan-out existed they all ran on the
// one connection, so they described the configured database and said nothing
// at all about the rest of the server.
func (p PostgresCollector) Collect(ch chan<- prometheus.Metric) {
	// copy the instance so that concurrent scrapes have independent instances
	inst := p.instance.copy()

	// The control database's name as configured. The catalog may correct it
	// later, but a failure before that still has to name something.
	controlName, _ := config.DatabaseFromDSN(inst.dsn)

	// Where the selected collectors run, decided before anything can go
	// wrong. Every early exit below reports what this scrape meant to cover,
	// and a scrape that selected no database-scoped collector meant to cover
	// no database, whatever the catalog holds and whether or not the
	// connection worked.
	cluster, local := p.byScope()
	databaseLocal := len(local) > 0

	permit, decision := admission.Admit(p.ctx, p.connSema)
	if decision != admission.Admitted {
		// Nothing was attempted, which is neither a success nor a connection
		// failure. Returning silently here published no series at all, so a
		// scrape that collected nothing was indistinguishable from one that
		// was never configured to collect.
		reason := deferralReason(decision)
		level.Warn(p.logger).Log("msg", "Deferred the control connection", "reason", reason)
		p.reportControlDeferral(ch, inst.name, controlName, databaseLocal, reason)
		p.retainedForControl(ch, inst.dsn, controlName, cluster, local)
		return
	}
	// Set up the database connection for the collector.
	err := inst.setup(p.ctx)
	if err != nil {
		level.Error(p.logger).Log("msg", "Error opening connection to database", "err", err)
		permit.Release()
		p.reportControlFailure(ch, inst.name, controlName, dbselect.ReasonConnect, databaseLocal)
		p.retainedForControl(ch, inst.dsn, controlName, cluster, local)
		return
	}

	// The catalog read is what the fan-out needs: which databases exist, and
	// which of them configuration keeps. With no database-local collector
	// selected there is no fan-out, so the query is not run. A cluster-scoped
	// collector that wants the catalogue reads it for itself, as its own
	// metric, rather than waiting on this one.
	var sel dbSelection
	if databaseLocal {
		var err error
		sel, err = selectDatabases(p.ctx, inst, p.excludeDatabases, p.includeDatabases, p.lastSuccess)
		if err != nil {
			level.Error(p.logger).Log("msg", "Error listing databases", "err", err)
		}
		// Held until this request is finished with these connections, so a
		// catalogue read in another request cannot retire one while this one
		// is still recording under it.
		defer sel.done()
		p.applyCensus(inst.dsn, sel)
	}
	p.reportSelection(ch, inst.name, sel, databaseLocal)
	selected := localSelected(sel, databaseLocal)

	// The cluster-wide collectors run on the connection that is already open,
	// once per server, which is what they describe.
	wg := sync.WaitGroup{}
	wg.Add(len(cluster))
	for name, c := range cluster {
		go func(name string, c Collector) {
			defer wg.Done()
			execute(p.ctx, name, c, inst, ch, p.logger, "", p.lastSuccess)
		}(name, c)
	}
	wg.Wait()

	// The selection this request made, which is the context its database-level
	// freshness belongs to. A narrow request records and reads its own, so it
	// neither claims a wider selection's coverage nor goes without a time of
	// its own.
	selection := selectionOf(local)
	control := sel.controlTarget()

	// Released before any database-local work, including the configured
	// connection database's own. This connection was opened for the cluster
	// collectors and the catalogue read, and holding it through the fan-out
	// would keep a permit from a database whose turn has come.
	//
	// It is what lets the configured connection database take its turn in order
	// rather than always first. Its database-local collectors used to run here,
	// on this connection, before any other database was offered one, so a
	// selected set whose first database spends the budget left the rest with no
	// opportunity however the rest were ordered.
	inst.Close()
	permit.Release()

	// Without a catalog read there is no target to take the name from, so it
	// comes from the connection string, which is where reportControlFailure
	// takes it from on the paths where the connection did not work.
	upName := controlName
	if control != nil {
		upName = control.datname
	}

	if !databaseLocal {
		// The observation from the cluster-wide work. No database-local turn
		// is coming on this path, so this is the one sample for this database.
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 1, inst.name, upName)

		// Nothing was selected, so nothing was attempted, covered, failed or
		// deferred. Reported as four zeros rather than left out: a
		// cluster-only request already publishes a selected count of zero,
		// and four series that appear or vanish with the request shape cannot
		// be alerted on. Before this, the deferral count was published on the
		// path where such a request had no permit and omitted on the path
		// where it worked, which is the same configuration reporting two
		// different shapes.
		p.reportOutcome(ch, inst.name, 0, 0, 0)
		return
	}

	// Every selected database the fan-out opens a connection for, the
	// configured connection database among them. It is an ordinary member here
	// so that there is one path for what a database-local attempt comes to and
	// one accounting of its freshness, rather than a second copy of both for
	// one database.
	//
	// It costs one connection and one session setup for that database on every
	// scrape that selects it for database-local work, including a server with
	// one database, because the connection opened for the cluster work has been
	// released by now.
	pending := make([]dbTarget, 0, len(sel.targets))
	controlPending := false
	for _, t := range sel.targets {
		if t.skipDatabaseLocal {
			continue
		}
		if t.control {
			// Its connection was made and it worked. A deferral of its local
			// work must not say otherwise.
			t.baselineConnected = true
			controlPending = true
		}
		pending = append(pending, t)
	}

	// The observation from the cluster-wide work, published here only when no
	// local turn of its own will publish one. One sample per database per
	// scrape: a second would be two answers under the same labels.
	if !controlPending {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 1, inst.name, upName)
	}

	// Rotated, so the database offered the first attempt is not the same one
	// every scrape. The order is a rotation and not a selection: every database
	// here is still visited, and the ones that get no attempt are still named
	// as deferred.
	//
	// The schedule is given the names, not a count, because it remembers which
	// database it chose rather than how many scrapes have been. A database that
	// stays selected while others come and go is otherwise stepped over
	// indefinitely.
	members := make([]string, 0, len(pending))
	for _, t := range pending {
		members = append(members, t.datname)
	}
	order := rotation.Order(p.rotation.Next(p.rotationKey(inst.dsn, selection), members), len(pending))
	rotated := make([]dbTarget, 0, len(pending))
	for _, i := range order {
		rotated = append(rotated, pending[i])
	}

	failed, deferred := p.collectDatabases(ch, rotated, local, inst.name, selection)

	// After the fan-out, because it is the only point where the numbers are
	// known. Published even when they are zero, so that a scrape which
	// deferred nothing says so rather than leaving a reader to wonder whether
	// the series is missing or the exporter kept up.
	p.reportOutcome(ch, inst.name, selected, failed, deferred)
}

// rotationKey is the schedule this request's database-local work belongs to.
//
// Scoped by engine, by the connection, and by the selection, so two collection
// profiles walking different sets do not advance each other's turn and the
// query engine's schedule is its own.
func (p PostgresCollector) rotationKey(dsn, selection string) rotation.Key {
	target, _ := targetid.ID(dsn)
	return rotation.Key{Engine: "collector", Target: target, Profile: selection}
}

// collectDatabases runs the database-local work across the databases the
// fan-out opens a connection for, and returns how many failed and how many were
// deferred without being started.
//
// A fixed number of workers, each taking the next database as it finishes the
// last. Reaching that number is not a reason to leave a database out: every
// database here is visited, and what stops one short is its own admission check
// against the scrape's deadline.
//
// Split out of Collect so this can be tested, which it could not be while it
// sat behind a control connection a test has no way to open. The query engine's
// scrapeTargets was split out of its scrape for the same reason.
func (p PostgresCollector) collectDatabases(ch chan<- prometheus.Metric, pending []dbTarget, local map[string]Collector, server, selection string) (failed, deferred int) {
	var deferredCount, failedCount atomic.Int32
	fanout.Run(len(pending), p.dbWorkers, func(i int) {
		switch p.collectDatabase(ch, pending[i], local, server, selection) {
		case outcomeDeferred:
			deferredCount.Add(1)
		case outcomeFailed:
			failedCount.Add(1)
		}
	})
	return int(failedCount.Load()), int(deferredCount.Load())
}

// unitOutcome is what one collector's turn came to on one database.
//
// Three outcomes rather than two, because a collector that never started and a
// collector that ran and failed call for different responses, and reporting
// them as the same thing wakes an operator alerting on failures whenever the
// exporter is busy.
type unitOutcome int

const (
	// unitRan: the collector ran and reported no error.
	unitRan unitOutcome = iota
	// unitFailed: the collector ran and did not finish, which includes a query
	// cancelled while it was in flight.
	unitFailed
	// unitDeferred: the collector never started, because the scrape had ended
	// before its turn came.
	unitDeferred
)

// localOutcome is what a database's database-local collectors came to together.
type localOutcome struct {
	ran      int
	failed   int
	deferred int
	// reason names why the deferred collectors did not start, and is empty
	// when none were deferred. One context ends one way and stays ended that
	// way, so every collector deferred on one database shares this.
	reason string
}

// runLocalCollectors runs every selected database-local collector against one
// open connection.
//
// The number of collectors is the number registered and selected, which
// configuration fixes, so this is not where a database count turns into a
// goroutine count. That is the fan-out across databases, which is bounded by
// the connection limit.
func runLocalCollectors(ctx context.Context, local map[string]Collector, inst *instance, ch chan<- prometheus.Metric, logger log.Logger, datname string, lastSuccess *lastsuccess.Store) localOutcome {
	var ran, failed, deferred atomic.Int32
	var mu sync.Mutex
	reason := ""

	wg := sync.WaitGroup{}
	wg.Add(len(local))
	for name, c := range local {
		go func(name string, c Collector) {
			defer wg.Done()
			outcome, why := execute(ctx, name, c, inst, ch, logger, datname, lastSuccess)
			switch outcome {
			case unitRan:
				ran.Add(1)
			case unitFailed:
				failed.Add(1)
			default:
				deferred.Add(1)
				mu.Lock()
				reason = why
				mu.Unlock()
			}
		}(name, c)
	}
	wg.Wait()

	return localOutcome{
		ran:      int(ran.Load()),
		failed:   int(failed.Load()),
		deferred: int(deferred.Load()),
		reason:   reason,
	}
}

// databaseOutcome is what one database's database-local work came to. Every
// selected database is exactly one of these, which is what makes the counts
// add up to the selected set.
type databaseOutcome int

const (
	// outcomeScraped: every selected database-local collector ran without
	// error on this database.
	outcomeScraped databaseOutcome = iota
	// outcomeFailed: work started and did not finish, because the connection
	// failed or a collector did.
	outcomeFailed
	// outcomeDeferred: work never started, so nothing is known to be wrong
	// with this database.
	outcomeDeferred
)

// applyCensus hands the server's own catalogue to the store that keeps
// per-database state, which is what authorises forgetting a database.
//
// This engine applies its own. The query engine applies one too, but only a
// request that runs application queries reaches that code, and a deployment
// collecting nothing but built-in metrics never does. State that built-in
// monitoring creates has to be maintained by built-in monitoring.
//
// Only a complete, successful, unfiltered read gets this far. A read that
// failed leaves no inventory, and a filtered one is missing a database that
// lost CONNECT, which is absent for a reason that has nothing to do with
// existing.
func (p PostgresCollector) applyCensus(dsn string, sel dbSelection) {
	if !sel.catalogRead || sel.inventory == nil {
		return
	}
	seed, _ := targetid.ID(dsn)
	databases := make(map[string]struct{}, len(sel.inventory))
	for _, name := range sel.inventory {
		databases[name] = struct{}{}
	}
	_, removed := p.lastSuccess.Apply(lastsuccess.Census{
		Target:    seed,
		Taken:     sel.taken,
		Databases: databases,
	})
	if removed > 0 {
		level.Info(p.logger).Log("msg", "Forgot databases the server no longer has",
			"target", seed, "records", removed)
	}
}

// collectDatabase opens one database and runs the database-local collectors
// against it.
//
// A database that cannot be opened reports database_up 0 and nothing else. The
// alternative is emitting no series for it at all, which reads the same as a
// database that was never selected.
func (p PostgresCollector) collectDatabase(ch chan<- prometheus.Metric, t dbTarget, local map[string]Collector, server, selection string) databaseOutcome {
	target, _ := targetid.ID(t.dsn)
	fail := func(reason string) {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, t.datname)
		ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1, server, t.datname, reason)
		p.publishRetained(ch, local, target, t.datname)
		p.publishDatabaseFreshness(ch, target, t.datname, selection)
	}
	permit, decision := admission.Admit(p.ctx, p.connSema)
	if decision != admission.Admitted {
		// Deferred, not failed. Nothing was connected to and nothing was
		// tried, so this says the exporter ran out of room rather than that
		// something is wrong with this database, and an alert on the error
		// series must not fire for it. database_up is 0 because it was not
		// reached, except for the configured connection database, whose
		// connection this scrape already made for the cluster-wide work. For
		// that one the observation stands.
		//
		// The reason says which of the three was observed: no permit
		// available to this work, the scrape's time gone, or the scrape
		// cancelled. It describes the admission outcome and prescribes
		// nothing, because the deadline comes from the timeout header, the
		// fallback, the caps and the parent context together.
		reason := deferralReason(decision)
		level.Warn(p.logger).Log("msg", "Deferred a database", "datname", t.datname, "reason", reason)
		up := 0.0
		if t.baselineConnected {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, up, server, t.datname)
		ch <- prometheus.MustNewConstMetric(collectorDeferredDatabaseDesc, prometheus.GaugeValue, 1,
			server, t.datname, reason)
		p.publishRetained(ch, local, target, t.datname)
		p.publishDatabaseFreshness(ch, target, t.datname, selection)
		return outcomeDeferred
	}
	defer permit.Release()

	// Held for as long as this database is being collected. A catalogue read
	// that started before this work can finish during it, and its list is then
	// older than whatever this is in the middle of doing. Both engines share
	// the store, so both have to protect their work the same way.
	defer p.lastSuccess.Hold(target)()

	dbInst, err := newInstance(t.dsn)
	if err == nil {
		err = dbInst.setup(p.ctx)
	}
	if err != nil {
		level.Error(p.logger).Log("msg", "Error opening connection to database", "datname", t.datname, "err", err)
		fail(dbselect.ReasonConnect)
		return outcomeFailed
	}
	defer dbInst.Close()
	ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 1, server, t.datname)

	// Counted, not assumed. Completion is every collector this request
	// selected for the database, so a collector that fails, and a collector
	// that never starts, both leave the database uncovered rather than letting
	// the ones that did run speak for it.
	//
	// Stamped from before the work rather than after it, so the database-level
	// time is never later than the collector times it summarises.
	begin := time.Now()
	outcome := runLocalCollectors(p.ctx, local, dbInst, ch, p.logger, t.datname, p.lastSuccess)
	if outcome.ran != len(local) {
		reportIncompleteDatabase(ch, server, t.datname, outcome)
		p.publishDatabaseFreshness(ch, target, t.datname, selection)
		return outcomeFailed
	}
	p.lastSuccess.Record(databaseKey(target, t.datname, selection), begin)
	p.publishDatabaseFreshness(ch, target, t.datname, selection)
	return outcomeScraped
}

// reportIncompleteDatabase names a database whose connection worked and whose
// database-local collection did not finish, and says what stopped it.
//
// Without it the aggregate count would say a database failed and no series
// would say which one, which is the shortfall averaged across a set that the
// per-database series exist to prevent.
//
// Both causes when both happened. A real collector failure replaced by the
// budget reason loses the thing that is actually wrong with this database, and
// a budget cause reported as a collector failure says something is wrong when
// nothing was tried. So this can publish two series for one database, and the
// database is still one incomplete attempt in the counts.
func reportIncompleteDatabase(ch chan<- prometheus.Metric, server, datname string, o localOutcome) {
	if o.failed > 0 {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1,
			server, datname, dbselect.ReasonScrape)
	}
	if o.deferred > 0 {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1,
			server, datname, o.reason)
	}
}

// retainedForControl publishes what the control connection's collectors last
// did, for a scrape that never opened it.
//
// The cluster-wide collectors describe the server and carry no database, and
// the database-local ones would have run on the control database, so the two
// are published under the names they were recorded under.
func (p PostgresCollector) retainedForControl(ch chan<- prometheus.Metric, dsn, controlName string, cluster, local map[string]Collector) {
	target, _ := targetid.ID(dsn)

	// Cluster-wide collectors describe the server. They are selected
	// independently of the database lists and recorded with no database.
	p.publishRetained(ch, cluster, target, "")

	for name := range local {
		database := controlName
		if database == "" {
			// The connection string does not name it and the connection is
			// not open, so the database this collector last succeeded on is
			// the only name there is.
			if recorded, _, ok := p.lastSuccess.Latest(lastsuccess.EngineCollector, lastsuccess.OneCollector, target, name); ok {
				database = recorded
			}
		}
		// Selection as it stands now, not as it stood when the success was
		// recorded. A database the configuration no longer selects is not part
		// of this scrape's answer, and its state is kept for a request that
		// selects it again.
		if database == "" || !p.controlSelected(database) {
			continue
		}
		publishRetainedOne(ch, p.lastSuccess, target, database, name)
	}

	// The database as a whole, resolved the same way and gated the same way,
	// and only for the selection this request made. A request that selected no
	// database-local collector asked nothing about database-local freshness,
	// and one that selected a different set is not answered by another set's
	// record.
	selection := selectionOf(local)
	if selection == "" {
		return
	}
	database := controlName
	if database == "" {
		if recorded, _, ok := p.lastSuccess.Latest(lastsuccess.EngineCollector, lastsuccess.WholeSelection, target, selection); ok {
			database = recorded
		}
	}
	if database != "" && p.controlSelected(database) {
		p.publishDatabaseFreshness(ch, target, database, selection)
	}
}

// databaseKey is what this engine remembers a complete database collection
// under: the connection, the database, and the selection that completed.
//
// The selection is part of the key because a request that asks for some
// collectors has not shown the rest would have worked. One record per database
// would let a narrow request answer for a wider one, and a rule written
// against the whole enabled set leaves an installation that never selects it
// with no time at all.
func databaseKey(target, datname, selection string) lastsuccess.Key {
	return lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		About:     lastsuccess.WholeSelection,
		Target:    target,
		Database:  datname,
		Collector: selection,
	}
}

// publishDatabaseFreshness publishes when this database was last covered
// completely, if this process has ever covered it.
//
// Published on the paths that collected nothing as well, because a database
// that is failing now is exactly the one whose last complete collection an
// operator wants the time of, and a series that disappears then disappears
// when it matters most.
func (p PostgresCollector) publishDatabaseFreshness(ch chan<- prometheus.Metric, target, datname, selection string) {
	if datname == "" || selection == "" {
		return
	}
	if when, ok := p.lastSuccess.At(databaseKey(target, datname, selection)); ok {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseLastSuccessDesc, prometheus.GaugeValue,
			float64(when.UnixNano())/1e9, datname, target)
	}
}

// publishRetained publishes what these collectors last did on this database,
// for a path that never reached them.
//
// A connection that fails, or work with no permit, runs no collector, and a
// series that disappears then disappears exactly when an operator wants to
// know how long this has been going on. Nothing is published for a collector
// that has never succeeded here, and nothing is recorded: this says what was
// already known.
func (p PostgresCollector) publishRetained(ch chan<- prometheus.Metric, collectors map[string]Collector, target, datname string) {
	publishRetained(ch, p.lastSuccess, collectors, target, datname)
}

// publishRetained publishes what each collector last did on one connection.
//
// A free function because the probe endpoint needs the same thing on its own
// early returns and is not a PostgresCollector. A path that runs no collector
// has to say what was already known, or the freshness series goes missing at
// the moment it is being read to find out how long something has been broken.
func publishRetained(ch chan<- prometheus.Metric, store *lastsuccess.Store, collectors map[string]Collector, target, datname string) {
	for name := range collectors {
		publishRetainedOne(ch, store, target, datname, name)
	}
}

// publishRetainedOne publishes what one collector last did on one database.
func publishRetainedOne(ch chan<- prometheus.Metric, store *lastsuccess.Store, target, datname, name string) {
	key := lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: name,
	}
	if when, ok := store.At(key); ok {
		ch <- prometheus.MustNewConstMetric(collectorLastSuccessDesc, prometheus.GaugeValue,
			float64(when.UnixNano())/1e9, name, datname, target)
	}
}

// byScope splits the enabled collectors by where they run.
//
// cluster runs once on the configured connection and reports no database.
// local runs once per selected database, including the configured one.
func (p PostgresCollector) byScope() (cluster, local map[string]Collector) {
	cluster = make(map[string]Collector, len(p.Collectors))
	local = make(map[string]Collector)
	for name, c := range p.Collectors {
		if collectorScopes[name] == scopeDatabase {
			local[name] = c
			continue
		}
		cluster[name] = c
	}
	return cluster, local
}

// Profile is the canonical set of database-local collectors this request
// selected, which is the name its retained per-database state is kept under.
//
// Exported so the handler can ask for the slot that state needs before
// serving the request, which is before any SQL. Empty when nothing
// database-local was selected, and then there is no per-database state and
// nothing to admit.
func (p PostgresCollector) Profile() string {
	_, local := p.byScope()
	return selectionOf(local)
}

// selectionOf is the canonical form of the database-local collectors a request
// selected, which is the context its database-level freshness belongs to.
//
// Empty when nothing database-local was selected, which is the one case with no
// context to record against: such a request asked about no database.
func selectionOf(local map[string]Collector) string {
	if len(local) == 0 {
		return ""
	}
	names := make([]string, 0, len(local))
	for name := range local {
		names = append(names, name)
	}
	return lastsuccess.Selection(names)
}

// deferralReason is the bounded label for work that was not admitted.
//
// Called only where the decision is not admission.Admitted. Admitted work is not
// deferred, and giving it a reason anyway would publish a database as deferred
// while it was being collected.
func deferralReason(d admission.Decision) string {
	switch d {
	case admission.Cancelled:
		return dbselect.ReasonCancelled
	case admission.Deadline:
		return dbselect.ReasonDeadline
	default:
		return dbselect.ReasonNoPermit
	}
}

// localSelected counts the databases this request meant to run database-local
// collectors against.
//
// One function, read by what reports the intention and by what reports the
// outcome. Two counts of the same thing can disagree, and the pair of them
// disagreeing is indistinguishable from the exporter losing a database.
func localSelected(sel dbSelection, databaseLocal bool) int {
	if !databaseLocal {
		return 0
	}
	selected := 0
	for _, t := range sel.targets {
		if !t.skipDatabaseLocal {
			selected++
		}
	}
	return selected
}

// reportOutcome publishes what the database-local work came to.
//
// Only failed and deferred are passed in. attempted and scraped are arithmetic
// on them and on the selected count, so the invariants hold by construction
// rather than by two call sites agreeing.
func (p PostgresCollector) reportOutcome(ch chan<- prometheus.Metric, server string, selected, failed, deferred int) {
	attempted := selected - deferred
	gauge := func(d *prometheus.Desc, v int) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, float64(v), server)
	}
	gauge(collectorAttemptedDesc, attempted)
	gauge(collectorScrapedDesc, attempted-failed)
	gauge(collectorFailedDesc, failed)
	gauge(collectorDeferredDesc, deferred)
}

// reportSelection publishes what this engine meant to cover, before it tries.
//
// databaseLocal says whether any database-scoped collector is enabled for this
// request. None means no database was meant to be covered, whatever the
// catalog holds, which is what collect[]=postgres.cluster asks for.
func (p PostgresCollector) reportSelection(ch chan<- prometheus.Metric, server string, sel dbSelection, databaseLocal bool) {
	selected := localSelected(sel, databaseLocal)
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, server)
	}
	// Known, unknown and not asked for are three different things.
	//
	// discovered is published only when the catalog was actually read. A
	// number here when it was not would be an invented total, and the number
	// it would invent is 1, which is exactly what a one-database server looks
	// like. discovery_success says the set is unknown instead.
	//
	// Neither is published when no database-local collector was selected,
	// because then the set was never asked for. A zero there would invent a
	// server with no databases, and a one would claim a catalogue nobody read.
	if databaseLocal {
		if sel.catalogRead {
			gauge(collectorDiscoveredDesc, float64(sel.discovered))
		}
		gauge(collectorDiscoverySuccessDesc, boolToFloat(sel.ok))
	}
	gauge(collectorSelectedDesc, float64(selected))
	gauge(collectorExcludedDesc, float64(len(sel.excluded)+sel.notIncluded))
	for _, name := range sel.excluded {
		ch <- prometheus.MustNewConstMetric(collectorExcludedDatabaseDesc, prometheus.GaugeValue, 1,
			server, name, dbselect.ReasonExcludeList)
	}
}

// reportControlFailure publishes the one thing that is known when the control
// connection never worked: this engine collected nothing, and why.
//
// No discovered total is published, because discovery did not run and a count
// here would be invented. Selected is 1 only when the rule would have kept the
// control database, so a scrape configured to collect from nothing does not
// report a shortfall it never intended.
func (p PostgresCollector) reportControlFailure(ch chan<- prometheus.Metric, server, datname, reason string, databaseLocal bool) {
	selected := 0
	if databaseLocal && p.controlSelected(datname) {
		selected = 1
	}
	ch <- prometheus.MustNewConstMetric(collectorSelectedDesc, prometheus.GaugeValue, float64(selected), server)
	// Only where the set of databases was wanted. A request that selected no
	// database-scoped collector never needed one, and a zero here says the
	// set has collapsed to the configured database, which is a claim about
	// something nobody asked about. Where database-local work was selected
	// the zero stands: its discovery could not happen, and that is exactly
	// what the series is for.
	if databaseLocal {
		ch <- prometheus.MustNewConstMetric(collectorDiscoverySuccessDesc, prometheus.GaugeValue, 0, server)
	}
	ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, datname)
	ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1, server, datname, reason)
	// Attempted and failed, not deferred: the connection was tried and did
	// not work. On a request that selected no database-local collector all
	// four are zero, because the connection that failed was not one this
	// engine was going to collect a database over.
	p.reportOutcome(ch, server, selected, selected, 0)
}

// reportControlDeferral publishes what is known when there was no connection
// permit for the control connection: nothing was tried.
//
// Separate from reportControlFailure because a deferral is not a failure. The
// connection was never opened, so nothing is known to be wrong with the
// server, and an alert on the error series must not fire for a busy exporter.
func (p PostgresCollector) reportControlDeferral(ch chan<- prometheus.Metric, server, datname string, databaseLocal bool, reason string) {
	selected := 0
	if databaseLocal && p.controlSelected(datname) {
		selected = 1
	}
	ch <- prometheus.MustNewConstMetric(collectorSelectedDesc, prometheus.GaugeValue, float64(selected), server)
	// Same rule as the failure path: the set of databases is reported on only
	// where it was wanted. A request that selected no database-scoped
	// collector asked for no set, and nothing about discovery is said for it.
	if databaseLocal {
		ch <- prometheus.MustNewConstMetric(collectorDiscoverySuccessDesc, prometheus.GaugeValue, 0, server)
	}
	ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, datname)
	// The per-connection diagnostic names the control database whether or not
	// database-local work was selected for it, because that connection is
	// what cluster-wide collection would have run on. The count beside it is
	// the local set, so on a cluster-only request the two describe different
	// things: one deferred connection, no deferred databases.
	ch <- prometheus.MustNewConstMetric(collectorDeferredDatabaseDesc, prometheus.GaugeValue, 1,
		server, datname, reason)
	p.reportOutcome(ch, server, selected, 0, selected)
}

// controlSelected reports whether the configured database would get
// database-local collection under the configured rule.
//
// With no name to test and a restriction configured, the answer is no: an
// include list that cannot be checked is not an include list that passed.
func (p PostgresCollector) controlSelected(datname string) bool {
	if datname == "" {
		return !p.includeDatabases.Given() && len(p.excludeDatabases.Names()) == 0
	}
	return len(dbselect.Apply([]string{datname}, p.excludeDatabases, p.includeDatabases).Selected) == 1
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// execute runs one collector and reports whether it worked. datname is the
// database it ran against, empty for a cluster-wide collector.
//
// The returned value is the same fact as the collector_success series it
// publishes, from the same variable, so a reader comparing the count of
// covered databases against the per-collector series cannot find them
// disagreeing. A collector that reports no data counts as not having worked,
// which is what that series has always said about it.
func execute(ctx context.Context, name string, c Collector, instance *instance, ch chan<- prometheus.Metric, logger log.Logger, datname string, lastSuccess *lastsuccess.Store) (unitOutcome, string) {
	target, _ := targetid.ID(instance.dsn)

	// When this collector last worked here, which the success below cannot
	// say: it reports this scrape, and the question is how long a failure has
	// been going on. Built before the work because a collector that never
	// starts publishes it too.
	key := lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: name,
	}
	publishUnitFreshness := func() {
		if when, ok := lastSuccess.At(key); ok {
			ch <- prometheus.MustNewConstMetric(collectorLastSuccessDesc, prometheus.GaugeValue,
				float64(when.UnixNano())/1e9, name, datname, target)
		}
	}

	// Read on the connection this database already holds, before any SQL. A
	// collector whose turn comes after the scrape has ended must not be
	// reported as a collector that failed: nothing ran, so nothing is known
	// about it, and an operator alerting on collector failures would be woken
	// by a busy exporter.
	//
	// No connection permit is asked for. The connection is open and already
	// holds one, and a second request per collector would let the bounded
	// worker pool exhaust the limiter with connections the pool is holding
	// itself. So the reasons available here are the two a context can give.
	if decision, ended := admission.Ended(ctx); ended {
		reason := deferralReason(decision)
		level.Warn(logger).Log("msg", "Deferred a collector", "name", name, "datname", datname, "reason", reason)
		ch <- prometheus.MustNewConstMetric(scrapeDeferredDesc, prometheus.GaugeValue, 1, name, datname, reason)
		publishUnitFreshness()
		return unitDeferred, reason
	}

	begin := time.Now()
	err := c.Update(ctx, instance, ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		if IsNoDataError(err) {
			level.Debug(logger).Log("msg", "collector returned no data", "name", name, "datname", datname, "duration_seconds", duration.Seconds(), "err", err)
		} else {
			level.Error(logger).Log("msg", "collector failed", "name", name, "datname", datname, "duration_seconds", duration.Seconds(), "err", err)
		}
		success = 0
	} else {
		level.Debug(logger).Log("msg", "collector succeeded", "name", name, "datname", datname, "duration_seconds", duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name, datname)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name, datname)

	if err == nil {
		lastSuccess.Record(key, begin)
	}
	publishUnitFreshness()
	if err != nil {
		// A query cancelled while it was in flight is a collector that ran and
		// did not finish, which is a different thing from one that never
		// started. Both can happen in one database, and they are reported
		// apart.
		return unitFailed, ""
	}
	return unitRan, ""
}

// collectorFlagAction generates a new action function for the given collector
// to track whether it has been explicitly enabled or disabled from the command line.
// A new action function is needed for each collector flag because the ParseContext
// does not contain information about which flag called the action.
// See: https://github.com/alecthomas/kingpin/issues/294
func collectorFlagAction(collector string) func(ctx *kingpin.ParseContext) error {
	return func(ctx *kingpin.ParseContext) error {
		forcedCollectors[collector] = true
		return nil
	}
}

// ErrNoData indicates the collector found no data to collect, but had no other error.
var ErrNoData = errors.New("collector returned no data")

func IsNoDataError(err error) bool {
	return err == ErrNoData
}
