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
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
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
		"When every database-local collector last ran on this database without error in one scrape, as seconds since the epoch. The per-collector series beside this one answers a narrower question: this says the database as a whole was covered. Absent for a database this process has never covered completely, which is not the same as one it last covered at the epoch. Only a request that selected every database-local collector can set it, because a request that selected some has not shown the rest would have worked. The target label is this process's identity for the connection; it is not stable across restarts.",
		[]string{"datname", "target"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"postgres_exporter: Whether a collector succeeded. datname is the database a database-local collector ran against, and is empty for a cluster-wide collector.",
		[]string{"collector", "datname"},
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
		"1 for each database this scrape did not cover, labelled with why: connect for a connection that failed, scrape for a database-local collector that failed on a connection that worked. A database with the second keeps database_up 1, because the connection is not what went wrong. Work that never started is in database_deferred instead. Reasons are a closed set, never an error message.",
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
		"Databases this scrape started database-local work on and did not finish, because the connection failed or a collector did. Work that never started is counted in databases_deferred instead, which is a different thing: a failure says something is wrong with that database and a deferral says the exporter ran out of room.",
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
		"1 for each selected database this scrape did not start work on, labelled with why: no_connection_permit for work that waited for a connection and did not get one. Also published for the configured connection database when that connection itself was deferred, whether or not selection kept it for database-local work, because that connection is what cluster-wide collection runs on. So this can name one more database than databases_deferred counts, in the same way database_up can. Reasons are a closed set, never an error message. A deferral is not an error and is not counted in database_error.",
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
	ctx      context.Context

	// lastSuccess outlives one scrape: how long a collector has been failing
	// on a database cannot be answered from the scrape that is failing.
	lastSuccess *lastsuccess.Store
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

// WithConnectionsSemaphore sets the semaphore for limiting the number of connections to the database instance.
func WithConnectionsSemaphore(sem *semaphore.Weighted) Option {
	return func(c *PostgresCollector) error {
		c.connSema = sem
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

	if !p.acquire() {
		// Nothing was attempted, which is neither a success nor a connection
		// failure. Returning silently here published no series at all, so a
		// scrape that collected nothing was indistinguishable from one that
		// was never configured to collect.
		p.reportControlDeferral(ch, inst.name, controlName, databaseLocal)
		p.retainedForControl(ch, inst.dsn, controlName, cluster, local)
		return
	}
	// Set up the database connection for the collector.
	err := inst.setup(p.ctx)
	if err != nil {
		level.Error(p.logger).Log("msg", "Error opening connection to database", "err", err)
		p.release()
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
		sel, err = selectDatabases(p.ctx, inst, p.excludeDatabases, p.includeDatabases)
		if err != nil {
			level.Error(p.logger).Log("msg", "Error listing databases", "err", err)
		}
	}
	p.reportSelection(ch, inst.name, sel, databaseLocal)
	selected := localSelected(sel, databaseLocal)

	// The control connection is held for the cluster-wide work and released
	// before the fan-out, so the per-database connections are bounded by the
	// same semaphore rather than competing with a permit this call is sitting
	// on.
	wg := sync.WaitGroup{}
	wg.Add(len(cluster))
	for name, c := range cluster {
		go func(name string, c Collector) {
			defer wg.Done()
			execute(p.ctx, name, c, inst, ch, p.logger, "", p.lastSuccess)
		}(name, c)
	}

	// The control database's own database-local collectors run on the
	// connection that is already open, so it is not opened twice.
	//
	// Their successes are counted because completion is all of them, not the
	// ones that ran: a database whose collection was partly left out is not a
	// database this scrape covered.
	control := sel.controlTarget()
	controlLocal := control != nil && !control.skipDatabaseLocal
	// Whether this request asked for every database-local collector the engine
	// has enabled. A request that named some cannot set the database-level
	// freshness time, because it has not shown that the rest would have
	// worked. Compared by count: a request can only narrow the enabled set,
	// never add to it, so equal counts mean the same set.
	wholeSet := len(local) == len(DatabaseScopedCollectors())
	controlBegin := time.Now()
	var controlSucceeded atomic.Int32
	if controlLocal {
		wg.Add(len(local))
		for name, c := range local {
			go func(name string, c Collector) {
				defer wg.Done()
				if execute(p.ctx, name, c, inst, ch, p.logger, control.datname, p.lastSuccess) {
					controlSucceeded.Add(1)
				}
			}(name, c)
		}
	}
	wg.Wait()
	inst.Close()
	p.release()

	// The control connection worked, whatever was selected to run over it.
	// Without a catalog read there is no target to take the name from, so it
	// comes from the connection string, which is where reportControlFailure
	// takes it from on the paths where the connection did not work.
	upName := controlName
	if control != nil {
		upName = control.datname
	}
	ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 1, inst.name, upName)

	// A collector that failed on the control database is that database not
	// covered, exactly as it is for the databases the fan-out opens. The
	// connection worked, so database_up above stays 1 and the diagnostic
	// beside it says what did not.
	controlFailed := 0
	if controlLocal && int(controlSucceeded.Load()) != len(local) {
		controlFailed = 1
		reportIncompleteDatabase(ch, inst.name, upName)
	}
	if controlLocal {
		controlTarget, _ := targetid.ID(inst.dsn)
		if controlFailed == 0 && wholeSet {
			p.lastSuccess.Record(databaseKey(controlTarget, upName), controlBegin)
		}
		p.publishDatabaseFreshness(ch, controlTarget, upName)
	}

	if !databaseLocal {
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

	var deferred, failed atomic.Int32
	fanout := sync.WaitGroup{}
	for _, t := range sel.targets {
		if t.control || t.skipDatabaseLocal {
			continue
		}
		fanout.Add(1)
		go func(t dbTarget) {
			defer fanout.Done()
			switch p.collectDatabase(ch, t, local, inst.name, wholeSet) {
			case outcomeDeferred:
				deferred.Add(1)
			case outcomeFailed:
				failed.Add(1)
			}
		}(t)
	}
	fanout.Wait()

	// After the fan-out, because it is the only point where the numbers are
	// known. Published even when they are zero, so that a scrape which
	// deferred nothing says so rather than leaving a reader to wonder whether
	// the series is missing or the exporter kept up.
	p.reportOutcome(ch, inst.name, selected, int(failed.Load())+controlFailed, int(deferred.Load()))
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

// collectDatabase opens one database and runs the database-local collectors
// against it.
//
// A database that cannot be opened reports database_up 0 and nothing else. The
// alternative is emitting no series for it at all, which reads the same as a
// database that was never selected.
func (p PostgresCollector) collectDatabase(ch chan<- prometheus.Metric, t dbTarget, local map[string]Collector, server string, wholeSet bool) databaseOutcome {
	target, _ := targetid.ID(t.dsn)
	fail := func(reason string) {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, t.datname)
		ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1, server, t.datname, reason)
		p.publishRetained(ch, local, target, t.datname)
		p.publishDatabaseFreshness(ch, target, t.datname)
	}
	if !p.acquire() {
		// Deferred, not failed. Nothing was connected to and nothing was
		// tried, so this says the exporter ran out of room rather than that
		// something is wrong with this database, and an alert on the error
		// series must not fire for it. database_up is still 0: it was not
		// reached.
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, t.datname)
		ch <- prometheus.MustNewConstMetric(collectorDeferredDatabaseDesc, prometheus.GaugeValue, 1,
			server, t.datname, dbselect.ReasonNoPermit)
		p.publishRetained(ch, local, target, t.datname)
		p.publishDatabaseFreshness(ch, target, t.datname)
		return outcomeDeferred
	}
	defer p.release()

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
	// that a later scheduling decision never starts, both leave the database
	// uncovered rather than letting the ones that did run speak for it.
	var succeeded atomic.Int32
	// Stamped from before the work rather than after it, so the database-level
	// time is never later than the collector times it summarises.
	begin := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(len(local))
	for name, c := range local {
		go func(name string, c Collector) {
			defer wg.Done()
			if execute(p.ctx, name, c, dbInst, ch, p.logger, t.datname, p.lastSuccess) {
				succeeded.Add(1)
			}
		}(name, c)
	}
	wg.Wait()
	if int(succeeded.Load()) != len(local) {
		reportIncompleteDatabase(ch, server, t.datname)
		p.publishDatabaseFreshness(ch, target, t.datname)
		return outcomeFailed
	}
	if wholeSet {
		p.lastSuccess.Record(databaseKey(target, t.datname), begin)
	}
	p.publishDatabaseFreshness(ch, target, t.datname)
	return outcomeScraped
}

// reportIncompleteDatabase names a database whose connection worked and whose
// database-local collection did not finish.
//
// Without it the aggregate count would say a database failed and no series
// would say which one, which is the shortfall averaged across a set that the
// per-database series exist to prevent.
func reportIncompleteDatabase(ch chan<- prometheus.Metric, server, datname string) {
	ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1,
		server, datname, dbselect.ReasonScrape)
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
			if recorded, _, ok := p.lastSuccess.Latest(lastsuccess.EngineCollector, target, name); ok {
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
		p.publishRetainedOne(ch, target, database, name)
	}

	// The database as a whole, resolved the same way and gated the same way.
	// A request that selected no database-local collector asked nothing about
	// database-local freshness, so nothing is said about it.
	if len(local) == 0 {
		return
	}
	database := controlName
	if database == "" {
		if recorded, _, ok := p.lastSuccess.Latest(lastsuccess.EngineCollector, target, lastsuccess.WholeDatabase); ok {
			database = recorded
		}
	}
	if database != "" && p.controlSelected(database) {
		p.publishDatabaseFreshness(ch, target, database)
	}
}

// databaseKey is what this engine remembers a complete database collection
// under: the connection and the database, with no collector, because the
// record is about the database rather than about one collector in it.
func databaseKey(target, datname string) lastsuccess.Key {
	return lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: lastsuccess.WholeDatabase,
	}
}

// publishDatabaseFreshness publishes when this database was last covered
// completely, if this process has ever covered it.
//
// Published on the paths that collected nothing as well, because a database
// that is failing now is exactly the one whose last complete collection an
// operator wants the time of, and a series that disappears then disappears
// when it matters most.
func (p PostgresCollector) publishDatabaseFreshness(ch chan<- prometheus.Metric, target, datname string) {
	if datname == "" {
		return
	}
	if when, ok := p.lastSuccess.At(databaseKey(target, datname)); ok {
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
	for name := range collectors {
		p.publishRetainedOne(ch, target, datname, name)
	}
}

// publishRetainedOne publishes what one collector last did on one database.
func (p PostgresCollector) publishRetainedOne(ch chan<- prometheus.Metric, target, datname, name string) {
	key := lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: name,
	}
	if when, ok := p.lastSuccess.At(key); ok {
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

func (p PostgresCollector) acquire() bool {
	if p.connSema == nil {
		return true
	}
	if err := p.connSema.Acquire(p.ctx, 1); err != nil {
		level.Warn(p.logger).Log("msg", "Failed to acquire semaphore", "err", err)
		return false
	}
	return true
}

func (p PostgresCollector) release() {
	if p.connSema != nil {
		p.connSema.Release(1)
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
func (p PostgresCollector) reportControlDeferral(ch chan<- prometheus.Metric, server, datname string, databaseLocal bool) {
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
		server, datname, dbselect.ReasonNoPermit)
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
func execute(ctx context.Context, name string, c Collector, instance *instance, ch chan<- prometheus.Metric, logger log.Logger, datname string, lastSuccess *lastsuccess.Store) bool {
	target, _ := targetid.ID(instance.dsn)
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

	// When this collector last worked here, which the success above cannot
	// say: it reports this scrape, and the question is how long a failure has
	// been going on.
	key := lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  datname,
		Collector: name,
	}
	if err == nil {
		lastSuccess.Record(key, begin)
	}
	if when, ok := lastSuccess.At(key); ok {
		ch <- prometheus.MustNewConstMetric(collectorLastSuccessDesc, prometheus.GaugeValue,
			float64(when.UnixNano())/1e9, name, datname, target)
	}
	return success == 1
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
