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
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
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
		"1 for each database the collectors did not reach, labelled with why: connect for a connection that failed, not_attempted for work that never started. Covers the same set as database_up, which includes the configured connection database whether or not selection kept it. Reasons are a closed set, never an error message.",
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
	scopeDatabase
	// scopeDatabaseUnlabelled is a database-local collector whose own metrics
	// do not carry the database name.
	//
	// It reads per-database catalogs, so it describes only the database it
	// runs in, but nothing in its output says which. Running it in two
	// databases produces series that are identical in name and labels, which a
	// registry rejects as duplicates and which an operator could not tell
	// apart anyway. So it runs on the configured connection alone, which is
	// what every collector did before the fan-out existed.
	//
	// This is a property of the metrics, not of the SQL. Giving these
	// collectors a datname label would move them to scopeDatabase, and that is
	// a change to published series rather than a change to scheduling.
	scopeDatabaseUnlabelled
)

func (s collectorScope) String() string {
	switch s {
	case scopeDatabase:
		return "database"
	case scopeDatabaseUnlabelled:
		return "database (unlabelled, control connection only)"
	default:
		return "cluster"
	}
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
}

type Option func(*PostgresCollector) error

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

	if !p.acquire() {
		// Nothing was attempted, which is neither a success nor a connection
		// failure. Returning silently here published no series at all, so a
		// scrape that collected nothing was indistinguishable from one that
		// was never configured to collect.
		p.reportControlFailure(ch, inst.name, controlName, dbselect.ReasonNotAttempted)
		return
	}
	// Set up the database connection for the collector.
	err := inst.setup(p.ctx)
	if err != nil {
		level.Error(p.logger).Log("msg", "Error opening connection to database", "err", err)
		p.release()
		p.reportControlFailure(ch, inst.name, controlName, dbselect.ReasonConnect)
		return
	}

	cluster, local, controlOnly := p.byScope()

	sel, err := selectDatabases(p.ctx, inst, p.excludeDatabases, p.includeDatabases)
	if err != nil {
		level.Error(p.logger).Log("msg", "Error listing databases", "err", err)
	}
	p.reportSelection(ch, inst.name, sel)

	// The control connection is held for the cluster-wide work and released
	// before the fan-out, so the per-database connections are bounded by the
	// same semaphore rather than competing with a permit this call is sitting
	// on.
	wg := sync.WaitGroup{}
	wg.Add(len(cluster))
	for name, c := range cluster {
		go func(name string, c Collector) {
			defer wg.Done()
			execute(p.ctx, name, c, inst, ch, p.logger, "")
		}(name, c)
	}

	// The control database's own database-local collectors run on the
	// connection that is already open, so it is not opened twice.
	control := sel.controlTarget()
	if control != nil && !control.skipDatabaseLocal {
		for _, set := range []map[string]Collector{local, controlOnly} {
			wg.Add(len(set))
			for name, c := range set {
				go func(name string, c Collector) {
					defer wg.Done()
					execute(p.ctx, name, c, inst, ch, p.logger, control.datname)
				}(name, c)
			}
		}
	}
	wg.Wait()
	inst.Close()
	p.release()

	if control != nil {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 1, inst.name, control.datname)
	}
	if len(local) == 0 {
		return
	}

	fanout := sync.WaitGroup{}
	for _, t := range sel.targets {
		if t.control || t.skipDatabaseLocal {
			continue
		}
		fanout.Add(1)
		go func(t dbTarget) {
			defer fanout.Done()
			p.collectDatabase(ch, t, local, inst.name)
		}(t)
	}
	fanout.Wait()
}

// collectDatabase opens one database and runs the database-local collectors
// against it.
//
// A database that cannot be opened reports database_up 0 and nothing else. The
// alternative is emitting no series for it at all, which reads the same as a
// database that was never selected.
func (p PostgresCollector) collectDatabase(ch chan<- prometheus.Metric, t dbTarget, local map[string]Collector, server string) {
	fail := func(reason string) {
		ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, t.datname)
		ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1, server, t.datname, reason)
	}
	if !p.acquire() {
		fail(dbselect.ReasonNotAttempted)
		return
	}
	defer p.release()

	dbInst, err := newInstance(t.dsn)
	if err == nil {
		err = dbInst.setup(p.ctx)
	}
	if err != nil {
		level.Error(p.logger).Log("msg", "Error opening connection to database", "datname", t.datname, "err", err)
		fail(dbselect.ReasonConnect)
		return
	}
	defer dbInst.Close()
	ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 1, server, t.datname)

	wg := sync.WaitGroup{}
	wg.Add(len(local))
	for name, c := range local {
		go func(name string, c Collector) {
			defer wg.Done()
			execute(p.ctx, name, c, dbInst, ch, p.logger, t.datname)
		}(name, c)
	}
	wg.Wait()
}

// byScope splits the enabled collectors by where they can run.
//
// cluster runs once on the configured connection and reports no database.
// local runs once per selected database. controlOnly runs once on the
// configured connection and reports that database, because it describes one
// database but its metrics do not say which.
func (p PostgresCollector) byScope() (cluster, local, controlOnly map[string]Collector) {
	cluster = make(map[string]Collector, len(p.Collectors))
	local = make(map[string]Collector)
	controlOnly = make(map[string]Collector)
	for name, c := range p.Collectors {
		switch collectorScopes[name] {
		case scopeDatabase:
			local[name] = c
		case scopeDatabaseUnlabelled:
			controlOnly[name] = c
		default:
			cluster[name] = c
		}
	}
	return cluster, local, controlOnly
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

// reportSelection publishes what this engine meant to cover, before it tries.
func (p PostgresCollector) reportSelection(ch chan<- prometheus.Metric, server string, sel dbSelection) {
	selected := 0
	for _, t := range sel.targets {
		if !t.skipDatabaseLocal {
			selected++
		}
	}
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, server)
	}
	// discovered is published only when the catalog was actually read. A
	// number here when it was not would be an invented total, and the number
	// it would invent is 1, which is exactly what a one-database server looks
	// like. discovery_success says the set is unknown instead.
	if sel.catalogRead {
		gauge(collectorDiscoveredDesc, float64(sel.discovered))
	}
	gauge(collectorSelectedDesc, float64(selected))
	gauge(collectorExcludedDesc, float64(len(sel.excluded)+sel.notIncluded))
	gauge(collectorDiscoverySuccessDesc, boolToFloat(sel.ok))
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
func (p PostgresCollector) reportControlFailure(ch chan<- prometheus.Metric, server, datname, reason string) {
	selected := 0
	if p.controlSelected(datname) {
		selected = 1
	}
	ch <- prometheus.MustNewConstMetric(collectorSelectedDesc, prometheus.GaugeValue, float64(selected), server)
	ch <- prometheus.MustNewConstMetric(collectorDiscoverySuccessDesc, prometheus.GaugeValue, 0, server)
	ch <- prometheus.MustNewConstMetric(collectorDatabaseUpDesc, prometheus.GaugeValue, 0, server, datname)
	ch <- prometheus.MustNewConstMetric(collectorDatabaseErrorDesc, prometheus.GaugeValue, 1, server, datname, reason)
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
func execute(ctx context.Context, name string, c Collector, instance *instance, ch chan<- prometheus.Metric, logger log.Logger, datname string) {
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
