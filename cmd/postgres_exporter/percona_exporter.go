package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	stdlog "log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/blang/semver/v4"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/collector"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/rotation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/semaphore"
)

type MetricResolution string

const (
	LR MetricResolution = "lr"
	MR MetricResolution = "mr"
	HR MetricResolution = "hr"
)

var (
	collectCustomQueryLr          = kingpin.Flag("collect.custom_query.lr", "Enable custom queries with low resolution directory.").Default("false").Envar("PG_EXPORTER_EXTEND_QUERY_LR").Bool()
	collectCustomQueryMr          = kingpin.Flag("collect.custom_query.mr", "Enable custom queries with medium resolution directory.").Default("false").Envar("PG_EXPORTER_EXTEND_QUERY_MR").Bool()
	collectCustomQueryHr          = kingpin.Flag("collect.custom_query.hr", "Enable custom queries with high resolution directory.").Default("false").Envar("PG_EXPORTER_EXTEND_QUERY_HR").Bool()
	collectCustomQueryLrDirectory = kingpin.Flag("collect.custom_query.lr.directory", "Path to custom queries with low resolution directory.").Envar("PG_EXPORTER_EXTEND_QUERY_LR_PATH").String()
	collectCustomQueryMrDirectory = kingpin.Flag("collect.custom_query.mr.directory", "Path to custom queries with medium resolution directory.").Envar("PG_EXPORTER_EXTEND_QUERY_MR_PATH").String()
	collectCustomQueryHrDirectory = kingpin.Flag("collect.custom_query.hr.directory", "Path to custom queries with high resolution directory.").Envar("PG_EXPORTER_EXTEND_QUERY_HR_PATH").String()

	// maxConnectionsSet says whether the operator passed the flag at all,
	// which its value cannot: the default is a real number and someone
	// passing that number explicitly still said it.
	maxConnectionsSet bool
	maxConnections    = kingpin.Flag("max-connections", "Maximum number of connections to use (DEPRECATED: use limits.connections)").Default("5").Envar("PG_EXPORTER_MAX_CONNECTIONS").IsSetByUser(&maxConnectionsSet).Int64()
)

// Handler returns a http.Handler that serves metrics. Can be used instead of
// run for hooking up custom HTTP servers.
//
// limits is the budget one scrape may spend. The request gate is built once,
// here, rather than per request: it is a limit on how many collections run at
// the same time, so it has to be the same object across them.
// excludeSelection and includeSelection are built-in monitoring's database set,
// resolved at startup. They are passed in rather than read from the
// configuration here because resolving them can fail, and a scrape is the wrong
// place to discover that: it has no way to report a configuration error except
// by serving no metrics.
// Stores are the pieces of state that outlive a request.
//
// Grouped rather than passed one by one, because each of them answers a
// question a single scrape cannot: how long a database has been failing, and
// where the last scrape of this profile got to. The next one costs no further
// parameter here.
type Stores struct {
	// LastSuccess is when each database and collector last worked.
	LastSuccess *lastsuccess.Store
	// Rotation is which selected database is offered the first attempt.
	Rotation *rotation.Store
}

// NewStores builds the state that outlives a request.
func NewStores() Stores {
	return Stores{LastSuccess: lastsuccess.New(), Rotation: rotation.New()}
}

func Handler(logger log.Logger, dsns []string, connSema *semaphore.Weighted, globalCollectors map[string]prometheus.Collector, limits budget.Limits, gate *semaphore.Weighted, queryCache *querycache.Cache, stores Stores, excludeSelection, includeSelection dbselect.List) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Refused before any SQL runs, and not queued. A queue turns an
		// overloaded exporter into a slow one, and the scrapes waiting in it
		// are already late by the time they reach a database.
		release, ok := admit(w, gate, limits)
		if !ok {
			return
		}
		defer release()

		// What the scraper asked for, if it said. Prometheus sends this as a
		// decimal, so 0.25 is a quarter of a second and reading it as a whole
		// number threw away everything a scraper with a sub-second budget
		// asked for and used the ten-second fallback instead.
		//
		// A header that is present and unusable is refused rather than
		// ignored. Falling back silently gives a scraper a budget it did not
		// ask for, which is how a 250ms request ran for two seconds.
		asked, err := requestedTimeout(r.Header.Values("X-Prometheus-Scrape-Timeout-Seconds"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Whatever is left of a deadline already on the request can only
		// shorten the budget.
		var parentRemaining time.Duration
		if deadline, ok := r.Context().Deadline(); ok {
			parentRemaining = time.Until(deadline)
		}

		// Collection stops before the reserve, which is what pays for
		// gathering, encoding and writing the response. A scrape that
		// collected everything and could not send it is a failed scrape.
		ctx, cancel := context.WithTimeout(r.Context(), limits.CollectionBudget(asked, parentRemaining))
		defer cancel()

		boundResponseWrite(w, limits.RequestBudget(asked, parentRemaining))

		filters := r.URL.Query()["collect[]"]
		level.Debug(logger).Log("msg", "Collect query", "filters", filters)

		f, err := parseCollectFilters(filters)
		if err != nil {
			// A name that selects nothing is worth a 400. Served as a success
			// it becomes a target that scrapes cleanly and reports no metrics,
			// which reads as a healthy database with nothing to say.
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		registry, err := makeRegistry(ctx, dsns, connSema, globalCollectors, f, limits, queryCache,
			stores, excludeSelection, includeSelection)
		if err != nil {
			// Only a request that named collectors turns a build failure into
			// a client error, because then the names came from the request.
			// Every other failure keeps the old behaviour of logging it and
			// serving whatever could be built.
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Delegate http serving to Prometheus client library, which will call collector.Collect.
		h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
			ErrorLog:      stdlog.New(log.NewStdlibAdapter(logger), "handler", 0),
		})

		h.ServeHTTP(w, r)
	})
}

// parseCollectFilters turns the collect[] query parameters into a Filters.
//
// Four selections reach the namespaced collectors:
//
//	postgres                     all of them, as before
//	postgres.cluster             those whose answer describes the whole server
//	postgres.database            those that read one database's catalogs
//	postgres.collector.<name>    one collector
//
// A collector is named at the third level rather than the second. At the
// second level the name of the collector that reads pg_database, which is
// "database", is also the name of the database-scoped group, and no rule about
// which wins is worth having: one reading selects fourteen collectors and the
// other selects one, and a scrape configuration cannot say which it meant. The
// extra level costs a word and removes the ambiguity for every collector name
// that will ever be registered.
//
// An empty selection means everything, which is what a scrape passing no
// collect[] at all gets.
func parseCollectFilters(filters []string) (Filters, error) {
	var f Filters
	if len(filters) == 0 {
		f.EnableAllCollectors = true
		return f, nil
	}
	for _, filter := range filters {
		switch filter {
		case "standard.process":
			f.EnableProcessCollector = true
		case "standard.go":
			f.EnableGoCollector = true
		case "version":
			f.EnableVersionCollector = true
		case "exporter":
			f.EnableDefaultCollector = true
		case "custom_query.hr":
			f.EnableHRCollector = true
		case "custom_query.mr":
			f.EnableMRCollector = true
		case "custom_query.lr":
			f.EnableLRCollector = true
		case "postgres":
			f.EnablePostgresCollector = true
			f.AllPostgresCollectors = true
		case "postgres.cluster":
			f.addPostgresCollectors(collector.ClusterScopedCollectors())
		case "postgres.database":
			f.addPostgresCollectors(collector.DatabaseScopedCollectors())
		default:
			name, ok := strings.CutPrefix(filter, "postgres.collector.")
			if ok && name != "" {
				// Not checked here. NewPostgresCollector owns the list of
				// collectors and says whether a name is unknown or turned off,
				// and the handler turns that into the 400.
				f.addPostgresCollectors([]string{name})
				continue
			}
			if strings.HasPrefix(filter, "postgres.") {
				return Filters{}, fmt.Errorf("unknown collect[] value %q: expected postgres, postgres.cluster, postgres.database or postgres.collector.<name>", filter)
			}
			// Anything outside the postgres namespace is ignored, as it has
			// always been: a PMM agent may send group names this exporter does
			// not implement.
		}
	}
	return f, nil
}

// Filters is a struct to enable or disable collectors.
type Filters struct {
	EnableAllCollectors     bool
	EnableLRCollector       bool
	EnableMRCollector       bool
	EnableHRCollector       bool
	EnableDefaultCollector  bool
	EnableGoCollector       bool
	EnableVersionCollector  bool
	EnableProcessCollector  bool
	EnablePostgresCollector bool

	// AllPostgresCollectors records a bare collect[]=postgres, which asks for
	// every enabled collector and so overrides any narrower selection sent
	// alongside it.
	AllPostgresCollectors bool

	// PostgresCollectors narrows the namespaced framework to these collectors.
	// Empty means every enabled one.
	//
	// What this buys is one exporter serving several Prometheus jobs at
	// different intervals. The registry assembles a whole response before
	// writing any of it, so a scrape's peak memory follows how many series it
	// builds at once, and the per-database collectors are the expensive ones.
	// Putting them on their own slower job makes every response smaller and
	// tightens the sample timestamps on the rest, since Prometheus stamps a
	// whole scrape with one time however long collection took.
	PostgresCollectors []string
}

// addPostgresCollectors records a selection.
//
// A group that expands to nothing has to leave the framework switched off. An
// empty list means "every enabled collector" to NewPostgresCollector, so a
// group whose members are all disabled would otherwise select all of them,
// which is the opposite of what was asked.
func (f *Filters) addPostgresCollectors(names []string) {
	if len(names) == 0 {
		return
	}
	f.EnablePostgresCollector = true
	f.PostgresCollectors = append(f.PostgresCollectors, names...)
}

// postgresCollectors is the list handed to NewPostgresCollector: the narrowed
// selection, or nothing at all when the request asked for everything.
func (f Filters) postgresCollectors() []string {
	if f.EnableAllCollectors || f.AllPostgresCollectors {
		return nil
	}
	return f.PostgresCollectors
}

// makeRegistry creates a new prometheus registry with default and percona exporters.
//
// It returns an error only when the namespaced collectors could not be built
// for a request that named them. The caller turns that into a 400, because the
// names came from the request. Everything else is logged and the registry is
// returned with whatever could be built, which is the behaviour a scrape has
// always had.
func makeRegistry(ctx context.Context, dsns []string, connSema *semaphore.Weighted, globalCollectors map[string]prometheus.Collector, filters Filters, limits budget.Limits, queryCache *querycache.Cache, stores Stores, excludeSelection, includeSelection dbselect.List) (*prometheus.Registry, error) {
	registry := prometheus.NewRegistry()

	// Always, whatever was selected. A refused request serves no metrics of
	// its own, so the count has to reach a scraper through the requests that
	// were not refused, and the counter is package-level so it survives the
	// registry being rebuilt per request.
	registry.MustRegister(collectionRejected)

	excludedDatabases := strings.Split(*excludeDatabases, ",")
	logger.Log("msg", "Excluded databases", "databases", fmt.Sprintf("%v", excludedDatabases))

	queriesPath := map[MetricResolution]string{
		HR: *collectCustomQueryHrDirectory,
		MR: *collectCustomQueryMrDirectory,
		LR: *collectCustomQueryLrDirectory,
	}

	// Shared by both engines: how to reach the server, and how much of it to
	// use at once. Database selection is deliberately not here. The two
	// engines pick their own databases and the sets are never intersected,
	// so neither one's include or exclude configuration may be applied to the
	// other.
	opts := []ExporterOpt{
		AutoDiscoverDatabases(*autoDiscoverDatabases),
		WithConnectionsSemaphore(connSema, limits.Connections),
		WithContext(ctx),
		// The cache belongs to the process. An exporter is built per request,
		// so one it owned would be empty on arrival and discarded on the way
		// out, which is what cache_seconds was doing.
		WithQueryCache(queryCache),
		WithLastSuccess(stores.LastSuccess),
		WithRotation(stores.Rotation),
	}

	if filters.EnableAllCollectors || filters.EnableDefaultCollector {
		defaultExporter := NewExporter(dsns, append(
			opts,
			CollectorName("exporter"),
			WithConstantLabels(*constantLabelsList), // This option depends on collectors name, so keep it after CollectorName option
			DisableDefaultMetrics(*disableDefaultMetrics),
			DisableSettingsMetrics(*disableSettingsMetrics),
			// Built-in monitoring's own selection, and only its own.
			ExcludeDatabases(excludedDatabases),
			IncludeDatabases(*includeDatabases),
		)...)
		registry.MustRegister(defaultExporter)
	}

	if filters.EnableAllCollectors || filters.EnableHRCollector {
		hrExporter := NewExporter(dsns,
			append(opts,
				CollectorName("custom_query.hr"),
				WithConstantLabels(*constantLabelsList), // This option depends on collectors name, so keep it after CollectorName option
				// No database lists. An application query names its own
				// databases, and an omitted list means every database the
				// engine reaches rather than the set built-in monitoring
				// chose.
				WithUserQueriesEnabled(HR),
				WithEnabled(*collectCustomQueryHr),
				DisableDefaultMetrics(true),
				DisableSettingsMetrics(true),
				WithUserQueriesPath(queriesPath),
			)...)
		registry.MustRegister(hrExporter)

	}

	if filters.EnableAllCollectors || filters.EnableMRCollector {
		mrExporter := NewExporter(dsns,
			append(opts,
				CollectorName("custom_query.mr"),
				WithConstantLabels(*constantLabelsList), // This option depends on collectors name, so keep it after CollectorName option
				// No database lists. An application query names its own
				// databases, and an omitted list means every database the
				// engine reaches rather than the set built-in monitoring
				// chose.
				WithUserQueriesEnabled(MR),
				WithEnabled(*collectCustomQueryMr),
				DisableDefaultMetrics(true),
				DisableSettingsMetrics(true),
				WithUserQueriesPath(queriesPath),
			)...)
		registry.MustRegister(mrExporter)
	}

	if filters.EnableAllCollectors || filters.EnableLRCollector {
		lrExporter := NewExporter(dsns,
			append(opts,
				CollectorName("custom_query.lr"),
				WithConstantLabels(*constantLabelsList), // This option depends on collectors name, so keep it after CollectorName option
				// No database lists. An application query names its own
				// databases, and an omitted list means every database the
				// engine reaches rather than the set built-in monitoring
				// chose.
				WithUserQueriesEnabled(LR),
				WithEnabled(*collectCustomQueryLr),
				DisableDefaultMetrics(true),
				DisableSettingsMetrics(true),
				WithUserQueriesPath(queriesPath),
			)...)
		registry.MustRegister(lrExporter)
	}

	if filters.EnableAllCollectors || filters.EnableGoCollector {
		registry.MustRegister(globalCollectors["standard.go"])
	}

	if filters.EnableAllCollectors || filters.EnableProcessCollector {
		registry.MustRegister(globalCollectors["standard.process"])
	}

	if filters.EnableAllCollectors || filters.EnableVersionCollector {
		registry.MustRegister(globalCollectors["version"])
	}

	if filters.EnableAllCollectors || filters.EnablePostgresCollector {
		// This chunk moved here from main.go
		// TODO(@sysadmind): Remove this with multi-target support. We are removing multiple DSN support
		dsn := ""
		if len(dsns) > 0 {
			dsn = dsns[0]
		}

		pe, err := collector.NewPostgresCollector(
			logger,
			excludedDatabases,
			dsn,
			filters.postgresCollectors(),
			collector.WithContext(ctx),
			collector.WithConnectionsSemaphore(connSema, limits.Connections),
			collector.WithLastSuccess(stores.LastSuccess),
			collector.WithRotation(stores.Rotation),
			// Built-in monitoring's own set, resolved at startup. Never
			// intersected with the legacy engine's, which builds its own from
			// its own configuration.
			collector.WithDatabaseSelection(excludeSelection, includeSelection),
		)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to create PostgresCollector", "err", err.Error())
			if len(filters.postgresCollectors()) > 0 {
				return nil, err
			}
		} else {
			registry.MustRegister(pe)
		}
	}

	return registry, nil
}

func (e *Exporter) loadCustomQueries(ctx context.Context, res MetricResolution, version semver.Version, server *Server) {
	if e.userQueriesPath[res] != "" {
		fi, err := os.ReadDir(e.userQueriesPath[res])
		if err != nil {
			level.Error(logger).Log("msg", fmt.Sprintf("failed read dir %q for custom query", e.userQueriesPath[res]),
				"err", err)
			return
		}
		level.Debug(logger).Log("msg", fmt.Sprintf("reading dir %q for custom query", e.userQueriesPath[res]))

		for _, v := range fi {
			if v.IsDir() {
				continue
			}

			if filepath.Ext(v.Name()) == ".yml" || filepath.Ext(v.Name()) == ".yaml" {
				path := filepath.Join(e.userQueriesPath[res], v.Name())
				e.addCustomQueriesFromFile(ctx, path, version, server)
			}
		}
	}
}

func (e *Exporter) addCustomQueriesFromFile(ctx context.Context, path string, version semver.Version, server *Server) {
	// Calculate the hashsum of the useQueries
	userQueriesData, err := os.ReadFile(path)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to reload user queries:"+path, "err", err)
		e.userQueriesError.WithLabelValues(path, "").Set(1)
		return
	}

	hashsumStr := fmt.Sprintf("%x", sha256.Sum256(userQueriesData))

	if err := addQueries(ctx, userQueriesData, version, server); err != nil {
		level.Error(logger).Log("msg", "Failed to reload user queries:"+path, "err", err)
		e.userQueriesError.WithLabelValues(path, hashsumStr).Set(1)
		return
	}

	// Mark user queries as successfully loaded
	e.userQueriesError.WithLabelValues(path, hashsumStr).Set(0)
}

// requestedTimeout reads the scrape timeout header.
//
// It takes the header's values rather than its value, because an absent
// header and a header present with nothing in it are different requests and
// Header.Get returns the empty string for both. Only an absent header is
// entitled to the configured fallback: a scraper that sent the header meant
// something by it, and if that something cannot be read, saying so is the
// only way it finds out.
//
// A value has to be a finite number of seconds that is positive and still
// positive as a duration. Those are not the same test. Durations are counted
// in nanoseconds, so a tenth of a nanosecond is a positive number that rounds
// to zero, and zero is the value this function returns for an absent header,
// so it would have been handed the fallback and run for as long as the
// fallback allows.
func requestedTimeout(values []string) (time.Duration, error) {
	if len(values) == 0 {
		return 0, nil
	}
	header := values[0]
	if strings.TrimSpace(header) == "" {
		return 0, fmt.Errorf("X-Prometheus-Scrape-Timeout-Seconds was sent with no value")
	}
	seconds, err := strconv.ParseFloat(header, 64)
	if err != nil {
		return 0, fmt.Errorf("X-Prometheus-Scrape-Timeout-Seconds %q is not a number", header)
	}
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, fmt.Errorf("X-Prometheus-Scrape-Timeout-Seconds %q is not a finite number", header)
	}
	if seconds <= 0 {
		return 0, fmt.Errorf("X-Prometheus-Scrape-Timeout-Seconds %q is not positive", header)
	}
	asked := time.Duration(seconds * float64(time.Second))
	if asked <= 0 {
		return 0, fmt.Errorf("X-Prometheus-Scrape-Timeout-Seconds %q is shorter than the smallest duration this can express", header)
	}
	return asked, nil
}

// collectionRejected counts refused admissions. A 503 that nothing counts is
// an overload nobody can see afterwards, because the request that would have
// reported it is the one being refused.
var collectionRejected = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "pg",
	Subsystem: "exporter",
	Name:      "collection_requests_rejected_total",
	Help:      "Collection requests refused because the configured number were already in flight. Refused before any SQL runs, and never queued.",
})

// NewCollectionGate builds the gate that limits how many collections run at
// once.
//
// One gate for the process, passed to every endpoint that collects, the same
// way the connection semaphore is. The limit is on what this process asks of
// a database, so an endpoint with a gate of its own is a second helping of
// the same budget.
func NewCollectionGate(limits budget.Limits) *semaphore.Weighted {
	return semaphore.NewWeighted(limits.ConcurrentRequests)
}

// admit takes a slot for this collection, or refuses the request.
//
// The returned function gives the slot back. It is held for the whole of
// ServeHTTP, which includes writing the response, so a client that stops
// reading cannot hold a slot longer than the write deadline allows.
func admit(w http.ResponseWriter, gate *semaphore.Weighted, limits budget.Limits) (func(), bool) {
	if !gate.TryAcquire(1) {
		collectionRejected.Inc()
		http.Error(w,
			fmt.Sprintf("%d collections already in flight", limits.ConcurrentRequests),
			http.StatusServiceUnavailable)
		return func() {}, false
	}
	return func() { gate.Release(1) }, true
}

// boundResponseWrite stops a client that will not read from holding its
// admission slot indefinitely.
//
// Reserving time for the response bounds how long collection may take, not
// how long writing takes. A reader that accepts the connection and stops
// reading leaves the handler blocked in Write, holding its slot, with every
// other scrape refused. The deadline is the whole request budget, because a
// write that outlives the budget is already late.
func boundResponseWrite(w http.ResponseWriter, budget time.Duration) {
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(budget)); err != nil {
		// A ResponseWriter that cannot take a deadline, which httptest's
		// recorder cannot. Nothing to do about it here, and the server's own
		// WriteTimeout still applies in production.
		level.Debug(logger).Log("msg", "This response cannot carry a write deadline", "err", err)
	}
}
