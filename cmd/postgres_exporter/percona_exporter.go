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
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/percona/postgres_exporter/internal/dbselect"
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
func Handler(logger log.Logger, dsns []string, connSema *semaphore.Weighted, globalCollectors map[string]prometheus.Collector, limits budget.Limits, gate *semaphore.Weighted) http.Handler {
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

		var f Filters
		if len(filters) == 0 {
			f.EnableAllCollectors = true
		} else {
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
				}
			}
		}

		registry := makeRegistry(ctx, dsns, connSema, globalCollectors, f, limits)

		// Delegate http serving to Prometheus client library, which will call collector.Collect.
		h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
			ErrorLog:      stdlog.New(log.NewStdlibAdapter(logger), "handler", 0),
		})

		h.ServeHTTP(w, r)
	})
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
}

// makeRegistry creates a new prometheus registry with default and percona exporters.
func makeRegistry(ctx context.Context, dsns []string, connSema *semaphore.Weighted, globalCollectors map[string]prometheus.Collector, filters Filters, limits budget.Limits) *prometheus.Registry {
	registry := prometheus.NewRegistry()

	// Always, whatever was selected. A refused request serves no metrics of
	// its own, so the count has to reach a scraper through the requests that
	// were not refused, and the counter is package-level so it survives the
	// registry being rebuilt per request.
	registry.MustRegister(collectionRejected)

	// Every connection this scrape opens gets a bound on how long it may take
	// to establish, shortened by whatever is left of the collection budget.
	// Done once here, for both engines, because the per-database connection
	// strings are derived from these and keep their parameters.
	remaining := time.Duration(0)
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	dsns = withConnectTimeout(dsns, limits.ConnectTimeoutFor(remaining))

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
		WithConnectionsSemaphore(connSema),
		WithContext(ctx),
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
			[]string{},
			collector.WithContext(ctx),
			collector.WithConnectionsSemaphore(connSema),
			// This engine selects its own databases and its set is never
			// intersected with the legacy engine's. The two are fed the same
			// values here because these flags are the only database selection
			// configuration that exists today. Separate keys wait on the
			// naming decision in contract §4.0b; the mechanism does not.
			collector.WithDatabaseSelection(
				dbselect.FromFlag(excludedDatabases),
				dbselect.FromFlag(strings.Split(*includeDatabases, ",")),
			),
		)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to create PostgresCollector", "err", err.Error())
		} else {
			registry.MustRegister(pe)
		}
	}

	return registry
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

// withConnectTimeout bounds how long each connection string may spend being
// established.
//
// A string that cannot be parsed is passed through unchanged. Refusing it here
// would turn a connection error, which the scrape reports per target, into a
// scrape that produces nothing, and the parse failure will be reported by
// whichever engine tries to use it.
func withConnectTimeout(dsns []string, timeout time.Duration) []string {
	out := make([]string, len(dsns))
	for i, dsn := range dsns {
		bounded, err := config.DSNWithConnectTimeout(dsn, timeout)
		if err != nil {
			out[i] = dsn
			continue
		}
		out[i] = bounded
	}
	return out
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
