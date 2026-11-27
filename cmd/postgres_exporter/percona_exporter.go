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
	"github.com/percona/postgres_exporter/internal/slots"
	"github.com/percona/postgres_exporter/internal/targetid"
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
	// Slots admits the collection profiles and probe connections, which are
	// the two things no catalogue read can bound because a request mints
	// them.
	Slots *slots.Store
	// live is the check installed on LastSuccess, kept so that a test can
	// wrap the real one rather than build a copy of it and then prove
	// something about the copy.
	live func(target string, act func(reachable bool))
}

// NewStores builds the state that outlives a request, with no admission limit
// configured, so nothing is refused and nothing is released.
func NewStores() Stores {
	return NewStoresWithLimits(slots.Limits{}, nil)
}

// NewStoresWithLimits builds it with the admission limits set.
//
// Releasing a profile's slot drops what that profile alone was keeping, which
// is its whole-selection records and its schedules. Not the per-collector
// records: those are about one collector on one database and are read by every
// profile that includes that collector, so they belong to none of them and are
// bounded by the collectors and the connections instead. Not the ownership
// relations either, because another profile may still be collecting that
// connection.
// configured is the connections this process is configured with. Their state
// belongs to the configuration, which does not expire, so a probe of one of
// them losing its slot must not take it away.
func NewStoresWithLimits(l slots.Limits, configured []string) Stores {
	s := Stores{LastSuccess: lastsuccess.New(), Rotation: rotation.New()}

	permanent := make(map[string]struct{}, len(configured))
	for _, dsn := range configured {
		id, _ := targetid.ID(dsn)
		permanent[id] = struct{}{}
	}

	// Whether a connection is still reachable by something that is not a
	// relation. A configured connection always is, and a probe connection is
	// while it holds a slot, which it does for as long as it is being probed.
	// Neither of those is a relation, so without this a context retiring takes
	// the history of a connection it merely discovered.
	//
	// This one is for the retirement a hold deferred. It runs with no store
	// lock held, so it may reach the admission, and it does what it decides
	// while the admission is held still, because a connection admitted
	// between the answer and the act is one whose fresh state would then be
	// deleted on the strength of an answer taken before it arrived.
	s.live = func(id string, act func(bool)) {
		s.Slots.While(func(holds func(slots.Key) bool) {
			if _, forever := permanent[id]; forever {
				act(true)
				return
			}
			act(holds(slots.Key{Kind: slots.Context, Name: id}))
		})
	}
	s.LastSuccess.WatchLive(func(id string, act func(bool)) { s.live(id, act) })

	s.Slots = slots.New(l, func(k slots.Key, stillHeld map[slots.Key]struct{}) {
		switch k.Kind {
		case slots.Profile:
			engine, selection, ok := strings.Cut(k.Name, ":")
			if !ok || selection == "" {
				return
			}
			records := s.LastSuccess.ForgetProfile(engine, selection)
			schedules := s.Rotation.ForgetProfile(engine, selection)
			if records > 0 || schedules > 0 {
				level.Info(logger).Log("msg", "Released a collection profile that stopped being scraped",
					"engine", engine, "records", records, "schedules", schedules)
			}
		case slots.Context:
			if _, forever := permanent[k.Name]; forever {
				// A probe of a connection this process is configured with.
				// The slot goes and the state stays, because the
				// configuration is what keeps it and that did not expire.
				return
			}
			// The same question, answered from what the admission handed
			// over rather than by asking it, because this runs under its
			// lock.
			live := func(id string) bool {
				if _, forever := permanent[id]; forever {
					return true
				}
				_, held := stillHeld[slots.Key{Kind: slots.Context, Name: id}]
				return held
			}
			records := s.LastSuccess.ForgetContext(k.Name, live)
			schedules := s.Rotation.ForgetContext(k.Name)
			if records > 0 || schedules > 0 {
				level.Info(logger).Log("msg", "Released a probe connection that stopped being probed",
					"target", k.Name, "records", records, "schedules", schedules)
			}
		}
	})
	return s
}

// profileKey names the slot one engine's selection needs.
//
// The engine is in the name because the two choose their collectors
// independently and a selection in one is not a selection in the other.
func profileKey(engine, selection string) slots.Key {
	return slots.Key{Kind: slots.Profile, Name: engine + ":" + selection}
}

func Handler(logger log.Logger, dsns []string, connSema *semaphore.Weighted, globalCollectors map[string]prometheus.Collector, limits budget.Limits, gate *semaphore.Weighted, queryCache *querycache.Cache, stores Stores, excludeSelection, includeSelection dbselect.List) http.Handler {
	// What gathering this endpoint's metrics came to, which the response
	// otherwise cannot say. Gathering can reject metrics while the collection
	// behind them succeeded, so pg_up and last_scrape_error stay 1 and 0 and
	// something asked for is still missing from the body.
	//
	// It belongs to the endpoint rather than to a request, so the count
	// survives the per-request registries and is not rebuilt with them.
	//
	// It belongs to the process and not to a request, because each request
	// builds its own registry and a counter rebuilt with it would reset on
	// every scrape and never show the operator anything.
	//
	// Built once per endpoint, so every request's handler registers its error
	// counter into the same one. promhttp handles the repeat registration by
	// reusing the collector already there, which is what lets a handler be
	// constructed per request against a shared registry at all.
	handlerMetrics := prometheus.NewRegistry()
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

		registry, needs, err := makeRegistry(ctx, dsns, connSema, globalCollectors, f, limits, queryCache,
			stores, excludeSelection, includeSelection)
		if err != nil {
			// Only a request that named collectors turns a build failure into
			// a client error, because then the names came from the request.
			// Every other failure keeps the old behaviour of logging it and
			// serving whatever could be built.
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Every slot this request needs, or none of them, and before any SQL.
		//
		// A collection profile is whatever set of collectors a request asks
		// for, so without a limit the state kept behind them grows for as
		// long as the process runs. A profile that has a slot keeps it until
		// it stops being asked for; one arriving when every slot is held is
		// refused here rather than served without the state it needs.
		//
		// Refused rather than served, because a profile with no slot has no
		// schedule either, and a selection that does not fit the budget would
		// then be offered the same first database on every request. That is
		// the starvation the schedule exists to prevent, and serving it as a
		// success would be a path that quietly does not meet the fairness
		// contract.
		//
		// The selection was parsed and the registry built before this, so a
		// request that was going to be refused for naming an unknown collector
		// is still refused for that and does not reserve anything.
		slotsHeld, refused, admitted := stores.Slots.Admit(needs...)
		if !admitted {
			profilesRejected.Inc()
			level.Warn(logger).Log("msg", "Refused a collection profile", "profile", refused.Name)
			http.Error(w, "collection profile "+refused.Name+" refused: every profile slot is held by a profile that is still being scraped",
				http.StatusServiceUnavailable)
			return
		}
		defer slotsHeld()
		// After the holds are given back, so a profile this request was using
		// is not released while it is still using it.
		defer stores.Slots.Sweep()

		// Delegate http serving to Prometheus client library, which will call collector.Collect.
		//
		// Served from both: the registry this request built, and the
		// endpoint's own, which carries the count of gathers that went wrong.
		//
		// The hook increments after gathering, so a request's own failure is
		// too late for the body built from it and appears in a later response.
		h := promhttp.HandlerFor(prometheus.Gatherers{registry, handlerMetrics}, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
			ErrorLog:      stdlog.New(log.NewStdlibAdapter(logger), "handler", 0),
			Registry:      handlerMetrics,
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
func makeRegistry(ctx context.Context, dsns []string, connSema *semaphore.Weighted, globalCollectors map[string]prometheus.Collector, filters Filters, limits budget.Limits, queryCache *querycache.Cache, stores Stores, excludeSelection, includeSelection dbselect.List) (*prometheus.Registry, []slots.Key, error) {
	registry := prometheus.NewRegistry()
	// The profiles this request will keep state under. Collected while the
	// registry is built, which runs no SQL, so the request can be refused
	// before it touches a server.
	var needs []slots.Key

	// Always, whatever was selected. A refused request serves no metrics of
	// its own, so the count has to reach a scraper through the requests that
	// were not refused, and the counter is package-level so it survives the
	// registry being rebuilt per request.
	registry.MustRegister(collectionRejected, profilesRejected, contextsRejected)

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
				return nil, nil, err
			}
		} else {
			registry.MustRegister(pe)
			if profile := pe.Profile(); profile != "" {
				needs = append(needs, profileKey(lastsuccess.EngineCollector, profile))
			}
		}
	}

	return registry, needs, nil
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

// profilesRejected counts requests refused because every profile slot was
// held.
//
// Separate from the gate's counter, because the two say different things. The
// gate says this process is busy right now and the request should be retried.
// This says the number of distinct collection profiles being scraped is larger
// than this exporter is configured to keep state for, which retrying does not
// fix and which an operator has to decide about.
var profilesRejected = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "pg",
	Subsystem: "exporter",
	Name:      "collection_profiles_rejected_total",
	Help:      "Collection requests refused because every profile slot was held by a profile still being scraped. Refused before any SQL runs.",
})

// contextsRejected counts probe requests refused because every connection slot
// was held.
var contextsRejected = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "pg",
	Subsystem: "exporter",
	Name:      "probe_connections_rejected_total",
	Help:      "Probe requests refused because every connection slot was held by a connection still being probed. Refused before any SQL runs.",
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
