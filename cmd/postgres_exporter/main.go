// Copyright 2021 The Prometheus Authors
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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/pgclamp"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/slots"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	vc "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/common/promslog"
	promslogflag "github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/prometheus/exporter-toolkit/web/kingpinflag"
	"golang.org/x/sync/semaphore"
)

var (
	c = config.Handler{
		Config: &config.Config{},
	}

	// Default is empty so you don't need a config file if using only DSN settings.
	configFile    = kingpin.Flag("config.file", "Postgres exporter configuration file.").Default("").String()
	webConfig     = kingpinflag.AddFlags(kingpin.CommandLine, ":9187")
	webConfigFile = kingpin.Flag(
		"web.config",
		"[EXPERIMENTAL] Path to config yaml file that can enable TLS or authentication.",
	).Default("").String() // added for compatibility reasons to not break it in PMM 2.
	metricsPath            = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").Envar("PG_EXPORTER_WEB_TELEMETRY_PATH").String()
	disableDefaultMetrics  = kingpin.Flag("disable-default-metrics", "Do not include default metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_DEFAULT_METRICS").Bool()
	disableSettingsMetrics = kingpin.Flag("disable-settings-metrics", "Do not include pg_settings metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_SETTINGS_METRICS").Bool()
	autoDiscoverDatabases  = kingpin.Flag("auto-discover-databases", "Whether to discover the databases on a server dynamically. (DEPRECATED)").Default("false").Envar("PG_EXPORTER_AUTO_DISCOVER_DATABASES").Bool()
	// queriesPath            = kingpin.Flag("extend.query-path", "Path to custom queries to run. (DEPRECATED)").Default("").Envar("PG_EXPORTER_EXTEND_QUERY_PATH").String()
	onlyDumpMaps       = kingpin.Flag("dumpmaps", "Do not run, simply dump the maps.").Bool()
	constantLabelsList = kingpin.Flag("constantLabels", "A list of label=value separated by comma(,). (DEPRECATED)").Default("").Envar("PG_EXPORTER_CONSTANT_LABELS").String()
	excludeDatabases   = kingpin.Flag("exclude-databases", "A list of databases to remove when autoDiscoverDatabases is enabled (DEPRECATED)").Default("").Envar("PG_EXPORTER_EXCLUDE_DATABASES").String()
	includeDatabases   = kingpin.Flag("include-databases", "A list of databases to include when autoDiscoverDatabases is enabled (DEPRECATED)").Default("").Envar("PG_EXPORTER_INCLUDE_DATABASES").String()
	metricPrefix       = kingpin.Flag("metric-prefix", "A metric prefix can be used to have non-default (not \"pg\") prefixes for each of the metrics").Default("pg").Envar("PG_EXPORTER_METRIC_PREFIX").String()
	logger             = log.NewNopLogger()
)

// Metric name parts.
const (
	// Namespace for all metrics.
	namespace = "pg"
	// Subsystems.
	exporter = "exporter"
	// The name of the exporter.
	exporterName = "postgres_exporter"
	// Metric label used for static string data thats handy to send to Prometheus
	// e.g. version
	staticLabelName = "static"
	// Metric label used for server identification.
	serverLabelName = "server"
)

// slogGoKitBridge adapts a *slog.Logger to the go-kit log.Logger interface so
// the rest of the codebase can keep using go-kit log/level patterns.
type slogGoKitBridge struct {
	l *slog.Logger
}

func (b *slogGoKitBridge) Log(keyvals ...interface{}) error {
	if len(keyvals) == 0 {
		return nil
	}
	lvl := slog.LevelInfo
	msg := ""
	var args []any
	for i := 0; i+1 < len(keyvals); i += 2 {
		k := fmt.Sprint(keyvals[i])
		v := keyvals[i+1]
		switch k {
		case "level":
			switch fmt.Sprint(v) {
			case "debug":
				lvl = slog.LevelDebug
			case "warn":
				lvl = slog.LevelWarn
			case "error":
				lvl = slog.LevelError
			}
		case "msg":
			msg = fmt.Sprint(v)
		default:
			args = append(args, k, v)
		}
	}
	b.l.Log(context.Background(), lvl, msg, args...)
	return nil
}

func main() {
	kingpin.Version(version.Print(exporterName))
	promlogConfig := &promslog.Config{}
	promslogflag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.HelpFlag.Short('h')
	webConfig.WebConfigFile = webConfigFile
	kingpin.Parse()
	logger = &slogGoKitBridge{promslog.New(promlogConfig)}

	if *onlyDumpMaps {
		dumpMaps()
		return
	}

	// Load the config file if specified, otherwise use default/DSN settings.
	//
	// A file that was named and could not be read is fatal. It used to be a
	// warning, after which the exporter ran on defaults. With limits in the
	// file, an operator who tightened a budget and mistyped a key would get
	// the default budget, and with monitoring.databases in it, one who asked
	// for two databases would get every database on the server, each with one
	// warning line to say so.
	if *configFile != "" {
		if err := c.ReloadConfig(*configFile, logger); err != nil {
			level.Error(logger).Log("msg", "Error loading config", "err", err)
			os.Exit(1)
		}
	} else {
		level.Debug(logger).Log("msg", "No config file provided, using default/DSN settings")
	}

	dsns, err := getDataSources()
	if err != nil {
		level.Error(logger).Log("msg", "Failed reading data sources", "err", err.Error())
		os.Exit(1)
	}

	excludedDatabases := strings.Split(*excludeDatabases, ",")
	logger.Log("msg", "Excluded databases", "databases", fmt.Sprintf("%v", excludedDatabases))

	if *autoDiscoverDatabases || *excludeDatabases != "" || *includeDatabases != "" {
		level.Warn(logger).Log("msg", "Scraping additional databases via auto discovery is DEPRECATED")
	}

	if *constantLabelsList != "" {
		level.Warn(logger).Log("msg", "Constant labels on all metrics is DEPRECATED")
	}

	versionCollector := vc.NewCollector(exporterName)
	psCollector := collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})
	goCollector := collectors.NewGoCollector()

	globalCollectors := map[string]prometheus.Collector{
		"standard.process": psCollector,
		"standard.go":      goCollector,
		"version":          versionCollector,
	}

	// The operational budget, from the configuration file and the one flag
	// that overlaps it. Validated here so a budget that cannot be satisfied
	// stops the exporter starting rather than producing scrapes that fail for
	// a reason nobody connects to the configuration.
	limits, err := resolveLimits(c.GetConfig(), maxConnectionsSet, *maxConnections)
	if err != nil {
		level.Error(logger).Log("msg", "Invalid limits", "err", err.Error())
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "Operational budget",
		"connections", limits.Connections,
		"concurrent_requests", limits.ConcurrentRequests,
		"request_timeout", limits.RequestTimeout,
		"max_request_timeout", limits.MaxRequestTimeout,
		"response_reserve_max", limits.ResponseReserveMax,
		"response_reserve_fraction", limits.ResponseReserveFraction)

	// Before anything opens a connection. Every connection this process
	// makes carries these ceilings, applied downwards only, so a stricter
	// setting on the role or the server is never raised.
	pgclamp.SetDefaults(limits.StatementTimeout, limits.LockTimeout, limits.ConnectTimeout)

	// Which databases built-in monitoring runs its database-local collectors
	// against, from the configuration file where it is written there and from
	// the deprecated flags otherwise. Resolved once, here, so that the one
	// thing that can go wrong with it stops the exporter starting.
	//
	// Resolved per scrape, a set configured in both places logged a line and
	// served a response with no metrics in it: HTTP 200 and a zero-length
	// body, which Prometheus records as a healthy target with nothing to say.
	// The README has always said this is refused at startup.
	excludeSelection, includeSelection, err := builtinDatabaseSelection(
		c.GetConfig(), excludedDatabases, strings.Split(*includeDatabases, ","))
	if err != nil {
		level.Error(logger).Log("msg", "Invalid database selection", "err", err.Error())
		os.Exit(1)
	}

	connSema := semaphore.NewWeighted(limits.Connections)
	// One admission gate for every endpoint that collects.
	collectionGate := NewCollectionGate(limits)
	// One cache for the process, so a result a query file asked to keep
	// survives the request that collected it.
	queryCache := querycache.New()
	// What no single scrape can answer: when each database was last collected
	// without error, and where the last scrape of a profile got to.
	state, err := resolveState(c.GetConfig())
	if err != nil {
		level.Error(logger).Log("msg", "Invalid state limits", "err", err.Error())
		os.Exit(1)
	}
	if state == (slots.Limits{}) {
		level.Info(logger).Log("msg", "No state limits configured, so no collection profile or probe connection is refused and none is released")
	} else {
		level.Info(logger).Log("msg", "State limits", "profiles", state.Profiles,
			"probe_connections", state.Contexts, "idle", state.Idle)
	}

	// The configured connections go in, so a probe of one of them losing its
	// slot does not take the configuration's own state with it.
	stores := NewStoresWithLimits(state, dsns)
	http.Handle(*metricsPath, Handler(logger, dsns, connSema, globalCollectors, limits, collectionGate, queryCache, stores,
		excludeSelection, includeSelection))

	if *metricsPath != "/" && *metricsPath != "" {
		landingConfig := web.LandingConfig{
			Name:        "Postgres Exporter",
			Description: "Prometheus PostgreSQL server Exporter",
			Version:     version.Info(),
			Links: []web.LandingLinks{
				{
					Address: *metricsPath,
					Text:    "Metrics",
				},
			},
		}
		landingPage, err := web.NewLandingPage(landingConfig)
		if err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
		}
		http.Handle("/", landingPage)
	}

	http.HandleFunc("/probe", handleProbe(logger, excludedDatabases, connSema, limits, collectionGate, stores))

	level.Info(logger).Log("msg", "Listening on address", "address", *webConfig.WebListenAddresses)
	// A backstop under the per-request write deadline. A handler sets its own
	// from the request's budget, and this covers anything that does not, plus
	// the time before a handler runs. Both exist because a client that stops
	// reading otherwise holds its connection, and its admission slot, for as
	// long as it likes.
	//
	// The read timeout is separate and deliberately generous: it bounds a
	// client that opens a connection and sends nothing.
	srv := &http.Server{
		WriteTimeout:      limits.MaxRequestTimeout,
		ReadHeaderTimeout: 30 * time.Second,
	}
	if err := web.ListenAndServe(srv, webConfig, promslog.New(&promslog.Config{})); err != nil {
		level.Error(logger).Log("msg", "Error running HTTP server", "err", err)
		os.Exit(1)
	}
}
