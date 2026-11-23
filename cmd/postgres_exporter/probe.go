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

package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/collector"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/semaphore"
)

func handleProbe(logger log.Logger, excludeDatabases []string, connSema *semaphore.Weighted, limits budget.Limits, gate *semaphore.Weighted) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// The probe collects, so it spends the same budget as /metrics. It
		// used to take neither the admission slot nor a deadline, so the
		// limits were process-wide in name and endpoint-wide in fact: enough
		// probes could run past every number the operator configured.
		release, ok := admit(w, gate, limits)
		if !ok {
			return
		}
		defer release()

		asked, err := requestedTimeout(r.Header.Values("X-Prometheus-Scrape-Timeout-Seconds"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var parentRemaining time.Duration
		if deadline, hasDeadline := r.Context().Deadline(); hasDeadline {
			parentRemaining = time.Until(deadline)
		}

		ctx, cancel := context.WithTimeout(r.Context(), limits.CollectionBudget(asked, parentRemaining))
		defer cancel()
		boundResponseWrite(w, limits.RequestBudget(asked, parentRemaining))

		conf := c.GetConfig()
		params := r.URL.Query()
		target := params.Get("target")
		if target == "" {
			http.Error(w, "target is required", http.StatusBadRequest)
			return
		}
		var authModule config.AuthModule
		authModuleName := params.Get("auth_module")
		if authModuleName == "" {
			level.Info(logger).Log("msg", "no auth_module specified, using default")
		} else {
			var ok bool
			authModule, ok = conf.AuthModules[authModuleName]
			if !ok {
				http.Error(w, fmt.Sprintf("auth_module %s not found", authModuleName), http.StatusBadRequest)
				return
			}
			if authModule.UserPass.Username == "" || authModule.UserPass.Password == "" {
				http.Error(w, fmt.Sprintf("auth_module %s has no username or password", authModuleName), http.StatusBadRequest)
				return
			}
		}

		dsn, err := authModule.ConfigureTarget(target)
		if err != nil {
			level.Error(logger).Log("msg", "failed to configure target", "err", err)
			http.Error(w, fmt.Sprintf("could not configure dsn for target: %v", err), http.StatusBadRequest)
			return
		}

		// TODO(@sysadmind): Timeout

		tl := log.With(logger, "target", target)

		registry := prometheus.NewRegistry()

		opts := []ExporterOpt{
			DisableDefaultMetrics(*disableDefaultMetrics),
			DisableSettingsMetrics(*disableSettingsMetrics),
			AutoDiscoverDatabases(*autoDiscoverDatabases),
			// WithUserQueriesPath(*queriesPath),
			WithConstantLabels(*constantLabelsList),
			ExcludeDatabases(excludeDatabases),
			IncludeDatabases(*includeDatabases),
			WithContext(ctx),
			WithConnectionsSemaphore(connSema),
		}

		remaining := time.Duration(0)
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			remaining = time.Until(deadline)
		}
		// Both engines on this endpoint get the bounded string. Giving it to
		// one and the original to the other leaves half the probe able to
		// hang on a server that accepts a connection and never answers.
		dsns := withConnectTimeout([]string{dsn.GetConnectionString()}, limits.ConnectTimeoutFor(remaining))
		exporter := NewExporter(dsns, opts...)

		// defer func() {
		// 	exporter.servers.Close()
		// }()
		registry.MustRegister(exporter)

		// Run the probe
		pc, err := collector.NewProbeCollector(ctx, tl, excludeDatabases, registry, dsns[0], connSema)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating probe collector", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Cleanup underlying connections to prevent connection leaks
		defer pc.Close()

		// TODO(@sysadmind): Remove the registry.MustRegister() call below and instead handle the collection here. That will allow
		// for the passing of context, handling of timeouts, and more control over the collection.
		// The current NewProbeCollector() implementation relies on the MustNewConstMetric() call to create the metrics which is not
		// ideal to use without the registry.MustRegister() call.
		_ = ctx

		registry.MustRegister(pc)

		// TODO check success, etc
		h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}
