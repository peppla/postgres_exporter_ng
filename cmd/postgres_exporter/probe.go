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
	"github.com/percona/postgres_exporter/internal/slots"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/semaphore"
)

func handleProbe(logger log.Logger, excludeDatabases []string, connSema *semaphore.Weighted, limits budget.Limits, gate *semaphore.Weighted, stores Stores) http.HandlerFunc {
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

		// A slot for this connection, taken before any SQL and after the
		// target has been validated above.
		//
		// This endpoint's target comes from the request, so nothing
		// configures how many distinct connections it will be asked about. It
		// keeps a schedule and a last-success history per connection, which is
		// what makes it a Prometheus target rather than a one-off check, and
		// which is also what grows without a limit here.
		//
		// Named by the connection's identity rather than by the target
		// string, so two spellings of one connection are one slot and a
		// connection string does not end up in a map key.
		id, _ := targetid.ID(dsn.GetConnectionString())
		held, refused, admitted := stores.Slots.Admit(slots.Key{Kind: slots.Context, Name: id})
		if !admitted {
			contextsRejected.Inc()
			level.Warn(logger).Log("msg", "Refused a probe connection", "target", loggableTarget(target))
			http.Error(w, "probe connection refused: every connection slot is held by one that is still being probed, "+refused.Name,
				http.StatusServiceUnavailable)
			return
		}
		defer held()
		defer stores.Slots.Sweep()

		// Redacted, because this label is attached to every line this request
		// logs and the target comes from the request.
		tl := log.With(logger, "target", loggableTarget(target))

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
			WithConnectionsSemaphore(connSema, limits.Connections),
			// This endpoint builds an exporter for each request, so anything
			// it needs to remember between requests has to come from the
			// process. Without the schedule its rotation pointer was nil,
			// which is no rotation at all, and discovery here can offer
			// several databases, so its selected set had the same database
			// offered the first attempt every time.
			WithRotation(stores.Rotation),
			// Without this the freshness series still appeared, because the
			// time of a success is published from the success itself. What was
			// lost was every earlier time: a request that could not connect
			// published no freshness at all, where it should report when each
			// database last worked. That is what the store is for.
			WithLastSuccess(stores.LastSuccess),
		}

		// Both engines on this endpoint open through the same clamping
		// connector, which bounds the attempt by this request's context.
		dsns := []string{dsn.GetConnectionString()}
		exporter := NewExporter(dsns, opts...)

		// defer func() {
		// 	exporter.servers.Close()
		// }()
		registry.MustRegister(exporter)

		// Run the probe
		pc, err := collector.NewProbeCollector(ctx, tl, excludeDatabases, registry, dsns[0], connSema, stores.LastSuccess)
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
