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
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/distribution"
	"github.com/percona/postgres_exporter/internal/pgclamp"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
)

// Server describes a connection to Postgres.
// Also it contains metrics map and query overrides.
type Server struct {
	db  *sql.DB
	dsn string
	// distribution is empty until something asks for it. Use
	// distributionOf rather than reading it.
	distribution string
	labels       prometheus.Labels
	master       bool
	// datname is the database this connection is on, used to decide where an
	// application query with its own databases list runs. Empty when it is
	// not known, which the caller resolves before running those queries.
	datname string
	// role is the login role this connection names, for the diagnostics that
	// have to tell two targets apart. Empty when the connection string names
	// none and libpq falls back to the operating system user.
	role string

	// Last version used to calculate metric map. If mismatch on scrape,
	// then maps are recalculated.
	lastMapVersion semver.Version
	// Currently active metric map
	metricMap map[string]MetricMapNamespace
	// Currently active query overrides
	queryOverrides map[string]string
	mappingMtx     sync.RWMutex
	// Currently cached metrics
}

// ServerOpt configures a server.
type ServerOpt func(*Server)

// ServerWithLabels configures a set of labels.
func ServerWithLabels(labels prometheus.Labels) ServerOpt {
	return func(s *Server) {
		for k, v := range labels {
			s.labels[k] = v
		}
		s.labels["collector"] = "exporter"
	}
}

// NewServer establishes a new connection using DSN.
func NewServer(dsn string, opts ...ServerOpt) (*Server, error) {
	fingerprint, err := parseFingerprint(dsn)
	if err != nil {
		return nil, err
	}

	db, err := pgclamp.OpenDSN(dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	level.Debug(logger).Log("msg", "Established new database connection", "fingerprint", fingerprint)

	role, _ := config.UserFromDSN(dsn)

	s := &Server{
		db:     db,
		role:   role,
		master: false,
		labels: prometheus.Labels{
			serverLabelName: fingerprint,
		},
	}
	// The distribution is detected on first use rather than here, because
	// detecting it is a query and NewServer has no budget to run one under.
	// It is cached per host and port, so the cost is one round trip per
	// server whichever scrape pays it.
	s.dsn = dsn

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// hasQueryMaps reports whether checkMapVersions has run to completion on this
// server.
//
// The distinction is nil against empty, and it matters. A fresh Server has a
// nil metricMap; checkMapVersions always assigns one, even when it assigns an
// empty one for a non-master server. So nil means the preparation step did not
// finish, and an empty map means it finished and there was nothing to prepare.
func (s *Server) hasQueryMaps() bool {
	s.mappingMtx.RLock()
	defer s.mappingMtx.RUnlock()
	return s.metricMap != nil
}

// Close disconnects from Postgres.
func (s *Server) Close() error {
	return s.db.Close()
}

// Ping checks connection availability and possibly invalidates the connection
// if it fails.
//
// ctx bounds the attempt. A ping without one waits as long as the driver
// feels like, which on a server that accepts a connection and then stops
// answering is forever, at a phase before any collector has started.
func (s *Server) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		if cerr := s.Close(); cerr != nil {
			level.Error(logger).Log("msg", "Error while closing non-pinging DB connection", "server", s, "err", cerr)
		}
		return err
	}
	return nil
}

// String returns server's fingerprint.
func (s *Server) String() string {
	return s.labels[serverLabelName]
}

// cacheIdentity returns this server's connection context, for keying what was
// collected over it and for saying which collection a diagnostic describes.
//
// The fingerprint cannot do either: it is host and port, which targets share.
// They differ by database, by role, and by anything else the connection string
// carries, including a search_path that changes which tables identical SQL
// reads.
func (s *Server) cacheIdentity() string {
	id, first := targetid.ID(s.dsn)
	if first {
		// Once per target per process, so an operator can read which
		// connection an identity stands for. The identity itself carries
		// nothing from the string, which is the point of it.
		level.Debug(logger).Log("msg", "Assigned a target identity", "target", id, "dsn", loggableDSN(s.dsn))
	}
	return id
}

// Scrape loads metrics.
//
// ctx carries the collection budget. Every query below has to run under it,
// or the budget bounds only the parts of a scrape that happen to consult it:
// a slow statement reached through a context-free Query runs to completion
// whatever the scrape was allowed.
func (s *Server) Scrape(ctx context.Context, ch chan<- prometheus.Metric, disableSettingsMetrics bool, cache *querycache.Cache, collector string) error {
	s.mappingMtx.RLock()
	defer s.mappingMtx.RUnlock()

	var err error

	if !disableSettingsMetrics && s.master {
		if err = querySettings(ctx, ch, s); err != nil {
			err = fmt.Errorf("error retrieving settings: %s", err)
		}
	}

	errMap := queryNamespaceMappings(ctx, ch, s, cache, collector)
	if len(errMap) > 0 {
		err = fmt.Errorf("queryNamespaceMappings returned %d errors", len(errMap))
		level.Error(logger).Log("msg", "NAMESPACE ERRORS FOUND")
		for namespace, err := range errMap {
			level.Error(logger).Log("namespace", namespace, "msg", err)
		}
	}

	return err
}

// Servers contains a collection of servers to Postgres.
type Servers struct {
	m       sync.Mutex
	servers map[string]*Server
	opts    []ServerOpt
}

// NewServers creates a collection of servers to Postgres.
func NewServers(opts ...ServerOpt) *Servers {
	return &Servers{
		servers: make(map[string]*Server),
		opts:    opts,
	}
}

// GetServer returns established connection from a collection.
func (s *Servers) GetServer(ctx context.Context, dsn string) (*Server, error) {
	s.m.Lock()
	defer s.m.Unlock()
	var err error
	var ok bool
	errCount := 0 // start at zero because we increment before doing work
	retries := 1
	var server *Server
	for {
		if errCount++; errCount > retries {
			return nil, err
		}
		server, ok = s.servers[dsn]
		if !ok {
			server, err = NewServer(dsn, s.opts...)
			if err != nil {
				if werr := waitBeforeRetry(ctx, errCount); werr != nil {
					return nil, err
				}
				continue
			}
			s.servers[dsn] = server
		}
		if err = server.Ping(ctx); err != nil {
			server.Close()
			delete(s.servers, dsn)
			time.Sleep(time.Duration(errCount) * time.Second)
			continue
		}
		break
	}
	return server, nil
}

// Close disconnects from all known servers.
func (s *Servers) Close() {
	s.m.Lock()
	defer s.m.Unlock()
	for _, server := range s.servers {
		if err := server.Close(); err != nil {
			level.Error(logger).Log("msg", "Failed to close connection", "server", server, "err", err)
		}
	}
}

// distributionOf reports which PostgreSQL distribution this server is,
// detecting it on first use under the caller's budget.
func (s *Server) distributionOf(ctx context.Context) string {
	if s.distribution == "" {
		s.distribution = distribution.Get(ctx, s.dsn, s.db)
	}
	return s.distribution
}

// waitBeforeRetry sleeps between connection attempts, and stops waiting when
// the scrape's budget runs out.
//
// A plain Sleep here kept a request alive past its own deadline doing nothing,
// and with several retries it outlasted the budget by more than the budget.
// The caller returns the error it already had rather than a context error,
// because what went wrong is the connection, not the waiting.
func waitBeforeRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
