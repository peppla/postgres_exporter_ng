// SPDX-License-Identifier: Apache-2.0
//
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
//
// New in postgres_exporter_ng, 2026. No copyright line: this is newly written
// code and its holder is not settled, so none is asserted.

// Server-side statement and lock ceilings that can only tighten.
//
// The first attempt at this put the settings in libpq's options parameter.
// That is applied at startup, after a role's own settings, so it overrode
// them: a role configured with statement_timeout 100ms got 5s. The contract
// says nothing in configuration may weaken a stricter server or role timeout,
// and a monitoring exporter raising a limit an administrator lowered is the
// wrong direction for a backstop. A value already written into the options
// string could also disable the limit outright, since 0 means no limit.
//
// So the ceiling is applied after connecting, by a statement that reads what
// the connection already has and takes the smaller of the two. It runs on
// every connection the pool opens rather than once per scrape, because a
// pooled connection opened later would otherwise have no ceiling at all.
package pgclamp

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/percona/postgres_exporter/config"
)

// clampQuery lowers one setting to at most the given number of milliseconds.
//
// pg_settings reports both of these in milliseconds, which is what makes the
// comparison possible without parsing "100ms" or "5s" by hand. A current
// value of 0 means no limit, so NULLIF turns it into NULL and LEAST, which
// ignores NULLs, yields the configured ceiling. Anything else yields the
// smaller of the two, so the setting only ever moves down.
// $2 is the exporter's ceiling and $3 is what the connection string asked
// for, with zero meaning no instruction in both cases. LEAST ignores NULLs,
// so whichever of the three is smallest and actually set wins, and a setting
// that is already stricter than all of them is left alone.
const clampQuery = `SELECT set_config($1,
		LEAST(NULLIF(setting::bigint, 0), NULLIF($2::bigint, 0), NULLIF($3::bigint, 0))::text, false)
	FROM pg_catalog.pg_settings WHERE name = $1`

// Connector wraps a driver connector and applies the ceilings to each new
// connection.
type Connector struct {
	inner     driver.Connector
	statement time.Duration
	lock      time.Duration
	// asked is what the connection string wanted, taken out of the string
	// before connecting so that a looser client value cannot replace a
	// stricter role setting before this package reads it.
	asked config.TimeoutOverrides
}

// Wrap returns a connector that clamps every connection it opens.
//
// A zero duration leaves that setting alone, which is what an operator asking
// for no ceiling gets.
func Wrap(inner driver.Connector, statement, lock time.Duration, asked config.TimeoutOverrides) *Connector {
	return &Connector{inner: inner, statement: statement, lock: lock, asked: asked}
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver { return c.inner.Driver() }

// Connect opens a connection and lowers its ceilings before handing it over.
//
// A failure to apply them closes the connection rather than returning one
// that is not bounded. A connection whose limits are unknown is worse than no
// connection: the scrape reports a failure for this database and moves on,
// where an unbounded one can hold work open past every budget.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}

	for _, setting := range []struct {
		name  string
		value time.Duration
		asked int64
	}{
		{"statement_timeout", c.statement, c.asked.StatementMS},
		{"lock_timeout", c.lock, c.asked.LockMS},
	} {
		if setting.value <= 0 && setting.asked <= 0 {
			continue
		}
		if err := clamp(ctx, conn, setting.name, setting.value, setting.asked); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("applying %s to a new connection: %w", setting.name, err)
		}
	}
	return conn, nil
}

// connect opens the connection and gives up when the caller's context does.
//
// The driver bounds its own startup with a whole number of seconds, so a
// quarter-second request still waited a full second for a server that accepts
// a socket and never speaks. Worse, the context it is handed does not reach
// its handshake at all. Running the attempt alongside the context and
// abandoning it is the only bound that matches what was asked for.
//
// An abandoned attempt is closed when it eventually finishes, rather than
// left to the garbage collector, because until then it is a connection on the
// server that this process is not counting.
func (c *Connector) connect(ctx context.Context) (driver.Conn, error) {
	type attempt struct {
		conn driver.Conn
		err  error
	}
	done := make(chan attempt, 1)
	go func() {
		conn, err := c.inner.Connect(ctx)
		done <- attempt{conn, err}
	}()

	select {
	case a := <-done:
		return a.conn, a.err
	case <-ctx.Done():
		go func() {
			a := <-done
			// Only a successful attempt has anything to close. A failed one
			// can carry a typed nil inside a non-nil interface, which is not
			// nil to compare against and panics when closed. That is the same
			// shape as the probe cleanup crash, and it bit here too.
			if a.err == nil && a.conn != nil {
				_ = a.conn.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// clamp runs the lowering statement on one connection.
func clamp(ctx context.Context, conn driver.Conn, name string, value time.Duration, askedMS int64) error {
	queryer, ok := conn.(driver.QueryerContext)
	if !ok {
		return fmt.Errorf("this driver cannot run a query on a new connection")
	}

	// Rounded up, because rounding a sub-millisecond ceiling down to zero
	// would read as no limit at all.
	ms := value.Milliseconds()
	if ms < 1 && value > 0 {
		ms = 1
	}

	rows, err := queryer.QueryContext(ctx, clampQuery, []driver.NamedValue{
		{Ordinal: 1, Value: name},
		{Ordinal: 2, Value: fmt.Sprint(ms)},
		{Ordinal: 3, Value: fmt.Sprint(askedMS)},
	})
	if err != nil {
		return err
	}
	return rows.Close()
}

// Open returns a database handle whose connections carry the ceilings.
//
// It takes the connector rather than building one, so the caller decides how
// the connection string is parsed and this package stays about the ceilings.
func Open(inner driver.Connector, statement, lock time.Duration, asked config.TimeoutOverrides) *sql.DB {
	return sql.OpenDB(Wrap(inner, statement, lock, asked))
}

// The ceilings every connection in this process gets.
//
// Package-level because the three places that open a connection are reached
// from constructors with no budget argument, and threading one through every
// caller of NewServer would be a wide change for a value that is the same
// everywhere: the limits are process-wide and settled before anything
// connects. SetDefaults is called once, from main, before the first scrape.
//
// It is a mutex rather than a plain variable because tests set it, and the
// race detector is right to object to a plain one.
var (
	defaultsMtx      sync.RWMutex
	defaultStatement time.Duration
	defaultLock      time.Duration
	defaultConnect   time.Duration
)

// SetDefaults records the ceilings for connections opened from now on.
func SetDefaults(statement, lock, connect time.Duration) {
	defaultsMtx.Lock()
	defer defaultsMtx.Unlock()
	defaultStatement, defaultLock, defaultConnect = statement, lock, connect
}

// Defaults reports the current ceilings.
func Defaults() (statement, lock, connect time.Duration) {
	defaultsMtx.RLock()
	defer defaultsMtx.RUnlock()
	return defaultStatement, defaultLock, defaultConnect
}

// OpenDSN opens a database handle whose connections carry the process
// ceilings.
//
// A handle, not a connection: nothing connects until something uses it, the
// same as sql.Open. The ceilings are read now rather than per connection, so
// a handle opened before SetDefaults keeps whatever was set then, which is
// why main sets them before serving.
func OpenDSN(dsn string) (*sql.DB, error) {
	cleaned, asked, err := config.StripTimeoutOverrides(dsn)
	if err != nil {
		return nil, err
	}

	// The typed configuration rather than the string, because connect_timeout
	// in a connection string is a whole number of seconds and a scrape can be
	// given less than one.
	cfg, err := pq.NewConfig(cleaned)
	if err != nil {
		return nil, err
	}
	statement, lock, connect := Defaults()
	if connect > 0 && (cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > connect) {
		cfg.ConnectTimeout = connect
	}

	connector, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		return nil, err
	}
	return Open(connector, statement, lock, asked), nil
}
