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
)

// clampQuery lowers one setting to at most the given number of milliseconds.
//
// pg_settings reports both of these in milliseconds, which is what makes the
// comparison possible without parsing "100ms" or "5s" by hand. A current
// value of 0 means no limit, so NULLIF turns it into NULL and LEAST, which
// ignores NULLs, yields the configured ceiling. Anything else yields the
// smaller of the two, so the setting only ever moves down.
const clampQuery = `SELECT set_config($1, LEAST(NULLIF(setting::bigint, 0), $2::bigint)::text, false)
	FROM pg_catalog.pg_settings WHERE name = $1`

// Connector wraps a driver connector and applies the ceilings to each new
// connection.
type Connector struct {
	inner     driver.Connector
	statement time.Duration
	lock      time.Duration
}

// Wrap returns a connector that clamps every connection it opens.
//
// A zero duration leaves that setting alone, which is what an operator asking
// for no ceiling gets.
func Wrap(inner driver.Connector, statement, lock time.Duration) *Connector {
	return &Connector{inner: inner, statement: statement, lock: lock}
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
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}

	for _, setting := range []struct {
		name  string
		value time.Duration
	}{
		{"statement_timeout", c.statement},
		{"lock_timeout", c.lock},
	} {
		if setting.value <= 0 {
			continue
		}
		if err := clamp(ctx, conn, setting.name, setting.value); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("applying %s to a new connection: %w", setting.name, err)
		}
	}
	return conn, nil
}

// clamp runs the lowering statement on one connection.
func clamp(ctx context.Context, conn driver.Conn, name string, value time.Duration) error {
	queryer, ok := conn.(driver.QueryerContext)
	if !ok {
		return fmt.Errorf("this driver cannot run a query on a new connection")
	}

	// Rounded up, because rounding a sub-millisecond ceiling down to zero
	// would read as no limit at all.
	ms := value.Milliseconds()
	if ms < 1 {
		ms = 1
	}

	rows, err := queryer.QueryContext(ctx, clampQuery, []driver.NamedValue{
		{Ordinal: 1, Value: name},
		{Ordinal: 2, Value: fmt.Sprint(ms)},
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
func Open(inner driver.Connector, statement, lock time.Duration) *sql.DB {
	return sql.OpenDB(Wrap(inner, statement, lock))
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
)

// SetDefaults records the ceilings for connections opened from now on.
func SetDefaults(statement, lock time.Duration) {
	defaultsMtx.Lock()
	defer defaultsMtx.Unlock()
	defaultStatement, defaultLock = statement, lock
}

// Defaults reports the current ceilings.
func Defaults() (statement, lock time.Duration) {
	defaultsMtx.RLock()
	defer defaultsMtx.RUnlock()
	return defaultStatement, defaultLock
}

// OpenDSN opens a database handle whose connections carry the process
// ceilings.
//
// A handle, not a connection: nothing connects until something uses it, the
// same as sql.Open. The ceilings are read now rather than per connection, so
// a handle opened before SetDefaults keeps whatever was set then, which is
// why main sets them before serving.
func OpenDSN(dsn string) (*sql.DB, error) {
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	statement, lock := Defaults()
	return Open(connector, statement, lock), nil
}
