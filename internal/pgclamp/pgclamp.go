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
	"errors"
	"fmt"
	"net"
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

// Connector opens connections with bounded startup and clamped ceilings.
//
// It holds the configuration rather than a built connector, because the
// startup bound and the socket that carries it belong to one attempt. A
// connector shared across attempts cannot be given a dialer for one of them.
type Connector struct {
	cfg            pq.Config
	statement      time.Duration
	lock           time.Duration
	connectCeiling time.Duration
	// asked is what the connection string wanted, taken out of the string
	// before connecting so that a looser client value cannot replace a
	// stricter role setting before this package reads it.
	asked config.TimeoutOverrides
}

// Wrap returns a connector that bounds and clamps every connection it opens.
//
// A zero duration leaves that setting alone, which is what an operator asking
// for no ceiling gets.
func Wrap(cfg pq.Config, statement, lock, connect time.Duration, asked config.TimeoutOverrides) *Connector {
	return &Connector{cfg: cfg, statement: statement, lock: lock, connectCeiling: connect, asked: asked}
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver { return &pq.Driver{} }

// holdingDialer remembers the socket it dialled so the attempt that owns it
// can be ended from outside.
//
// This is the cancellation hook the driver does not provide. Its startup
// handshake is a blocking read on this socket, and closing the socket is what
// makes that read return. Abandoning the goroutine instead left the socket
// open after the collection had finished and its connection permit had been
// given back, so four sequential scrapes against a server that never answers
// held four connections with a configured limit of one.
type holdingDialer struct {
	mtx    sync.Mutex
	conn   net.Conn
	closed bool
}

func (d *holdingDialer) keep(conn net.Conn, err error) (net.Conn, error) {
	if err != nil {
		return conn, err
	}
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.closed {
		// The attempt was already given up on, so this socket is nobody's.
		_ = conn.Close()
		return nil, errAttemptAbandoned
	}
	d.conn = conn
	return conn, nil
}

// abandon closes the socket, which ends the handshake the driver is waiting
// on. Safe to call more than once and before anything has been dialled.
func (d *holdingDialer) abandon() {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.closed = true
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
}

func (d *holdingDialer) Dial(network, address string) (net.Conn, error) {
	return d.keep(net.Dial(network, address))
}

func (d *holdingDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	return d.keep(net.DialTimeout(network, address, timeout))
}

func (d *holdingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	var dialer net.Dialer
	return d.keep(dialer.DialContext(ctx, network, address))
}

// errAttemptAbandoned is returned to the driver when its socket was taken
// away because the caller's budget ran out.
var errAttemptAbandoned = errors.New("the connection attempt was abandoned because the request budget expired")

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

// connect opens the connection, bounded by the caller's budget.
//
// The bound has two parts. The driver is given a connect_timeout no longer
// than what is left, as a real duration rather than a whole number of
// seconds. And the socket is held by a dialer this function can reach, so
// when the budget runs out the socket is closed, the handshake ends and the
// attempt returns. The function waits for that to happen before returning, so
// nothing it started is still running when its caller releases a connection
// permit.
func (c *Connector) connect(ctx context.Context) (driver.Conn, error) {
	// Nothing to spend. Starting an attempt here would take a connection and
	// a server backend for a result that cannot be used.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cfg := c.cfg
	if c.connectCeiling > 0 && (cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > c.connectCeiling) {
		cfg.ConnectTimeout = c.connectCeiling
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 &&
			(cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > remaining) {
			cfg.ConnectTimeout = remaining
		}
	}

	inner, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		return nil, err
	}
	dialer := &holdingDialer{}
	inner.Dialer(dialer)

	type attempt struct {
		conn driver.Conn
		err  error
	}
	done := make(chan attempt, 1)
	go func() {
		conn, err := inner.Connect(ctx)
		done <- attempt{conn, err}
	}()

	select {
	case a := <-done:
		return a.conn, a.err
	case <-ctx.Done():
		// End the attempt rather than leave it running, then wait for it.
		dialer.abandon()
		a := <-done
		// A failed attempt can carry a nil connection inside a non-nil
		// interface, which is not nil to compare against and panics when
		// closed.
		if a.err == nil && a.conn != nil {
			_ = a.conn.Close()
		}
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
	// The driver's effective configuration, not the string. It merges the
	// connection string with the environment, so PGOPTIONS asking for no
	// limit reached startup unchecked while the string was clean. Cleaning
	// what the driver will actually use is the only place every source has
	// been merged.
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, err
	}

	cleanedOptions, fromOptions := config.StripOptionsOverrides(cfg.Options)
	cfg.Options = cleanedOptions
	fromRuntime := config.StripRuntimeOverrides(cfg.Runtime)
	asked := fromOptions.Tighten(fromRuntime)

	statement, lock, connect := Defaults()
	return sql.OpenDB(Wrap(cfg, statement, lock, connect, asked)), nil
}
