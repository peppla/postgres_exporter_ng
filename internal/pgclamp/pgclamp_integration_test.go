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

// These need a running PostgreSQL server, because what is being tested is the
// setting a session ends up with, and only a server can say what that is.
// Reading the connection string back tells you what was asked for, not what
// happened, which is how the first version of this work shipped a ceiling
// that silently raised a stricter one.
//go:build integration

package pgclamp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// withRole creates a login role with the given settings and returns a DSN for
// it, plus a cleanup.
func withRole(t *testing.T, settings map[string]string) string {
	t.Helper()
	dsn := os.Getenv("DATA_SOURCE_NAME")
	if dsn == "" {
		t.Fatal("DATA_SOURCE_NAME is not set")
	}
	admin, err := sql.Open("postgres", strings.Split(dsn, ",")[0])
	if err != nil {
		t.Fatalf("connecting as the administrator: %s", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	role := fmt.Sprintf("clamp_%d", time.Now().UnixNano()%100000)
	drop := func() {
		_, _ = admin.Exec(`DROP ROLE IF EXISTS ` + role)
	}
	drop()
	t.Cleanup(drop)

	if _, err := admin.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'secret'`); err != nil {
		t.Fatalf("creating the role: %s", err)
	}
	if _, err := admin.Exec(`GRANT pg_monitor TO ` + role); err != nil {
		t.Fatalf("granting pg_monitor: %s", err)
	}
	for name, value := range settings {
		if _, err := admin.Exec(`ALTER ROLE ` + role + ` SET ` + name + ` = '` + value + `'`); err != nil {
			t.Fatalf("setting %s on the role: %s", name, err)
		}
	}

	// Same server, same database, different login.
	return strings.Replace(strings.Split(dsn, ",")[0], "//root:root@", "//"+role+":secret@", 1)
}

// effective reads what a session actually has, in milliseconds.
func effective(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	var ms int64
	err := db.QueryRowContext(context.Background(),
		`SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = $1`, name).Scan(&ms)
	if err != nil {
		t.Fatalf("reading %s: %s", name, err)
	}
	return ms
}

// TestClampOnlyLowers is the finding this package exists for.
//
// The first implementation wrote these settings into libpq's options
// parameter, which is applied after a role's own settings and therefore
// overrode them. A role an administrator had limited to 100ms got the
// exporter's 5s instead, which is a monitoring tool raising a limit somebody
// set deliberately.
func TestClampOnlyLowers(t *testing.T) {
	for _, tc := range []struct {
		name          string
		roleStatement string
		roleLock      string
		limit         time.Duration
		lockLimit     time.Duration
		wantStatement int64
		wantLock      int64
	}{
		{
			// The role is stricter. It stays.
			name:          "a stricter role setting survives",
			roleStatement: "100ms", roleLock: "20ms",
			limit: 5 * time.Second, lockLimit: 500 * time.Millisecond,
			wantStatement: 100, wantLock: 20,
		},
		{
			// The role is looser. The ceiling applies.
			name:          "a looser role setting is tightened",
			roleStatement: "60s", roleLock: "30s",
			limit: 5 * time.Second, lockLimit: 500 * time.Millisecond,
			wantStatement: 5000, wantLock: 500,
		},
		{
			// Zero means no limit at all, so the ceiling applies rather than
			// being read as the smallest value.
			name:          "no role setting at all is tightened",
			roleStatement: "0", roleLock: "0",
			limit: 5 * time.Second, lockLimit: 500 * time.Millisecond,
			wantStatement: 5000, wantLock: 500,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := withRole(t, map[string]string{
				"statement_timeout": tc.roleStatement,
				"lock_timeout":      tc.roleLock,
			})

			SetDefaults(tc.limit, tc.lockLimit)
			defer SetDefaults(0, 0)

			db, err := OpenDSN(dsn)
			if err != nil {
				t.Fatalf("opening: %s", err)
			}
			defer db.Close()

			if got := effective(t, db, "statement_timeout"); got != tc.wantStatement {
				t.Errorf("statement_timeout is %dms, want %dms", got, tc.wantStatement)
			}
			if got := effective(t, db, "lock_timeout"); got != tc.wantLock {
				t.Errorf("lock_timeout is %dms, want %dms", got, tc.wantLock)
			}
		})
	}
}

// TestClampCannotBeBypassedByTheConnectionString covers the other half of the
// finding.
//
// A connection string carrying options with statement_timeout=0 used to defeat
// the limit entirely, because the check for an existing value was a substring
// match rather than a comparison. The ceiling is applied after connecting now,
// so whatever the string asked for, the session still ends up bounded.
func TestClampCannotBeBypassedByTheConnectionString(t *testing.T) {
	dsn := withRole(t, nil) + "&options=-c%20statement_timeout%3D0%20-c%20lock_timeout%3D0"

	SetDefaults(5*time.Second, 500*time.Millisecond)
	defer SetDefaults(0, 0)

	db, err := OpenDSN(dsn)
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	if got := effective(t, db, "statement_timeout"); got != 5000 {
		t.Errorf("statement_timeout is %dms, want 5000ms: a connection string disabled the ceiling", got)
	}
	if got := effective(t, db, "lock_timeout"); got != 500 {
		t.Errorf("lock_timeout is %dms, want 500ms: a connection string disabled the ceiling", got)
	}
}

// TestClampAppliesToEveryPooledConnection is why this is a connector rather
// than something run once per scrape.
//
// A pool opens connections when it needs them. A ceiling applied to the first
// one says nothing about the tenth, and the tenth is the one serving a scrape
// during a busy moment.
func TestClampAppliesToEveryPooledConnection(t *testing.T) {
	dsn := withRole(t, map[string]string{"statement_timeout": "0"})

	SetDefaults(5*time.Second, 500*time.Millisecond)
	defer SetDefaults(0, 0)

	db, err := OpenDSN(dsn)
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	// Hold several connections open at once so the pool has to make them.
	conns := make([]*sql.Conn, 0, 4)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("taking connection %d: %s", i, err)
		}
		conns = append(conns, conn)

		var ms int64
		if err := conn.QueryRowContext(context.Background(),
			`SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'`).Scan(&ms); err != nil {
			t.Fatalf("reading the setting on connection %d: %s", i, err)
		}
		if ms != 5000 {
			t.Errorf("connection %d has statement_timeout %dms, want 5000ms", i, ms)
		}
	}
}

// TestClampBoundsARealStatement checks the server enforces what was set,
// rather than merely reporting it.
func TestClampBoundsARealStatement(t *testing.T) {
	dsn := withRole(t, nil)

	SetDefaults(300*time.Millisecond, 100*time.Millisecond)
	defer SetDefaults(0, 0)

	db, err := OpenDSN(dsn)
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	start := time.Now()
	_, err = db.ExecContext(context.Background(), "SELECT pg_sleep(30)")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a thirty-second statement finished, so nothing bounded it")
	}
	if !strings.Contains(err.Error(), "canceling statement due to statement timeout") {
		t.Errorf("the statement ended with %q, not a statement timeout", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("the statement took %s, so the ceiling was not what stopped it", elapsed)
	}
}
