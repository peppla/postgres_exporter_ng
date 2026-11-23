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
	"net/url"
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
	//
	// Parsed rather than string-replaced. The first version substituted the
	// literal "//root:root@", so a fixture whose administrator is anyone else
	// kept the administrator's own credentials and every test below silently
	// measured the administrator's session instead of the role's. ChatGPT hit
	// exactly that with a different password. A connection string this cannot
	// rewrite fails the test rather than being handed back unchanged.
	u, err := url.Parse(strings.Split(dsn, ",")[0])
	if err != nil {
		t.Fatalf("DATA_SOURCE_NAME is not a URL, so this test cannot change its login: %s", err)
	}
	if u.User == nil || u.User.Username() == "" {
		t.Fatal("DATA_SOURCE_NAME names no user, so there is nothing to change")
	}
	u.User = url.UserPassword(role, "secret")
	rewritten := u.String()
	if rewritten == strings.Split(dsn, ",")[0] {
		t.Fatal("the connection string was not changed, so this test would run as the administrator")
	}
	return rewritten
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

			SetDefaults(tc.limit, tc.lockLimit, 3*time.Second)
			defer SetDefaults(0, 0, 3*time.Second)

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

	SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 3*time.Second)

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

	SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 3*time.Second)

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

	SetDefaults(300*time.Millisecond, 100*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 3*time.Second)

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

// TestStrictRoleSurvivesALooserConnectionString is the combination the
// earlier cases missed.
//
// They tested a strict role with no client override, and a client override
// with no strict role. Neither established which wins when both are present,
// and both were present in the case that mattered: the client's value is
// applied at startup, before the ceiling is compared against the session, so
// a looser client value had already erased the administrator's stricter one
// by the time anything looked.
//
// Both forms are covered, because lib/pq forwards either.
func TestStrictRoleSurvivesALooserConnectionString(t *testing.T) {
	for _, tc := range []struct {
		name          string
		suffix        string
		wantStatement int64
		wantLock      int64
	}{
		{
			name:          "no override at all",
			suffix:        "",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "options asking for no limit",
			suffix:        "&options=-c%20statement_timeout%3D0%20-c%20lock_timeout%3D0",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "options asking for more",
			suffix:        "&options=-c%20statement_timeout%3D60s%20-c%20lock_timeout%3D30s",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "startup parameters asking for no limit",
			suffix:        "&statement_timeout=0&lock_timeout=0",
			wantStatement: 100, wantLock: 20,
		},
		{
			// The server normalises setting names, so these reach a session
			// exactly as the canonical spelling does. Matching only the
			// lowercase underscore form let all three through.
			name:          "options in upper case",
			suffix:        "&options=-c%20STATEMENT_TIMEOUT%3D0%20-c%20LOCK_TIMEOUT%3D0",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "options written with dashes",
			suffix:        "&options=--statement-timeout%3D0%20--lock-timeout%3D0",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "startup parameters in upper case",
			suffix:        "&STATEMENT_TIMEOUT=0&LOCK_TIMEOUT=0",
			wantStatement: 100, wantLock: 20,
		},
		{
			// A client asking for less than everything else gets it. The
			// ceiling only ever tightens, and so does the client.
			name:          "options asking for less than the role",
			suffix:        "&options=-c%20statement_timeout%3D50ms%20-c%20lock_timeout%3D10ms",
			wantStatement: 50, wantLock: 10,
		},
		{
			name:          "startup parameters asking for less than the role",
			suffix:        "&statement_timeout=50ms&lock_timeout=10ms",
			wantStatement: 50, wantLock: 10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := withRole(t, map[string]string{
				"statement_timeout": "100ms",
				"lock_timeout":      "20ms",
			}) + tc.suffix

			SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
			defer SetDefaults(0, 0, 0)

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

// TestUnrelatedOptionsSurvive checks that taking the two timeouts out of the
// options string leaves the rest of it working.
//
// An operator who put work_mem there meant it, and losing it would be a
// different setting silently changing because of how this package reads the
// string.
func TestUnrelatedOptionsSurvive(t *testing.T) {
	dsn := withRole(t, nil) +
		"&options=-c%20work_mem%3D65536%20-c%20statement_timeout%3D0%20-c%20application_name%3Dclamp_check"

	SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 0)

	db, err := OpenDSN(dsn)
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	var workMem, appName string
	if err := db.QueryRowContext(context.Background(),
		`SELECT setting FROM pg_catalog.pg_settings WHERE name = 'work_mem'`).Scan(&workMem); err != nil {
		t.Fatalf("reading work_mem: %s", err)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT setting FROM pg_catalog.pg_settings WHERE name = 'application_name'`).Scan(&appName); err != nil {
		t.Fatalf("reading application_name: %s", err)
	}

	if workMem != "65536" {
		t.Errorf("work_mem is %q, want 65536: removing the timeouts took something else with it", workMem)
	}
	if appName != "clamp_check" {
		t.Errorf("application_name is %q, want clamp_check", appName)
	}
	if got := effective(t, db, "statement_timeout"); got != 5000 {
		t.Errorf("statement_timeout is %dms, want 5000ms", got)
	}
}

// TestEnvironmentOptionsCannotWeakenTheRole is the last precedence path.
//
// The driver merges PGOPTIONS into its configuration after the connection
// string has been parsed, so cleaning the string left the environment
// untouched: a role limited to 100ms with PGOPTIONS asking for no limit ended
// up at the exporter's 5s. The cleaning happens on the driver's effective
// configuration now, which is where every source has been merged.
func TestEnvironmentOptionsCannotWeakenTheRole(t *testing.T) {
	for _, tc := range []struct {
		name          string
		pgoptions     string
		wantStatement int64
		wantLock      int64
	}{
		{
			name:          "asking for no limit",
			pgoptions:     "-c statement_timeout=0 -c lock_timeout=0",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "asking for more",
			pgoptions:     "-c statement_timeout=60s -c lock_timeout=30s",
			wantStatement: 100, wantLock: 20,
		},
		{
			name:          "asking for less",
			pgoptions:     "-c statement_timeout=50ms -c lock_timeout=10ms",
			wantStatement: 50, wantLock: 10,
		},
		{
			name:          "in upper case",
			pgoptions:     "-c STATEMENT_TIMEOUT=0 -c LOCK_TIMEOUT=0",
			wantStatement: 100, wantLock: 20,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := withRole(t, map[string]string{
				"statement_timeout": "100ms",
				"lock_timeout":      "20ms",
			})

			t.Setenv("PGOPTIONS", tc.pgoptions)
			SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
			defer SetDefaults(0, 0, 0)

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

// TestEnvironmentOptionsKeepUnrelatedSettings checks that cleaning PGOPTIONS
// leaves the rest of it working, the same as cleaning a connection string.
func TestEnvironmentOptionsKeepUnrelatedSettings(t *testing.T) {
	dsn := withRole(t, nil)
	t.Setenv("PGOPTIONS", "-c work_mem=65536 -c statement_timeout=0")

	SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 0)

	db, err := OpenDSN(dsn)
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	var workMem string
	if err := db.QueryRowContext(context.Background(),
		`SELECT setting FROM pg_catalog.pg_settings WHERE name = 'work_mem'`).Scan(&workMem); err != nil {
		t.Fatalf("reading work_mem: %s", err)
	}
	if workMem != "65536" {
		t.Errorf("work_mem is %q, want 65536: cleaning PGOPTIONS took something else with it", workMem)
	}
	if got := effective(t, db, "statement_timeout"); got != 5000 {
		t.Errorf("statement_timeout is %dms, want 5000ms", got)
	}
}
