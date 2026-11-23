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

package config

import (
	"strings"
	"testing"
	"time"
)

// TestDSNWithConnectTimeout pins the value written into the connection string.
//
// The rounding rule is the part worth pinning. pq reads the value in whole
// seconds and treats zero as "wait forever", so anything under a second has to
// round up. Rounding down would replace a short bound with no bound at all,
// which is the opposite of what the caller asked for.
func TestDSNWithConnectTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dsn     string
		timeout time.Duration
		want    string
		wantErr bool
	}{
		{
			name: "a url gains the parameter", dsn: "postgresql://u:p@host:5432/db?sslmode=disable",
			timeout: 3 * time.Second, want: "connect_timeout=3",
		},
		{
			name: "a whole number of seconds is not rounded up", dsn: "postgresql://u@host/db",
			timeout: 5 * time.Second, want: "connect_timeout=5",
		},
		{
			name: "a fraction rounds up", dsn: "postgresql://u@host/db",
			timeout: 1500 * time.Millisecond, want: "connect_timeout=2",
		},
		{
			// Rounding down here would write zero, which pq reads as no
			// timeout, so a very short budget would remove the bound.
			name: "less than a second becomes one, not zero", dsn: "postgresql://u@host/db",
			timeout: 10 * time.Millisecond, want: "connect_timeout=1",
		},
		{
			name: "a shorter value already in the string is kept",
			dsn:  "postgresql://u@host/db?connect_timeout=1", timeout: 3 * time.Second,
			want: "connect_timeout=1",
		},
		{
			// The budget is a ceiling: an operator may ask for less and not
			// for more.
			name: "a longer value already in the string is shortened",
			dsn:  "postgresql://u@host/db?connect_timeout=30", timeout: 3 * time.Second,
			want: "connect_timeout=3",
		},
		{
			name:    "a keyword string gains it too",
			dsn:     "host=localhost user=postgres dbname=db sslmode=disable",
			timeout: 3 * time.Second, want: "connect_timeout=3",
		},
		{
			name: "no timeout asked for changes nothing",
			dsn:  "postgresql://u@host/db", timeout: 0, want: "postgresql://u@host/db",
		},
		{
			name: "an unparseable string is an error",
			dsn:  "::not a dsn::", timeout: time.Second, wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DSNWithConnectTimeout(tc.dsn, tc.timeout)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("DSNWithConnectTimeout(%q): %s", tc.dsn, err)
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("DSNWithConnectTimeout(%q, %s) = %q, expected it to contain %q",
					tc.dsn, tc.timeout, got, tc.want)
			}
		})
	}
}

// TestDSNWithConnectTimeoutKeepsTheDatabase checks that adding the parameter
// does not disturb what the string already said.
//
// The two helpers are used together, and a connection string can carry the
// database in two places, so a rewrite that moved or duplicated it would
// change which database is reached.
func TestDSNWithConnectTimeoutKeepsTheDatabase(t *testing.T) {
	for _, in := range []string{
		"postgresql://u:p@host:5432/mydb?sslmode=disable",
		"host=localhost user=postgres dbname=mydb sslmode=disable",
	} {
		before, okBefore := DatabaseFromDSN(in)
		out, err := DSNWithConnectTimeout(in, 3*time.Second)
		if err != nil {
			t.Fatalf("DSNWithConnectTimeout(%q): %s", in, err)
		}
		after, okAfter := DatabaseFromDSN(out)
		if before != after || okBefore != okAfter {
			t.Errorf("%q named %q (%v) and now names %q (%v)", in, before, okBefore, after, okAfter)
		}

		// And the pair still compose in the order the collector uses them.
		withDB, err := DSNWithDatabase(out, "other")
		if err != nil {
			t.Fatalf("DSNWithDatabase after a timeout was set: %s", err)
		}
		if name, ok := DatabaseFromDSN(withDB); !ok || name != "other" {
			t.Errorf("after both rewrites the string names %q (%v), want other", name, ok)
		}
		if !strings.Contains(withDB, "connect_timeout=3") {
			t.Errorf("the timeout was lost when the database was changed: %q", withDB)
		}
	}
}

// TestParseKeyValueHandlesQuotedValues covers the parser both engines use to
// read a libpq keyword string.
//
// Each had its own copy that split on plain spaces and on the first equals
// sign. That rejects any connection string with a quoted value containing a
// space, and reads one containing an equals sign in the wrong place. The
// options string the session timeouts are written into has both.
func TestParseKeyValueHandlesQuotedValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want map[string]string
	}{
		{
			name: "plain pairs",
			in:   "host=localhost port=5432 dbname=postgres",
			want: map[string]string{"host": "localhost", "port": "5432", "dbname": "postgres"},
		},
		{
			name: "quoted pairs, as pq.ParseURL emits them",
			in:   "dbname='postgres' host='127.0.0.1' port='5432'",
			want: map[string]string{"dbname": "postgres", "host": "127.0.0.1", "port": "5432"},
		},
		{
			// The case the session timeouts need: spaces and equals signs
			// inside one quoted value.
			name: "an options value with spaces and equals signs",
			in:   "host='127.0.0.1' options='-c statement_timeout=5000 -c lock_timeout=500' user='root'",
			want: map[string]string{
				"host":    "127.0.0.1",
				"options": "-c statement_timeout=5000 -c lock_timeout=500",
				"user":    "root",
			},
		},
		{
			// The same fault reached anyone whose password had a space in it.
			name: "a password with a space",
			in:   "host='h' password='two words' user='u'",
			want: map[string]string{"host": "h", "password": "two words", "user": "u"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseKeyValue(tc.in)
			if err != nil {
				t.Fatalf("ParseKeyValue(%q): %s", tc.in, err)
			}
			for k, want := range tc.want {
				if got[k] != want {
					t.Errorf("%s = %q, want %q", k, got[k], want)
				}
			}
			if len(got) != len(tc.want) {
				t.Errorf("parsed %d keys, want %d: %v", len(got), len(tc.want), got)
			}
		})
	}
}
