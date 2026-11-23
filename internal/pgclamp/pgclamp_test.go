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

package pgclamp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/percona/postgres_exporter/config"
)

// TestConnectGivesUpWithTheContext is the sub-second startup bound.
//
// The driver bounds its own startup with a whole number of seconds, so a
// quarter-second request waited a full second against a server that accepts a
// socket and never speaks, and the context it is handed never reaches its
// handshake. Running the attempt alongside the context is the only bound that
// matches what the request asked for.
func TestConnectGivesUpWithTheContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %s", err)
	}
	defer listener.Close()

	// Accept and say nothing, which is what a wedged server looks like.
	go func() {
		var held []net.Conn
		defer func() {
			for _, c := range held {
				_ = c.Close()
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()

	SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 0)

	db, err := OpenDSN(fmt.Sprintf("postgresql://u:p@%s/d?sslmode=disable", listener.Addr().String()))
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = db.PingContext(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("connecting to a server that never answers succeeded")
	}
	// The driver's own bound here is the three-second ceiling, rounded to
	// whole seconds, so anything near a second means the context was ignored.
	if elapsed > 700*time.Millisecond {
		t.Errorf("a quarter-second attempt took %s", elapsed)
	}
}

// TestConnectRefusesAnAlreadyExpiredContext covers the caller that arrives
// with nothing left.
//
// Starting a connection for a request that is already over spends a
// connection slot and a server backend on work whose result cannot be used.
func TestConnectRefusesAnAlreadyExpiredContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %s", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	SetDefaults(5*time.Second, 500*time.Millisecond, 3*time.Second)
	defer SetDefaults(0, 0, 0)

	db, err := OpenDSN(fmt.Sprintf("postgresql://u:p@%s/d?sslmode=disable", listener.Addr().String()))
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	start := time.Now()
	err = db.PingContext(ctx)
	if err == nil {
		t.Fatal("an expired context produced a connection")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("the error was %v, which is acceptable as long as it was prompt", err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("an expired context took %s to be refused", elapsed)
	}
}

// TestStripTimeoutOverridesLeavesTheRestAlone pins the string surgery without
// needing a server.
func TestStripTimeoutOverridesLeavesTheRestAlone(t *testing.T) {
	for _, tc := range []struct {
		name          string
		in            string
		wantStatement int64
		wantLock      int64
		wantGone      []string
		wantKept      []string
	}{
		{
			name:     "nothing to strip",
			in:       "postgresql://u@h/db?sslmode=disable",
			wantKept: []string{"sslmode=disable"},
		},
		{
			name:          "startup parameters",
			in:            "postgresql://u@h/db?statement_timeout=0&lock_timeout=20ms&sslmode=disable",
			wantStatement: 0, wantLock: 20,
			wantGone: []string{"statement_timeout", "lock_timeout"},
			wantKept: []string{"sslmode=disable"},
		},
		{
			name:          "options entries, with an unrelated one kept",
			in:            "postgresql://u@h/db?options=-c%20work_mem%3D65536%20-c%20statement_timeout%3D60s",
			wantStatement: 60000,
			wantGone:      []string{"statement_timeout"},
			wantKept:      []string{"work_mem"},
		},
		{
			name:          "a bare number is milliseconds",
			in:            "postgresql://u@h/db?statement_timeout=250",
			wantStatement: 250,
			wantGone:      []string{"statement_timeout"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, asked, err := config.StripTimeoutOverrides(tc.in)
			if err != nil {
				t.Fatalf("StripTimeoutOverrides(%q): %s", tc.in, err)
			}
			if asked.StatementMS != tc.wantStatement {
				t.Errorf("statement asked for %d, want %d", asked.StatementMS, tc.wantStatement)
			}
			if asked.LockMS != tc.wantLock {
				t.Errorf("lock asked for %d, want %d", asked.LockMS, tc.wantLock)
			}
			for _, gone := range tc.wantGone {
				if contains(out, gone) {
					t.Errorf("%q is still in %q", gone, out)
				}
			}
			for _, kept := range tc.wantKept {
				if !contains(out, kept) {
					t.Errorf("%q was lost from %q", kept, out)
				}
			}
		})
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
