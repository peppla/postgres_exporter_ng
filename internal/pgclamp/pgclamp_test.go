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
	"sync"
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

// countingListener accepts connections and reports how many are still open,
// deciding that from the socket itself rather than from anything the test
// does afterwards.
type countingListener struct {
	listener net.Listener
	mtx      sync.Mutex
	open     int
	peak     int
}

func newCountingListener(t *testing.T) *countingListener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %s", err)
	}
	c := &countingListener{listener: l}
	go c.serve()
	t.Cleanup(func() { _ = l.Close() })
	return c
}

// serve accepts and never answers the startup message, which is what a wedged
// server looks like. A connection is counted until the peer closes it, which
// a read of zero bytes reports.
func (c *countingListener) serve() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return
		}
		c.mtx.Lock()
		c.open++
		if c.open > c.peak {
			c.peak = c.open
		}
		c.mtx.Unlock()

		go func(conn net.Conn) {
			defer conn.Close()
			buf := make([]byte, 1024)
			for {
				if _, err := conn.Read(buf); err != nil {
					c.mtx.Lock()
					c.open--
					c.mtx.Unlock()
					return
				}
			}
		}(conn)
	}
}

func (c *countingListener) counts() (open, peak int) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.open, c.peak
}

func (c *countingListener) addr() string { return c.listener.Addr().String() }

// TestAbandonedAttemptsDoNotAccumulate is the resource half of the startup
// bound, which returning promptly says nothing about.
//
// The first version of this returned to the caller on time by leaving the
// attempt running in a goroutine. That is worse than waiting: the permit for
// that connection goes back while the socket is still open, so four
// sequential scrapes against a server that never answers held four connections
// against a configured limit of one. ChatGPT reproduced exactly that.
//
// What is asserted here is the listener's own count of open sockets, taken
// while the test is still running and without closing anything itself.
func TestAbandonedAttemptsDoNotAccumulate(t *testing.T) {
	server := newCountingListener(t)

	SetDefaults(5*time.Second, 500*time.Millisecond, 500*time.Millisecond)
	defer SetDefaults(0, 0, 0)

	db, err := OpenDSN(fmt.Sprintf("postgresql://u:p@%s/d?sslmode=disable", server.addr()))
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	for attempt := 1; attempt <= 4; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		start := time.Now()
		err := db.PingContext(ctx)
		elapsed := time.Since(start)
		cancel()

		if err == nil {
			t.Fatalf("attempt %d connected to a server that never answers", attempt)
		}
		if elapsed > 400*time.Millisecond {
			t.Errorf("attempt %d took %s, so the budget did not bound it", attempt, elapsed)
		}

		// The exporter closes its end before returning, so the server sees
		// the close straight afterwards. A short wait for it to notice is
		// propagation, not patience: nothing in this test closes anything,
		// so if the socket were still held it would stay held.
		open := waitForClose(server, 2*time.Second)
		if open > 0 {
			t.Errorf("after attempt %d, %d sockets are still open, so the attempt outlived its budget", attempt, open)
		}

		// The one that matters. An attempt left running returns its permit
		// while still holding a socket, so sequential scrapes pile up: four
		// connections against a configured limit of one.
		if _, peak := server.counts(); peak > 1 {
			t.Errorf("after attempt %d the server had seen %d connections at once, with a limit of one", attempt, peak)
		}
	}
}

// waitForClose returns the number of sockets still open, giving the server a
// bounded moment to observe a close the client has already made.
func waitForClose(server *countingListener, within time.Duration) int {
	deadline := time.Now().Add(within)
	for {
		open, _ := server.counts()
		if open == 0 || time.Now().After(deadline) {
			return open
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCancellationEndsAnAttemptWithoutADeadline isolates the cancellation
// hook from the driver's own timeout.
//
// A context carrying a deadline shortens the driver's connect_timeout, and
// that alone ends the attempt, so the test above cannot tell the two
// mechanisms apart. A context that is cancelled rather than timed out carries
// no deadline, so the driver keeps whatever ceiling it was given and only
// closing the socket ends the attempt.
//
// That is the shape of a scrape whose client hung up: the request context is
// cancelled, with no deadline left to shorten anything.
func TestCancellationEndsAnAttemptWithoutADeadline(t *testing.T) {
	server := newCountingListener(t)

	// A ceiling far longer than this test is willing to wait, so the driver's
	// own bound cannot be what ends the attempt.
	SetDefaults(5*time.Second, 500*time.Millisecond, 30*time.Second)
	defer SetDefaults(0, 0, 0)

	db, err := OpenDSN(fmt.Sprintf("postgresql://u:p@%s/d?sslmode=disable", server.addr()))
	if err != nil {
		t.Fatalf("opening: %s", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- db.PingContext(ctx) }()

	// Wait until the attempt has a socket, so cancelling has something to end.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if open, _ := server.counts(); open > 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the attempt never reached the server")
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	cancel()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("connecting to a server that never answers succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelling the request did not end the connection attempt")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("the attempt took %s to end after cancellation", elapsed)
	}

	if open := waitForClose(server, 3*time.Second); open > 0 {
		t.Errorf("%d sockets are still open after cancellation, so the attempt kept its connection", open)
	}
}
