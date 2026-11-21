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

package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// loadLimitsConfig parses a configuration file written for one test.
func loadLimitsConfig(t *testing.T, content string) (*config.Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postgres_exporter.yml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the fixture: %s", err)
	}
	h := &config.Handler{}
	err := h.ReloadConfig(path, log.NewNopLogger())
	return h.GetConfig(), err
}

// TestLimitsConfiguration pins what the file can say and what it may not.
func TestLimitsConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		wantErr string
		check   func(*testing.T, budget.Limits)
	}{
		{
			name:    "no limits section",
			content: "auth_modules: {}\n",
			check: func(t *testing.T, l budget.Limits) {
				if l != budget.Defaults() {
					t.Errorf("an empty file did not produce the approved defaults: %+v", l)
				}
			},
		},
		{
			name:    "one key overrides one default",
			content: "limits:\n  connections: 12\n",
			check: func(t *testing.T, l budget.Limits) {
				if l.Connections != 12 {
					t.Errorf("connections = %d, want 12", l.Connections)
				}
				if l.ConcurrentRequests != budget.Defaults().ConcurrentRequests {
					t.Errorf("an unrelated default changed: %d", l.ConcurrentRequests)
				}
			},
		},
		{
			name:    "durations and the reserve",
			content: "limits:\n  request_timeout: 4s\n  max_request_timeout: 30s\n  response_reserve:\n    max: 500ms\n    fraction: 0.25\n",
			check: func(t *testing.T, l budget.Limits) {
				if l.RequestTimeout != 4*time.Second {
					t.Errorf("request_timeout = %s", l.RequestTimeout)
				}
				if l.MaxRequestTimeout != 30*time.Second {
					t.Errorf("max_request_timeout = %s", l.MaxRequestTimeout)
				}
				if l.ResponseReserveMax != 500*time.Millisecond {
					t.Errorf("response_reserve.max = %s", l.ResponseReserveMax)
				}
				if l.ResponseReserveFraction != 0.25 {
					t.Errorf("response_reserve.fraction = %v", l.ResponseReserveFraction)
				}
			},
		},
		{
			// Zero is a mistake worth reporting. It reaches Validate because
			// the parsed value is a pointer, so a written zero and an omitted
			// key do not collapse into the same thing.
			name:    "an explicit zero is refused, not read as the default",
			content: "limits:\n  connections: 0\n",
			wantErr: "connections must be at least 1",
		},
		{
			name:    "a relationship that cannot hold",
			content: "limits:\n  request_timeout: 90s\n",
			wantErr: "could never be used",
		},
		{
			name:    "a key written with no value",
			content: "limits:\n  connections:\n",
			wantErr: "limits.connections is present with no value",
		},
		{
			name:    "the section written with no value",
			content: "limits:\n",
			wantErr: "limits is present with no value",
		},
		{
			name:    "a misspelled key is refused rather than ignored",
			content: "limits:\n  connection: 5\n",
			wantErr: "field connection not found",
		},
		{
			name:    "a duration without a unit",
			content: "limits:\n  request_timeout: 5\n",
			wantErr: "not a valid duration string",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadLimitsConfig(t, tc.content)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, got none", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loading the configuration: %s", err)
			}
			tc.check(t, cfg.Limits.Budget())
		})
	}
}

// TestResolveLimits covers the overlap between the file and the one flag that
// sets the same number.
func TestResolveLimits(t *testing.T) {
	withConnections, err := loadLimitsConfig(t, "limits:\n  connections: 9\n")
	if err != nil {
		t.Fatalf("loading the fixture: %s", err)
	}
	withoutConnections, err := loadLimitsConfig(t, "limits:\n  concurrent_requests: 2\n")
	if err != nil {
		t.Fatalf("loading the fixture: %s", err)
	}

	t.Run("no file and no flag gives the defaults", func(t *testing.T) {
		got, err := resolveLimits(nil, false, 5)
		if err != nil {
			t.Fatalf("resolving: %s", err)
		}
		if got != budget.Defaults() {
			t.Errorf("got %+v, want the defaults", got)
		}
	})

	t.Run("the flag alone", func(t *testing.T) {
		got, err := resolveLimits(withoutConnections, true, 20)
		if err != nil {
			t.Fatalf("resolving: %s", err)
		}
		if got.Connections != 20 {
			t.Errorf("connections = %d, want 20", got.Connections)
		}
		if got.ConcurrentRequests != 2 {
			t.Errorf("the file's other key was lost: %d", got.ConcurrentRequests)
		}
	})

	t.Run("the file alone", func(t *testing.T) {
		got, err := resolveLimits(withConnections, false, 5)
		if err != nil {
			t.Fatalf("resolving: %s", err)
		}
		if got.Connections != 9 {
			t.Errorf("connections = %d, want 9", got.Connections)
		}
	})

	t.Run("both is refused", func(t *testing.T) {
		_, err := resolveLimits(withConnections, true, 20)
		if err == nil || !strings.Contains(err.Error(), "both set the number of connections") {
			t.Fatalf("expected a refusal naming both, got %v", err)
		}
	})

	t.Run("an invalid flag value is reported", func(t *testing.T) {
		_, err := resolveLimits(withoutConnections, true, 0)
		if err == nil || !strings.Contains(err.Error(), "connections must be at least 1") {
			t.Fatalf("expected a validation error, got %v", err)
		}
	})
}

// TestConcurrentRequestsAreRefusedNotQueued covers the gate.
//
// A scrape beyond the limit has to be refused before any SQL runs. Queueing it
// turns an overloaded exporter into a slow one, and a scrape that waits its
// turn is already stale when it reaches a database.
//
// The first request is held open by a listener that accepts connections and
// never answers, so the exporter waits for a PostgreSQL handshake that never
// comes until its budget runs out. The accept is the synchronisation point:
// once it returns, that request is certainly in flight.
func TestConcurrentRequestsAreRefusedNotQueued(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %s", err)
	}
	defer listener.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		held := []net.Conn{}
		defer func() {
			for _, c := range held {
				c.Close()
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
			select {
			case accepted <- struct{}{}:
			default:
			}
		}
	}()

	dsn := fmt.Sprintf("postgresql://u:p@%s/d?sslmode=disable", listener.Addr().String())
	limits := budget.Defaults()
	limits.ConcurrentRequests = 1
	limits.RequestTimeout = 2 * time.Second

	h := Handler(log.NewNopLogger(), []string{dsn},
		semaphore.NewWeighted(5), map[string]prometheus.Collector{}, limits)

	first := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics?collect[]=postgres", nil))
		first <- rec.Code
	}()

	select {
	case <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the first request never tried to connect, so nothing was held")
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics?collect[]=postgres", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("a second concurrent scrape returned %d, want %d",
			rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "already in flight") {
		t.Errorf("the refusal does not say why: %q", rec.Body.String())
	}

	select {
	case <-first:
	case <-time.After(30 * time.Second):
		t.Fatal("the first request never finished")
	}

	// Once it is done the gate is free again, so the next scrape is served.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics?collect[]=exporter", nil))
	if rec.Code == http.StatusServiceUnavailable {
		t.Error("the gate was not released when the request finished")
	}
}

// TestRequestBudgetBoundsACollection checks that the budget reaches the
// context collection runs under.
//
// The same unanswering listener holds the connection. With a short budget the
// request has to come back in about that long rather than at the ten-second
// fallback, which is what shows the header was read and spent.
func TestRequestBudgetBoundsACollection(t *testing.T) {
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

	dsn := fmt.Sprintf("postgresql://u:p@%s/d?sslmode=disable", listener.Addr().String())
	h := Handler(log.NewNopLogger(), []string{dsn},
		semaphore.NewWeighted(5), map[string]prometheus.Collector{}, budget.Defaults())

	req := httptest.NewRequest("GET", "/metrics?collect[]=postgres", nil)
	req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", "1")

	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), req)
	elapsed := time.Since(start)

	// The budget is one second less the reserve, so anything near the
	// ten-second fallback means the header was ignored.
	if elapsed > 5*time.Second {
		t.Errorf("a one-second scrape took %s, so the requested budget was not applied", elapsed)
	}
}
