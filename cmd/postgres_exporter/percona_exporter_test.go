// Copyright 2026 The Prometheus Authors
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
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

func TestParseCollectFilters(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want Filters
	}{
		{
			name: "no selection collects everything",
			in:   nil,
			want: Filters{EnableAllCollectors: true},
		},
		{
			name: "bare postgres keeps selecting the whole framework",
			in:   []string{"postgres"},
			want: Filters{EnablePostgresCollector: true},
		},
		{
			name: "a single postgres collector",
			in:   []string{"postgres.stat_user_indexes"},
			want: Filters{
				EnablePostgresCollector: true,
				PostgresCollectors:      []string{"stat_user_indexes"},
			},
		},
		{
			name: "several postgres collectors accumulate",
			in:   []string{"postgres.stat_user_indexes", "postgres.locks"},
			want: Filters{
				EnablePostgresCollector: true,
				PostgresCollectors:      []string{"stat_user_indexes", "locks"},
			},
		},
		{
			name: "per-collector selection mixes with the other groups",
			in:   []string{"standard.go", "postgres.locks", "custom_query.hr"},
			want: Filters{
				EnableGoCollector:       true,
				EnableHRCollector:       true,
				EnablePostgresCollector: true,
				PostgresCollectors:      []string{"locks"},
			},
		},
		{
			// Validation belongs to NewPostgresCollector, which owns the list of
			// collectors; the handler turns its error into a 400. The parser
			// passes the name through untouched.
			name: "an unknown collector name is passed through, not dropped",
			in:   []string{"postgres.does_not_exist"},
			want: Filters{
				EnablePostgresCollector: true,
				PostgresCollectors:      []string{"does_not_exist"},
			},
		},
		{
			// "postgres." with nothing after it would otherwise select a
			// collector named "", which NewPostgresCollector would reject with a
			// confusing message.
			name: "an empty collector name is ignored",
			in:   []string{"postgres."},
			want: Filters{},
		},
		{
			name: "an unrelated value is ignored, as before",
			in:   []string{"nonsense"},
			want: Filters{},
		},
		{
			// The group presets expand to the ENABLED members of each side of
			// the cluster/database split. kingpin has not parsed flags in a test
			// binary, so every collector reads as disabled and both groups
			// expand to nothing here.
			//
			// That makes this a direct test of the empty-group guard, which is
			// the part that matters: NewPostgresCollector reads an empty filter
			// list as "every enabled collector", so a group that expands to
			// nothing must leave EnablePostgresCollector false rather than
			// quietly turning the whole framework on. In a real process the
			// groups are non-empty and this case does not arise.
			name: "a group expanding to nothing leaves the framework off",
			in:   []string{"postgres.cluster"},
			want: Filters{},
		},
		{
			name: "the same for the database group",
			in:   []string{"postgres.database"},
			want: Filters{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCollectFilters(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseCollectFilters(%q)\n got: %+v\nwant: %+v", tc.in, got, tc.want)
			}
		})
	}
}

// A per-collector collect[] selection names a collector in the request, so a
// name that cannot be satisfied is a client error. Before this was added such a
// request produced a successful scrape containing no metrics, which Prometheus
// records as a healthy but empty target — a silent hole rather than a visible
// failure.
//
// Neither case needs a database: NewPostgresCollector validates the names
// before it looks at the DSN.
//
// Note on the second case: kingpin has not parsed flags in a test binary, so
// every collector's enabled flag reads false and ANY valid name lands on the
// "disabled" branch here — including stat_user_tables, which is enabled by
// default in a real process. The case therefore exercises that error branch,
// not the registered default of the collector it names. A test asserting that a
// legitimately enabled collector is accepted would need the flags parsed, and
// then a reachable DSN, which is integration territory.
func TestHandlerRejectsBadCollectorSelection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		wantBody string
	}{
		{
			name:     "a collector that does not exist",
			query:    "?collect[]=postgres.does_not_exist",
			wantBody: "missing collector: does_not_exist",
		},
		{
			name:     "a collector that is not enabled in this process",
			query:    "?collect[]=postgres.statio_user_indexes",
			wantBody: "disabled collector: statio_user_indexes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := Handler(log.NewNopLogger(), nil, semaphore.NewWeighted(1), map[string]prometheus.Collector{})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics"+tc.query, nil))

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d (body: %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("body = %q, want it to contain %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}
