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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/querycache"
	"github.com/percona/postgres_exporter/internal/slots"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// handlerWithSlots builds the real handler over an address nothing answers on,
// with the profile admission set to a stated number.
//
// Nothing has to answer. What is under test happens before any SQL, so a
// request that is admitted fails to connect and still returns 200 with its
// diagnostics, and a request that is refused returns 503 without trying.
func handlerWithSlots(t *testing.T, l slots.Limits) (http.Handler, Stores) {
	t.Helper()
	// Collector flags are kingpin flags, and their pointers hold false until
	// something parses. Without this every named collector is reported
	// disabled and a request for one is a 400, so the cases below would be
	// about flag parsing rather than about admission.
	enableDefaultCollectors(t)
	limits := budget.Defaults()
	stores := NewStoresWithLimits(l)
	h := Handler(log.NewNopLogger(), []string{"postgresql://u:p@127.0.0.1:1/app?sslmode=disable"},
		semaphore.NewWeighted(limits.Connections), map[string]prometheus.Collector{},
		limits, NewCollectionGate(limits), querycache.New(), stores,
		dbselect.Absent(), dbselect.Absent())
	return h, stores
}

// enableDefaultCollectors applies the documented default for every collector
// flag, once for the test binary.
func enableDefaultCollectors(t *testing.T) {
	t.Helper()
	defaultsOnce.Do(func() {
		kingpin.CommandLine.Terminate(nil)
		if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
			t.Fatalf("applying the default collector flags: %s", err)
		}
	})
}

var defaultsOnce sync.Once

// get sends one request and returns its status and body.
func get(t *testing.T, h http.Handler, query string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics?"+query, nil))
	return rec.Code, rec.Body.String()
}

// profileQuery asks for one named collector, which is one distinct profile.
func profileQuery(collectors ...string) string {
	parts := make([]string, 0, len(collectors))
	for _, name := range collectors {
		parts = append(parts, "collect[]=postgres.collector."+name)
	}
	return strings.Join(parts, "&")
}

// TestAProfileBeyondTheLimitIsRefusedBeforeAnySQL is the admission rule at the
// endpoint.
//
// Refused rather than served without state. A profile with no slot has no
// schedule either, and a selection that does not fit the budget would then be
// offered the same first database on every request, which is the starvation
// the schedule exists to prevent. A successful response that quietly does not
// meet the fairness contract is worse than a refusal that says so.
func TestAProfileBeyondTheLimitIsRefusedBeforeAnySQL(t *testing.T) {
	h, stores := handlerWithSlots(t, slots.Limits{Profiles: 1, Idle: time.Hour})

	// One distinct profile, which takes the only slot.
	if code, body := get(t, h, profileQuery("stat_user_tables")); code != http.StatusOK {
		t.Fatalf("the first profile was answered %d: %s", code, body)
	}
	// The same profile again, which is the same slot.
	if code, body := get(t, h, profileQuery("stat_user_tables")); code != http.StatusOK {
		t.Fatalf("the same profile was answered %d on its second request: %s", code, body)
	}

	// A different set of collectors is a different profile, and there is no
	// room for it.
	code, body := get(t, h, profileQuery("stat_user_tables", "stat_user_indexes"))
	if code != http.StatusServiceUnavailable {
		t.Errorf("a second profile was answered %d, want 503: %s", code, body)
	}
	if !strings.Contains(body, "refused") {
		t.Errorf("the refusal says %q, which does not say what happened", strings.TrimSpace(body))
	}
	if held := stores.Slots.Held(slots.Profile); held != 1 {
		t.Errorf("%d profile slots held, want the one that was admitted", held)
	}

	// The admitted profile is unaffected.
	if code, body := get(t, h, profileQuery("stat_user_tables")); code != http.StatusOK {
		t.Errorf("the admitted profile was answered %d after another was refused: %s", code, body)
	}
}

// TestNoProfileLimitRefusesNothing is how this ships until the numbers are
// decided.
func TestNoProfileLimitRefusesNothing(t *testing.T) {
	h, stores := handlerWithSlots(t, slots.Limits{})

	for _, collectors := range [][]string{
		{"stat_user_tables"},
		{"stat_user_indexes"},
		{"stat_user_tables", "stat_user_indexes"},
		{"stat_user_tables", "statio_user_tables"},
	} {
		if code, body := get(t, h, profileQuery(collectors...)); code != http.StatusOK {
			t.Errorf("%v was answered %d with no limit configured: %s", collectors, code, body)
		}
	}
	if held := stores.Slots.Held(slots.Profile); held == 0 {
		t.Error("no profile took a slot, so this says nothing about a limit not being applied")
	}
}

// TestARefusedProfileIsNotRememberedAsAHolder is the property a stream of
// refusals must not break.
//
// A refused request must not leave the name it asked for holding a slot, or a
// client sending requests that are always refused fills the store with names
// nothing is scraping.
//
// A /metrics request needs one slot, so this reaches the capacity check and
// not the rollback that gives back slots already taken when a later one is
// refused. That path needs a request wanting two, and it is covered where it
// lives, in internal/slots.
func TestARefusedProfileIsNotRememberedAsAHolder(t *testing.T) {
	h, stores := handlerWithSlots(t, slots.Limits{Profiles: 1, Idle: time.Hour})

	if code, _ := get(t, h, profileQuery("stat_user_tables")); code != http.StatusOK {
		t.Fatal("the first profile was refused")
	}
	for i := 0; i < 20; i++ {
		if code, _ := get(t, h, profileQuery("stat_user_indexes")); code != http.StatusServiceUnavailable {
			t.Fatalf("refusal %d was answered %d", i, code)
		}
	}
	if held := stores.Slots.Held(slots.Profile); held != 1 {
		t.Errorf("%d profile slots held after twenty refusals, want 1", held)
	}
}

// TestAnUnknownCollectorIsStillA400 keeps validation in front of admission.
//
// A request that names a collector this build does not have is a client error,
// and it must stay one rather than becoming a capacity refusal, and it must
// not reserve anything on its way to failing.
func TestAnUnknownCollectorIsStillA400(t *testing.T) {
	h, stores := handlerWithSlots(t, slots.Limits{Profiles: 1, Idle: time.Hour})

	code, _ := get(t, h, profileQuery("no_such_collector"))
	if code != http.StatusBadRequest {
		t.Errorf("an unknown collector was answered %d, want 400", code)
	}
	if held := stores.Slots.Held(slots.Profile); held != 0 {
		t.Errorf("%d profile slots held by a request that was never valid", held)
	}
}
