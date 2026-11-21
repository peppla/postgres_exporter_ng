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

package budget

import (
	"strings"
	"testing"
	"time"
)

// TestDefaultsMatchTheContract pins the approved values. They are a decision,
// not an implementation detail, so a change to one should fail a test rather
// than ship.
func TestDefaultsMatchTheContract(t *testing.T) {
	d := Defaults()
	for _, tc := range []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"connections", d.Connections, int64(5)},
		{"concurrent requests", d.ConcurrentRequests, int64(4)},
		{"request timeout fallback", d.RequestTimeout, 10 * time.Second},
		{"maximum request budget", d.MaxRequestTimeout, 60 * time.Second},
		{"response reserve cap", d.ResponseReserveMax, time.Second},
		{"response reserve fraction", d.ResponseReserveFraction, 0.10},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, tc.got, tc.want)
		}
	}
	if err := d.Validate(); err != nil {
		t.Errorf("the approved defaults do not validate: %s", err)
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*Limits)
		wantErr string
	}{
		{"the defaults", func(*Limits) {}, ""},
		{"no connections", func(l *Limits) { l.Connections = 0 }, "connections must be at least 1"},
		{"no requests", func(l *Limits) { l.ConcurrentRequests = 0 }, "concurrent_requests must be at least 1"},
		{"a negative fallback", func(l *Limits) { l.RequestTimeout = -time.Second }, "request_timeout must be positive"},
		{"a zero cap", func(l *Limits) { l.MaxRequestTimeout = 0 }, "max_request_timeout must be positive"},
		{
			// A fallback above the cap can never be used, because the cap
			// shortens it. Written that way it means something other than
			// what it says, so it is refused.
			name:    "a fallback longer than the cap",
			change:  func(l *Limits) { l.RequestTimeout = 90 * time.Second },
			wantErr: "could never be used",
		},
		{"a zero reserve cap", func(l *Limits) { l.ResponseReserveMax = 0 }, "response_reserve.max must be positive"},
		{
			// A fraction of one reserves the whole budget and collects
			// nothing, which is a configuration that produces empty scrapes
			// rather than slow ones.
			name:    "a fraction of one",
			change:  func(l *Limits) { l.ResponseReserveFraction = 1 },
			wantErr: "above 0 and below 1",
		},
		{"a fraction of zero", func(l *Limits) { l.ResponseReserveFraction = 0 }, "above 0 and below 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := Defaults()
			tc.change(&l)
			err := l.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q, got none", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestRequestBudget pins the rule that nothing can lengthen a budget.
func TestRequestBudget(t *testing.T) {
	l := Defaults()
	for _, tc := range []struct {
		name            string
		asked           time.Duration
		parentRemaining time.Duration
		want            time.Duration
	}{
		{"no header falls back", 0, 0, 10 * time.Second},
		{"a shorter request is honoured", 5 * time.Second, 0, 5 * time.Second},
		{"a longer request is capped", 5 * time.Minute, 0, 60 * time.Second},
		{"a parent deadline shortens the fallback", 0, 3 * time.Second, 3 * time.Second},
		{"a parent deadline shortens a request", 20 * time.Second, 4 * time.Second, 4 * time.Second},
		{"a parent deadline cannot lengthen one", 5 * time.Second, 30 * time.Second, 5 * time.Second},
		{"a parent deadline cannot beat the cap", 5 * time.Minute, 5 * time.Minute, 60 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := l.RequestBudget(tc.asked, tc.parentRemaining); got != tc.want {
				t.Errorf("RequestBudget(%s, %s) = %s, want %s", tc.asked, tc.parentRemaining, got, tc.want)
			}
		})
	}
}

// TestCollectionBudgetLeavesTheReserve covers the half of the rule that is
// easy to get backwards: the reserve is the smaller of a cap and a fraction,
// so a short budget keeps most of itself for collection.
func TestCollectionBudgetLeavesTheReserve(t *testing.T) {
	l := Defaults()
	for _, tc := range []struct {
		name        string
		asked       time.Duration
		wantReserve time.Duration
		wantCollect time.Duration
	}{
		{"the fallback", 0, time.Second, 9 * time.Second},
		{"a long budget reserves the cap, not a tenth", 60 * time.Second, time.Second, 59 * time.Second},
		{"a short budget reserves a tenth, not the cap", 2 * time.Second, 200 * time.Millisecond, 1800 * time.Millisecond},
		{"a very short budget still collects", 100 * time.Millisecond, 10 * time.Millisecond, 90 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := l.RequestBudget(tc.asked, 0)
			if got := l.ResponseReserve(budget); got != tc.wantReserve {
				t.Errorf("ResponseReserve(%s) = %s, want %s", budget, got, tc.wantReserve)
			}
			if got := l.CollectionBudget(tc.asked, 0); got != tc.wantCollect {
				t.Errorf("CollectionBudget(%s) = %s, want %s", tc.asked, got, tc.wantCollect)
			}
		})
	}
}

// TestCollectionBudgetIsAlwaysPositive is the invariant the fraction bound
// exists for. Validate rejects a fraction of one or more, so there is no
// configuration where the reserve swallows the whole budget and collection
// gets no time at all.
func TestCollectionBudgetIsAlwaysPositive(t *testing.T) {
	l := Defaults()
	for _, budget := range []time.Duration{
		time.Nanosecond, time.Microsecond, time.Millisecond,
		time.Second, time.Minute, time.Hour,
	} {
		if got := l.CollectionBudget(budget, 0); got <= 0 {
			t.Errorf("a budget of %s left %s for collection", budget, got)
		}
	}
}
