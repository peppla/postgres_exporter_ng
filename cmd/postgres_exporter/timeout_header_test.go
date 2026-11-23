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
	"testing"
	"time"
)

// TestRequestedTimeout pins what a scraper can ask for.
//
// The cases that matter are the ones that used to be read as "no header": a
// value too small to be a duration, and a header sent with nothing in it.
// Both took the configured fallback, so a request asking for almost no time
// at all ran for as long as the fallback allows.
func TestRequestedTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  []string
		want    time.Duration
		wantErr string
	}{
		{name: "no header at all", values: nil, want: 0},
		{name: "whole seconds", values: []string{"5"}, want: 5 * time.Second},
		{name: "a fraction", values: []string{"0.25"}, want: 250 * time.Millisecond},
		{name: "a small but expressible value", values: []string{"0.000000001"}, want: time.Nanosecond},
		{
			// Positive, finite, and zero once it is a duration. This used to
			// return 0 with no error, which is what an absent header returns,
			// so the request got the fallback it did not ask for.
			name:    "smaller than a nanosecond",
			values:  []string{"0.0000000001"},
			wantErr: "shorter than the smallest duration",
		},
		{
			// Header.Get cannot tell this from an absent header. Values can.
			name:    "present with no value",
			values:  []string{""},
			wantErr: "sent with no value",
		},
		{name: "present with only spaces", values: []string{"  "}, wantErr: "sent with no value"},
		{name: "not a number", values: []string{"garbage"}, wantErr: "is not a number"},
		{name: "not finite", values: []string{"NaN"}, wantErr: "is not a finite number"},
		{name: "infinite", values: []string{"Inf"}, wantErr: "is not a finite number"},
		{name: "zero", values: []string{"0"}, wantErr: "is not positive"},
		{name: "negative", values: []string{"-1"}, wantErr: "is not positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := requestedTimeout(tc.values)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, got %s", tc.wantErr, got)
				}
				if !contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("requestedTimeout(%q): %s", tc.values, err)
			}
			if got != tc.want {
				t.Errorf("requestedTimeout(%q) = %s, want %s", tc.values, got, tc.want)
			}
		})
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || len(haystack) >= len(needle) && indexOfSub(haystack, needle) >= 0
}

func indexOfSub(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
