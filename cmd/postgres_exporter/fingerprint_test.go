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

import "testing"

// TestParseFingerprintAcceptsQuotedValues is the regression for a connection
// string that could not be used at all.
//
// The fingerprint is taken before anything connects, so a string it rejects
// never reaches a database. It split the keyword form on plain spaces, so any
// quoted value containing one failed: an options string, a password with a
// space, an application_name with a space.
func TestParseFingerprintAcceptsQuotedValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "a plain url",
			dsn:  "postgresql://u:p@127.0.0.1:5432/db?sslmode=disable",
			want: "127.0.0.1:5432",
		},
		{
			name: "a url carrying session timeouts",
			dsn:  "postgresql://u:p@127.0.0.1:5432/db?options=-c+statement_timeout%3D5000+-c+lock_timeout%3D500&sslmode=disable",
			want: "127.0.0.1:5432",
		},
		{
			name: "a keyword string with an options value",
			dsn:  "host=127.0.0.1 port=5432 options='-c statement_timeout=5000 -c lock_timeout=500'",
			want: "127.0.0.1:5432",
		},
		{
			name: "a password with a space",
			dsn:  "host=127.0.0.1 port=5432 password='two words'",
			want: "127.0.0.1:5432",
		},
		{
			name: "defaults when the string says nothing",
			dsn:  "dbname=postgres",
			want: "localhost:5432",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFingerprint(tc.dsn)
			if err != nil {
				t.Fatalf("parseFingerprint(%q): %s", tc.dsn, err)
			}
			if got != tc.want {
				t.Errorf("parseFingerprint(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}
