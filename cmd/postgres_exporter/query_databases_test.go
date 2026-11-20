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

//go:build !integration

package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

// TestQueryDatabaseListParsing pins the distinction an omitted list and an
// empty one have to keep. They are opposite instructions and a []string cannot
// tell them apart, so the presence has to survive YAML.
func TestQueryDatabaseListParsing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yaml    string
		given   bool
		names   []string
		wantErr string
	}{
		{
			name:  "omitted means all databases",
			yaml:  "q:\n  query: SELECT 1\n",
			given: false,
		},
		{
			name:  "explicit empty means none",
			yaml:  "q:\n  query: SELECT 1\n  databases: []\n",
			given: true,
			names: []string{},
		},
		{
			name:  "a list of names",
			yaml:  "q:\n  query: SELECT 1\n  databases:\n    - db1\n    - db2\n",
			given: true,
			names: []string{"db1", "db2"},
		},
		{
			// A null is not an omission. The two look alike in a file and mean
			// opposite things, so guessing is how a query silently runs
			// everywhere or nowhere.
			name:    "a null value is rejected",
			yaml:    "q:\n  query: SELECT 1\n  databases:\n",
			wantErr: "write an explicit []",
		},
		{
			name:    "a scalar is rejected",
			yaml:    "q:\n  query: SELECT 1\n  databases: db1\n",
			wantErr: "must be a list",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queries UserQueries
			err := yaml.Unmarshal([]byte(tc.yaml), &queries)
			if err == nil {
				err = rejectNullDatabases([]byte(tc.yaml))
			}
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
				t.Fatalf("unmarshal: %s", err)
			}
			got := queries["q"].Databases
			if got.given != tc.given {
				t.Errorf("given = %v, want %v", got.given, tc.given)
			}
			if len(got.names) != len(tc.names) {
				t.Errorf("names = %q, want %q", got.names, tc.names)
			}
		})
	}
}

// TestQueryRunsOn pins where a query runs, including the precedence the
// contract is explicit about: master wins over the list, and means the
// configured connection database even when that database is absent from it.
func TestQueryRunsOn(t *testing.T) {
	absent := queryDatabaseList{}
	empty := queryDatabaseList{given: true, names: []string{}}
	listed := queryDatabaseList{given: true, names: []string{"db1", "db2"}}

	for _, tc := range []struct {
		name     string
		list     queryDatabaseList
		master   bool
		datname  string
		isMaster bool
		want     bool
	}{
		{"omitted list runs everywhere", absent, false, "anything", false, true},
		{"omitted list runs on the master too", absent, false, "postgres", true, true},
		{"empty list runs nowhere", empty, false, "db1", false, false},
		{"empty list runs nowhere, master included", empty, false, "postgres", true, false},
		{"a listed database runs", listed, false, "db1", false, true},
		{"an unlisted database does not", listed, false, "db3", false, false},
		{"an unlisted database does not, even as master", listed, false, "postgres", true, false},

		// master: true means the configured connection database and only it.
		{"master runs on the master", listed, true, "postgres", true, true},
		{"master does not run elsewhere", listed, true, "db1", false, false},
		{"master beats an empty list", empty, true, "postgres", true, true},
		{"master beats a list that omits the master", listed, true, "postgres", true, true},
		{"master with no list still means the master alone", absent, true, "db1", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.list.runsOn(tc.datname, tc.master, tc.isMaster); got != tc.want {
				t.Errorf("runsOn(%q, master=%v, isMaster=%v) = %v, want %v",
					tc.datname, tc.master, tc.isMaster, got, tc.want)
			}
		})
	}
}
