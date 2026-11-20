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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
)

// writeConfig puts a configuration file on disk and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postgres_exporter.yml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the fixture: %s", err)
	}
	return path
}

// TestMonitoringDatabaseKeys pins what each way of writing the two keys means.
//
// The case this exists for is the difference between a key left out and a key
// written as an empty list. They are opposite instructions, and a []string
// cannot tell them apart because both arrive with length zero.
func TestMonitoringDatabaseKeys(t *testing.T) {
	for _, tc := range []struct {
		name     string
		content  string
		wantErr  string
		incGiven bool
		incNames []string
		excGiven bool
		excNames []string
	}{
		{
			name:    "neither key",
			content: "auth_modules: {}\n",
		},
		{
			name:     "an empty include list selects nothing",
			content:  "monitoring:\n  databases:\n    include: []\n",
			incGiven: true, incNames: []string{},
		},
		{
			name:     "a list of names",
			content:  "monitoring:\n  databases:\n    include:\n      - app\n      - reports\n",
			incGiven: true, incNames: []string{"app", "reports"},
		},
		{
			name:     "both keys",
			content:  "monitoring:\n  databases:\n    include: [app]\n    exclude: [staging]\n",
			incGiven: true, incNames: []string{"app"},
			excGiven: true, excNames: []string{"staging"},
		},
		{
			name:     "an empty exclude list is not an absent one",
			content:  "monitoring:\n  databases:\n    exclude: []\n",
			excGiven: true, excNames: []string{},
		},
		{
			// A null and an omission mean opposite things here, so the file
			// is refused rather than read as one of them.
			name:    "include written with no value",
			content: "monitoring:\n  databases:\n    include:\n",
			wantErr: "monitoring.databases.include is present with no value",
		},
		{
			name:    "exclude written with no value",
			content: "monitoring:\n  databases:\n    exclude:\n",
			wantErr: "monitoring.databases.exclude is present with no value",
		},
		{
			name:    "an explicit null is the same mistake",
			content: "monitoring:\n  databases:\n    include: null\n",
			wantErr: "monitoring.databases.include is present with no value",
		},
		{
			name:    "a misspelled key is refused rather than ignored",
			content: "monitoring:\n  databases:\n    includes: [app]\n",
			wantErr: "field includes not found",
		},
		{
			name:    "a scalar where a list belongs",
			content: "monitoring:\n  databases:\n    include: app\n",
			wantErr: "must be a list of database names",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{}
			err := h.ReloadConfig(writeConfig(t, tc.content), log.NewNopLogger())

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

			sel := h.GetConfig().Monitoring.Databases
			checkList(t, "include", sel.Include, tc.incGiven, tc.incNames)
			checkList(t, "exclude", sel.Exclude, tc.excGiven, tc.excNames)
		})
	}
}

func checkList(t *testing.T, key string, got NameList, wantGiven bool, wantNames []string) {
	t.Helper()
	if got.Given() != wantGiven {
		t.Errorf("%s: Given() = %v, want %v", key, got.Given(), wantGiven)
	}
	if len(got.Names()) != len(wantNames) {
		t.Fatalf("%s: got %v, want %v", key, got.Names(), wantNames)
	}
	for i := range wantNames {
		if got.Names()[i] != wantNames[i] {
			t.Errorf("%s: got %v, want %v", key, got.Names(), wantNames)
		}
	}

	// The selection the engines consume has to carry presence too, or the
	// distinction is lost one layer below where it was parsed.
	if sel := got.Selection(); sel.Given() != wantGiven {
		t.Errorf("%s: Selection().Given() = %v, want %v", key, sel.Given(), wantGiven)
	}
}
