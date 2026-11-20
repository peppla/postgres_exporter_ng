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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
)

// loadConfig parses a configuration file written for one test.
func loadConfig(t *testing.T, content string) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postgres_exporter.yml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the fixture: %s", err)
	}
	h := &config.Handler{}
	if err := h.ReloadConfig(path, log.NewNopLogger()); err != nil {
		t.Fatalf("loading the fixture: %s", err)
	}
	return h.GetConfig()
}

// TestBuiltinDatabaseSelection pins where built-in monitoring's database set
// comes from, and what happens when it is configured twice.
func TestBuiltinDatabaseSelection(t *testing.T) {
	// An unset flag arrives as a slice holding one empty string, because
	// strings.Split("", ",") does not return an empty slice.
	unset := []string{""}

	for _, tc := range []struct {
		name        string
		content     string
		excludeFlag []string
		includeFlag []string
		wantErr     string
		wantExc     dbselect.List
		wantInc     dbselect.List
	}{
		{
			name:        "nothing configured anywhere",
			content:     "auth_modules: {}\n",
			excludeFlag: unset, includeFlag: unset,
			wantExc: dbselect.Absent(), wantInc: dbselect.Absent(),
		},
		{
			name:        "the flags alone",
			content:     "auth_modules: {}\n",
			excludeFlag: []string{"template_like"}, includeFlag: unset,
			wantExc: dbselect.Of([]string{"template_like"}), wantInc: dbselect.Absent(),
		},
		{
			name:        "the file alone",
			content:     "monitoring:\n  databases:\n    include: [app]\n",
			excludeFlag: unset, includeFlag: unset,
			wantExc: dbselect.Absent(), wantInc: dbselect.Of([]string{"app"}),
		},
		{
			// The whole point of carrying presence this far: an empty list in
			// the file has to arrive as an empty list, not as no list.
			name:        "an empty list in the file stays empty",
			content:     "monitoring:\n  databases:\n    include: []\n",
			excludeFlag: unset, includeFlag: unset,
			wantExc: dbselect.Absent(), wantInc: dbselect.Of([]string{}),
		},
		{
			// Each key is independent of the other, so a file that writes one
			// of them leaves the other flag in place.
			name:        "one key from the file, the other from a flag",
			content:     "monitoring:\n  databases:\n    include: [app]\n",
			excludeFlag: []string{"staging"}, includeFlag: unset,
			wantExc: dbselect.Of([]string{"staging"}), wantInc: dbselect.Of([]string{"app"}),
		},
		{
			name:        "both sources for include",
			content:     "monitoring:\n  databases:\n    include: [app]\n",
			excludeFlag: unset, includeFlag: []string{"other"},
			wantErr: "monitoring.databases.include and --include-databases",
		},
		{
			name:        "both sources for exclude",
			content:     "monitoring:\n  databases:\n    exclude: [staging]\n",
			excludeFlag: []string{"other"}, includeFlag: unset,
			wantErr: "monitoring.databases.exclude and --exclude-databases",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exclude, include, err := builtinDatabaseSelection(
				loadConfig(t, tc.content), tc.excludeFlag, tc.includeFlag)

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
				t.Fatalf("resolving the selection: %s", err)
			}
			sameList(t, "exclude", exclude, tc.wantExc)
			sameList(t, "include", include, tc.wantInc)
		})
	}
}

// TestBuiltinDatabaseSelectionWithoutAConfigFile checks the path taken when no
// configuration file was given at all, where GetConfig returns nil.
func TestBuiltinDatabaseSelectionWithoutAConfigFile(t *testing.T) {
	exclude, include, err := builtinDatabaseSelection(nil, []string{"staging"}, []string{""})
	if err != nil {
		t.Fatalf("resolving the selection: %s", err)
	}
	sameList(t, "exclude", exclude, dbselect.Of([]string{"staging"}))
	sameList(t, "include", include, dbselect.Absent())
}

func sameList(t *testing.T, key string, got, want dbselect.List) {
	t.Helper()
	if got.Given() != want.Given() {
		t.Errorf("%s: Given() = %v, want %v", key, got.Given(), want.Given())
	}
	if len(got.Names()) != len(want.Names()) {
		t.Fatalf("%s: got %v, want %v", key, got.Names(), want.Names())
	}
	for i := range want.Names() {
		if got.Names()[i] != want.Names()[i] {
			t.Errorf("%s: got %v, want %v", key, got.Names(), want.Names())
		}
	}
}
