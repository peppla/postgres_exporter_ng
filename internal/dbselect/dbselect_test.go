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

package dbselect

import (
	"reflect"
	"testing"
)

// TestApply pins the selection rule, which decides coverage and had no test
// before it was extracted here. The rule is: exclusion wins, an inclusion list that was
// given is a whitelist, and an inclusion list that was given empty selects
// nothing rather than everything.
func TestApply(t *testing.T) {
	names := []string{"postgres", "app", "tenant_1", "template0"}

	for _, tc := range []struct {
		name    string
		exclude List
		include List
		want    []string
	}{
		{
			name: "no filters keeps everything in order",
			want: []string{"postgres", "app", "tenant_1", "template0"},
		},
		{
			name:    "exclusion removes by exact name",
			exclude: Of([]string{"template0", "postgres"}),
			want:    []string{"app", "tenant_1"},
		},
		{
			name:    "a non-empty inclusion list is a whitelist",
			include: Of([]string{"app"}),
			want:    []string{"app"},
		},
		{
			name:    "exclusion beats inclusion for the same name",
			exclude: Of([]string{"app"}),
			include: Of([]string{"app", "tenant_1"}),
			want:    []string{"tenant_1"},
		},
		{
			name:    "inclusion naming nothing present selects nothing",
			include: Of([]string{"absent"}),
			want:    []string{},
		},
		{
			name:    "excluding everything selects nothing",
			exclude: Of(names),
			want:    []string{},
		},
		{
			// Pep, 2026-11-20. The opposite reading, that an empty list means
			// no restriction, is what a plain []string gives you by accident.
			name:    "an inclusion list given empty selects nothing",
			include: Of([]string{}),
			want:    []string{},
		},
		{
			name:    "an inclusion list given empty beats an exclusion list too",
			exclude: Of([]string{"app"}),
			include: Of([]string{}),
			want:    []string{},
		},
		{
			name:    "an exclusion list given empty excludes nothing",
			exclude: Of([]string{}),
			want:    []string{"postgres", "app", "tenant_1", "template0"},
		},
		{
			// The inherited flag surface: unset arrives as one empty string.
			name:    "an unset comma-separated flag is an absent list",
			exclude: FromFlag([]string{""}),
			include: FromFlag([]string{""}),
			want:    []string{"postgres", "app", "tenant_1", "template0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Apply(names, tc.exclude, tc.include)
			if !reflect.DeepEqual(got.Selected, tc.want) {
				t.Errorf("got %q, want %q", got.Selected, tc.want)
			}
			// Everything the rule left out is accounted for, as one or the
			// other. A database that fell out of both would be invisible.
			if len(got.Selected)+len(got.Excluded)+got.NotIncluded != len(names) {
				t.Errorf("%d selected + %d excluded + %d not included != %d offered",
					len(got.Selected), len(got.Excluded), got.NotIncluded, len(names))
			}
			for _, name := range got.Excluded {
				if !contains(tc.exclude.Names(), name) {
					t.Errorf("%q reported as excluded but is not in the exclude list", name)
				}
			}
			// The input must not be modified: the caller reuses it.
			if !reflect.DeepEqual(names, []string{"postgres", "app", "tenant_1", "template0"}) {
				t.Errorf("Apply modified its input: %q", names)
			}
		})
	}
}
