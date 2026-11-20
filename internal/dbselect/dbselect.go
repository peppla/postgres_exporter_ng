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

// Package dbselect holds the rule that decides which databases built-in
// monitoring covers.
//
// It lives on its own because two engines apply it. The legacy fan-out and the
// namespaced collectors each build their own target set and must not share
// one, but they must apply the same rule to build it. Two copies of a rule
// that is supposed to be identical is how they stop being identical.
package dbselect

// The reasons a database is not in the selected set, and the reasons a
// selected database produced no data. These are metric label values, so the
// set is closed and short: the monitoring contract allows bounded reason codes
// and forbids SQL text, credentials or arbitrary error strings.
//
// They live here with the selection rule because both engines report against
// the same vocabulary. Each engine counts its own intentions, but "excluded"
// has to mean the same thing in both or the two reports cannot be read
// together.
const (
	// ReasonExcludeList is a database an exclude list named.
	ReasonExcludeList = "exclude_list"
	// ReasonNotIncluded is a database a given include list left out.
	ReasonNotIncluded = "not_in_include_list"

	// ReasonConnect is a database the exporter could not connect to.
	ReasonConnect = "connect"
	// ReasonScrape is a query or collector that failed on a database the
	// exporter did reach.
	ReasonScrape = "scrape"
	// ReasonNotAttempted is work that never started, so it is neither a
	// success nor a connection failure.
	ReasonNotAttempted = "not_attempted"
)

// List is a configured list of database names that remembers whether it was
// configured at all.
//
// An explicitly empty list selects nothing, while an omitted one means no
// restriction. Those are opposite instructions, and a []string cannot tell
// them apart: both arrive with length zero. Collapsing them selects every
// database on a server where the operator asked for none.
type List struct {
	names []string
	given bool
}

// Of records a list the operator supplied, empty or not.
func Of(names []string) List {
	return List{names: names, given: true}
}

// Absent is a list that was not configured.
func Absent() List { return List{} }

// FromFlag converts a comma-separated flag value that has already been split.
// An unset flag reaches this as a slice holding one empty string, because
// strings.Split("", ",") does not return an empty slice, so a list of nothing
// but empty names counts as absent rather than as an explicit empty list.
func FromFlag(names []string) List {
	for _, n := range names {
		if n != "" {
			return Of(names)
		}
	}
	return Absent()
}

// Names returns the configured names. The caller must not modify them.
func (l List) Names() []string { return l.names }

// Given reports whether the list was configured at all.
func (l List) Given() bool { return l.given }

// Result is what the configuration did to the catalog the server reported.
//
// Excluding a database is a decision, not a fault, and the two must not be
// conflated. Keeping the names apart from the count is about cardinality
// rather than principle: the names an operator wrote in an exclude list are
// bounded by the configuration, while the complement of an include list can be
// the whole server, so only the first is reported per database.
type Result struct {
	Selected []string
	// Excluded holds the names the operator listed and the server has, so a
	// typo in the list shows up as a name that never appears here.
	Excluded []string
	// NotIncluded counts the databases left out by a given include list.
	NotIncluded int
}

// Apply applies the exclusion and inclusion rules to the names the server
// offered:
//
//	include omitted, exclude omitted     every eligible database
//	include omitted, exclude given       every eligible database but those named
//	include given and empty              no databases, whatever exclude says
//	include given and non-empty          the named databases, minus exclusions
//
// Exclusion is checked first, so a database named in both lists is reported as
// excluded rather than as not included. Exclusion takes precedence, and the
// report should say which rule applied.
func Apply(names []string, exclude, include List) Result {
	r := Result{Selected: make([]string, 0, len(names))}
	for _, name := range names {
		if contains(exclude.names, name) {
			r.Excluded = append(r.Excluded, name)
			continue
		}
		// An include list that was given restricts the set, including when it
		// is empty: an operator who writes an empty list has asked for no
		// database-local collection, not for all of it.
		if include.given && !contains(include.names, name) {
			r.NotIncluded++
			continue
		}
		r.Selected = append(r.Selected, name)
	}
	return r
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
