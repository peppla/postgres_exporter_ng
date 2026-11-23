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

// Which databases built-in monitoring runs against.
//
//	monitoring:
//	  databases:
//	    include: [app, reports]
//	    exclude: [staging]
//
//	neither key     every connectable, non-template database
//	include only    the listed databases
//	exclude only    every eligible database but the listed ones
//	both            the included databases minus the excluded ones
//	include: []     no databases, whatever exclude says
//
// Exclusion wins over inclusion. These keys configure built-in monitoring
// alone. Application queries keep their own per-query databases list, and no
// key here narrows it.

package config

import (
	"fmt"

	"github.com/percona/postgres_exporter/internal/dbselect"
	"gopkg.in/yaml.v3"
)

// Monitoring is the built-in monitoring section of the configuration file.
type Monitoring struct {
	Databases DatabaseSelection `yaml:"databases"`
}

// DatabaseSelection is the include and exclude pair.
type DatabaseSelection struct {
	Include NameList `yaml:"include"`
	Exclude NameList `yaml:"exclude"`
}

// NameList is a list of database names that remembers whether its key was
// written at all.
//
// An omitted list and an empty one are opposite instructions. Omitting include
// selects every eligible database and writing `include: []` selects none, and
// a plain []string cannot tell them apart because both arrive with length
// zero. The difference has to survive parsing or the configuration means
// whichever of the two the code happens to assume.
type NameList struct {
	names []string
	given bool
}

// Given reports whether the key was written.
func (l NameList) Given() bool { return l.given }

// Names returns the listed names.
func (l NameList) Names() []string { return l.names }

// Selection converts the list into the form both engines' selection rule takes.
func (l NameList) Selection() dbselect.List {
	if !l.given {
		return dbselect.Absent()
	}
	return dbselect.Of(l.names)
}

// UnmarshalYAML records presence as well as content.
func (l *NameList) UnmarshalYAML(value *yaml.Node) error {
	var names []string
	if err := value.Decode(&names); err != nil {
		return fmt.Errorf("must be a list of database names: %w", err)
	}
	l.names = names
	l.given = true
	return nil
}

// rejectNullSelectionLists fails a configuration that writes one of these keys
// with nothing after it.
//
// `include:` with no value and `include: []` look similar in a file and mean
// opposite things, one selecting every database and the other selecting none.
// Guessing which the author meant is how a server ends up monitoring
// everything or nothing without anyone being told.
//
// It cannot be done in UnmarshalYAML. gopkg.in/yaml.v3 does not call a custom
// unmarshaler at all for a null value, measured rather than assumed, so the
// key's presence has to be read from the document separately from the typed
// parse. The same is true of yaml.v2, which the query-file engine uses.
func rejectNullSelectionLists(content []byte) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(content, &doc); err != nil {
		// The typed parse reports its own error for a malformed file. This
		// pass looks for one shape and stays quiet about everything else.
		return nil
	}
	databases := childOf(childOf(documentRoot(&doc), "monitoring"), "databases")
	for _, key := range []string{"include", "exclude"} {
		value := childOf(databases, key)
		if value != nil && value.Tag == "!!null" {
			return fmt.Errorf("monitoring.databases.%s is present with no value: write an explicit [] for no databases, or leave the key out for all of them", key)
		}
	}
	return nil
}
