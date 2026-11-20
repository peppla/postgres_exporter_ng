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

// Where an application query runs.
//
// A custom query names its own databases. This is the application-query
// engine's selection and it has nothing to do with built-in monitoring's: the
// two sets are never intersected, so an operator can monitor PostgreSQL in one
// set of databases and run application queries in a different one.
//
//	databases omitted     every database the engine reaches
//	databases: [db1, db2] those two
//	databases: []         none
//	master: true          the configured connection database, and only it
//
// master wins over the list. It means the configured connection database, not
// a primary or standby role test and not the first name in the list, and a
// master query runs there even when that database is absent from databases.
// The list can neither suppress a master query nor redirect it.

package main

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

// queryDatabaseList is a per-query databases field that remembers whether it
// was written at all.
//
// An omitted list and an empty one are opposite instructions, and a []string
// cannot tell them apart because both arrive with length zero. The difference
// is the whole of the first two rows above, so it has to survive parsing.
type queryDatabaseList struct {
	names []string
	given bool
}

// UnmarshalYAML records presence as well as content.
func (l *queryDatabaseList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var names []string
	if err := unmarshal(&names); err != nil {
		return fmt.Errorf("databases must be a list of database names: %w", err)
	}
	l.names = names
	l.given = true
	return nil
}

// rejectNullDatabases fails a query file that writes `databases:` with nothing
// after it.
//
// A null and an omitted key look alike in a file and mean opposite things
// here, one being every database and the other being none, so guessing which
// the author meant is how an application query silently runs everywhere or
// nowhere.
//
// This cannot be done in UnmarshalYAML. gopkg.in/yaml.v2 does not call a
// custom unmarshaler at all for a null value, which was measured rather than
// assumed, so the key's presence has to be read from the document separately
// from the typed parse.
func rejectNullDatabases(content []byte) error {
	var raw map[string]map[string]interface{}
	if err := yaml.Unmarshal(content, &raw); err != nil {
		// The typed parse reports its own error for a malformed file. This
		// pass only looks for one specific shape and stays quiet otherwise.
		return nil
	}
	for name, fields := range raw {
		value, present := fields["databases"]
		if present && value == nil {
			return fmt.Errorf("query %q: databases is present with no value: write an explicit [] for no databases, or leave the field out for all of them", name)
		}
	}
	return nil
}

// runsOn reports whether a query with this list, and this master flag, should
// run against the named database.
//
// isMaster says whether the connection is the configured one. master: true
// queries run there and nowhere else, whatever the list says, which is checked
// here rather than at parse time because it is a statement about execution.
func (l queryDatabaseList) runsOn(datname string, master, isMaster bool) bool {
	if master {
		return isMaster
	}
	if !l.given {
		return true
	}
	for _, n := range l.names {
		if n == datname {
			return true
		}
	}
	return false
}
