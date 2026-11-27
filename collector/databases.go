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

// Which databases the namespaced collectors run against.
//
// This set is built here and nowhere else. The legacy engine builds its own,
// and the two are never intersected: an operator can monitor PostgreSQL in one
// set of databases and run application queries in a different set, and
// filtering a set that was already narrowed for the other engine would not
// implement that. They apply the same rule, from internal/dbselect, to their
// own inputs.

package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/targetid"
)

// catalogQuery lists every database the server has, says which one the
// connection is on, and says which of them this role may collect from.
//
// Returning the connection's own database rather than excluding it is what
// lets every target be named without a second round trip.
//
// Eligibility is a column rather than a WHERE clause, so that one read answers
// two questions that look the same and are not. Which databases may be
// collected from is datallowconn, not a template, and the role holding
// CONNECT. Which databases exist is all of them. A filtered read cannot
// authorise forgetting a database, because a database that lost CONNECT is
// missing from it for a reason that has nothing to do with existing, and
// removing its history on that basis would throw away the record at the moment
// it explains something.
//
// A database that later fails to answer is still eligible and is reported as a
// failure rather than dropped.
const catalogQuery = `SELECT datname, datname = current_database() AS is_current,
	       datallowconn AND NOT datistemplate
	         AND has_database_privilege(current_user, datname, 'connect') AS eligible
	FROM pg_database`

// dbTarget is one database the collectors will run against.
type dbTarget struct {
	datname string
	dsn     string
	// target is the identity of this database's connection, which is what its
	// records are keyed by.
	target string
	// control marks the database the configured connection is on. Its
	// connection is the one cluster-wide collectors use, so it is opened
	// whether or not it is in the selected set.
	control bool
	// skipDatabaseLocal marks a target that is opened for cluster-wide work
	// only, because the selection rule left its database out. Excluding the
	// control database from database-local collection does not stop its
	// connection being used for cluster metrics.
	skipDatabaseLocal bool

	// baselineConnected marks a target this scrape has already connected to
	// once, for the cluster-wide work and the catalogue read. Only the
	// configured connection database is ever marked.
	//
	// It exists so a deferral does not contradict an observation. The
	// connection to this database was made and it worked. If its own turn for
	// database-local work never comes, saying it could not be reached would
	// publish an assertion against something already seen.
	baselineConnected bool
}

// dbSelection is what one pass over the catalog decided.
type dbSelection struct {
	targets []dbTarget
	// discovered is every connectable database the server reported.
	discovered int
	// excluded names the databases an exclude list matched. Databases a given
	// include list left out are counted in notIncluded and not named, because
	// that set can be the whole server.
	excluded    []string
	notIncluded int
	// ok is false when the catalog could not be read. The control database is
	// still a target in that case, so coverage collapses to one database, and
	// saying so is the difference between that and a one-database server.
	ok bool
	// catalogRead says whether discovered is a number the server gave. When
	// the catalog could not be read there is no total to publish, and the
	// total it would otherwise report is 1, which looks exactly like a
	// one-database server.
	catalogRead bool
	// inventory is every database the server reported, eligible or not, and
	// is set only when the read completed. It is what authorises forgetting a
	// database, and only a complete unfiltered read can.
	inventory []string
	// taken is when the catalogue read began, which is the generation of the
	// inventory. Before the read rather than after it: what it may authorise
	// forgetting is what was already there when it started looking.
	taken time.Time
	// release ends the holds this selection took on the connections it built.
	// Called when the request is done with them, which is not when one
	// database's collection finishes: a request can record under a connection
	// at any point before it ends.
	release []func()
}

// done ends every hold this selection took.
func (s dbSelection) done() {
	for _, release := range s.release {
		release()
	}
}

// selectDatabases reads the catalog over the control connection and applies
// the configured rule to it.
//
// The control database is always a target, whatever the rule says about it.
// Excluding it from database-local collection is a real request and is handled
// by marking it, not by dropping it: its connection is what the cluster-wide
// collectors run on, and §4.0b says cluster-wide collection keeps working when
// database-local collection for that database does not.
func selectDatabases(ctx context.Context, inst *instance, exclude, include dbselect.List, store *lastsuccess.Store) (dbSelection, error) {
	controlName, _ := config.DatabaseFromDSN(inst.dsn)
	seed, _ := targetid.ID(inst.dsn)

	// controlTargetFor applies the rule to the configured database. It runs on
	// every path, including the ones where discovery failed: a database the
	// operator excluded stays excluded whether or not the catalog could be
	// read, and an earlier version skipped the rule on the failure paths, so a
	// catalog error turned an exclusion back into collection.
	//
	// The target itself is always present. Being left out means no
	// database-local collection there, not that the connection stops being
	// used for cluster-wide work.
	controlTargetFor := func(sel *dbSelection, name string) dbTarget {
		t := dbTarget{datname: name, dsn: inst.dsn, control: true}
		if name == "" {
			// No name to test the rule against. With any restriction
			// configured, that is not a database this engine can show was
			// wanted, so it gets cluster-wide work only.
			if include.Given() || len(exclude.Names()) > 0 {
				t.skipDatabaseLocal = true
				sel.notIncluded++
			}
			return t
		}
		decided := dbselect.Apply([]string{name}, exclude, include)
		if len(decided.Selected) == 0 {
			t.skipDatabaseLocal = true
			sel.excluded = append(sel.excluded, decided.Excluded...)
			sel.notIncluded += decided.NotIncluded
		}
		return t
	}

	// Whatever happens below, the configured connection is a target: it is
	// what the cluster-wide collectors run on.
	//
	// The name it is reported and filtered under comes from the server, not
	// from the connection string. A DSN can name two different databases, in
	// its path and in a dbname parameter, and which one is connected to
	// depends on a sort order inside the driver. Asking costs one row and only
	// on the path where discovery has already failed, and it turns a guess
	// that decides whether an exclusion applies into a fact.
	alone := func(err error) (dbSelection, error) {
		if name, qerr := queryCurrentDatabase(ctx, inst); qerr == nil {
			controlName = name
		}
		sel := dbSelection{ok: false}
		sel.targets = []dbTarget{controlTargetFor(&sel, controlName)}
		return sel, err
	}

	taken := time.Now()
	names, inventory, catalogControl, err := readCatalog(ctx, inst)
	if err != nil {
		return alone(err)
	}
	if catalogControl != "" {
		controlName = catalogControl
	}

	sel := dbSelection{ok: true, catalogRead: true, discovered: len(names),
		inventory: inventory, taken: taken}
	if catalogControl != "" {
		sel.discovered++
	}

	sel.targets = append(sel.targets, controlTargetFor(&sel, controlName))

	applied := dbselect.Apply(names, exclude, include)
	sel.excluded = append(sel.excluded, applied.Excluded...)
	sel.notIncluded += applied.NotIncluded

	for _, name := range applied.Selected {
		dsn, err := config.DSNWithDatabase(inst.dsn, name)
		if err != nil {
			// The control DSN parsed when the instance was built, so this
			// should not happen. Dropping the database quietly would be a
			// shortfall nothing reports, so the selection is marked as
			// incomplete instead.
			sel.ok = false
			continue
		}
		// Registered as built from this connection, which is what lets this
		// server's catalogue decide its state is gone, and held until this
		// request is done with it. Registered here because here is where the
		// connection string is built.
		id, _ := targetid.ID(dsn)
		sel.release = append(sel.release, store.Own(seed, id, name))
		sel.targets = append(sel.targets, dbTarget{datname: name, dsn: dsn, target: id})
	}

	return sel, nil
}

// readCatalog returns the connectable databases other than the connection's
// own, and the connection's own name as the server reports it.
//
// The server's answer is preferred over the name parsed from the DSN, because
// a connection string need not carry a database at all: libpq then falls back
// to the user name, and the catalog knows what that resolved to.
func readCatalog(ctx context.Context, inst *instance) (names, inventory []string, control string, err error) {
	rows, err := inst.getDB().QueryContext(ctx, catalogQuery)
	if err != nil {
		return nil, nil, "", fmt.Errorf("listing databases: %w", err)
	}
	defer rows.Close() // nolint: errcheck

	for rows.Next() {
		var name string
		var current, eligible bool
		if err := rows.Scan(&name, &current, &eligible); err != nil {
			return nil, nil, "", fmt.Errorf("reading databases: %w", err)
		}
		// Every database the server has, templates and unreachable ones
		// included. This is the half that says what exists.
		inventory = append(inventory, name)
		if !eligible {
			continue
		}
		if current {
			control = name
			continue
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "", fmt.Errorf("reading databases: %w", err)
	}
	return names, inventory, control, nil
}

// controlTarget returns the target for the configured connection, which is
// always present.
func (s dbSelection) controlTarget() *dbTarget {
	for i := range s.targets {
		if s.targets[i].control {
			return &s.targets[i]
		}
	}
	return nil
}

// queryCurrentDatabase asks the server which database this connection is on.
//
// The catalog query answers this as a side effect when it works. This exists
// for when it does not: the connection is still healthy, and the name decides
// whether the configured exclusion and inclusion rules apply to this database,
// so it is worth one row rather than a guess at a connection string.
func queryCurrentDatabase(ctx context.Context, inst *instance) (string, error) {
	var name string
	err := inst.getDB().QueryRowContext(ctx, "SELECT current_database()").Scan(&name)
	return name, err
}
