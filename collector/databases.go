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

	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
)

// catalogQuery lists the databases this role may connect to, including the one
// the connection is on, which is flagged.
//
// Returning the connection's own database rather than excluding it is what
// lets every target be named without a second round trip. "Connectable" here
// is a catalog property: datallowconn, not a template, and the role holding
// CONNECT. A database that later fails to answer is still part of this set and
// is reported as a failure rather than dropped from it.
const catalogQuery = `SELECT datname, datname = current_database() AS is_current
	FROM pg_database
	WHERE datallowconn = true
	  AND datistemplate = false
	  AND has_database_privilege(current_user, datname, 'connect')`

// dbTarget is one database the collectors will run against.
type dbTarget struct {
	datname string
	dsn     string
	// control marks the database the configured connection is on. Its
	// connection is the one cluster-wide collectors use, so it is opened
	// whether or not it is in the selected set.
	control bool
	// skipDatabaseLocal marks a target that is opened for cluster-wide work
	// only, because the selection rule left its database out. Excluding the
	// control database from database-local collection does not stop its
	// connection being used for cluster metrics.
	skipDatabaseLocal bool
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
}

// selectDatabases reads the catalog over the control connection and applies
// the configured rule to it.
//
// The control database is always a target, whatever the rule says about it.
// Excluding it from database-local collection is a real request and is handled
// by marking it, not by dropping it: its connection is what the cluster-wide
// collectors run on, and §4.0b says cluster-wide collection keeps working when
// database-local collection for that database does not.
func selectDatabases(ctx context.Context, inst *instance, exclude, include dbselect.List) (dbSelection, error) {
	controlName, _ := config.DatabaseFromDSN(inst.dsn)
	// Whatever happens below, the configured connection is a target: it is
	// what the cluster-wide collectors run on.
	alone := func(err error) (dbSelection, error) {
		return dbSelection{
			targets:    []dbTarget{{datname: controlName, dsn: inst.dsn, control: true}},
			discovered: 1,
			ok:         false,
		}, err
	}

	names, catalogControl, err := readCatalog(ctx, inst)
	if err != nil {
		return alone(err)
	}
	if catalogControl != "" {
		controlName = catalogControl
	}

	sel := dbSelection{ok: true, discovered: len(names)}
	if catalogControl != "" {
		sel.discovered++
	}

	// The control database goes through the same rule as the rest. Being left
	// out means no database-local collection there, not that its connection
	// stops being used, so it stays a target either way.
	control := dbTarget{datname: controlName, dsn: inst.dsn, control: true}
	if catalogControl != "" {
		decided := dbselect.Apply([]string{catalogControl}, exclude, include)
		if len(decided.Selected) == 0 {
			control.skipDatabaseLocal = true
			sel.excluded = append(sel.excluded, decided.Excluded...)
			sel.notIncluded += decided.NotIncluded
		}
	}
	sel.targets = append(sel.targets, control)

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
		sel.targets = append(sel.targets, dbTarget{datname: name, dsn: dsn})
	}

	return sel, nil
}

// readCatalog returns the connectable databases other than the connection's
// own, and the connection's own name as the server reports it.
//
// The server's answer is preferred over the name parsed from the DSN, because
// a connection string need not carry a database at all: libpq then falls back
// to the user name, and the catalog knows what that resolved to.
func readCatalog(ctx context.Context, inst *instance) (names []string, control string, err error) {
	rows, err := inst.getDB().QueryContext(ctx, catalogQuery)
	if err != nil {
		return nil, "", fmt.Errorf("listing databases: %w", err)
	}
	defer rows.Close() // nolint: errcheck

	for rows.Next() {
		var name string
		var current bool
		if err := rows.Scan(&name, &current); err != nil {
			return nil, "", fmt.Errorf("reading databases: %w", err)
		}
		if current {
			control = name
			continue
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading databases: %w", err)
	}
	return names, control, nil
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
