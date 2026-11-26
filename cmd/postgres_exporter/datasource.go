// Copyright 2021 The Prometheus Authors
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

package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/admission"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
)

// The reason codes both engines report against live in internal/dbselect, so
// "excluded" means the same thing wherever it is published.
const (
	reasonExcludeList  = dbselect.ReasonExcludeList
	reasonConnect      = dbselect.ReasonConnect
	reasonScrape       = dbselect.ReasonScrape
	reasonNotAttempted = dbselect.ReasonNotAttempted
	reasonNoPermit     = dbselect.ReasonNoPermit
	reasonDeadline     = dbselect.ReasonDeadline
	reasonCancelled    = dbselect.ReasonCancelled
)

// deferralReason is the bounded label for work that was not admitted.
//
// Called only where the decision is not admission.Admitted. Admitted work is not
// deferred, and giving it a reason anyway would publish a database as deferred
// while it was being scraped.
func deferralReason(d admission.Decision) string {
	switch d {
	case admission.Cancelled:
		return reasonCancelled
	case admission.Deadline:
		return reasonDeadline
	default:
		return reasonNoPermit
	}
}

// databaseRef identifies one database for reporting: which server, and which
// database on it.
//
// The server half is not decoration. The exporter accepts several DSNs, and
// two servers can each hold a database called postgres. Keyed on the name
// alone, their outcomes land on the same series and one overwrites the other,
// so a failure on one server can be erased by a success on another. It is the
// same host:port fingerprint the exporter already labels its other
// per-server metrics with, so it carries no credentials.
type databaseRef struct {
	server  string
	datname string
	// identity is the process's identity for the connection this database is
	// reached over. The server label is host and port, which several targets
	// share, so it cannot key anything remembered between scrapes. Empty for a
	// database that was excluded, which is never connected to.
	identity string
}

// scrapeTarget is one database the scrape will try to cover, and how to report
// it.
//
// The name is what lets a shortfall say which database went dark. It is known
// whenever discovery ran, because the catalog query now returns the connection
// database alongside the rest. It is empty when discovery is disabled, where
// the only target is the configured DSN and the name would have to come from
// parsing a connection string.
type scrapeTarget struct {
	dsn string
	// seed is the configured connection this target was discovered through.
	//
	// Kept because the databases of one seed are scheduled together: a
	// schedule handed one seed's members forgets every member of the others,
	// so each seed needs its own.
	seed string
	databaseRef
}

// discovery is what one pass over the seed DSNs worked out.
//
// discovered counts the databases the server reported as connectable,
// including the connection database. It used to be built by adding one for the
// seed, because the catalog query excluded current_database(); the query
// returns it now, so the count is the server's own answer rather than the
// server's answer plus an assumption.
//
// With more than one seed DSN pointing at the same server the count is a sum
// and will overstate it. One seed is the normal case and the only one PMM
// produces; the alternative is deduplicating databases across seeds, which
// needs a notion of server identity this code does not have.
type discovery struct {
	targets    []scrapeTarget
	discovered int
	// excluded holds the databases the operator's exclude list matched.
	excluded []databaseRef
	// notIncluded counts databases a non-empty include list left out.
	notIncluded int
	ok          bool
	// connectFailures counts the seeds whose connection itself did not work,
	// and connectSuccesses the seeds whose connection did, whatever the
	// catalogue read then came to.
	//
	// Both, because either on its own loses an observation this scrape made. A
	// scrape that could not log in used to report the server as up, because
	// the fan-out after it attempted nothing and so reported no failure of its
	// own. Counting only the failures then lost the other half: one seed that
	// connected and another that was refused reported the server as down,
	// while the first connection had worked and its catalogue had been read.
	connectFailures  int
	connectSuccesses int
}

// connstringRe superficially validates a DSN that is not a URI. The syntax is
// complex and does not need parsing here, only a valid-looking keyword pair.
var connstringRe = regexp.MustCompile(`^ *[a-zA-Z0-9]+ *= *[^= ]+`)

// parsesAsDSN says whether a seed is one discovery will work with at all.
//
// The two branches are the ones the seed loop applies, and they are syntax
// only: nothing here connects or reads.
func parsesAsDSN(dsn string) bool {
	if strings.HasPrefix(dsn, "postgresql://") || strings.HasPrefix(dsn, "postgres://") {
		_, err := url.Parse(dsn)
		return err == nil
	}
	return connstringRe.MatchString(dsn)
}

// masterSeed is the configured seed whose own database carries the queries that
// run on one server rather than on each database.
//
// Taken from the configured list, so it does not depend on the order the seeds
// are visited in. It used to be assigned as the loop went, which made it
// whichever seed the loop reached last and would make it a data race as soon as
// discovery and collection overlap, as well as moving the placement whenever
// the visiting order changed.
//
// It is the last seed that parses, which is what the assignment in the loop
// came to, not the first as the comment beside it said. That behaviour is
// unchanged here on purpose: this makes it stable, not different.
func masterSeed(dsns []string) string {
	master := ""
	for _, dsn := range dsns {
		if parsesAsDSN(dsn) {
			master = dsn
		}
	}
	return master
}

func (e *Exporter) discoverDatabaseDSNs() discovery {
	d := discovery{ok: true}

	// Settled before anything runs, and not touched again while it does.
	e.masterDSN = masterSeed(e.dsn)

	for _, dsn := range e.dsn {
		d.targets = append(d.targets, e.discoverSeed(dsn, &d)...)
	}
	sortTargets(d.targets)
	return d
}

// discoverSeed reads one seed's catalogue and returns the targets it yields,
// adding what it observed to d.
//
// One seed at a time, because the caller collects a seed's databases before
// reading the next one. Reading every seed first and collecting afterwards is
// what let one stalled seed spend the whole budget while databases that had
// already been found waited behind it.
func (e *Exporter) discoverSeed(dsn string, d *discovery) []scrapeTarget {
	dsns := make(map[string]databaseRef)
	var dsnURI *url.URL
	var dsnConnstring string

	if strings.HasPrefix(dsn, "postgresql://") || strings.HasPrefix(dsn, "postgres://") {
		var err error
		dsnURI, err = url.Parse(dsn)
		if err != nil {
			level.Error(logger).Log("msg", "Unable to parse DSN as URI", "dsn", loggableDSN(dsn), "err", err)
			// The set could not be determined for this seed, and the
			// seed itself is dropped. Reporting success here would
			// publish "nothing to cover, and that is correct".
			d.ok = false
			return nil
		}
	} else if connstringRe.MatchString(dsn) {
		dsnConnstring = dsn
	} else {
		level.Error(logger).Log("msg", "Unable to parse DSN as either URI or connstring", "dsn", loggableDSN(dsn))
		d.ok = false
		return nil
	}
	seed := dsn
	// The host:port identity this seed's databases are reported under.
	// Discovery has already accepted the DSN above, so a failure here
	// leaves the identity empty rather than dropping the seed.
	fingerprint, err := parseFingerprint(dsn)
	if err != nil {
		level.Error(logger).Log("msg", "Unable to identify the server for a DSN", "dsn", loggableDSN(dsn), "err", err)
	}
	dsns[seed] = databaseRef{server: fingerprint, identity: identityOf(seed)}

	// Before the read, not after it. What this may authorise forgetting is
	// what was already there when it began looking, and a read that finishes
	// late must not erase a record a newer scrape has just written.
	taken := time.Now()

	catalog, connected, err := e.getDatabaseNames(dsn)
	if connected {
		d.connectSuccesses++
	}
	if err != nil {
		var connectErr *ErrorConnectToServer
		if errors.As(err, &connectErr) {
			d.connectFailures++
		}
		// Discovery failed for this seed. The DSN list keeps the seed
		// alone, so coverage silently collapsed to one database -- which
		// is exactly what ok=false exists to stop being silent.
		d.ok = false
		// The seed's own database exists whatever the catalog query did,
		// because the connection to it is how the query was attempted, and it
		// is already in the map. Returning nothing here would drop it.
		d.discovered++
		return targetsOf(seed, dsns)
	}
	// The eligible rows only. The read now returns every database the server
	// has, so that a later component can tell one that was dropped from one
	// that merely stopped being reachable, and counting all of them here would
	// put templates into a coverage denominator that never had them.
	d.discovered += len(eligible(catalog))
	if name, found := currentName(catalog); found {
		dsns[seed] = databaseRef{server: fingerprint, datname: name, identity: identityOf(seed)}
	} else {
		// The connection database is not in its own catalog. The query
		// also filters out templates and databases this role has no
		// CONNECT privilege on, so this says the database is outside the
		// eligible set, not specifically that it became unconnectable
		// mid-scrape. Connecting to a template database is one way to
		// get here. It is still a target and still counted, but there is
		// no name to report it under and inventing one would be worse.
		d.discovered++
	}

	// A complete, successful read of every database the server has. Only this
	// can authorise forgetting one: a name missing from a filtered read may
	// have lost CONNECT, and a name missing from a failed or partial read was
	// never looked for. Every path above this returns before reaching here.
	e.forgetDatabasesTheServerNoLongerHas(lastsuccess.Census{
		Target:    identityOf(seed),
		Taken:     taken,
		Databases: nameSet(inventory(catalog)),
	})

	sel := dbselect.Apply(otherThanCurrent(catalog), e.excludeDatabases, e.includeDatabases)
	for _, name := range sel.Excluded {
		d.excluded = append(d.excluded, databaseRef{server: fingerprint, datname: name})
	}
	d.notIncluded += sel.NotIncluded

	// The configured database is subject to the same rules as any other,
	// but it cannot be dropped from the DSN list: it is the connection
	// the cluster-wide work runs on. Exclusion of database-local
	// collection for it is a separate change.
	for _, databaseName := range sel.Selected {
		if dsnURI != nil {
			if dsnURI.Host == "" && strings.HasPrefix(dsnURI.Path, "/") {
				dsnURI.Path = "/" + databaseName
			} else {
				dsnURI.Path = databaseName
			}
			dsn = dsnURI.String()
		} else {
			// replacing one dbname with another is complicated.
			// just append new dbname to override.
			dsn = fmt.Sprintf("%s dbname=%s", dsnConnstring, databaseName)
		}
		dsns[dsn] = databaseRef{server: fingerprint, datname: databaseName, identity: identityOf(dsn)}
	}

	return targetsOf(seed, dsns)
}

// targetsOf turns the seed's map into targets in a fixed order. Map iteration
// order is random, and a random order makes a failure report read differently
// every scrape for no reason.
func targetsOf(seed string, dsns map[string]databaseRef) []scrapeTarget {
	targets := make([]scrapeTarget, 0, len(dsns))
	for dsn, ref := range dsns {
		targets = append(targets, scrapeTarget{dsn: dsn, seed: seed, databaseRef: ref})
	}
	sortTargets(targets)
	return targets
}

// sortTargets puts targets in a fixed order.
func sortTargets(targets []scrapeTarget) {
	sort.Slice(targets, func(i, j int) bool { return targets[i].dsn < targets[j].dsn })
}

// forgetDatabasesTheServerNoLongerHas hands a census to the stores that keep
// per-database state.
//
// Separate from the read so the refusal is in one place: a caller that has
// anything less than a complete successful inventory does not reach it.
func (e *Exporter) forgetDatabasesTheServerNoLongerHas(c lastsuccess.Census) {
	if removed := e.lastSuccess.Forget(c); removed > 0 {
		level.Info(logger).Log("msg", "Forgot databases the server no longer has",
			"target", c.Target, "records", removed)
	}
}

// nameSet turns a list of database names into a set.
func nameSet(names []string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, name := range names {
		out[name] = struct{}{}
	}
	return out
}

// identityOf returns this process's identity for a connection string, logging
// the mapping the first time it is assigned so an operator can read which
// connection an identity stands for.
func identityOf(dsn string) string {
	id, first := targetid.ID(dsn)
	if first {
		level.Debug(logger).Log("msg", "Assigned a target identity", "target", id, "dsn", loggableDSN(dsn))
	}
	return id
}

// getDatabaseNames reads one seed's catalogue, and says separately whether its
// connection worked.
//
// Separately because the two are different observations and a scrape can make
// one without the other. A connection that worked is a connection that worked
// even if the catalogue read after it failed, and losing that leaves the
// exporter reporting a server as unreachable that it had just reached.
func (e *Exporter) getDatabaseNames(dsn string) (names []catalogDatabase, connected bool, err error) {
	if e.connSema != nil {
		if err := e.connSema.Acquire(e.ctx, 1); err != nil {
			level.Warn(logger).Log("msg", "Failed to acquire semaphore", "err", err)
			// No connection was attempted, so nothing was observed about the
			// server either way.
			return nil, false, err
		}
		defer e.connSema.Release(1)
	}

	server, err := e.GetServer(e.ctx, dsn)
	if err != nil {
		level.Error(logger).Log("msg", "Error opening connection to database", "dsn", loggableDSN(dsn), "err", err)
		// Typed, so the caller can tell a connection that did not work from a
		// catalogue query that failed on one that did. scrapeDSN reports the
		// same distinction for the same reason.
		return nil, false, &ErrorConnectToServer{fmt.Sprintf("Error opening connection to database (%s): %s", loggableDSN(dsn), err.Error())}
	}
	defer server.Close()

	dbNames, err := queryDatabases(e.ctx, server)
	if err != nil {
		level.Error(logger).Log("msg", "Error querying databases", "dsn", loggableDSN(dsn), "err", err)
		return nil, true, err
	}

	return dbNames, true, nil
}

// scrapeDSN collects one target. It writes the database name back into the
// target once it knows it, so the caller can report that database's outcome:
// without discovery the name is not known before the connection is open.
func (e *Exporter) scrapeDSN(ch chan<- prometheus.Metric, target *scrapeTarget) error {
	dsn := target.dsn
	server, err := e.GetServer(e.ctx, dsn)
	if err != nil {
		return &ErrorConnectToServer{fmt.Sprintf("Error opening connection to database (%s): %s", loggableDSN(dsn), err.Error())}
	}
	defer server.Close()

	// Check if autoDiscoverDatabases is false, set dsn as master database (Default: false)
	if !e.autoDiscoverDatabases || e.masterDSN == dsn {
		server.master = true
	}

	server.datname = target.datname

	// Discovery names every database it finds. Without discovery the
	// connection string usually names one, in a URL path or in dbname, and
	// reading it costs no round trip. A per-database series needs the name,
	// and Prometheus reads an empty label as absent, so two targets on one
	// host would share a label set without it.
	//
	// It names none when the string omits the database, where libpq falls
	// back to the role name, and when the string names two. The server is
	// asked below, where a query needs the name to decide where it runs.
	if server.datname == "" {
		if name, ok := config.DatabaseFromDSN(dsn); ok {
			server.datname = name
		}
	}

	// Check if map versions need to be updated
	if err := e.checkMapVersions(e.ctx, ch, server); err != nil {
		// Proceeding is only defensible when there is something to proceed
		// with. GetServer builds a fresh Server for every scrape, so when this
		// step fails there is no previous map: metricMap is nil,
		// queryNamespaceMappings iterates nothing, Scrape returns no error,
		// and the database is reported as covered while none of its queries
		// ran. Custom query files are loaded by this same step, so they do not
		// run either.
		//
		// The connection worked, so this is a collection failure and not a
		// connection one. Returning ErrorConnectToServer here would change
		// what pg_up means.
		if !server.hasQueryMaps() {
			return fmt.Errorf("could not prepare query maps for %s, so nothing was collected: %w", server, err)
		}
		level.Warn(logger).Log("msg", "Proceeding with outdated query maps, as the Postgres version could not be determined", "err", err)
	}

	// Two things need the name of the database this connection is on: a query
	// that names the databases it runs on, and the per-database diagnostics a
	// cached namespace publishes. Discovery supplies it when it runs, and the
	// connection string supplies it above when it names one. What is left is a
	// connection string that names none, where libpq falls back to the role
	// name and only the server knows the answer.
	//
	// Asked after the query maps exist, because that is what says whether
	// anything needs it, and not asked at all otherwise: a file with no
	// databases list and no cache_seconds pays nothing.
	//
	// A failure is fatal only where the name decides what runs. Leaving the
	// name empty there would make every restricted query quietly not run while
	// the scrape reported the database as covered, which is the shape of claim
	// these metrics exist to prevent. Where only a label needs it, a failure
	// leaves the label empty and collection goes ahead, because a diagnostic
	// that cannot be labelled is not a reason to collect nothing.
	if server.datname == "" {
		forSelection := serverNeedsDatabaseName(server)
		// The freshness diagnostic needs the name too, and it is published for
		// every database, not only the ones a query restricts itself to. Asked
		// once: after the first success the store knows the name, and asking
		// again would be a round trip per scrape for an answer already held.
		remembered, _, known := e.lastSuccess.Latest(lastsuccess.EngineQuery, lastsuccess.OneCollector, target.identity, e.collectorName)
		if forSelection || serverCachesANamespace(server) || !known {
			var name string
			if err := server.db.QueryRowContext(e.ctx, "SELECT current_database()").Scan(&name); err != nil {
				if forSelection {
					return fmt.Errorf("could not determine the current database on %s, so queries restricted by database name could not run: %w", server, err)
				}
				level.Warn(logger).Log("msg", "Could not determine the current database, so this scrape is reported without one",
					"server", server, "err", err)
				server.datname = remembered
			} else {
				server.datname = name
			}
		} else {
			server.datname = remembered
		}
	}

	// What the caller reports this target's outcome against.
	target.datname = server.datname

	return server.Scrape(e.ctx, ch, e.disableSettingsMetrics, e.queryCache, e.collectorName)
}

// try to get the DataSource
// DATA_SOURCE_NAME always wins so we do not break older versions
// reading secrets from files wins over secrets in environment variables
// DATA_SOURCE_NAME > DATA_SOURCE_{USER|PASS}_FILE > DATA_SOURCE_{USER|PASS}
func getDataSources() ([]string, error) {
	var dsn = os.Getenv("DATA_SOURCE_NAME")
	if len(dsn) != 0 {
		return strings.Split(dsn, ","), nil
	}

	var user, pass, uri string

	dataSourceUserFile := os.Getenv("DATA_SOURCE_USER_FILE")
	if len(dataSourceUserFile) != 0 {
		fileContents, err := os.ReadFile(dataSourceUserFile)
		if err != nil {
			return nil, fmt.Errorf("failed loading data source user file %s: %s", dataSourceUserFile, err.Error())
		}
		user = strings.TrimSpace(string(fileContents))
	} else {
		user = os.Getenv("DATA_SOURCE_USER")
	}

	dataSourcePassFile := os.Getenv("DATA_SOURCE_PASS_FILE")
	if len(dataSourcePassFile) != 0 {
		fileContents, err := os.ReadFile(dataSourcePassFile)
		if err != nil {
			return nil, fmt.Errorf("failed loading data source pass file %s: %s", dataSourcePassFile, err.Error())
		}
		pass = strings.TrimSpace(string(fileContents))
	} else {
		pass = os.Getenv("DATA_SOURCE_PASS")
	}

	ui := url.UserPassword(user, pass).String()
	dataSrouceURIFile := os.Getenv("DATA_SOURCE_URI_FILE")
	if len(dataSrouceURIFile) != 0 {
		fileContents, err := os.ReadFile(dataSrouceURIFile)
		if err != nil {
			return nil, fmt.Errorf("failed loading data source URI file %s: %s", dataSrouceURIFile, err.Error())
		}
		uri = strings.TrimSpace(string(fileContents))
	} else {
		uri = os.Getenv("DATA_SOURCE_URI")
	}

	// No datasources found. This allows us to support the multi-target pattern
	// without an explicit datasource.
	if uri == "" {
		return []string{}, nil
	}

	dsn = "postgresql://" + ui + "@" + uri

	return []string{dsn}, nil
}

// serverCachesANamespace reports whether any loaded namespace asks to be
// cached, which is what makes the database name worth a round trip for a label
// alone: the age series is published per database, and two targets reporting
// no database share a label set.
func serverCachesANamespace(server *Server) bool {
	server.mappingMtx.RLock()
	defer server.mappingMtx.RUnlock()
	for _, mapping := range server.metricMap {
		if mapping.cacheSeconds > 0 {
			return true
		}
	}
	return false
}

// serverNeedsDatabaseName reports whether any namespace loaded for this server
// decides where it runs from the database name.
//
// Most query files contain nothing that does, so the lookup is usually
// skipped. queryDatabaseList.needsDatabaseName holds the rule, so that what is
// looked up here and what is decided in runsOn cannot drift apart.
func serverNeedsDatabaseName(server *Server) bool {
	server.mappingMtx.RLock()
	defer server.mappingMtx.RUnlock()
	for _, mapping := range server.metricMap {
		if mapping.databases.needsDatabaseName(mapping.master) {
			return true
		}
	}
	return false
}
