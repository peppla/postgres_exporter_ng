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
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/prometheus/client_golang/prometheus"
)

// The reason codes both engines report against live in internal/dbselect, so
// "excluded" means the same thing wherever it is published.
const (
	reasonExcludeList  = dbselect.ReasonExcludeList
	reasonConnect      = dbselect.ReasonConnect
	reasonScrape       = dbselect.ReasonScrape
	reasonNotAttempted = dbselect.ReasonNotAttempted
)

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
}

func (e *Exporter) discoverDatabaseDSNs() discovery {
	d := discovery{ok: true}
	// connstring syntax is complex (and not sure if even regular).
	// we don't need to parse it, so just superficially validate that it starts
	// with a valid-ish keyword pair
	connstringRe := regexp.MustCompile(`^ *[a-zA-Z0-9]+ *= *[^= ]+`)

	// DSN to how that database is reported. The name is empty when it is
	// not known.
	dsns := make(map[string]databaseRef)
	for _, dsn := range e.dsn {
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
				continue
			}
		} else if connstringRe.MatchString(dsn) {
			dsnConnstring = dsn
		} else {
			level.Error(logger).Log("msg", "Unable to parse DSN as either URI or connstring", "dsn", loggableDSN(dsn))
			d.ok = false
			continue
		}
		seed := dsn
		// The host:port identity this seed's databases are reported under.
		// Discovery has already accepted the DSN above, so a failure here
		// leaves the identity empty rather than dropping the seed.
		fingerprint, err := parseFingerprint(dsn)
		if err != nil {
			level.Error(logger).Log("msg", "Unable to identify the server for a DSN", "dsn", loggableDSN(dsn), "err", err)
		}
		dsns[seed] = databaseRef{server: fingerprint}

		// If autoDiscoverDatabases is true, set first dsn as master database (Default: false)
		e.masterDSN = dsn

		catalog, err := e.getDatabaseNames(dsn)
		if err != nil {
			// Discovery failed for this seed. The DSN list keeps the seed
			// alone, so coverage silently collapsed to one database -- which
			// is exactly what ok=false exists to stop being silent.
			d.ok = false
			// The seed's own database exists whatever the catalog query did,
			// because the connection to it is how the query was attempted.
			d.discovered++
			continue
		}
		d.discovered += len(catalog)
		if name, found := currentName(catalog); found {
			dsns[seed] = databaseRef{server: fingerprint, datname: name}
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
			dsns[dsn] = databaseRef{server: fingerprint, datname: databaseName}
		}
	}

	d.targets = make([]scrapeTarget, 0, len(dsns))
	for dsn, ref := range dsns {
		d.targets = append(d.targets, scrapeTarget{dsn: dsn, databaseRef: ref})
	}
	// Map iteration order is random, and a random order makes a failure
	// report read differently every scrape for no reason.
	sort.Slice(d.targets, func(i, j int) bool { return d.targets[i].dsn < d.targets[j].dsn })

	return d
}

func (e *Exporter) getDatabaseNames(dsn string) ([]catalogDatabase, error) {
	if e.connSema != nil {
		if err := e.connSema.Acquire(e.ctx, 1); err != nil {
			level.Warn(logger).Log("msg", "Failed to acquire semaphore", "err", err)
			return nil, err
		}
		defer e.connSema.Release(1)
	}

	server, err := e.GetServer(e.ctx, dsn)
	if err != nil {
		level.Error(logger).Log("msg", "Error opening connection to database", "dsn", loggableDSN(dsn), "err", err)
		return nil, err
	}
	defer server.Close()

	dbNames, err := queryDatabases(e.ctx, server)
	if err != nil {
		level.Error(logger).Log("msg", "Error querying databases", "dsn", loggableDSN(dsn), "err", err)
		return nil, err
	}

	return dbNames, nil
}

func (e *Exporter) scrapeDSN(ch chan<- prometheus.Metric, target scrapeTarget) error {
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

	// An application query can name the databases it runs on, so the server
	// has to know which database it is. Discovery supplies the name when it
	// runs; without discovery there is one target and no name, so it is asked
	// for here.
	//
	// Asked only when some loaded query actually restricts itself by name, and
	// only after the query maps exist, because that is what says whether any
	// does. A file with no databases list anywhere pays nothing.
	//
	// A failure here is a collection failure, not an absent match. Leaving the
	// name empty would make every restricted query quietly not run while the
	// scrape reported the database as covered, which is the shape of claim
	// these metrics exist to prevent.
	if server.datname == "" && serverNeedsDatabaseName(server) {
		var name string
		if err := server.db.QueryRowContext(e.ctx, "SELECT current_database()").Scan(&name); err != nil {
			return fmt.Errorf("could not determine the current database on %s, so queries restricted by database name could not run: %w", server, err)
		}
		server.datname = name
	}

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
