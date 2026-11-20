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
	"github.com/prometheus/client_golang/prometheus"
)

// Reasons a database the server offered is not in the selected set. These are
// label values, so the set is closed and short: the monitoring contract allows
// bounded reason codes and forbids arbitrary strings.
const (
	reasonExcludeList = "exclude_list"
	reasonNotIncluded = "not_in_include_list"
)

// Reasons a selected database produced no data. Also label values, also a
// closed set. Nothing derived from an error message belongs here.
const (
	reasonConnect      = "connect"
	reasonScrape       = "scrape"
	reasonNotAttempted = "not_attempted"
)

// databaseList is a configured list of database names that remembers whether
// it was configured at all.
//
// Pep decided on 2026-11-20 that an explicitly empty list selects nothing,
// while an omitted one means no restriction. Those are opposite instructions,
// and a []string cannot tell them apart: both arrive with length zero. The
// monitoring contract calls this out by name, because collapsing them is the
// kind of thing that happens silently and then selects every database on a
// server where the operator asked for none.
//
// The command-line surface cannot express an explicit empty list yet. An unset
// flag and an empty one both arrive as absent, which is the safe direction.
// Naming the configuration keys, and deciding how an empty list is written in
// them, is still open.
type databaseList struct {
	names []string
	given bool
}

// listOf records a list the operator supplied, empty or not.
func listOf(names []string) databaseList {
	return databaseList{names: names, given: true}
}

// listFromFlag converts the inherited comma-separated flag value. An unset
// flag reaches this as a slice holding one empty string, because
// strings.Split("", ",") does not return an empty slice, so a list of nothing
// but empty names counts as absent rather than as an explicit empty list.
func listFromFlag(names []string) databaseList {
	for _, n := range names {
		if n != "" {
			return listOf(names)
		}
	}
	return databaseList{}
}

// selection is what the configuration did to the catalog the server reported.
//
// Excluding a database is a decision, not a fault, and the contract is explicit
// that the two must not be conflated. Keeping the names apart from the count is
// about cardinality rather than principle: the names an operator wrote in an
// exclude list are bounded by the configuration, while the complement of an
// include list can be the whole server, so only the first is reported per
// database.
type selection struct {
	selected []string
	// excluded holds the names the operator listed and the server has, so a
	// typo in the list shows up as a name that never appears here.
	excluded []string
	// notIncluded counts the databases left out by a non-empty include list.
	notIncluded int
}

// selectDatabases applies the exclusion and inclusion rules to the names
// discovery returned. Extracted from discoverDatabaseDSNs so the rule can be
// tested without a database: it decides coverage, and nothing was testing it.
//
// The rule, as decided:
//
//	include omitted, exclude omitted     every eligible database
//	include omitted, exclude given       every eligible database but those named
//	include given and empty              no databases, whatever exclude says
//	include given and non-empty          the named databases, minus exclusions
//
// Exclusion is checked first, so a database named in both lists is reported as
// excluded rather than as not included. Exclusion takes precedence, and the
// report should say which rule applied.
func selectDatabases(names []string, exclude, include databaseList) selection {
	s := selection{selected: make([]string, 0, len(names))}
	for _, name := range names {
		if contains(exclude.names, name) {
			s.excluded = append(s.excluded, name)
			continue
		}
		// An include list that was given restricts the set, including when it
		// is empty: an operator who writes an empty list has asked for no
		// database-local collection, not for all of it.
		if include.given && !contains(include.names, name) {
			s.notIncluded++
			continue
		}
		s.selected = append(s.selected, name)
	}
	return s
}

// scrapeTarget is one database the scrape will try to cover, and the name to
// report it under.
//
// The name is what lets a shortfall say which database went dark. It is known
// whenever discovery ran, because the catalog query now returns the connection
// database alongside the rest. It is empty when discovery is disabled, where
// the only target is the configured DSN and the name would have to come from
// parsing a connection string.
type scrapeTarget struct {
	dsn     string
	datname string
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
	excluded []string
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

	// DSN to database name. The name is empty when it is not known.
	dsns := make(map[string]string)
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
		dsns[seed] = ""

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
			dsns[seed] = name
		} else {
			// The connection database is not in its own catalog, which means
			// it stopped being connectable while this connection was open.
			// It is still a target, and still counted, but there is no name
			// to report it under and inventing one would be worse.
			d.discovered++
		}

		sel := selectDatabases(otherThanCurrent(catalog), e.excludeDatabases, e.includeDatabases)
		d.excluded = append(d.excluded, sel.excluded...)
		d.notIncluded += sel.notIncluded

		// The configured database is subject to the same rules as any other,
		// but it cannot be dropped from the DSN list: it is the connection
		// the cluster-wide work runs on. Exclusion of database-local
		// collection for it is a separate change.
		for _, databaseName := range sel.selected {
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
			dsns[dsn] = databaseName
		}
	}

	d.targets = make([]scrapeTarget, 0, len(dsns))
	for dsn, name := range dsns {
		d.targets = append(d.targets, scrapeTarget{dsn: dsn, datname: name})
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

	server, err := e.GetServer(dsn)
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

func (e *Exporter) scrapeDSN(ch chan<- prometheus.Metric, dsn string) error {
	server, err := e.GetServer(dsn)
	if err != nil {
		return &ErrorConnectToServer{fmt.Sprintf("Error opening connection to database (%s): %s", loggableDSN(dsn), err.Error())}
	}
	defer server.Close()

	// Check if autoDiscoverDatabases is false, set dsn as master database (Default: false)
	if !e.autoDiscoverDatabases || e.masterDSN == dsn {
		server.master = true
	}

	// Check if map versions need to be updated
	if err := e.checkMapVersions(ch, server); err != nil {
		level.Warn(logger).Log("msg", "Proceeding with outdated query maps, as the Postgres version could not be determined", "err", err)
	}

	return server.Scrape(ch, e.disableSettingsMetrics)
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
