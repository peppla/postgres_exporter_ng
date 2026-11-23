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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	settingUnits = []string{
		"ms", "s", "min", "h", "d",
		"B", "kB", "MB", "GB", "TB",
	}
)

// Query the pg_settings view containing runtime variables
func querySettings(ctx context.Context, ch chan<- prometheus.Metric, server *Server) error {
	level.Debug(logger).Log("msg", "Querying pg_setting view", "server", server)

	// pg_settings docs: https://www.postgresql.org/docs/current/static/view-pg-settings.html
	//
	// NOTE: If you add more vartypes here, you must update the supported
	// types in normaliseUnit() below
	//
	// Settings intentionally ignored due to invalid format:
	// - `sync_commit_cancel_wait`, specific to Azure Postgres, see https://github.com/prometheus-community/postgres_exporter/issues/523
	// - `google_dataplex.max_messages`, specific to Google Cloud SQL, see https://github.com/prometheus-community/postgres_exporter/issues/1240
	query := "SELECT name, setting, COALESCE(unit, ''), short_desc, vartype FROM pg_settings WHERE vartype IN ('bool', 'integer', 'real') AND name NOT IN ('sync_commit_cancel_wait', 'google_dataplex.max_messages');"

	rows, err := server.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Error running query on database %q: %s %v", server, namespace, err)
	}
	defer rows.Close() // nolint: errcheck

	for rows.Next() {
		s := &pgSetting{}
		err = rows.Scan(&s.name, &s.setting, &s.unit, &s.shortDesc, &s.vartype)
		if err != nil {
			return fmt.Errorf("Error retrieving rows on %q: %s %v", server, namespace, err)
		}

		metric, err := s.metric(server.labels)
		if err != nil {
			// One setting, not the whole view. The rest of pg_settings is
			// readable and an operator losing all of it over one row would
			// learn less, not more.
			level.Warn(logger).Log("msg", "Skipping a setting that cannot be published", "err", err)
			continue
		}
		ch <- metric
	}

	// Next stops on error exactly as it stops at the end of the rows, so
	// without this a settings query that failed partway reports success.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error retrieving rows on %q: %s %v", server, namespace, err)
	}

	return nil
}

// pgSetting is represents a PostgreSQL runtime variable as returned by the
// pg_settings view.
type pgSetting struct {
	name, setting, unit, shortDesc, vartype string
}

// metric turns one pg_settings row into a metric, or says why it cannot.
//
// It used to panic on three things: a unit it did not recognise, a vartype the
// query does not ask for, and a descriptor the registry refuses. All three come
// from the server rather than from this exporter, and a panic here is a panic
// in a scrape worker, where nothing recovers it. A setting the exporter cannot
// read is one missing metric, not a reason to stop the process.
func (s *pgSetting) metric(labels prometheus.Labels) (prometheus.Metric, error) {
	var (
		err       error
		name      = strings.ReplaceAll(s.name, ".", "_")
		unit      = s.unit // nolint: ineffassign
		shortDesc = fmt.Sprintf("Server Parameter: %s", s.name)
		subsystem = "settings"
		val       float64
	)

	switch s.vartype {
	case "bool":
		if s.setting == "on" {
			val = 1
		}
	case "integer", "real":
		if val, unit, err = s.normaliseUnit(); err != nil {
			return nil, fmt.Errorf("setting %q: %w", s.name, err)
		}

		if len(unit) > 0 {
			name = fmt.Sprintf("%s_%s", name, unit)
			shortDesc = fmt.Sprintf("%s [Units converted to %s.]", shortDesc, unit)
		}
	default:
		return nil, fmt.Errorf("setting %q has the unsupported vartype %q", s.name, s.vartype)
	}

	desc := newDesc(subsystem, name, shortDesc, labels)
	metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, val)
	if err != nil {
		return nil, fmt.Errorf("setting %q cannot be published: %w", s.name, err)
	}
	return metric, nil
}

// Removes units from any of the setting values.
// This is mostly because of a irregularity regarding AWS RDS Aurora
// https://github.com/prometheus-community/postgres_exporter/issues/619
func (s *pgSetting) sanitizeValue() {
	for _, unit := range settingUnits {
		if strings.HasSuffix(s.setting, unit) {
			endPos := len(s.setting) - len(unit) - 1
			s.setting = s.setting[:endPos]
			return
		}
	}
}

// TODO: fix linter override
// nolint: nakedret
func (s *pgSetting) normaliseUnit() (val float64, unit string, err error) {
	s.sanitizeValue()

	val, err = strconv.ParseFloat(s.setting, 64)
	if err != nil {
		return val, unit, fmt.Errorf("Error converting setting %q value %q to float: %s", s.name, s.setting, err)
	}

	// Units defined in: https://www.postgresql.org/docs/current/static/config-setting.html
	switch s.unit {
	case "":
		return
	case "ms", "s", "min", "h", "d":
		unit = "seconds"
	case "B", "kB", "MB", "GB", "TB", "1kB", "2kB", "4kB", "8kB", "16kB", "32kB", "64kB", "16MB", "32MB", "64MB":
		unit = "bytes"
	default:
		err = fmt.Errorf("unknown unit for runtime variable: %q", s.unit)
		return
	}

	// -1 is special, don't modify the value
	if val == -1 {
		return
	}

	switch s.unit {
	case "ms":
		val /= 1000
	case "min":
		val *= 60
	case "h":
		val *= 60 * 60
	case "d":
		val *= 60 * 60 * 24
	case "kB":
		val *= math.Pow(2, 10)
	case "MB":
		val *= math.Pow(2, 20)
	case "GB":
		val *= math.Pow(2, 30)
	case "TB":
		val *= math.Pow(2, 40)
	case "1kB":
		val *= math.Pow(2, 10)
	case "2kB":
		val *= math.Pow(2, 11)
	case "4kB":
		val *= math.Pow(2, 12)
	case "8kB":
		val *= math.Pow(2, 13)
	case "16kB":
		val *= math.Pow(2, 14)
	case "32kB":
		val *= math.Pow(2, 15)
	case "64kB":
		val *= math.Pow(2, 16)
	case "16MB":
		val *= math.Pow(2, 24)
	case "32MB":
		val *= math.Pow(2, 25)
	case "64MB":
		val *= math.Pow(2, 26)
	}

	return
}
