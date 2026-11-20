package collector

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	registerCollector("extensions", defaultEnabled, scopeDatabase, NewExtensionsCollector)
}

// Both of these describe one database, not the server.
//
// pg_extension is a per-database catalog: an extension installed in one
// database is absent from another. pg_available_extensions looks server-wide,
// because the files it lists are, but its installed_version column reports
// what is installed in the connection's own database, so it reads differently
// from each one.
//
// Neither said which database it described, so two databases produced label
// sets identical in every respect. That is why this collector ran on the
// configured connection alone while the other database-local collectors fanned
// out. The datname label is synthesized with current_database(), the way the
// table and index collectors do it, because no underlying view carries it.
var pgExtensions = map[string]*prometheus.Desc{
	"pg_available_extensions": prometheus.NewDesc(
		"pg_available_extensions",
		"Extensions that are available for installation, and the version installed in this database if any",
		[]string{
			"datname",
			"name",
			"default_version",
			"installed_version",
		},
		prometheus.Labels{},
	),
	"pg_extensions": prometheus.NewDesc(
		"pg_extensions",
		"Extensions installed in this database",
		[]string{
			"datname",
			"name",
			"relocatable",
			"version",
		},
		prometheus.Labels{},
	),
}

type ExtensionsCollector struct {
	logger log.Logger
}

func NewExtensionsCollector(collectorConfig collectorConfig) (Collector, error) {
	return &ExtensionsCollector{logger: collectorConfig.logger}, nil
}

func (e *ExtensionsCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	err := e.scrapeAvailableExtensions(ctx, instance.db, ch)
	if err != nil {
		return err
	}

	err = e.scrapeInstalledExtensions(ctx, instance.db, ch)
	if err != nil {
		return err
	}

	return nil
}

func (e *ExtensionsCollector) scrapeInstalledExtensions(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	// current_database() comes back as a column rather than from a second
	// query, so naming the database costs no extra round trip.
	rowsExtensions, err := db.QueryContext(ctx, `SELECT current_database(), extname, extrelocatable, extversion FROM pg_extension`)

	if err != nil {
		return err
	}
	defer rowsExtensions.Close()

	for rowsExtensions.Next() {
		var datname string
		var extname string
		var extrelocatable bool
		var extversion string
		if err := rowsExtensions.Scan(&datname, &extname, &extrelocatable, &extversion); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			pgExtensions["pg_extensions"],
			prometheus.GaugeValue,
			1,
			datname,
			extname,
			strconv.FormatBool(extrelocatable),
			extversion,
		)
	}

	// Next returning false does not mean the rows ended; it also means the
	// query failed partway, or was cancelled. Returning nil here reports a
	// truncated collection as a complete one.
	return rowsExtensions.Err()
}

func (e *ExtensionsCollector) scrapeAvailableExtensions(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.QueryContext(ctx, `SELECT current_database(), name, default_version, installed_version FROM pg_available_extensions`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var datname string
		var name sql.NullString
		var defaultVersion sql.NullString
		var installedVersion sql.NullString
		if err := rows.Scan(&datname, &name, &defaultVersion, &installedVersion); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			pgExtensions["pg_available_extensions"],
			prometheus.GaugeValue,
			1,
			datname,
			name.String,
			defaultVersion.String,
			installedVersion.String,
		)
	}

	return rows.Err()
}

var _ = (Collector)(&ExtensionsCollector{})
