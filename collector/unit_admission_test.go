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

// A collector the scrape never started, inside a database it did reach.
//
// The database holds an open connection and the permit that went with it, so
// there is no second permit to ask for and no wait to observe. What can end the
// work here is the scrape's own context, and a collector whose turn comes after
// that must not be reported as a collector that failed. Nothing ran, so nothing
// is known about it.

package collector

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/percona/postgres_exporter/internal/dbselect"
	"github.com/percona/postgres_exporter/internal/lastsuccess"
	"github.com/percona/postgres_exporter/internal/targetid"
	"github.com/prometheus/client_golang/prometheus"
)

// endsAfterNChecks reports no error for the first n reads of Err and the end of
// the wrapped context from then on.
//
// One read of Err happens per collector, at its admission check, so this stages
// the scrape ending between one collector's turn and the next. Which collector
// gets the first read is up to the scheduler, so the tests below assert the
// counts and the series rather than naming the collector that ran.
type endsAfterNChecks struct {
	context.Context
	n     int64
	calls atomic.Int64
}

func (c *endsAfterNChecks) Err() error {
	if c.calls.Add(1) <= c.n {
		return nil
	}
	return c.Context.Err()
}

// countingCollector records whether it ran and returns what it was told to.
type countingCollector struct {
	ran *atomic.Int32
	err error
}

func (c countingCollector) Update(context.Context, *instance, chan<- prometheus.Metric) error {
	c.ran.Add(1)
	return c.err
}

// unitSeries groups what one collector run published, by collector name.
func unitSeries(emit func(ch chan<- prometheus.Metric)) map[string]map[string]MetricResult {
	ch := make(chan prometheus.Metric, 1024)
	emit(ch)
	close(ch)
	out := map[string]map[string]MetricResult{}
	for m := range ch {
		r := readMetric(m)
		name := r.labels["collector"]
		if out[name] == nil {
			out[name] = map[string]MetricResult{}
		}
		out[name][familyName(m)] = r
	}
	return out
}

// localCollectors builds n collectors that all behave the same way, so that
// whichever one the scheduler reaches first, the outcome is the same.
func localCollectors(n int, ran *atomic.Int32, err error) map[string]Collector {
	out := map[string]Collector{}
	for i := 0; i < n; i++ {
		out[fmt.Sprintf("stat_fixture_%d", i)] = countingCollector{ran: ran, err: err}
	}
	return out
}

// expiredAfter wraps an already expired context so the first n admission checks
// pass and the rest see the expiry.
func expiredAfter(t *testing.T, n int64) context.Context {
	t.Helper()
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return &endsAfterNChecks{Context: expired, n: n}
}

// TestNoCollectorStartsAfterTheScrapeHasEnded is round 138's first case, a
// database with zero units started.
func TestNoCollectorStartsAfterTheScrapeHasEnded(t *testing.T) {
	const collectors = 3

	var ran atomic.Int32
	local := localCollectors(collectors, &ran, nil)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)

	var outcome localOutcome
	series := unitSeries(func(ch chan<- prometheus.Metric) {
		outcome = runLocalCollectors(expired, local, &instance{dsn: "postgresql://probe@198.51.100.9:5432/app"},
			ch, log.NewNopLogger(), "app", lastsuccess.New())
	})

	if got := ran.Load(); got != 0 {
		t.Errorf("%d collectors ran SQL on a scrape that had already ended", got)
	}
	if outcome.deferred != collectors || outcome.ran != 0 || outcome.failed != 0 {
		t.Fatalf("outcome is %+v, want all %d deferred and nothing else", outcome, collectors)
	}
	if outcome.reason != dbselect.ReasonDeadline {
		t.Errorf("the deferral reason is %q, want %q", outcome.reason, dbselect.ReasonDeadline)
	}

	for name := range local {
		if _, ok := series[name]["pg_scrape_collector_deferred"]; !ok {
			t.Errorf("%s never started and has no deferral series: %v", name, series[name])
		}
		if series[name]["pg_scrape_collector_deferred"].labels["reason"] != dbselect.ReasonDeadline {
			t.Errorf("%s is deferred for reason %q", name, series[name]["pg_scrape_collector_deferred"].labels["reason"])
		}
		if _, ok := series[name]["pg_scrape_collector_success"]; ok {
			t.Errorf("%s never ran and published a success series, which reads as a collector that failed", name)
		}
		if _, ok := series[name]["pg_scrape_collector_duration_seconds"]; ok {
			t.Errorf("%s never ran and published a duration", name)
		}
	}
}

// TestACollectorThatRanAndOneThatDidNotAreReportedApart is round 138's second
// case, one success followed by an unstarted unit.
func TestACollectorThatRanAndOneThatDidNotAreReportedApart(t *testing.T) {
	var ran atomic.Int32
	local := localCollectors(2, &ran, nil)

	var outcome localOutcome
	series := unitSeries(func(ch chan<- prometheus.Metric) {
		outcome = runLocalCollectors(expiredAfter(t, 1), local, &instance{dsn: "postgresql://probe@198.51.100.9:5432/app"},
			ch, log.NewNopLogger(), "app", lastsuccess.New())
	})

	// The fixture's own premise. Without exactly one run, this proves nothing
	// about telling the two apart.
	if got := ran.Load(); got != 1 {
		t.Fatalf("%d collectors ran, want exactly 1: the fixture did not stage one turn before the end", got)
	}
	if outcome.ran != 1 || outcome.deferred != 1 || outcome.failed != 0 {
		t.Fatalf("outcome is %+v, want one ran and one deferred", outcome)
	}

	succeeded, deferred := 0, 0
	for name := range local {
		_, hasSuccess := series[name]["pg_scrape_collector_success"]
		_, hasDeferral := series[name]["pg_scrape_collector_deferred"]
		if hasSuccess && hasDeferral {
			t.Errorf("%s is reported both as having run and as never started", name)
		}
		if hasSuccess {
			succeeded++
			if series[name]["pg_scrape_collector_success"].value != 1 {
				t.Errorf("%s ran without error and is reported as a failure", name)
			}
		}
		if hasDeferral {
			deferred++
		}
	}
	if succeeded != 1 || deferred != 1 {
		t.Errorf("%d collectors carry a success series and %d carry a deferral, want one of each", succeeded, deferred)
	}
}

// TestAQueryFailureAndADeferralBothSurvive is round 138's third case, one SQL
// error followed by an unstarted unit, and its requirement that the budget
// reason must not replace the real error.
func TestAQueryFailureAndADeferralBothSurvive(t *testing.T) {
	var ran atomic.Int32
	local := localCollectors(2, &ran, errors.New("relation \"pg_stat_fixture\" does not exist"))

	var outcome localOutcome
	series := unitSeries(func(ch chan<- prometheus.Metric) {
		outcome = runLocalCollectors(expiredAfter(t, 1), local, &instance{dsn: "postgresql://probe@198.51.100.9:5432/app"},
			ch, log.NewNopLogger(), "app", lastsuccess.New())
	})

	if got := ran.Load(); got != 1 {
		t.Fatalf("%d collectors ran, want exactly 1", got)
	}
	if outcome.failed != 1 || outcome.deferred != 1 || outcome.ran != 0 {
		t.Fatalf("outcome is %+v, want one failed and one deferred", outcome)
	}

	// The database-level diagnostic has to carry both, which is the part a
	// single reason label would lose.
	published := publishedByReason(func(ch chan<- prometheus.Metric) {
		reportIncompleteDatabase(ch, "198.51.100.9:5432", "app", outcome)
	})
	if _, ok := published[dbselect.ReasonScrape]; !ok {
		t.Errorf("a collector failed and the database does not report it: %v", published)
	}
	if _, ok := published[dbselect.ReasonDeadline]; !ok {
		t.Errorf("a collector never started and the database does not report the cause: %v", published)
	}

	failures, deferrals := 0, 0
	for name := range local {
		if s, ok := series[name]["pg_scrape_collector_success"]; ok {
			failures++
			if s.value != 0 {
				t.Errorf("%s returned an error and is reported as a success", name)
			}
		}
		if _, ok := series[name]["pg_scrape_collector_deferred"]; ok {
			deferrals++
		}
	}
	if failures != 1 || deferrals != 1 {
		t.Errorf("%d collectors carry a success series and %d a deferral, want one of each", failures, deferrals)
	}
}

// TestADeferredCollectorKeepsItsLastSuccess is round 138's requirement that the
// prior matching time survives. A collector that did not run this scrape still
// has to say when it last did, or an operator cannot tell a collector that has
// been deferred once from one that has never worked.
func TestADeferredCollectorKeepsItsLastSuccess(t *testing.T) {
	store := lastsuccess.New()
	dsn := "postgresql://probe@198.51.100.9:5432/app"
	// The identity the code will look the record up under. It is assigned per
	// DSN inside the process and is not the host and port, so a fixture that
	// writes the address here records under a key nothing reads.
	target, _ := targetid.ID(dsn)
	when := time.Now().Add(-time.Hour).Truncate(time.Second)
	store.Record(lastsuccess.Key{
		Engine:    lastsuccess.EngineCollector,
		Target:    target,
		Database:  "app",
		Collector: "stat_fixture_0",
	}, when)

	var ran atomic.Int32
	local := localCollectors(1, &ran, nil)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)

	series := unitSeries(func(ch chan<- prometheus.Metric) {
		runLocalCollectors(expired, local, &instance{dsn: dsn}, ch, log.NewNopLogger(), "app", store)
	})

	if ran.Load() != 0 {
		t.Fatal("the collector ran, so this says nothing about a deferred one")
	}
	got, ok := series["stat_fixture_0"]["pg_collector_last_success_timestamp_seconds"]
	if !ok {
		t.Fatalf("a deferred collector lost the time it last worked: %v", series["stat_fixture_0"])
	}
	if int64(got.value) != when.Unix() {
		t.Errorf("the retained time is %v, want %v", int64(got.value), when.Unix())
	}
}

// publishedByReason groups series by their reason label, for the database-level
// diagnostic that can carry two of them at once.
func publishedByReason(emit func(ch chan<- prometheus.Metric)) map[string]MetricResult {
	ch := make(chan prometheus.Metric, 64)
	emit(ch)
	close(ch)
	out := map[string]MetricResult{}
	for m := range ch {
		r := readMetric(m)
		out[r.labels["reason"]] = r
	}
	return out
}
