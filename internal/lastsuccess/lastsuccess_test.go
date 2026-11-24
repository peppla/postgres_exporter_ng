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

package lastsuccess

import (
	"sync"
	"testing"
	"time"
)

// TestNeverSucceededIsNotZero is the distinction the store exists to keep. A
// database nothing has read yet and a database last read at the epoch are
// different answers, and a zero timestamp reports the second for the first.
func TestNeverSucceededIsNotZero(t *testing.T) {
	s := New()
	if when, ok := s.At(Key{Target: "1", Database: "app"}); ok {
		t.Errorf("a key nothing recorded reports a success at %s", when)
	}
	s.Record(Key{Target: "1", Database: "app"}, time.Unix(0, 0))
	if _, ok := s.At(Key{Target: "1", Database: "app"}); !ok {
		t.Error("a success recorded at the epoch was not recorded")
	}
}

// TestEachPartOfTheKeySeparates covers what makes two successes different
// successes.
func TestEachPartOfTheKeySeparates(t *testing.T) {
	base := Key{Target: "1", Database: "app", Collector: "stat_user_tables"}
	when := time.Now()

	for _, tc := range []struct {
		field  string
		change func(Key) Key
	}{
		{"Target", func(k Key) Key { k.Target = "2"; return k }},
		{"Database", func(k Key) Key { k.Database = "other"; return k }},
		{"Collector", func(k Key) Key { k.Collector = "stat_database"; return k }},
	} {
		t.Run(tc.field, func(t *testing.T) {
			s := New()
			s.Record(base, when)
			if _, ok := s.At(tc.change(base)); ok {
				t.Errorf("a key differing only in %s read the other key's success", tc.field)
			}
		})
	}
}

// TestAnEarlierRecordingDoesNotMoveItBack covers the fan-out, which records
// from several goroutines and finishes in no particular order.
func TestAnEarlierRecordingDoesNotMoveItBack(t *testing.T) {
	s := New()
	key := Key{Target: "1", Database: "app"}
	later := time.Now()
	earlier := later.Add(-time.Minute)

	s.Record(key, later)
	s.Record(key, earlier)

	got, ok := s.At(key)
	if !ok {
		t.Fatal("the key went missing")
	}
	if !got.Equal(later) {
		t.Errorf("the store holds %s, want the later %s", got, later)
	}
}

// TestConcurrentRecordingIsSafe covers the fan-out under the race detector.
func TestConcurrentRecordingIsSafe(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Record(Key{Target: "1", Database: "app", Collector: string(rune('a' + i))}, time.Now())
		}(i)
	}
	wg.Wait()
	if s.Len() != 20 {
		t.Errorf("%d keys were recorded, want 20", s.Len())
	}
}

// TestANilStoreIsUsable keeps callers that have none from needing a guard at
// every call.
func TestANilStoreIsUsable(t *testing.T) {
	var s *Store
	s.Record(Key{Target: "1"}, time.Now())
	if _, ok := s.At(Key{Target: "1"}); ok {
		t.Error("a nil store reported a success")
	}
	if s.Len() != 0 {
		t.Error("a nil store reported keys")
	}
}

// TestLatestAnswersForATargetWhoseDatabaseIsUnknown covers the case a scrape
// has when its connection has just failed: it knows which connection it is and
// not which database that connection reaches, because the name comes from the
// server.
func TestLatestAnswersForATargetWhoseDatabaseIsUnknown(t *testing.T) {
	s := New()
	older := time.Now().Add(-time.Hour)
	newer := time.Now()

	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "app", Collector: "exporter"}, older)
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "reporting", Collector: "exporter"}, newer)
	// Another job and another target, neither of which this may answer with.
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "app", Collector: "custom_query.lr"}, newer)
	s.Record(Key{Engine: EngineQuery, Target: "2", Database: "other", Collector: "exporter"}, newer)

	database, when, ok := s.Latest(EngineQuery, OneCollector, "1", "exporter")
	if !ok {
		t.Fatal("a target with recorded successes reports none")
	}
	if database != "reporting" {
		t.Errorf("the latest success is on %q, want reporting", database)
	}
	if !when.Equal(newer) {
		t.Errorf("the latest success is at %s, want %s", when, newer)
	}

	if _, _, ok := s.Latest(EngineQuery, OneCollector, "3", "exporter"); ok {
		t.Error("a target with no successes reported one")
	}
	if _, _, ok := s.Latest(EngineQuery, OneCollector, "1", "custom_query.hr"); ok {
		t.Error("a job with no successes reported another job's")
	}
}

// TestSelectionIsSortedAndJoined pins the canonical form directly.
//
// The collector package builds it from a map, whose order is random, so a test
// that goes through one can pass with the names already in order. This passes
// them out of order on purpose.
func TestSelectionIsSortedAndJoined(t *testing.T) {
	if got := Selection([]string{"stat_user_tables", "pg_database", "stat_user_indexes"}); got != "pg_database,stat_user_indexes,stat_user_tables" {
		t.Errorf("Selection returned %q, want the names sorted and joined", got)
	}
	if got := Selection([]string{"b", "a"}); got != Selection([]string{"a", "b"}) {
		t.Error("the same two names in two orders are two different selections")
	}
	if got := Selection(nil); got != "" {
		t.Errorf("Selection(nil) is %q, want none", got)
	}
	// The caller's slice is not reordered under it: the collector package
	// passes a slice it goes on to use.
	names := []string{"b", "a"}
	Selection(names)
	if names[0] != "b" {
		t.Error("Selection sorted the caller's slice in place")
	}
}
