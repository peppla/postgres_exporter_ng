package lastsuccess

import (
	"testing"
	"time"
)

func census(target string, taken time.Time, names ...string) Census {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return Census{Target: target, Taken: taken, Databases: set}
}

// TestADatabaseTheServerNoLongerHasIsForgotten is what the component is for.
func TestADatabaseTheServerNoLongerHasIsForgotten(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "gone"}, long)
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "kept"}, long)

	if removed := s.Forget(census("1", time.Now(), "kept")); removed != 1 {
		t.Errorf("%d records removed, want 1", removed)
	}
	if _, ok := s.At(Key{Engine: EngineQuery, Target: "1", Database: "gone"}); ok {
		t.Error("a database the server does not have kept its record")
	}
	if _, ok := s.At(Key{Engine: EngineQuery, Target: "1", Database: "kept"}); !ok {
		t.Error("a database the server still has lost its record")
	}
}

// TestLosingConnectIsNotBeingDropped is the case a filtered read cannot tell
// apart. The census holds every database the server has, so a database this
// role may no longer connect to is still in it and keeps its history.
func TestLosingConnectIsNotBeingDropped(t *testing.T) {
	s := New()
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "restricted"}, time.Now().Add(-time.Hour))

	// The census is the whole inventory, restricted database included.
	if removed := s.Forget(census("1", time.Now(), "restricted", "other")); removed != 0 {
		t.Errorf("%d records removed, want 0", removed)
	}
	if _, ok := s.At(Key{Engine: EngineQuery, Target: "1", Database: "restricted"}); !ok {
		t.Error("a database that is still there lost its history because this role cannot reach it")
	}
}

// TestAnOlderCensusDoesNotEraseNewerWork covers one finishing late.
func TestAnOlderCensusDoesNotEraseNewerWork(t *testing.T) {
	s := New()
	started := time.Now().Add(-time.Minute)

	// The database was created after that census began looking, so the census
	// never saw it and cannot say it is absent.
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "new"}, time.Now())

	if removed := s.Forget(census("1", started, "old")); removed != 0 {
		t.Errorf("%d records removed, want 0: a census cannot remove what it never saw", removed)
	}
	if _, ok := s.At(Key{Engine: EngineQuery, Target: "1", Database: "new"}); !ok {
		t.Error("a record established after the census began was erased by it")
	}
}

// TestOnlyTheCensusOwnConnectionIsTouched keeps one context out of another's.
func TestOnlyTheCensusOwnConnectionIsTouched(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "gone"}, long)
	s.Record(Key{Engine: EngineQuery, Target: "2", Database: "gone"}, long)

	if removed := s.Forget(census("1", time.Now(), "kept")); removed != 1 {
		t.Errorf("%d records removed, want 1", removed)
	}
	if _, ok := s.At(Key{Engine: EngineQuery, Target: "2", Database: "gone"}); !ok {
		t.Error("another connection's record was removed by a census that never read it")
	}
}

// TestEveryProfileOfThatConnectionIsCovered is why a narrow request cannot do
// the removing but a census can.
func TestEveryProfileOfThatConnectionIsCovered(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	// The same database under two profiles and both engines, one of them a
	// whole-selection record nothing is scraping now.
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "gone", Collector: "exporter"}, long)
	s.Record(Key{Engine: EngineCollector, Target: "1", Database: "gone", Collector: "stat_user_tables"}, long)
	s.Record(Key{Engine: EngineCollector, Target: "1", Database: "gone",
		Collector: Selection([]string{"stat_user_tables", "stat_database"})}, long)

	if removed := s.Forget(census("1", time.Now(), "kept")); removed != 3 {
		t.Errorf("%d records removed, want 3: an idle profile would keep the database for ever", removed)
	}
	if s.Len() != 0 {
		t.Errorf("%d records remain", s.Len())
	}
}

// TestAnIncompleteCensusAuthorisesNothing is the refusal. Any doubt leaves
// every record alone, which is why the caller passes no census at all rather
// than a short one.
func TestAnIncompleteCensusAuthorisesNothing(t *testing.T) {
	s := New()
	s.Record(Key{Engine: EngineQuery, Target: "1", Database: "gone"}, time.Now().Add(-time.Hour))

	for _, c := range []struct {
		name string
		c    Census
	}{
		{"no databases at all", Census{Target: "1", Taken: time.Now()}},
		{"no connection named", Census{Taken: time.Now(), Databases: map[string]struct{}{}}},
	} {
		if removed := s.Forget(c.c); removed != 0 {
			t.Errorf("%s removed %d records, want 0", c.name, removed)
		}
	}
	if s.Len() != 1 {
		t.Errorf("%d records remain, want the one left alone", s.Len())
	}
}

// TestACollectorRecordWithNoDatabaseIsLeftAlone covers the cluster-wide
// records, which are about the server and not about any database.
func TestACollectorRecordWithNoDatabaseIsLeftAlone(t *testing.T) {
	s := New()
	s.Record(Key{Engine: EngineCollector, Target: "1", Collector: "stat_bgwriter"}, time.Now().Add(-time.Hour))

	if removed := s.Forget(census("1", time.Now(), "kept")); removed != 0 {
		t.Errorf("%d records removed, want 0", removed)
	}
}
