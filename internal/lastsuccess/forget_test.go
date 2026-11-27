package lastsuccess

import (
	"sync"
	"testing"
	"time"
)

// childOf is the identity discovery assigns the connection it builds to open
// one of a seed's databases.
//
// Every discovered database is collected over its own connection, so its
// records are keyed by that connection and not by the seed's. The fixtures
// below use that shape because production does. An earlier version of this
// file keyed every case on the seed itself, which no discovered database ever
// uses, and so every case passed against a cleanup that could not remove a
// discovered database at all.
func childOf(seed, database string) string { return seed + "/" + database }

// census is a complete catalogue read through one seed.
//
// present is what the server listed. gone names databases the seed has built a
// connection for in the past and the server no longer has: they are still
// among the seed's connections, which is what gives the census anything to act
// on.
func census(seed string, taken time.Time, present []string, _ ...string) Census {
	set := make(map[string]struct{}, len(present))
	for _, n := range present {
		set[n] = struct{}{}
	}
	return Census{Target: seed, Taken: taken, Databases: set}
}

// own registers the connections a seed built, the way discovery does, and
// returns the release for the holds that come with them.
//
// The store keeps the relation now, so a census carries only what the server
// said. A fixture that did not register its connections would hand the store
// an inventory it has no authority over, which is a census that removes
// nothing, which is the defect these cases exist for.
func own(s *Store, seed string, databases ...string) func() {
	releases := make([]func(), 0, len(databases))
	for _, name := range databases {
		releases = append(releases, s.Own(seed, childOf(seed, name), name))
	}
	return func() {
		for _, release := range releases {
			release()
		}
	}
}

// recordChild notes a success for a database under the connection built to
// reach it.
func recordChild(s *Store, seed, database string, at time.Time) Key {
	own(s, seed, database)()
	key := Key{Engine: EngineQuery, Target: childOf(seed, database), Database: database}
	s.Record(key, at)
	return key
}

// TestADatabaseTheServerNoLongerHasIsForgotten is what the component is for.
func TestADatabaseTheServerNoLongerHasIsForgotten(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	gone := recordChild(s, "1", "gone", long)
	kept := recordChild(s, "1", "kept", long)

	_, removed := s.Apply(census("1", time.Now(), []string{"kept"}, "gone"))
	if removed != 1 {
		t.Errorf("%d records removed, want 1", removed)
	}
	if _, ok := s.At(gone); ok {
		t.Error("a database the server does not have kept its record")
	}
	if _, ok := s.At(kept); !ok {
		t.Error("a database the server still has lost its record")
	}
}

// TestLosingConnectIsNotBeingDropped is the case a filtered read cannot tell
// apart. The census holds every database the server has, so a database this
// role may no longer connect to is still in it and keeps its history.
func TestLosingConnectIsNotBeingDropped(t *testing.T) {
	s := New()
	restricted := recordChild(s, "1", "restricted", time.Now().Add(-time.Hour))

	// The census is the whole inventory, restricted database included.
	if _, removed := s.Apply(census("1", time.Now(), []string{"restricted", "other"})); removed != 0 {
		t.Errorf("%d records removed, want 0", removed)
	}
	if _, ok := s.At(restricted); !ok {
		t.Error("a database that is still there lost its history because this role cannot reach it")
	}
}

// TestAnOlderCensusDoesNotEraseNewerWork covers one finishing late.
func TestAnOlderCensusDoesNotEraseNewerWork(t *testing.T) {
	s := New()
	started := time.Now().Add(-time.Minute)

	// The database was created after that census began looking, so the census
	// never saw it and cannot say it is absent.
	fresh := recordChild(s, "1", "new", time.Now())

	if _, removed := s.Apply(census("1", started, []string{"old"}, "new")); removed != 0 {
		t.Errorf("%d records removed, want 0: a census cannot remove what it never saw", removed)
	}
	if _, ok := s.At(fresh); !ok {
		t.Error("a record established after the census began was erased by it")
	}
}

// TestANewerInventoryProtectsAgainstAnOlderOneFinishingLate is the case a last
// success alone cannot cover.
//
// A database that exists and keeps failing has an old success and nothing
// newer. Reading only that time, it is indistinguishable from a database that
// was dropped an hour ago, so an older catalogue read finishing late removes
// history that a newer read had just established is still there. The newer
// read's generation is what says otherwise, and it has to be kept apart from
// the success time: a failed collection must not be given a success it did not
// have in order to protect itself.
func TestANewerInventoryProtectsAgainstAnOlderOneFinishingLate(t *testing.T) {
	s := New()
	old := time.Now().Add(-time.Hour)
	failing := recordChild(s, "1", "failing", old)

	// The newer read saw the database. Nothing collected from it since.
	newer := time.Now().Add(-time.Minute)
	confirmed, _ := s.Apply(census("1", newer, []string{"failing"}))
	if confirmed != 1 {
		t.Fatalf("the newer inventory confirmed %d records, want 1", confirmed)
	}

	// The older read began before that one and omits the database, because it
	// read the server at a moment this process has since moved past.
	older := time.Now().Add(-30 * time.Minute)
	if _, removed := s.Apply(census("1", older, nil, "failing")); removed != 0 {
		t.Errorf("an older inventory removed %d records after a newer one confirmed the database exists", removed)
	}
	if _, ok := s.At(failing); !ok {
		t.Error("a database a newer inventory had just confirmed lost its history to an older one")
	}
}

// TestConfirmingPresenceIsNotASuccess keeps the operator's answer honest.
//
// The only reason this store holds a generation at all is so that cleanup can
// tell a database that exists and fails from one that is gone. If confirming
// presence moved the public time instead, every failing database would report
// that it last succeeded moments ago, and it would do so at exactly the point
// an operator is reading that number to find out what is broken.
func TestConfirmingPresenceIsNotASuccess(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	key := recordChild(s, "1", "failing", long)

	if _, _ = s.Apply(census("1", time.Now(), []string{"failing"})); true {
		at, ok := s.At(key)
		if !ok {
			t.Fatal("confirming a database exists removed its record")
		}
		if !at.Equal(long) {
			t.Errorf("the last success moved to %v from %v: confirming presence was published as a collection that worked", at, long)
		}
	}

	database, at, ok := s.Latest(EngineQuery, OneCollector, childOf("1", "failing"), "")
	if !ok || database != "failing" || !at.Equal(long) {
		t.Errorf("Latest returned (%q, %v, %v), want (failing, %v, true)", database, at, ok, long)
	}
}

// TestATargetBeingCollectedIsNotForgotten covers work in flight.
//
// A catalogue read taken before a scrape started can finish while that scrape
// is in the middle of a database. Its list is older than the work, so it is in
// no position to decide the work is pointless.
func TestATargetBeingCollectedIsNotForgotten(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	key := recordChild(s, "1", "busy", long)

	release := s.Hold(childOf("1", "busy"))
	if _, removed := s.Apply(census("1", time.Now(), nil, "busy")); removed != 0 {
		t.Errorf("%d records removed while the target was being collected, want 0", removed)
	}
	if _, ok := s.At(key); !ok {
		t.Fatal("a target being collected lost its history")
	}

	// Once nothing is working on it the same census would remove it, which is
	// what makes the hold the reason it survived above and not something else.
	release()
	if _, removed := s.Apply(census("1", time.Now(), nil, "busy")); removed != 1 {
		t.Errorf("%d records removed after the collection finished, want 1", removed)
	}
}

// TestTwoScrapesOnOneTargetBothHaveToFinish covers the counting.
func TestTwoScrapesOnOneTargetBothHaveToFinish(t *testing.T) {
	s := New()
	recordChild(s, "1", "busy", time.Now().Add(-time.Hour))

	first := s.Hold(childOf("1", "busy"))
	second := s.Hold(childOf("1", "busy"))
	first()
	if _, removed := s.Apply(census("1", time.Now(), nil, "busy")); removed != 0 {
		t.Errorf("%d records removed while a second scrape was still collecting, want 0", removed)
	}
	second()
	if _, removed := s.Apply(census("1", time.Now(), nil, "busy")); removed != 1 {
		t.Errorf("%d records removed once both scrapes finished, want 1", removed)
	}
}

// TestOnlyTheCensusOwnConnectionIsTouched keeps one context out of another's.
//
// Two seeds, each with a connection to a database of the same name. They may
// be different servers, or the same server as different roles. Either way a
// catalogue read through one says nothing about the other.
func TestOnlyTheCensusOwnConnectionIsTouched(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	recordChild(s, "1", "gone", long)
	other := recordChild(s, "2", "gone", long)

	if _, removed := s.Apply(census("1", time.Now(), []string{"kept"}, "gone")); removed != 1 {
		t.Errorf("%d records removed, want 1", removed)
	}
	if _, ok := s.At(other); !ok {
		t.Error("another connection's record was removed by a census that never read it")
	}
}

// TestEveryProfileOfThatConnectionIsCovered is why a narrow request cannot do
// the removing but a census can.
func TestEveryProfileOfThatConnectionIsCovered(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	own(s, "1", "gone")()
	target := childOf("1", "gone")
	// The same database under two profiles and both engines, one of them a
	// whole-selection record nothing is scraping now.
	s.Record(Key{Engine: EngineQuery, Target: target, Database: "gone", Collector: "exporter"}, long)
	s.Record(Key{Engine: EngineCollector, Target: target, Database: "gone", Collector: "stat_user_tables"}, long)
	s.Record(Key{Engine: EngineCollector, Target: target, Database: "gone", About: WholeSelection,
		Collector: Selection([]string{"stat_user_tables", "stat_database"})}, long)

	if _, removed := s.Apply(census("1", time.Now(), []string{"kept"}, "gone")); removed != 3 {
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
	recordChild(s, "1", "gone", time.Now().Add(-time.Hour))

	for _, c := range []struct {
		name string
		c    Census
	}{
		{"no databases at all", Census{Target: "1", Taken: time.Now()}},
		{"no connection named", Census{Taken: time.Now(), Databases: map[string]struct{}{}}},
	} {
		if _, removed := s.Apply(c.c); removed != 0 {
			t.Errorf("%s removed %d records, want 0", c.name, removed)
		}
	}
	if s.Len() != 1 {
		t.Errorf("%d records remain, want the one left alone", s.Len())
	}
}

// TestACensusWithNoConnectionsOfItsOwnRemovesNothing is the state a seed is in
// when it has discovered nothing.
//
// It is also what the cleanup looked like in production for every discovered
// database: the census named the seed, every record was under a connection
// derived from it, and the two were never related, so a complete and correct
// catalogue removed nothing.
func TestACensusWithNoConnectionsOfItsOwnRemovesNothing(t *testing.T) {
	s := New()
	recordChild(s, "1", "gone", time.Now().Add(-time.Hour))

	// A census read through a seed that built nothing.
	if _, removed := s.Apply(census("2", time.Now(), []string{"kept"})); removed != 0 {
		t.Errorf("%d records removed, want 0", removed)
	}
}

// TestARecordOnTheSeedsOwnConnectionIsCoveredToo keeps the seed in scope.
//
// The seed is a target like any other and its own database is collected over
// it. That database is always in its own catalogue, so this is about the shape
// being handled rather than about a removal ever happening here.
func TestARecordOnTheSeedsOwnConnectionIsCoveredToo(t *testing.T) {
	s := New()
	long := time.Now().Add(-time.Hour)
	key := Key{Engine: EngineQuery, Target: "1", Database: "postgres"}
	s.Record(key, long)

	if _, removed := s.Apply(census("1", time.Now(), []string{"postgres"})); removed != 0 {
		t.Errorf("%d records removed, want 0", removed)
	}
	if _, removed := s.Apply(census("1", time.Now(), []string{"other"})); removed != 1 {
		t.Errorf("%d records removed, want 1: the seed's own database was not covered", removed)
	}
}

// TestAClusterRecordOnTheSeedIsLeftAlone covers the cluster-wide records,
// which are about the server and not about any database.
func TestAClusterRecordOnTheSeedIsLeftAlone(t *testing.T) {
	s := New()
	s.Record(Key{Engine: EngineCollector, Target: "1", Collector: "stat_bgwriter"}, time.Now().Add(-time.Hour))

	if _, removed := s.Apply(census("1", time.Now(), []string{"kept"})); removed != 0 {
		t.Errorf("%d records removed, want 0", removed)
	}
}

// TestAClusterRecordOnADroppedDatabasesConnectionGoes is the other half.
//
// A connection to a database that no longer exists cannot collect anything,
// cluster-wide or not, so nothing kept under it is an answer to any question.
func TestAClusterRecordOnADroppedDatabasesConnectionGoes(t *testing.T) {
	s := New()
	own(s, "1", "gone")()
	s.Record(Key{Engine: EngineCollector, Target: childOf("1", "gone"), Collector: "stat_bgwriter"},
		time.Now().Add(-time.Hour))

	if _, removed := s.Apply(census("1", time.Now(), []string{"kept"}, "gone")); removed != 1 {
		t.Errorf("%d records removed, want 1", removed)
	}
}

// TestCensusesAndCollectionOverlap runs the store the way a process does, with
// several requests recording and several catalogue reads applying at once.
//
// It is for the race detector and for the invariant that survives any
// interleaving: a database the server still has keeps its history, whatever
// order the reads and the writes land in.
func TestCensusesAndCollectionOverlap(t *testing.T) {
	s := New()
	present := []string{"a", "b", "c"}
	for _, name := range present {
		recordChild(s, "1", name, time.Now().Add(-time.Hour))
	}
	recordChild(s, "1", "gone", time.Now().Add(-time.Hour))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			taken := time.Now().Add(-time.Duration(i) * time.Second)
			s.Apply(census("1", taken, present, "gone"))
		}(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := present[0]
			release := s.Hold(childOf("1", name))
			defer release()
			recordChild(s, "1", name, time.Now())
		}()
	}
	wg.Wait()

	for _, name := range present {
		if _, ok := s.At(Key{Engine: EngineQuery, Target: childOf("1", name), Database: name}); !ok {
			t.Errorf("%s is still on the server and lost its history", name)
		}
	}
	if _, ok := s.At(Key{Engine: EngineQuery, Target: childOf("1", "gone"), Database: "gone"}); ok {
		t.Error("a database every census said was absent kept its record")
	}
}

// TestAConnectionBuiltByTwoSeedsBelongsToBoth keeps a cleanup from depending
// on one of them.
//
// Two seeds can be the same server and the same role, differing only in which
// database the seed itself opens, and each then builds the same connection
// string for a third database. That is one connection with two ways into it,
// so either seed's catalogue has to be able to answer for it. Keeping only the
// first leaves the cleanup working just while that seed is scraped.
func TestAConnectionBuiltByTwoSeedsBelongsToBoth(t *testing.T) {
	s := New()
	target := "shared"
	s.Own("A", target, "app")()
	s.Own("B", target, "app")()

	for _, seed := range []string{"A", "B"} {
		if database, ok := s.Owned(seed)[target]; !ok || database != "app" {
			t.Errorf("seed %s owns %v, and the connection it built for app is not among them", seed, s.Owned(seed))
		}
	}
	if owned := s.Owned("C"); len(owned) != 0 {
		t.Errorf("a seed that built nothing owns %v", owned)
	}
}

// TestANewerInventoryThroughEitherSeedProtectsASharedConnection is the other
// half of that, at the point it matters.
func TestANewerInventoryThroughEitherSeedProtectsASharedConnection(t *testing.T) {
	s := New()
	const target = "shared"
	s.Own("A", target, "app")()
	s.Own("B", target, "app")()

	long := time.Now().Add(-time.Hour)
	key := Key{Engine: EngineQuery, Target: target, Database: "app"}
	s.Record(key, long)

	// The newer read, through the second seed, saw the database.
	if confirmed, _ := s.Apply(Census{Target: "B", Taken: time.Now().Add(-time.Minute),
		Databases: map[string]struct{}{"app": {}}}); confirmed != 1 {
		t.Fatalf("the newer inventory confirmed %d records, want 1", confirmed)
	}
	// The older read, through the first, began earlier and does not list it.
	if _, removed := s.Apply(Census{Target: "A", Taken: time.Now().Add(-30 * time.Minute),
		Databases: map[string]struct{}{}}); removed != 0 {
		t.Errorf("an older inventory removed %d records after a newer one through the other seed confirmed the database", removed)
	}
	if _, ok := s.At(key); !ok {
		t.Error("a shared connection lost its history to the seed that was read first")
	}
}

// TestOwningAConnectionHoldsIt is what closes the window between discovering a
// connection and recording under it.
//
// A request reads a connection's identity while discovering it and records
// under that identity later. A catalogue read finishing in between would
// retire the relation, and what the request then records would have no owner,
// where no later catalogue read can reach it. Registering and holding together
// means the connection cannot be retired while the request still has it.
func TestOwningAConnectionHoldsIt(t *testing.T) {
	s := New()
	const target = "1/gone"
	release := s.Own("1", target, "gone")

	key := Key{Engine: EngineQuery, Target: target, Database: "gone"}
	s.Record(key, time.Now().Add(-time.Hour))

	if _, removed := s.Apply(census("1", time.Now(), nil)); removed != 0 {
		t.Errorf("%d records removed while the request still had the connection", removed)
	}
	if _, ok := s.Owned("1")[target]; !ok {
		t.Error("the relation was retired while the request still had the connection")
	}

	release()
	if _, removed := s.Apply(census("1", time.Now(), nil)); removed != 1 {
		t.Errorf("%d records removed once the request let go, want 1", removed)
	}
	if _, ok := s.Owned("1")[target]; ok {
		t.Error("the relation outlived every record under it")
	}
}

// TestAPublicationDuringCleanupIsNotOrphaned is the interleaving the hold
// exists for.
//
// An old request publishes while a catalogue read that began before it is
// removing what that read proved gone. The record it writes must still be
// reachable by a later catalogue read, which means the relation under it has
// to survive the first one.
func TestAPublicationDuringCleanupIsNotOrphaned(t *testing.T) {
	s := New()
	const target = "1/gone"

	// The request discovers the connection and holds it.
	release := s.Own("1", target, "gone")

	// A catalogue read taken before the request began finishes now, with the
	// database absent.
	taken := time.Now().Add(-time.Minute)
	if _, removed := s.Apply(Census{Target: "1", Taken: taken, Databases: map[string]struct{}{}}); removed != 0 {
		t.Fatalf("%d records removed, and there were none to remove", removed)
	}

	// The request publishes afterwards, for work it did before that read
	// finished.
	key := Key{Engine: EngineQuery, Target: target, Database: "gone"}
	s.Record(key, taken.Add(-time.Second))
	if _, ok := s.Owned("1")[target]; !ok {
		t.Fatal("the record was published under a connection nothing owns")
	}

	// The request ends, and the next complete read is what removes it.
	release()
	_, removed := s.Apply(Census{Target: "1", Taken: time.Now(), Databases: map[string]struct{}{}})
	if removed != 1 {
		t.Errorf("%d records removed by the read after the request ended, want 1", removed)
	}
	if s.Len() != 0 || len(s.Owned("1")) != 0 {
		t.Errorf("%d records and %d relations remain", s.Len(), len(s.Owned("1")))
	}
}

// TestARelationOutlivesOneProfilesRecord is why removing a record does not
// authorise retiring the connection it was about.
//
// Two profiles collect the same database. One profile's record is old enough
// for a census to remove and the other has just been written, so it stays.
// Retiring the relation on the strength of the first would leave the second
// with no owner.
func TestARelationOutlivesOneProfilesRecord(t *testing.T) {
	s := New()
	const target = "1/gone"
	s.Own("1", target, "gone")()

	old := Key{Engine: EngineQuery, Target: target, Database: "gone", Collector: "quiet"}
	busy := Key{Engine: EngineQuery, Target: target, Database: "gone", Collector: "busy"}
	s.Record(old, time.Now().Add(-time.Hour))

	taken := time.Now()
	s.Record(busy, taken.Add(time.Second))

	if _, removed := s.Apply(Census{Target: "1", Taken: taken, Databases: map[string]struct{}{}}); removed != 1 {
		t.Errorf("%d records removed, want the one the census is older than", removed)
	}
	if _, ok := s.Owned("1")[target]; !ok {
		t.Error("the relation was retired while another profile's record under it survived")
	}
	if _, ok := s.At(busy); !ok {
		t.Error("the record a census could not remove was removed anyway")
	}
}

// TestForgettingAProfileLeavesWhatOtherProfilesRead is what a released slot
// may and may not take with it.
//
// A whole-selection record exists because that selection was scraped. A
// per-collector record is about one collector on one database and is read by
// every profile that includes that collector, so it belongs to none of them
// and is bounded by the collectors and the connections instead. The ownership
// relation stays for the same reason: another profile may still be collecting
// that connection.
func TestForgettingAProfileLeavesWhatOtherProfilesRead(t *testing.T) {
	s := New()
	const target = "1/app"
	s.Own("1", target, "app")()

	now := time.Now()
	going := Selection([]string{"stat_database", "stat_user_tables"})
	staying := Selection([]string{"stat_database"})
	mine := Key{Engine: EngineCollector, About: WholeSelection, Target: target, Database: "app", Collector: going}
	theirs := Key{Engine: EngineCollector, About: WholeSelection, Target: target, Database: "app", Collector: staying}
	shared := Key{Engine: EngineCollector, Target: target, Database: "app", Collector: "stat_database"}
	other := Key{Engine: EngineQuery, About: WholeSelection, Target: target, Database: "app", Collector: going}
	for _, key := range []Key{mine, theirs, shared, other} {
		s.Record(key, now)
	}

	if removed := s.ForgetProfile(EngineCollector, going); removed != 1 {
		t.Errorf("%d records removed, want the one that profile alone was keeping", removed)
	}
	if _, ok := s.At(mine); ok {
		t.Error("the released profile's own record survived")
	}
	if _, ok := s.At(theirs); !ok {
		t.Error("another profile's record went with it")
	}
	if _, ok := s.At(shared); !ok {
		t.Error("a per-collector record, which every profile including that collector reads, went with it")
	}
	if _, ok := s.At(other); !ok {
		t.Error("the other engine's record for the same selection went with it")
	}
	if _, ok := s.Owned("1")[target]; !ok {
		t.Error("the ownership relation went with a profile, leaving live records with no owner")
	}
}

// TestAProfileBeingScrapedKeepsItsRecords keeps expiry off work in flight.
func TestAProfileBeingScrapedKeepsItsRecords(t *testing.T) {
	s := New()
	const target = "1/app"
	release := s.Own("1", target, "app")

	selection := Selection([]string{"stat_database"})
	key := Key{Engine: EngineCollector, About: WholeSelection, Target: target, Database: "app", Collector: selection}
	s.Record(key, time.Now())

	if removed := s.ForgetProfile(EngineCollector, selection); removed != 0 {
		t.Errorf("%d records removed while a request was holding the connection", removed)
	}
	release()
	if removed := s.ForgetProfile(EngineCollector, selection); removed != 1 {
		t.Errorf("%d records removed once the request finished, want 1", removed)
	}
}
