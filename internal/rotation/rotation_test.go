package rotation

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// names builds a sorted member list of n databases.
func names(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("db%02d", i))
	}
	return out
}

// TestEverySelectedDatabaseEventuallyComesFirst is the property the whole thing
// exists for. Over as many scrapes as there are databases, each one has had a
// turn at the front.
func TestEverySelectedDatabaseEventuallyComesFirst(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7} {
		s := New()
		k := Key{Engine: "collector", Target: "1", Profile: "all"}
		members := names(n)

		first := map[string]int{}
		for scrape := 0; scrape < n; scrape++ {
			first[members[s.Next(k, members)]]++
		}

		for _, name := range members {
			if first[name] != 1 {
				t.Errorf("with %d databases, %s was first %d times in %d scrapes, want once",
					n, name, first[name], n)
			}
		}
	}
}

// TestADatabaseThatStaysSelectedIsNotStarvedByMembershipChanges is the case a
// counter reduced by the set size fails.
//
// With the set alternating between two sizes, the counter advances every scrape
// while the position it lands on cycles through a subset of the members, so a
// database selected every time can go indefinitely without being offered the
// first attempt. With one worker and a budget only the first database consumes,
// that database is never attempted at all. Against the counter this case shows
// a:16, b:8, zz:8 and z never once.
func TestADatabaseThatStaysSelectedIsNotStarvedByMembershipChanges(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	even := []string{"a", "z"}
	odd := []string{"a", "b", "z", "zz"}

	firstOf := map[string]int{}
	for call := 0; call < 32; call++ {
		members := even
		if call%2 == 1 {
			members = odd
		}
		start := s.Next(k, members)
		if start < 0 || start >= len(members) {
			t.Fatalf("call %d chose position %d of %d members", call, start, len(members))
		}
		firstOf[members[start]]++
	}

	// a and z are in every one of the thirty-two selections.
	for _, name := range []string{"a", "z"} {
		if firstOf[name] == 0 {
			t.Errorf("%s is in every selection and never came first: %v", name, firstOf)
		}
	}
}

// TestEveryDatabaseIsStillVisited is the property rotation must not break. The
// order is a rotation, not a selection: a database left out here would have no
// series at all, which reads as one nobody selected.
func TestEveryDatabaseIsStillVisited(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}
	members := names(5)

	for scrape := 0; scrape < 20; scrape++ {
		order := Order(s.Next(k, members), len(members))
		seen := map[int]bool{}
		for _, i := range order {
			if seen[i] {
				t.Fatalf("scrape %d visits position %d twice: %v", scrape, i, order)
			}
			seen[i] = true
		}
		if len(seen) != len(members) {
			t.Fatalf("scrape %d visits %d of %d positions: %v", scrape, len(seen), len(members), order)
		}
	}
}

// TestTwoProfilesDoNotAdvanceEachOther keeps one collection profile's schedule
// out of another's. Two scrape jobs with different collect[] sets walk
// different sets, so sharing a schedule would move a job's turn on without it
// having had one.
func TestTwoProfilesDoNotAdvanceEachOther(t *testing.T) {
	s := New()
	wide := Key{Engine: "collector", Target: "1", Profile: "all"}
	narrow := Key{Engine: "collector", Target: "1", Profile: "stat_user_tables"}
	members := names(4)

	for i := 0; i < 3; i++ {
		s.Next(wide, members)
	}
	if got := s.Next(narrow, members); got != 0 {
		t.Errorf("the narrow profile's first scrape starts at %d, want 0: another profile moved its schedule", got)
	}
	if got := s.Next(wide, members); got != 3 {
		t.Errorf("the wide profile's fourth scrape starts at %d, want 3", got)
	}
}

// TestTheTwoEnginesAndTwoTargetsStayApart covers the rest of the key.
func TestTheTwoEnginesAndTwoTargetsStayApart(t *testing.T) {
	s := New()
	members := names(3)
	for _, k := range []Key{
		{Engine: "collector", Target: "1", Profile: "all"},
		{Engine: "query", Target: "1", Profile: "all"},
		{Engine: "collector", Target: "2", Profile: "all"},
	} {
		if got := s.Next(k, members); got != 0 {
			t.Errorf("%+v starts at %d, want 0", k, got)
		}
	}
	if s.Len() != 3 {
		t.Errorf("the store holds %d schedules for three distinct keys", s.Len())
	}
}

// TestOverlappingRequestsTakeDifferentDatabases is the atomicity requirement.
// Two requests on one schedule that both started at the same database would
// attempt the same ones and leave the same ones out, which is the behaviour
// rotation exists to end.
//
// As many requests as there are databases, because more than that must repeat:
// the set is then smaller than the concurrency, which is not the schedule
// failing.
func TestOverlappingRequestsTakeDifferentDatabases(t *testing.T) {
	const databases = 64

	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}
	members := names(databases)

	var mu sync.Mutex
	seen := map[int]int{}
	var wg sync.WaitGroup
	wg.Add(databases)
	for i := 0; i < databases; i++ {
		go func() {
			defer wg.Done()
			got := s.Next(k, members)
			mu.Lock()
			seen[got]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) != databases {
		t.Fatalf("%d of %d overlapping requests started on a database of their own", len(seen), databases)
	}
}

// TestARemovedDatabaseDoesNotRestartTheSchedule covers discovery dropping the
// database that was chosen last. The successor is taken from the members
// present now, so the schedule carries on rather than going back to the front.
func TestARemovedDatabaseDoesNotRestartTheSchedule(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	full := []string{"a", "b", "c", "d"}
	if got := s.Next(k, full); got != 0 {
		t.Fatalf("the first scrape starts at %d, want 0", got)
	}
	if got := s.Next(k, full); got != 1 {
		t.Fatalf("the second scrape starts at %d, want 1", got)
	}

	// b, the one chosen last, is gone.
	withoutB := []string{"a", "c", "d"}
	got := s.Next(k, withoutB)
	if withoutB[got] != "c" {
		t.Errorf("after b was removed the schedule chose %s, want c, the next after it", withoutB[got])
	}
}

// TestAScrapeThatFinishedNothingStillUsedItsTurn is the requirement that the
// schedule does not depend on how much work completed. A schedule that only
// moves on a completion leaves the same databases at the front of a budget
// nothing fits in, which is the measured failure.
func TestAScrapeThatFinishedNothingStillUsedItsTurn(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}
	members := names(3)

	// Next is told nothing about outcomes and takes nothing back, so a scrape
	// that collected nothing has still moved the schedule.
	first := s.Next(k, members)
	second := s.Next(k, members)
	if first == second {
		t.Errorf("two scrapes both started at %d", first)
	}
}

// TestAnEmptySetIsPosition is a scrape that selected nothing.
func TestAnEmptySetIsPosition(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}
	if got := s.Next(k, nil); got != 0 {
		t.Errorf("an empty selection chose position %d", got)
	}
	if s.Len() != 0 {
		t.Errorf("an empty selection recorded %d schedules", s.Len())
	}
	for _, n := range []int{0, -1} {
		if got := Order(0, n); got != nil {
			t.Errorf("a set of %d databases gave the order %v", n, got)
		}
	}
}

// TestTheStoreGrowsWithProfilesNotDatabases is the bound this store claims.
func TestTheStoreGrowsWithProfilesNotDatabases(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}
	members := names(1000)

	for scrape := 0; scrape < 500; scrape++ {
		s.Next(k, members)
	}
	if s.Len() != 1 {
		t.Errorf("500 scrapes over 1000 databases left %d schedules, want 1", s.Len())
	}

	for i := 0; i < 4; i++ {
		s.Next(Key{Engine: "collector", Target: "1", Profile: fmt.Sprintf("profile%d", i)}, members)
	}
	if s.Len() != 5 {
		t.Errorf("the store holds %d schedules for five profiles", s.Len())
	}
}

// TestANilStoreHasNoRotation says what a missing schedule does. It is the
// sorted order every scrape, which is the behaviour that starves the tail of a
// set that does not fit the budget, so every shipped request path is given the
// process's store.
func TestANilStoreHasNoRotation(t *testing.T) {
	var s *Store
	members := names(4)
	for scrape := 0; scrape < 5; scrape++ {
		if got := s.Next(Key{}, members); got != 0 {
			t.Errorf("a nil store chose position %d, want 0 every time", got)
		}
	}
	if got := s.Len(); got != 0 {
		t.Errorf("a nil store holds %d schedules", got)
	}
}

// TestANewcomerEveryScrapeDoesNotStarveASelectedDatabase is the churn case.
//
// The set is exactly two members every scrape, so nothing here depends on a
// growing number of databases competing for capacity. One member stays
// selected throughout. The other is replaced each scrape by a newcomer whose
// name sorts after the previous one and before the member that stays.
//
// Choosing the first current name after the one chosen last time picks the
// newcomer every scrape, so the member that is always selected never comes
// first and, with one worker and a budget the first attempt consumes, is never
// attempted at all.
func TestANewcomerEveryScrapeDoesNotStarveASelectedDatabase(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	firstOf := map[string]int{}
	for call := 0; call < 32; call++ {
		members := []string{fmt.Sprintf("a%04d", call), "z"}
		firstOf[members[s.Next(k, members)]]++
	}

	if firstOf["z"] == 0 {
		t.Errorf("z was selected on every one of 32 scrapes and never came first: %v", firstOf)
	}
}

// TestFairnessOverChurnIsBounded says how long a selected database can wait.
//
// Not just that it comes first eventually, which an unbounded wait also
// satisfies. With two members and a newcomer every scrape, the one that stays
// has to come first about half the time, because it is the only member with a
// turn outstanding whenever it is not the one just served.
func TestFairnessOverChurnIsBounded(t *testing.T) {
	const scrapes = 40

	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	firstOf := map[string]int{}
	longestWait, since := 0, 0
	for call := 0; call < scrapes; call++ {
		members := []string{fmt.Sprintf("a%04d", call), "z"}
		chosen := members[s.Next(k, members)]
		firstOf[chosen]++
		if chosen == "z" {
			if since > longestWait {
				longestWait = since
			}
			since = 0
			continue
		}
		since++
	}

	if firstOf["z"] < scrapes/2-1 {
		t.Errorf("z came first %d times in %d scrapes, want about half: %v", firstOf["z"], scrapes, firstOf)
	}
	if longestWait > 2 {
		t.Errorf("z waited %d scrapes between turns, want no more than 2", longestWait)
	}
}

// TestTheInputOrderDoesNotDecideWhoIsSkipped is the collector engine's own
// input, where the configured connection database comes first and the rest
// follow in sorted order, so the list as a whole is not sorted.
//
// Against a rule that chose the first name after the one chosen last time,
// a_review was never chosen: the first name after postgres is z_review, and
// after z_review the rule wrapped to the front, which is postgres again. Six
// short-budget requests through the engine attempted z_review, postgres,
// z_review, postgres, z_review and postgres, and a_review was deferred on all
// six.
func TestTheInputOrderDoesNotDecideWhoIsSkipped(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}
	members := []string{"postgres", "a_review", "z_review"}

	first := map[string]int{}
	for call := 0; call < len(members); call++ {
		first[members[s.Next(k, members)]]++
	}

	for _, name := range members {
		if first[name] != 1 {
			t.Errorf("%s came first %d times in %d calls, want once: %v", name, first[name], len(members), first)
		}
	}
}

// TestASelectionThatCollapsesLeavesNothingWaiting is the case a two-member
// test cannot reach.
//
// Reconciling the waiting list against the current members is the only thing
// that keeps this store from growing, and it happens inside Next. A selection
// that falls to one member, or to none, still has to go through it: a
// thousand-database selection collapsing to one left nine hundred and
// ninety-nine names waiting, and ten further calls did not shift them.
func TestASelectionThatCollapsesLeavesNothingWaiting(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	members := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		members = append(members, strconv.Itoa(i))
	}
	s.Next(k, members)
	if s.Waiting(k) != len(members)-1 {
		t.Fatalf("a thousand members left %d waiting, so the fixture is not what this is about", s.Waiting(k))
	}

	// Down to one, ten times over.
	for i := 0; i < 10; i++ {
		s.Next(k, members[:1])
	}
	if waiting := s.Waiting(k); waiting != 0 {
		t.Errorf("a schedule over one member is holding %d names", waiting)
	}

	// And back up, then down to none.
	s.Next(k, members)
	s.Next(k, nil)
	if waiting := s.Waiting(k); waiting != 0 {
		t.Errorf("a schedule over no members at all is holding %d names", waiting)
	}
	if s.Len() != 0 {
		t.Errorf("a selection with nothing in it kept %d schedules", s.Len())
	}
}
