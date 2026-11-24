package rotation

import (
	"fmt"
	"sync"
	"testing"
)

// TestEverySelectedDatabaseEventuallyComesFirst is the property the whole thing
// exists for. Over as many scrapes as there are databases, each one has had a
// turn at the front.
func TestEverySelectedDatabaseEventuallyComesFirst(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7} {
		s := New()
		k := Key{Engine: "collector", Target: "1", Profile: "all"}

		first := map[int]int{}
		for scrape := 0; scrape < n; scrape++ {
			order := Order(s.Take(k), n)
			if len(order) != n {
				t.Fatalf("with %d databases the order has %d entries", n, len(order))
			}
			first[order[0]]++
		}

		for i := 0; i < n; i++ {
			if first[i] != 1 {
				t.Errorf("with %d databases, database %d was first %d times in %d scrapes, want once",
					n, i, first[i], n)
			}
		}
	}
}

// TestEveryDatabaseIsStillVisited is the property rotation must not break. The
// order is a rotation, not a selection: a database left out here would have no
// series at all, which reads as one nobody selected.
func TestEveryDatabaseIsStillVisited(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	for scrape := 0; scrape < 20; scrape++ {
		order := Order(s.Take(k), 5)
		seen := map[int]bool{}
		for _, i := range order {
			if seen[i] {
				t.Fatalf("scrape %d visits position %d twice: %v", scrape, i, order)
			}
			seen[i] = true
		}
		if len(seen) != 5 {
			t.Fatalf("scrape %d visits %d of 5 positions: %v", scrape, len(seen), order)
		}
	}
}

// TestTwoProfilesDoNotAdvanceEachOther keeps one collection profile's schedule
// out of another's. Two scrape jobs with different collect[] sets walk
// different sets, so sharing a sequence would move a job's turn on without it
// having had one.
func TestTwoProfilesDoNotAdvanceEachOther(t *testing.T) {
	s := New()
	wide := Key{Engine: "collector", Target: "1", Profile: "all"}
	narrow := Key{Engine: "collector", Target: "1", Profile: "stat_user_tables"}

	// The wide profile takes several turns.
	for i := 0; i < 5; i++ {
		s.Take(wide)
	}
	if got := s.Take(narrow); got != 0 {
		t.Errorf("the narrow profile's first turn is %d, want 0: another profile moved its schedule", got)
	}
	if got := s.Take(wide); got != 5 {
		t.Errorf("the wide profile's next turn is %d, want 5", got)
	}
}

// TestTheTwoEnginesAndTwoTargetsStayApart covers the rest of the key.
func TestTheTwoEnginesAndTwoTargetsStayApart(t *testing.T) {
	s := New()
	for _, k := range []Key{
		{Engine: "collector", Target: "1", Profile: "all"},
		{Engine: "query", Target: "1", Profile: "all"},
		{Engine: "collector", Target: "2", Profile: "all"},
	} {
		if got := s.Take(k); got != 0 {
			t.Errorf("%+v starts at %d, want 0", k, got)
		}
	}
	if s.Len() != 3 {
		t.Errorf("the store holds %d schedules for three distinct keys", s.Len())
	}
}

// TestOverlappingRequestsTakeDifferentTurns is the atomicity requirement. Two
// requests on one schedule that both started at the same offset would attempt
// the same databases and leave the same ones out, which is the behaviour
// rotation exists to end.
func TestOverlappingRequestsTakeDifferentTurns(t *testing.T) {
	const requests = 200

	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	var mu sync.Mutex
	seen := map[uint64]int{}
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			got := s.Take(k)
			mu.Lock()
			seen[got]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) != requests {
		t.Fatalf("%d of %d overlapping requests got a turn of their own", len(seen), requests)
	}
	for turn, count := range seen {
		if count != 1 {
			t.Errorf("turn %d was handed out %d times", turn, count)
		}
	}
}

// TestTheScheduleDoesNotRestartWhenTheSetChanges covers discovery adding or
// removing a database. The sequence counts rather than being reduced to the
// size of the set, so a change in size moves where an offset lands without
// putting the schedule back to the beginning.
func TestTheScheduleDoesNotRestartWhenTheSetChanges(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	// Three scrapes over three databases, so the next turn is 3.
	for i := 0; i < 3; i++ {
		Order(s.Take(k), 3)
	}

	// Discovery now offers four. The turn after those three is still the
	// fourth, not the first.
	offset := s.Take(k)
	if offset != 3 {
		t.Fatalf("the turn after three scrapes is %d, want 3", offset)
	}
	order := Order(offset, 4)
	if order[0] != 3 {
		t.Errorf("with the set grown to four, the scrape starts at %d, want 3: the schedule restarted", order[0])
	}
}

// TestAScrapeThatFinishedNothingStillUsedItsTurn is the requirement that the
// schedule does not depend on how much work completed. A schedule that only
// moves on a completion leaves the same databases at the front of a budget
// nothing fits in, which is the measured failure.
func TestAScrapeThatFinishedNothingStillUsedItsTurn(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	// Take tells the caller nothing about outcomes and takes nothing back, so
	// a scrape that collected nothing has still moved the schedule.
	first := s.Take(k)
	second := s.Take(k)
	if second != first+1 {
		t.Errorf("two scrapes took turns %d and %d, want consecutive", first, second)
	}
}

// TestAnEmptySetIsNotAnOrder covers a scrape that selected nothing.
func TestAnEmptySetIsNotAnOrder(t *testing.T) {
	for _, n := range []int{0, -1} {
		if got := Order(7, n); got != nil {
			t.Errorf("a set of %d databases gave the order %v", n, got)
		}
	}
}

// TestTheStoreGrowsWithProfilesNotDatabases is the bound this store claims.
func TestTheStoreGrowsWithProfilesNotDatabases(t *testing.T) {
	s := New()
	k := Key{Engine: "collector", Target: "1", Profile: "all"}

	for scrape := 0; scrape < 500; scrape++ {
		Order(s.Take(k), 1000)
	}
	if s.Len() != 1 {
		t.Errorf("500 scrapes over 1000 databases left %d schedules, want 1", s.Len())
	}

	for i := 0; i < 4; i++ {
		s.Take(Key{Engine: "collector", Target: "1", Profile: fmt.Sprintf("profile%d", i)})
	}
	if s.Len() != 5 {
		t.Errorf("the store holds %d schedules for five profiles", s.Len())
	}
}

// TestANilStoreIsUsable covers the engines that are built without one, so a
// missing schedule is slow fairness rather than a panic.
func TestANilStoreIsUsable(t *testing.T) {
	var s *Store
	if got := s.Take(Key{}); got != 0 {
		t.Errorf("a nil store handed out turn %d", got)
	}
	if got := s.Len(); got != 0 {
		t.Errorf("a nil store holds %d schedules", got)
	}
}
