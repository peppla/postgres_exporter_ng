package slots

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// clock is a hand-wound clock, so a case says what it means by quiet rather
// than waiting for it.
type clock struct {
	mtx sync.Mutex
	at  time.Time
}

func newClock() *clock { return &clock{at: time.Now()} }

func (c *clock) now() time.Time {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.at
}

func (c *clock) advance(d time.Duration) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.at = c.at.Add(d)
}

func profile(name string) Key { return Key{Kind: Profile, Name: name} }
func context(name string) Key { return Key{Kind: Context, Name: name} }

// TestAHolderIsNotEvictedByNewcomers is the guarantee admission exists for.
//
// Least recently used gives no guarantee here: a stream of names nobody is
// scraping takes a slot away from a profile that is being scraped. Under
// admission nothing another requester does can free a slot.
func TestAHolderIsNotEvictedByNewcomers(t *testing.T) {
	c := newClock()
	s := New(Limits{Profiles: 2, Idle: time.Hour}, nil).WithClock(c.now)

	for _, name := range []string{"a", "b"} {
		release, _, ok := s.Admit(profile(name))
		if !ok {
			t.Fatalf("%s was refused with the store empty", name)
		}
		release()
	}

	// Fifty newcomers, none of which may take a slot from a or b.
	for i := 0; i < 50; i++ {
		release, refused, ok := s.Admit(profile("new"))
		if ok {
			release()
			t.Fatalf("newcomer %d was admitted with every slot held", i)
		}
		if refused != profile("new") {
			t.Fatalf("the refusal names %v", refused)
		}
	}

	for _, name := range []string{"a", "b"} {
		release, _, ok := s.Admit(profile(name))
		if !ok {
			t.Errorf("%s lost its slot to a newcomer", name)
			continue
		}
		release()
	}
	if held := s.Held(Profile); held != 2 {
		t.Errorf("%d profiles hold a slot, want 2", held)
	}
}

// TestARefusedRequestLeavesNothingBehind covers partial admission.
//
// A request that takes one slot and is then refused another has reserved state
// it will never use, and a client repeating that fills the store with names
// nothing is scraping.
func TestARefusedRequestLeavesNothingBehind(t *testing.T) {
	c := newClock()
	s := New(Limits{Profiles: 2, Contexts: 0, Idle: time.Hour}, nil).WithClock(c.now)

	// Fill the profiles with two that stay.
	for _, name := range []string{"a", "b"} {
		release, _, _ := s.Admit(profile(name))
		release()
	}

	// This request wants a context, which is unlimited, and a profile, which
	// is not. It must end up holding neither.
	release, refused, ok := s.Admit(context("probe-one"), profile("new"))
	if ok {
		release()
		t.Fatal("a request was admitted with no profile slot free")
	}
	if refused != profile("new") {
		t.Errorf("the refusal names %v, want the profile", refused)
	}
	if held := s.Held(Context); held != 0 {
		t.Errorf("%d context slots survive a refused request, want 0", held)
	}
	if held := s.Held(Profile); held != 2 {
		t.Errorf("%d profile slots, want the two that were already there", held)
	}
}

// TestASlotInUseIsNeverReleased keeps work in flight out of it.
func TestASlotInUseIsNeverReleased(t *testing.T) {
	c := newClock()
	s := New(Limits{Profiles: 1, Idle: time.Minute}, nil).WithClock(c.now)

	held, _, ok := s.Admit(profile("slow"))
	if !ok {
		t.Fatal("the first request was refused")
	}

	// Long past the window, with the request still running.
	c.advance(time.Hour)
	if released := s.Sweep(); released != 0 {
		t.Errorf("a sweep released %d slots while a request was holding one", released)
	}
	if _, _, ok := s.Admit(profile("other")); ok {
		t.Error("a newcomer took the slot of a request that was still running")
	}

	held()
	if released := s.Sweep(); released != 1 {
		t.Errorf("a sweep released %d slots once the request finished, want 1", released)
	}
}

// TestAQuietSlotIsReleasedAndItsOwnerToldIsWhatBoundsThis covers the only
// thing that frees a slot.
func TestAQuietSlotIsReleasedAndItsOwnerTold(t *testing.T) {
	c := newClock()
	var told []Key
	s := New(Limits{Profiles: 2, Idle: time.Minute}, func(k Key) {
		told = append(told, k)
	}).WithClock(c.now)

	for _, name := range []string{"a", "b"} {
		release, _, _ := s.Admit(profile(name))
		release()
	}

	// a keeps being asked for and b does not.
	for i := 0; i < 3; i++ {
		c.advance(30 * time.Second)
		release, _, ok := s.Admit(profile("a"))
		if !ok {
			t.Fatalf("a was refused on request %d", i)
		}
		release()
	}

	release, _, ok := s.Admit(profile("new"))
	if !ok {
		t.Fatal("a newcomer was refused with a slot quiet for longer than the window")
	}
	release()

	if len(told) != 1 || told[0] != profile("b") {
		t.Errorf("the owner was told about %v, want just b", told)
	}
	if held := s.Held(Profile); held != 2 {
		t.Errorf("%d profiles hold a slot, want 2", held)
	}
	if _, _, ok := s.Admit(profile("b")); ok {
		t.Error("b still held its slot after going quiet past the window")
	}
}

// TestAKindDoesNotSpendAnothersLimit keeps the two apart.
func TestAKindDoesNotSpendAnothersLimit(t *testing.T) {
	c := newClock()
	s := New(Limits{Profiles: 1, Contexts: 1, Idle: time.Hour}, nil).WithClock(c.now)

	release, _, ok := s.Admit(profile("same"), context("same"))
	if !ok {
		t.Fatal("one profile and one context did not fit in one of each")
	}
	release()
	if s.Held(Profile) != 1 || s.Held(Context) != 1 {
		t.Errorf("%d profiles and %d contexts, want one of each", s.Held(Profile), s.Held(Context))
	}
}

// TestNoLimitConfiguredRefusesNothing is the state this ships in until the
// three values are decided.
func TestNoLimitConfiguredRefusesNothing(t *testing.T) {
	s := New(Limits{}, nil)
	for i := 0; i < 100; i++ {
		release, _, ok := s.Admit(profile(string(rune('a'+i%26)) + string(rune('a'+i/26))))
		if !ok {
			t.Fatalf("request %d was refused with no limit configured", i)
		}
		release()
	}
	if released := s.Sweep(); released != 0 {
		t.Errorf("a sweep released %d slots with no window configured", released)
	}
}

// TestTheSameNameTwiceIsOneSlot covers a request that needs one name for two
// reasons.
func TestTheSameNameTwiceIsOneSlot(t *testing.T) {
	s := New(Limits{Profiles: 1, Idle: time.Hour}, nil)
	release, _, ok := s.Admit(profile("one"), profile("one"))
	if !ok {
		t.Fatal("one name twice did not fit in one slot")
	}
	release()
	if held := s.Held(Profile); held != 1 {
		t.Errorf("%d slots for one name, want 1", held)
	}
	// Released once, so nothing is still holding it.
	if released := New(Limits{Profiles: 1, Idle: time.Nanosecond}, nil).Sweep(); released != 0 {
		t.Errorf("a fresh store swept %d slots", released)
	}
}

// TestConcurrentAdmissionIsSafe is for the race detector and for the limit
// holding under contention.
func TestConcurrentAdmissionIsSafe(t *testing.T) {
	s := New(Limits{Profiles: 4, Idle: time.Hour}, nil)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			release, _, ok := s.Admit(profile(string(rune('a' + i%16))))
			if ok {
				release()
			}
		}(i)
	}
	wg.Wait()

	if held := s.Held(Profile); held > 4 {
		t.Errorf("%d profiles hold a slot, want at most 4", held)
	}
}

// TestRetirementAndReadmissionAreOrdered is the gap between removing a slot
// and dropping what it was keeping.
//
// Removing the slot under the lock and running the callback after it leaves a
// moment in which another request is admitted under the same name and
// publishes, and the callback that follows then deletes what the new admission
// has just written while its lease is held.
//
// The second request here is what a real one would do in that moment. What it
// must not be is admitted while the retirement is still running.
func TestRetirementAndReadmissionAreOrdered(t *testing.T) {
	c := newClock()
	var s *Store
	var retiring, admittedDuring atomic.Bool
	var wg sync.WaitGroup
	var retired []Key

	s = New(Limits{Profiles: 1, Idle: time.Minute}, func(k Key) {
		retired = append(retired, k)
		retiring.Store(true)
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, _, ok := s.Admit(profile(k.Name))
			if !ok {
				return
			}
			if retiring.Load() {
				admittedDuring.Store(true)
			}
			release()
		}()
		// Long enough for that request to get in if anything lets it.
		time.Sleep(100 * time.Millisecond)
		retiring.Store(false)
	}).WithClock(c.now)

	release, _, ok := s.Admit(profile("a"))
	if !ok {
		t.Fatal("the first request was refused")
	}
	release()

	c.advance(time.Hour)
	if released := s.Sweep(); released != 1 {
		t.Fatalf("a sweep released %d slots, want 1", released)
	}
	wg.Wait()

	if len(retired) != 1 || retired[0] != profile("a") {
		t.Fatalf("the owner was told about %v", retired)
	}
	if admittedDuring.Load() {
		t.Error("a name was readmitted while what it was keeping was still being dropped")
	}
}
