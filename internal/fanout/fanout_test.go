package fanout

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestEveryItemIsVisited is the premise the deferral series rest on. An item
// this skipped would be a selected database with no series at all.
func TestEveryItemIsVisited(t *testing.T) {
	for _, n := range []int{1, 2, 7, 100, 1000} {
		seen := make([]atomic.Int32, n)
		Run(n, 4, func(i int) { seen[i].Add(1) })
		for i := range seen {
			if got := seen[i].Load(); got != 1 {
				t.Fatalf("with %d items, item %d ran %d times, want 1", n, i, got)
			}
		}
	}
}

// TestNoItemsIsNotAnError covers a scrape that selected nothing.
func TestNoItemsIsNotAnError(t *testing.T) {
	for _, n := range []int{0, -1} {
		ran := false
		Run(n, 4, func(int) { ran = true })
		if ran {
			t.Errorf("with n=%d the work function ran", n)
		}
	}
}

// TestConcurrencyIsCappedByTheWorkerCount holds every running item until the
// cap is reached, which only happens if no more than that many run at once.
func TestConcurrencyIsCappedByTheWorkerCount(t *testing.T) {
	const items, workers = 200, 5

	var running, peak atomic.Int32
	var mu sync.Mutex
	Run(items, workers, func(int) {
		now := running.Add(1)
		mu.Lock()
		if now > peak.Load() {
			peak.Store(now)
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		running.Add(-1)
	})

	// The premise first. A cap that was never reached would make the bound
	// below pass for a run that was serial for some other reason.
	if peak.Load() != workers {
		t.Fatalf("peak concurrency was %d, want exactly %d: the cap was never reached", peak.Load(), workers)
	}
}

// TestWorkerCountDoesNotGrowWithTheWorkCount is ChatGPT's acceptance check for
// the bounded pool, in the small.
//
// One goroutine per item is what this replaced, so the measurement that matters
// is the goroutine count while the work is held, at two sizes that differ by two
// orders of magnitude.
func TestWorkerCountDoesNotGrowWithTheWorkCount(t *testing.T) {
	const workers = 4

	measure := func(items int) int {
		release := make(chan struct{})
		var held atomic.Int32
		done := make(chan struct{})

		base := runtime.NumGoroutine()
		go func() {
			Run(items, workers, func(int) {
				held.Add(1)
				<-release
			})
			close(done)
		}()

		// Wait for every worker to be parked in the work function, so the
		// count below is of workers and not of a race with their startup.
		deadline := time.Now().Add(5 * time.Second)
		for int(held.Load()) < workers {
			if time.Now().After(deadline) {
				close(release)
				<-done
				t.Fatalf("with %d items only %d workers started", items, held.Load())
			}
			time.Sleep(time.Millisecond)
		}

		peak := runtime.NumGoroutine() - base
		close(release)
		<-done
		return peak
	}

	small := measure(10)
	large := measure(1000)

	// The pool plus the goroutine driving it. A per-item fan-out would put the
	// large figure near a thousand.
	if large > small+2 {
		t.Errorf("goroutines above the baseline: %d items gave %d, %d items gave %d, want no growth with the item count",
			10, small, 1000, large)
	}
	if small > workers+2 {
		t.Errorf("%d workers over 10 items used %d goroutines above the baseline", workers, small)
	}
}

// TestAWorkerTakesTheNextItemWhenItFinishes keeps one item slow and requires
// the rest to be finished by workers that moved on, rather than waiting.
//
// The assertion is what was true when the slow item was released, not what was
// true at the end. An earlier version checked only that all fifty items
// eventually finished, which a strided slice per worker also satisfies: the
// items behind the slow one finish after the watchdog gives up and releases it.
// That mutation passed every test in this file.
func TestAWorkerTakesTheNextItemWhenItFinishes(t *testing.T) {
	const items, workers = 50, 4

	var done, doneWhenReleased atomic.Int32
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})

	go func() {
		<-slowStarted
		deadline := time.Now().Add(5 * time.Second)
		for int(done.Load()) < items-1 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		doneWhenReleased.Store(done.Load())
		close(releaseSlow)
	}()

	var once sync.Once
	Run(items, workers, func(i int) {
		if i == 0 {
			once.Do(func() { close(slowStarted) })
			<-releaseSlow
			done.Add(1)
			return
		}
		done.Add(1)
	})

	if int(done.Load()) != items {
		t.Errorf("%d of %d items finished", done.Load(), items)
	}
	if got := int(doneWhenReleased.Load()); got != items-1 {
		t.Errorf("when the slow item was released %d of the other %d items had finished, want all of them: %d were waiting behind it",
			got, items-1, items-1-got)
	}
}

// TestAWorkerCountBelowOneStillDoesTheWork covers a misconfigured limit. Doing
// nothing would be a scrape that silently collected nothing.
func TestAWorkerCountBelowOneStillDoesTheWork(t *testing.T) {
	for _, workers := range []int{0, -3} {
		var ran atomic.Int32
		Run(5, workers, func(int) { ran.Add(1) })
		if ran.Load() != 5 {
			t.Errorf("with %d workers, %d of 5 items ran", workers, ran.Load())
		}
	}
}
