package admission

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

func TestWorkIsAdmittedWhenNothingLimitsIt(t *testing.T) {
	permit, decision := Admit(context.Background(), nil)
	defer permit.Release()

	if decision != Admitted {
		t.Errorf("work with no limiter and a live context was not admitted, got %v", decision)
	}
}

// TestAnExpiredDeadlineStopsWorkWithNoLimiter is the case that was missing.
//
// With no connection limit configured the old code returned "go ahead" without
// reading the context at all. Every database was then attempted after the
// deadline had passed, each connection failed on the dead context, and the
// scrape reported a cluster of databases it could not connect to. What had
// actually run out was the exporter's own time.
func TestAnExpiredDeadlineStopsWorkWithNoLimiter(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, decision := Admit(ctx, nil)

	if decision != Deadline {
		t.Errorf("work reached after the deadline was %v, want %v", decision, Deadline)
	}
}

// TestACancelledScrapeIsNotADeadline keeps the two apart.
//
// A cancelled scrape was stopped from outside rather than reaching its own
// deadline, so reporting it as a deadline names a limit that was never
// reached.
func TestACancelledScrapeIsNotADeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, decision := Admit(ctx, nil)

	if decision != Cancelled {
		t.Errorf("work under a cancelled scrape was %v, want %v", decision, Cancelled)
	}
}

func TestAnExpiredDeadlineStopsWorkWhileAPermitIsFree(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	sema := semaphore.NewWeighted(1)

	_, decision := Admit(ctx, sema)

	if decision != Deadline {
		t.Errorf("work reached after the deadline was %v, want %v", decision, Deadline)
	}
	// The permit has to be untouched, not taken and not leaked, or a scrape
	// that still has time cannot get one.
	if !sema.TryAcquire(1) {
		t.Error("the permit was taken by work that was never admitted")
	}
}

func TestAnAdmittedPieceOfWorkHoldsThePermit(t *testing.T) {
	sema := semaphore.NewWeighted(1)

	permit, decision := Admit(context.Background(), sema)

	if decision != Admitted {
		t.Fatalf("work with a free permit was %v, want %v", decision, Admitted)
	}
	if sema.TryAcquire(1) {
		t.Error("a second piece of work got the only permit while the first held it")
	}
	permit.Release()
	if !sema.TryAcquire(1) {
		t.Error("the permit was not given back")
	}
}

// TestTimeRunningOutWhileWaitingForAPermitNamesThePermit covers the case where
// both are true.
//
// No permit was available to this work when it asked, and then its time ran
// out. The observation about permits is what distinguishes this from work that
// was simply reached too late.
func TestTimeRunningOutWhileWaitingForAPermitNamesThePermit(t *testing.T) {
	sema := semaphore.NewWeighted(1)
	if !sema.TryAcquire(1) {
		t.Fatal("the fixture could not take the only permit")
	}
	defer sema.Release(1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, decision := Admit(ctx, sema)
	waited := time.Since(start)

	if decision != NoPermit {
		t.Errorf("work that waited for a full limit was %v, want %v", decision, NoPermit)
	}
	if waited < 40*time.Millisecond {
		t.Errorf("the call gave up after %s without waiting for a permit", waited)
	}
}

func TestWorkWaitsForAPermitAndTakesItWhenOneIsFreed(t *testing.T) {
	sema := semaphore.NewWeighted(1)
	if !sema.TryAcquire(1) {
		t.Fatal("the fixture could not take the only permit")
	}

	released := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(released)
		sema.Release(1)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	permit, decision := Admit(ctx, sema)
	defer permit.Release()

	<-released
	if decision != Admitted {
		t.Errorf("work that waited and got a permit was %v, want %v", decision, Admitted)
	}
}

// expiringContext is live for its first Err call and done for every one after.
//
// It pins the second check in Admit. A permit can be granted at the same moment
// the deadline passes, because the semaphore grants one when it holds the lock
// and the context is not yet done. Nothing in a real context exposes that
// instant reliably, so the instant is staged here instead.
type expiringContext struct {
	context.Context
	calls int
}

func (c *expiringContext) Err() error {
	c.calls++
	if c.calls > 1 {
		return context.DeadlineExceeded
	}
	return nil
}

func TestAPermitGrantedAsTheDeadlinePassesIsGivenBack(t *testing.T) {
	sema := semaphore.NewWeighted(1)
	ctx := &expiringContext{Context: context.Background()}

	_, decision := Admit(ctx, sema)

	if decision != Deadline {
		t.Errorf("work whose deadline passed as it was admitted was %v, want %v", decision, Deadline)
	}
	if ctx.calls < 2 {
		t.Errorf("the context was read %d times, so it was not checked after the permit was granted", ctx.calls)
	}
	if !sema.TryAcquire(1) {
		t.Error("the permit was held by work that was never admitted, so a scrape with time left cannot get one")
	}
}

// cancelAfterFirstCheck is live for one Err call and cancelled from then on.
//
// It stages the instant between the entry check and the limiter's own check of
// Done. Nothing in a real context exposes that instant reliably, so it is
// staged here.
type cancelAfterFirstCheck struct {
	context.Context
	cancel context.CancelFunc
	calls  int
}

func (c *cancelAfterFirstCheck) Err() error {
	c.calls++
	if c.calls == 1 {
		c.cancel()
		return nil
	}
	return c.Context.Err()
}

// TestAvailabilityIsObservedRatherThanInferred covers a limiter that was
// entirely free being reported as a full one.
//
// Acquire fails only when the context ends, and it reads the context before it
// looks at capacity, so it can fail without looking at capacity at all. An
// earlier version read that failure as proof that no permit was available, and
// reported a limiter that was entirely free as a missing connection permit.
//
// The fixture cancels the scrape in the instant after the entry check, so
// Acquire returns without ever reaching its capacity branch, while the one
// permit stays free throughout.
func TestAvailabilityIsObservedRatherThanInferred(t *testing.T) {
	inner, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelAfterFirstCheck{Context: inner, cancel: cancel}
	sema := semaphore.NewWeighted(1)

	_, decision := Admit(ctx, sema)

	// The fixture's own premise first. Without this, a permit that was taken
	// and never returned would read as a limiter that was legitimately full.
	if !sema.TryAcquire(1) {
		t.Fatal("the permit was not free, so this case proves nothing")
	}
	sema.Release(1)

	if decision == NoPermit {
		t.Error("a free limiter was reported as a missing connection permit")
	}
	if decision != Cancelled {
		t.Errorf("work cancelled before the limiter was asked was %v, want %v", decision, Cancelled)
	}
}

// deadlineAfterFirstCheck reports no error once, then the expired deadline of
// the context it wraps, whose Done channel is closed throughout.
//
// It stages the same instant as cancelAfterFirstCheck, for a deadline rather
// than a cancellation. Both are needed: with only the cancellation staging,
// Admit's availability observation can be removed and every test still passes,
// because the cancellation branch in the wait path returns the right answer
// for the wrong reason.
type deadlineAfterFirstCheck struct {
	context.Context
	calls int
}

func (c *deadlineAfterFirstCheck) Err() error {
	c.calls++
	if c.calls == 1 {
		return nil
	}
	return c.Context.Err()
}

// TestAFreeLimiterIsNotReportedAsFullWhenTimeRunsOut is the deadline half of
// the same case.
//
// The limiter has a permit free the whole way through. Acquire reads the
// context before it looks at capacity, so it returns the deadline error
// without ever reaching the capacity branch. Reading that error as proof of a
// full limiter reports a limiter that was free.
func TestAFreeLimiterIsNotReportedAsFullWhenTimeRunsOut(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	ctx := &deadlineAfterFirstCheck{Context: expired}
	sema := semaphore.NewWeighted(1)

	_, decision := Admit(ctx, sema)

	// The fixture's own premise first.
	if !sema.TryAcquire(1) {
		t.Fatal("the permit was not free, so this case proves nothing")
	}
	sema.Release(1)

	if decision == NoPermit {
		t.Error("a free limiter was reported as a missing connection permit")
	}
	if decision != Deadline {
		t.Errorf("work whose time ran out before the limiter was asked was %v, want %v", decision, Deadline)
	}
}

// TestCancellationWhileWaitingForAPermitNamesTheCancellation covers the wait.
//
// The work was refused a permit and then the scrape was cancelled. It would
// have had its permit had the scrape continued, so the cancellation is what
// stopped it.
func TestCancellationWhileWaitingForAPermitNamesTheCancellation(t *testing.T) {
	sema := semaphore.NewWeighted(1)
	if !sema.TryAcquire(1) {
		t.Fatal("the fixture could not take the only permit")
	}
	defer sema.Release(1)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	defer cancel()

	_, decision := Admit(ctx, sema)

	if decision != Cancelled {
		t.Errorf("work cancelled while waiting for a permit was %v, want %v", decision, Cancelled)
	}
}

func TestReleasingAPermitNobodyIssuedDoesNothing(t *testing.T) {
	var permit Permit

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("releasing a permit that was never issued panicked with %v", r)
		}
	}()
	permit.Release()
}

func TestTheDecisionsAreDistinct(t *testing.T) {
	// Admitted has to be the zero value, because a Decision that was never
	// set reads as the permissive answer and a reader has to be able to rely
	// on that rather than guess.
	var unset Decision
	if unset != Admitted {
		t.Error("the zero Decision is not Admitted")
	}
	seen := map[Decision]string{}
	for value, name := range map[Decision]string{
		Admitted:  "Admitted",
		NoPermit:  "NoPermit",
		Deadline:  "Deadline",
		Cancelled: "Cancelled",
	} {
		if other, clash := seen[value]; clash {
			t.Errorf("%s and %s share a value, so the reason label cannot tell them apart", name, other)
		}
		seen[value] = name
	}
	if len(seen) != 4 {
		t.Errorf("%d distinct decisions, want 4", len(seen))
	}
}

// TestACancelledScrapeIsNotBlamedOnTheLimit covers the same cancellation with
// a limiter present.
//
// The limit is free here, so nothing about it stopped this work. Reporting it
// as a connection-permit deferral would send an operator to raise a limit that
// was never reached.
func TestACancelledScrapeIsNotBlamedOnTheLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sema := semaphore.NewWeighted(1)

	_, decision := Admit(ctx, sema)

	if decision != Cancelled {
		t.Errorf("work under a cancelled scrape with a free permit was %v, want %v", decision, Cancelled)
	}
	if !sema.TryAcquire(1) {
		t.Error("the free permit was taken by work that was never admitted")
	}
}
