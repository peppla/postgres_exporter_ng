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

func TestACancelledScrapeStopsWorkWithNoLimiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, decision := Admit(ctx, nil)

	if decision != Deadline {
		t.Errorf("work under a cancelled scrape was %v, want %v", decision, Deadline)
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

// TestAFullLimitSaysSoRatherThanBlamingTheDeadline separates the two
// deferrals.
//
// The semaphore returns the context's error for every failure, so the engine
// cannot tell from that error whether it waited. This call waited, because the
// context was live when it started, so the connection limit is what this piece
// of work ran into.
func TestAFullLimitSaysSoRatherThanBlamingTheDeadline(t *testing.T) {
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
	if NoPermit == Deadline {
		t.Error("the two deferrals share a value, so the reason label cannot tell them apart")
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

	if decision != Deadline {
		t.Errorf("work under a cancelled scrape with a free permit was %v, want %v", decision, Deadline)
	}
	if !sema.TryAcquire(1) {
		t.Error("the free permit was taken by work that was never admitted")
	}
}
