// Package admission decides whether one piece of per-database work may start.
//
// Both engines fan out across databases under one connection limit and one
// scrape context, and both report why a database was not collected. The
// answers here are deferrals rather than failures, because nothing was tried,
// so nothing is known about the database itself.
//
// The decision lives here rather than in either engine because the engines
// report against one vocabulary. Two copies of this rule can disagree, and
// their disagreement reads as the exporter losing a database.
package admission

import (
	"context"
	"errors"

	"golang.org/x/sync/semaphore"
)

// Decision is what admission came to for one piece of work.
type Decision int

const (
	// Admitted means the work may start. When a limiter issued a permit the
	// returned Permit holds it, and Release gives it back.
	Admitted Decision = iota

	// NoPermit means no connection permit was available to this work when it
	// asked, and none arrived before the scrape's time ran out.
	NoPermit

	// Deadline means the scrape's time ran out, and no shortage of connection
	// permits was observed for this work.
	Deadline

	// Cancelled means the scrape was cancelled. It says nothing about time
	// remaining: a request whose client has gone away is cancelled with its
	// whole budget unspent.
	Cancelled
)

// Permit is a connection permit that was issued, if one was.
//
// A zero Permit holds nothing and its Release does nothing, so the caller
// releases unconditionally and does not have to know whether a limiter was
// configured.
type Permit struct {
	sema *semaphore.Weighted
}

// Release gives the permit back.
func (p Permit) Release() {
	if p.sema != nil {
		p.sema.Release(1)
	}
}

// Admit takes a slot for one piece of work, or says why it may not start.
//
// sema may be nil, which means no connection limit is configured.
func Admit(ctx context.Context, sema *semaphore.Weighted) (Permit, Decision) {
	// Read before admission, and read whether or not a limiter exists. With
	// no limiter there is nothing to ask, and the engines took that as
	// permission, so work started on a context that had already ended and
	// failed at the connection. main always constructs a limiter, so the
	// shipped binary reaches that only through the exported API.
	if decision, ended := contextDecision(ctx); ended {
		return Permit{}, decision
	}

	if sema == nil {
		return Permit{}, Admitted
	}

	// Whether a permit was available is observed here, never inferred from a
	// later error. TryAcquire takes one only when a permit is free and nobody
	// is queued, which is the condition Acquire's own fast path uses, so this
	// neither jumps the queue nor changes who is served next.
	if sema.TryAcquire(1) {
		return withPermit(ctx, sema)
	}

	// Nothing was available at that moment, so this work has to wait.
	if err := sema.Acquire(ctx, 1); err != nil {
		// Acquire fails only when the context ends, and it checks the context
		// before its capacity branch, so it can fail without reaching that
		// branch at all. The error says nothing about permits. What is known
		// about permits is the observation above.
		//
		// So the two are reported apart on how the context ended. Time running
		// out while this work waited for a permit it had already been refused
		// is what NoPermit describes. Cancellation is not about the budget: it
		// arrives with any amount of time left, and the work would have had
		// its permit had the scrape continued.
		if errors.Is(err, context.Canceled) {
			return Permit{}, Cancelled
		}
		return Permit{}, NoPermit
	}

	return withPermit(ctx, sema)
}

// withPermit reads the context once more for work that now holds a permit.
//
// A permit can be granted at the same moment the context ends, because the
// limiter grants one while holding its own lock and nothing synchronises that
// instant with the context. Holding it through work that cannot run keeps a
// connection slot from a scrape that could still use it.
func withPermit(ctx context.Context, sema *semaphore.Weighted) (Permit, Decision) {
	if decision, ended := contextDecision(ctx); ended {
		sema.Release(1)
		return Permit{}, decision
	}
	return Permit{sema: sema}, Admitted
}

// contextDecision reports how the scrape ended, and whether it has.
func contextDecision(ctx context.Context) (Decision, bool) {
	err := ctx.Err()
	switch {
	case err == nil:
		return Admitted, false
	case errors.Is(err, context.Canceled):
		return Cancelled, true
	default:
		// A context reports deadline or cancellation and nothing else. A
		// context that reported something else still ended, and ran out of
		// time as far as this decision can tell.
		return Deadline, true
	}
}
