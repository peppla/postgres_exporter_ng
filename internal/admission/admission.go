// Package admission decides whether one piece of per-database work may start.
//
// Both engines fan out across databases under one connection limit and one
// scrape deadline, and both have to say which of the two stopped a database
// from being collected. A limit that is full and a deadline that has passed
// are deferrals rather than failures, because nothing was tried, but they are
// not the same deferral: one says the exporter is running more work than its
// connection budget allows, and the other says the scrape interval is shorter
// than the work takes.
//
// The decision lives here rather than in either engine because the engines
// report against one vocabulary. Two copies of this rule can disagree, and
// their disagreement reads as the exporter losing a database.
package admission

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// Decision is what admission came to for one piece of work.
type Decision int

const (
	// Admitted means the work may start. When a limiter issued a permit the
	// returned Permit holds it, and Release gives it back.
	Admitted Decision = iota

	// NoPermit means the work waited for a connection permit and the wait
	// ended without one. The connection limit is the proximate cause.
	NoPermit

	// Deadline means the scrape had already run out of time when this work
	// was reached. Nothing waited and nothing was contacted.
	Deadline
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
	// Checked before admission, and checked whether or not a limiter exists.
	// Without this, work reached after the deadline has passed is admitted,
	// opens a connection that fails on the dead context, and is reported as a
	// database the exporter could not connect to. The database is then named
	// as broken when what ran out was the exporter's own time, which sends
	// whoever is on call to the wrong place.
	if ctx.Err() != nil {
		return Permit{}, Deadline
	}

	if sema == nil {
		return Permit{}, Admitted
	}

	if err := sema.Acquire(ctx, 1); err != nil {
		// Acquire fails only when the context is done, so there is no second
		// error to tell apart here. The context was live on entry, so what
		// ended was the wait for a permit, and the limit being full is what
		// made this work wait at all.
		return Permit{}, NoPermit
	}

	// Checked again, because a permit can be granted at the same moment the
	// deadline passes. Keeping it would hold a connection slot for work that
	// cannot run, and the work would then fail at the connection and be
	// reported as a broken database for the second time.
	if ctx.Err() != nil {
		sema.Release(1)
		return Permit{}, Deadline
	}

	return Permit{sema: sema}, Admitted
}
