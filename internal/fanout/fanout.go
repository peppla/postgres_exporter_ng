// Package fanout runs a fixed number of goroutines over a list of work items.
//
// Both engines fan out across the databases a scrape selected. A goroutine per
// database queues on the connection limiter, so the limit bounds open
// connections and not the work waiting behind them.
//
// Here the goroutine count comes from the connection limit instead, and each
// worker takes the next database as it finishes the last one. The limit on
// concurrency is not a limit on how many databases a scrape covers: workers keep
// taking work while any is left, and what stops a scrape short is its deadline,
// which each item observes for itself.
package fanout

import (
	"sync"
	"sync/atomic"
)

// Run calls work once for every index in [0, n), with at most workers of them
// running at a time, and returns when all of them are done.
//
// Every index is visited, including after a scrape's time has run out. The
// caller's work function is what observes that and reports the item deferred,
// because an item dropped here would leave a selected database with no series
// at all, which reads the same as a database that was never selected. So this
// takes no context: stopping early is not one of its jobs.
func Run(n, workers int, work func(i int)) {
	if n <= 0 {
		return
	}
	if workers < 1 {
		// A limit of zero or less would mean no work at all, and silently
		// collecting nothing is the outcome every deferral series exists to
		// make visible. One worker is slow and correct.
		workers = 1
	}
	if workers > n {
		workers = n
	}

	// The next index to hand out. Shared because the point is that no worker
	// owns a slice of the work: a database that takes ten times as long as the
	// others must not leave its neighbours unvisited while other workers idle.
	var next atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				work(i)
			}
		}()
	}
	wg.Wait()
}
