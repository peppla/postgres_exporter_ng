// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// New in postgres_exporter_ng, 2026. No copyright line: this is newly written
// code and its holder is not settled, so none is asserted.

// What one scrape is allowed to cost.
//
// The contract asks for several budgets rather than one, because the things
// that run out are different: connections and database load are instance-wide,
// collection latency belongs to a scrape, and query time belongs to a
// statement. They are held together here so that the relationships between
// them are stated once and checked once.
//
// These are starting values, not measured operating limits. The exporter-side
// cost of a scrape has been measured; what a scrape costs the monitored server
// has only been measured for one collector on one shape. A default here says
// what the exporter is allowed to spend, not what it needs.
package budget

import (
	"fmt"
	"time"
)

// Limits is the operational budget, as an operator configures it.
//
// Only the controls this package enforces are here. A control that is written
// in a configuration file and changes nothing is worse than one that does not
// exist, because it reads as a limit that is in force.
type Limits struct {
	// Connections is the number of open PostgreSQL connections the process
	// may hold, across both engines, counting pooled and idle ones.
	Connections int64

	// ConcurrentRequests is how many collection requests may be in flight.
	// Beyond it a request is refused before any SQL runs, rather than queued.
	ConcurrentRequests int64

	// RequestTimeout is the collection budget for a request that does not say
	// what it wants, which is the inherited fallback for scrapers that send no
	// timeout header.
	RequestTimeout time.Duration

	// MaxRequestTimeout caps the budget however long a client asks for. A
	// parent deadline can shorten it and nothing can lengthen it.
	MaxRequestTimeout time.Duration

	// ResponseReserveMax and ResponseReserveFraction set aside the end of the
	// budget for gathering, encoding and writing the response. Collection has
	// to stop before it, or a scrape that collected everything still returns
	// nothing. The reserve is the smaller of the two, so a short budget keeps
	// most of itself for collection and a long one does not reserve minutes.
	ResponseReserveMax      time.Duration
	ResponseReserveFraction float64

	// ConnectTimeout bounds one attempt to establish a connection.
	//
	// It is not a convenience. The driver waits indefinitely without it, and
	// the context a caller passes to PingContext does not reach its dial or
	// its startup handshake, so a server that accepts a connection and then
	// says nothing holds a scrape open past every other limit here.
	ConnectTimeout time.Duration

	// StatementTimeout bounds one statement on the server. It is a backstop
	// rather than a target: a collector query that takes seconds is already a
	// problem, and this is what stops it being an unbounded one.
	StatementTimeout time.Duration

	// LockTimeout bounds how long a statement waits for a lock. Monitoring
	// should give way to work rather than queue behind it, so this is much
	// shorter than the statement timeout and capped by it.
	LockTimeout time.Duration
}

// Defaults are the values approved in contract §6.0.
func Defaults() Limits {
	return Limits{
		Connections:             5,
		ConcurrentRequests:      4,
		RequestTimeout:          10 * time.Second,
		MaxRequestTimeout:       60 * time.Second,
		ResponseReserveMax:      time.Second,
		ResponseReserveFraction: 0.10,
		ConnectTimeout:          3 * time.Second,
		StatementTimeout:        5 * time.Second,
		LockTimeout:             500 * time.Millisecond,
	}
}

// Validate checks each value and the relationships between them.
//
// Checked at startup rather than at scrape time. A budget that cannot be
// satisfied should stop the exporter starting, not produce scrapes that fail
// in a way nobody connects to the configuration.
func (l Limits) Validate() error {
	if l.Connections < 1 {
		return fmt.Errorf("connections must be at least 1, got %d", l.Connections)
	}
	if l.ConcurrentRequests < 1 {
		return fmt.Errorf("concurrent_requests must be at least 1, got %d", l.ConcurrentRequests)
	}
	if l.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be positive, got %s", l.RequestTimeout)
	}
	if l.MaxRequestTimeout <= 0 {
		return fmt.Errorf("max_request_timeout must be positive, got %s", l.MaxRequestTimeout)
	}
	if l.RequestTimeout > l.MaxRequestTimeout {
		return fmt.Errorf("request_timeout %s is longer than max_request_timeout %s, so the fallback could never be used",
			l.RequestTimeout, l.MaxRequestTimeout)
	}
	if l.ResponseReserveMax <= 0 {
		return fmt.Errorf("response_reserve.max must be positive, got %s", l.ResponseReserveMax)
	}
	if l.ResponseReserveFraction <= 0 || l.ResponseReserveFraction >= 1 {
		return fmt.Errorf("response_reserve.fraction must be above 0 and below 1, got %v", l.ResponseReserveFraction)
	}
	if l.ConnectTimeout <= 0 {
		return fmt.Errorf("connect_timeout must be positive, got %s", l.ConnectTimeout)
	}
	if l.ConnectTimeout > l.MaxRequestTimeout {
		return fmt.Errorf("connect_timeout %s is longer than max_request_timeout %s, so connecting could use the whole budget",
			l.ConnectTimeout, l.MaxRequestTimeout)
	}
	if l.StatementTimeout <= 0 {
		return fmt.Errorf("statement_timeout must be positive, got %s", l.StatementTimeout)
	}
	if l.StatementTimeout > l.MaxRequestTimeout {
		return fmt.Errorf("statement_timeout %s is longer than max_request_timeout %s, so one statement could use the whole budget",
			l.StatementTimeout, l.MaxRequestTimeout)
	}
	if l.LockTimeout <= 0 {
		return fmt.Errorf("lock_timeout must be positive, got %s", l.LockTimeout)
	}
	if l.LockTimeout > l.StatementTimeout {
		return fmt.Errorf("lock_timeout %s is longer than statement_timeout %s, so the statement would be cut off first and the lock wait would never be reached",
			l.LockTimeout, l.StatementTimeout)
	}
	return nil
}

// RequestBudget is how long a request may take in total.
//
// asked is what the client's timeout header says, or zero when it sent none.
// parentRemaining is what is left of any deadline already on the request, or
// zero when there is none. The result is the shortest of the three, because
// each is a ceiling and none of them is a target.
func (l Limits) RequestBudget(asked, parentRemaining time.Duration) time.Duration {
	budget := asked
	if budget <= 0 {
		budget = l.RequestTimeout
	}
	if budget > l.MaxRequestTimeout {
		budget = l.MaxRequestTimeout
	}
	if parentRemaining > 0 && parentRemaining < budget {
		budget = parentRemaining
	}
	return budget
}

// ResponseReserve is the part of a budget kept back for producing the
// response.
func (l Limits) ResponseReserve(budget time.Duration) time.Duration {
	reserve := time.Duration(float64(budget) * l.ResponseReserveFraction)
	if reserve > l.ResponseReserveMax {
		reserve = l.ResponseReserveMax
	}
	return reserve
}

// CollectionBudget is how long collection may take, which is the request
// budget less the reserve.
//
// The fraction is below one, so this is always positive for a positive budget:
// a short request collects for less time rather than not at all.
func (l Limits) CollectionBudget(asked, parentRemaining time.Duration) time.Duration {
	budget := l.RequestBudget(asked, parentRemaining)
	return budget - l.ResponseReserve(budget)
}

// ConnectTimeoutFor bounds one connection attempt, shortened by whatever is
// left of the collection budget.
//
// remaining of zero or less means there is no budget to consult, so the
// configured value stands on its own.
func (l Limits) ConnectTimeoutFor(remaining time.Duration) time.Duration {
	if remaining > 0 && remaining < l.ConnectTimeout {
		return remaining
	}
	return l.ConnectTimeout
}
