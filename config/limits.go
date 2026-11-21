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

// What one scrape is allowed to cost, as an operator writes it.
//
//	limits:
//	  connections: 5
//	  concurrent_requests: 4
//	  request_timeout: 10s
//	  max_request_timeout: 60s
//	  connect_timeout: 3s
//	  response_reserve:
//	    max: 1s
//	    fraction: 0.1
//
// Every key is optional and takes the approved default when left out. The
// rules the values have to satisfy live in internal/budget, so the
// configuration file and the code that spends the budget cannot disagree
// about what a valid one is.

package config

import (
	"time"

	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/prometheus/common/model"
)

// Limits is the limits section of the configuration file.
//
// Every field is a pointer so that a key left out and a key written as zero
// stay different. `connections: 0` is a mistake worth reporting, and with a
// plain int64 it would be indistinguishable from an omitted key and would
// quietly become the default.
type Limits struct {
	Connections        *int64           `yaml:"connections"`
	ConcurrentRequests *int64           `yaml:"concurrent_requests"`
	RequestTimeout     *model.Duration  `yaml:"request_timeout"`
	MaxRequestTimeout  *model.Duration  `yaml:"max_request_timeout"`
	ResponseReserve    *ResponseReserve `yaml:"response_reserve"`
	ConnectTimeout     *model.Duration  `yaml:"connect_timeout"`
}

// ResponseReserve is the part of a request budget kept back for producing the
// response. It exposes the cap and the fraction rather than a fixed formula.
type ResponseReserve struct {
	Max      *model.Duration `yaml:"max"`
	Fraction *float64        `yaml:"fraction"`
}

// Budget overlays what the file said onto the approved defaults.
//
// It does not validate. The caller validates once, so a value that came from
// the file and a value that came from a flag are checked by the same rule.
func (l Limits) Budget() budget.Limits {
	out := budget.Defaults()
	if l.Connections != nil {
		out.Connections = *l.Connections
	}
	if l.ConcurrentRequests != nil {
		out.ConcurrentRequests = *l.ConcurrentRequests
	}
	if l.RequestTimeout != nil {
		out.RequestTimeout = time.Duration(*l.RequestTimeout)
	}
	if l.MaxRequestTimeout != nil {
		out.MaxRequestTimeout = time.Duration(*l.MaxRequestTimeout)
	}
	if l.ConnectTimeout != nil {
		out.ConnectTimeout = time.Duration(*l.ConnectTimeout)
	}
	if l.ResponseReserve != nil {
		if l.ResponseReserve.Max != nil {
			out.ResponseReserveMax = time.Duration(*l.ResponseReserve.Max)
		}
		if l.ResponseReserve.Fraction != nil {
			out.ResponseReserveFraction = *l.ResponseReserve.Fraction
		}
	}
	return out
}

// limitKeyPaths are the keys where writing nothing after the name would
// silently take the default.
func limitKeyPaths() [][]string {
	return [][]string{
		{"limits"},
		{"limits", "connections"},
		{"limits", "concurrent_requests"},
		{"limits", "request_timeout"},
		{"limits", "max_request_timeout"},
		{"limits", "response_reserve"},
		{"limits", "response_reserve", "max"},
		{"limits", "response_reserve", "fraction"},
		{"limits", "connect_timeout"},
	}
}
