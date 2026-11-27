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

package main

import (
	"fmt"

	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/budget"
	"github.com/percona/postgres_exporter/internal/slots"
)

// resolveLimits settles the operational budget from the configuration file and
// the one flag that overlaps it.
//
// --max-connections and limits.connections set the same number, so writing
// both is refused rather than resolved. The flag has a default, so "both
// given" is read from whether the operator set it rather than from its value:
// someone who passes the default explicitly still said it.
func resolveLimits(cfg *config.Config, maxConnectionsSet bool, maxConnections int64) (budget.Limits, error) {
	limits := budget.Defaults()
	if cfg != nil {
		limits = cfg.Limits.Budget()
	}

	if maxConnectionsSet {
		if cfg != nil && cfg.Limits.Connections != nil {
			return budget.Limits{}, fmt.Errorf(
				"limits.connections and --max-connections both set the number of connections: remove one")
		}
		limits.Connections = maxConnections
	}

	if err := limits.Validate(); err != nil {
		return budget.Limits{}, fmt.Errorf("limits: %w", err)
	}
	return limits, nil
}

// resolveState settles how much the exporter may remember between scrapes.
//
// Nothing to overlay onto, unlike the budget. These have no approved defaults,
// so a file that says nothing leaves them zero, and zero is no limit
// configured: nothing is refused and nothing is released. That is what this
// exporter does until the three values are chosen.
func resolveState(cfg *config.Config) (slots.Limits, error) {
	if cfg == nil {
		return slots.Limits{}, nil
	}
	if err := cfg.State.Validate(); err != nil {
		return slots.Limits{}, fmt.Errorf("state: %w", err)
	}
	return cfg.State.Slots(), nil
}
