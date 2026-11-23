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
