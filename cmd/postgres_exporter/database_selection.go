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
	"github.com/percona/postgres_exporter/internal/dbselect"
)

// builtinDatabaseSelection says which databases built-in monitoring runs
// against, from the configuration file where it is written there and from the
// deprecated flags otherwise.
//
// Both sources configure the same set, so writing both is refused rather than
// resolved. A precedence rule would mean an operator who set the environment
// variable years ago and the key today gets one of them silently, and which
// one is not visible from either place. The message names both.
//
// This governs built-in monitoring alone. Application queries select their own
// databases per query and nothing here narrows them.
func builtinDatabaseSelection(cfg *config.Config, excludeFlag, includeFlag []string) (exclude, include dbselect.List, err error) {
	exclude, include = dbselect.FromFlag(excludeFlag), dbselect.FromFlag(includeFlag)
	if cfg == nil {
		return exclude, include, nil
	}

	for _, pair := range []struct {
		key      string
		flag     string
		fromFile config.NameList
		fromFlag *dbselect.List
	}{
		{"monitoring.databases.exclude", "--exclude-databases", cfg.Monitoring.Databases.Exclude, &exclude},
		{"monitoring.databases.include", "--include-databases", cfg.Monitoring.Databases.Include, &include},
	} {
		if !pair.fromFile.Given() {
			continue
		}
		if pair.fromFlag.Given() {
			return dbselect.Absent(), dbselect.Absent(), fmt.Errorf(
				"%s and %s both configure the same set of databases: remove one",
				pair.key, pair.flag)
		}
		*pair.fromFlag = pair.fromFile.Selection()
	}
	return exclude, include, nil
}
