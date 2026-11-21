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

package collector

import (
	"database/sql"
	"strconv"
)

// nullStringLabel turns a nullable column into a label value.
//
// An empty string is what Prometheus treats as an absent label, so a NULL
// becomes empty rather than the word "unknown". The older collectors in this
// package use "unknown", which is a value an operator can also legitimately
// have in their data.
func nullStringLabel(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

// nullIntLabel turns a nullable integer column into a label value.
func nullIntLabel(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatInt(v.Int64, 10)
}
