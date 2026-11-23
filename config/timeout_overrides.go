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

// Taking the statement and lock timeouts out of a connection string.
//
// A client can set either of them two ways: as a connection parameter of its
// own, which lib/pq forwards as a startup parameter, or inside the options
// string. Both are applied at startup, after a role's own settings, so both
// replace them. Measured against PostgreSQL 18: with a role limited to
// statement_timeout 100ms, a connection string asking for 0 produced a session
// with no limit at all.
//
// That matters because the exporter's ceiling is applied after connecting by
// comparing against what the session already has. If a client value has
// already replaced the role's, the comparison is against the client's value
// and the administrator's setting is gone. So the client's request is taken
// out of the string and handed to the comparison as one more candidate, which
// leaves the role's setting visible and still honours a client asking for
// less.

package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TimeoutOverrides are the statement and lock timeouts a connection string
// asked for, in milliseconds. Zero means the string did not ask, or asked for
// no limit, which are the same instruction to a ceiling that only tightens.
type TimeoutOverrides struct {
	StatementMS int64
	LockMS      int64
}

// overrideSettings are the two settings taken out of a connection string.
var overrideSettings = []string{"statement_timeout", "lock_timeout"}

// optionSetting matches one setting inside an options string, in either of the
// forms libpq accepts.
var optionSetting = regexp.MustCompile(`(?:^|\s)(?:-c\s*|--)([a-zA-Z_]+)=(\S+)`)

// StripTimeoutOverrides removes the statement and lock timeouts from a
// connection string and reports what it had asked for.
//
// Everything else in the string is left alone, including other settings in
// options, because an operator setting work_mem there means it.
func StripTimeoutOverrides(in string) (string, TimeoutOverrides, error) {
	var asked TimeoutOverrides
	parsed, err := dsnFromString(in)
	if err != nil {
		return "", asked, err
	}
	if parsed.query == nil {
		parsed.query = url.Values{}
	}

	// As connection parameters of their own.
	for _, name := range overrideSettings {
		value := parsed.query.Get(name)
		if value == "" {
			continue
		}
		ms, perr := durationSettingMS(value)
		if perr != nil {
			return "", asked, fmt.Errorf("%s=%q in the connection string: %w", name, value, perr)
		}
		asked.set(name, ms)
		parsed.query.Del(name)
	}

	// And inside options, where what is left has to keep working.
	if options := parsed.query.Get("options"); options != "" {
		kept := options
		for _, m := range optionSetting.FindAllStringSubmatch(options, -1) {
			whole, name, value := m[0], m[1], m[2]
			if !isOverrideSetting(name) {
				continue
			}
			ms, perr := durationSettingMS(value)
			if perr != nil {
				return "", asked, fmt.Errorf("%s=%q in the options string: %w", name, value, perr)
			}
			asked.set(name, ms)
			kept = strings.Replace(kept, whole, "", 1)
		}
		kept = strings.Join(strings.Fields(kept), " ")
		if kept == "" {
			parsed.query.Del("options")
		} else {
			parsed.query.Set("options", kept)
		}
	}

	return parsed.GetConnectionString(), asked, nil
}

func (o *TimeoutOverrides) set(name string, ms int64) {
	switch name {
	case "statement_timeout":
		o.StatementMS = ms
	case "lock_timeout":
		o.LockMS = ms
	}
}

func isOverrideSetting(name string) bool {
	for _, s := range overrideSettings {
		if s == name {
			return true
		}
	}
	return false
}

// durationSettingMS reads a PostgreSQL duration setting as milliseconds.
//
// A bare number is milliseconds for both of these settings, which is what
// PostgreSQL documents as their unit. A suffix overrides that. Zero means no
// limit and is returned as zero, which every caller here reads as "no
// instruction" rather than as the shortest possible timeout.
func durationSettingMS(value string) (int64, error) {
	trimmed := strings.TrimSpace(strings.Trim(value, `'"`))
	if trimmed == "" {
		return 0, fmt.Errorf("has no value")
	}

	for _, unit := range []struct {
		suffix string
		scale  time.Duration
	}{
		{"us", time.Microsecond},
		{"ms", time.Millisecond},
		{"min", time.Minute},
		{"s", time.Second},
		{"h", time.Hour},
		{"d", 24 * time.Hour},
	} {
		if !strings.HasSuffix(trimmed, unit.suffix) {
			continue
		}
		number := strings.TrimSpace(strings.TrimSuffix(trimmed, unit.suffix))
		n, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return 0, fmt.Errorf("is not a number followed by a unit")
		}
		if n < 0 {
			return 0, fmt.Errorf("is negative")
		}
		ms := time.Duration(n * float64(unit.scale)).Milliseconds()
		if ms < 1 && n > 0 {
			// Shorter than a millisecond, which PostgreSQL cannot express for
			// these settings. Rounding down to zero would turn the strictest
			// request into no limit at all.
			ms = 1
		}
		return ms, nil
	}

	n, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("is not a number of milliseconds")
	}
	if n < 0 {
		return 0, fmt.Errorf("is negative")
	}
	return n, nil
}
