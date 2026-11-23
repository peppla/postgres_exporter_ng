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
//
// The name may use dashes where the setting uses underscores, and may be in
// any case, because the server normalises both. Matching only the lowercase
// underscore spelling let STATEMENT_TIMEOUT and --statement-timeout through,
// and each of those reaches the session exactly as the canonical spelling
// does.
var optionSetting = regexp.MustCompile(`(?:^|\s)(?:-c\s*|--)([a-zA-Z_-]+)=(\S+)`)

// settingName normalises a setting name the way the server does, so the
// spellings it accepts are all recognised here.
func settingName(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "-", "_"))
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

// StripOptionsOverrides removes the statement and lock timeouts from an
// options string and reports what it had asked for.
//
// Separate from StripTimeoutOverrides because the driver resolves its options
// from more than the connection string: PGOPTIONS in the environment is added
// after the string has been parsed. Cleaning the string alone left that path
// untouched, so an environment variable asking for no limit still erased a
// role's stricter one. This is applied to the driver's effective
// configuration instead, where every source has already been merged.
//
// Unrelated settings are preserved, because an operator who set work_mem in
// PGOPTIONS meant it.
func StripOptionsOverrides(options string) (string, TimeoutOverrides) {
	var asked TimeoutOverrides
	if options == "" {
		return options, asked
	}
	kept := options
	for _, m := range optionSetting.FindAllStringSubmatch(options, -1) {
		whole, name, value := m[0], settingName(m[1]), m[2]
		if !isOverrideSetting(name) {
			continue
		}
		ms, err := durationSettingMS(value)
		if err != nil {
			// Unreadable to this package is unreadable to the server too, so
			// it is left in place for the server to reject rather than
			// silently removed.
			continue
		}
		asked.set(name, ms)
		kept = strings.Replace(kept, whole, "", 1)
	}
	return strings.Join(strings.Fields(kept), " "), asked
}

// StripRuntimeOverrides removes the statement and lock timeouts from the
// driver's runtime parameters, which is where a connection parameter the
// driver does not recognise ends up, and reports what they asked for.
func StripRuntimeOverrides(runtime map[string]string) TimeoutOverrides {
	var asked TimeoutOverrides
	for key, value := range runtime {
		name := settingName(key)
		if !isOverrideSetting(name) || value == "" {
			continue
		}
		ms, err := durationSettingMS(value)
		if err != nil {
			continue
		}
		asked.set(name, ms)
		delete(runtime, key)
	}
	return asked
}

// Tighten keeps the stricter of two sets of requests, treating zero as no
// request rather than as the shortest possible timeout.
func (o TimeoutOverrides) Tighten(other TimeoutOverrides) TimeoutOverrides {
	return TimeoutOverrides{
		StatementMS: tighter(o.StatementMS, other.StatementMS),
		LockMS:      tighter(o.LockMS, other.LockMS),
	}
}

func tighter(a, b int64) int64 {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	case b < a:
		return b
	default:
		return a
	}
}
