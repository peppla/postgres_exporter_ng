// Copyright 2021 The Prometheus Authors
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

package main

import (
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/lib/pq"
	"github.com/percona/postgres_exporter/config"
	"github.com/percona/postgres_exporter/internal/targetid"
)

// convert a string to the corresponding ColumnUsage
func stringToColumnUsage(s string) (ColumnUsage, error) {
	var u ColumnUsage
	var err error
	switch s {
	case "DISCARD":
		u = DISCARD

	case "LABEL":
		u = LABEL

	case "COUNTER":
		u = COUNTER

	case "GAUGE":
		u = GAUGE

	case "HISTOGRAM":
		u = HISTOGRAM

	case "MAPPEDMETRIC":
		u = MAPPEDMETRIC

	case "DURATION":
		u = DURATION

	default:
		err = fmt.Errorf("wrong ColumnUsage given : %s", s)
	}

	return u, err
}

// Convert database.sql types to float64s for Prometheus consumption. Null types are mapped to NaN. string and []byte
// types are mapped as NaN and !ok
func dbToFloat64(t interface{}) (float64, bool) {
	switch v := t.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.Unix()), true
	case []byte:
		// Try and convert to string and then parse to a float64
		strV := string(v)
		result, err := strconv.ParseFloat(strV, 64)
		if err != nil {
			level.Info(logger).Log("msg", "Could not parse []byte", "err", err)
			return math.NaN(), false
		}
		return result, true
	case string:
		result, err := strconv.ParseFloat(v, 64)
		if err != nil {
			level.Info(logger).Log("msg", "Could not parse string", "err", err)
			return math.NaN(), false
		}
		return result, true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	case nil:
		return math.NaN(), true
	default:
		return math.NaN(), false
	}
}

// Convert database.sql types to uint64 for Prometheus consumption. Null types are mapped to 0. string and []byte
// types are mapped as 0 and !ok
func dbToUint64(t interface{}) (uint64, bool) {
	switch v := t.(type) {
	case uint64:
		return v, true
	case int64:
		return uint64(v), true
	case float64:
		return uint64(v), true
	case time.Time:
		return uint64(v.Unix()), true
	case []byte:
		// Try and convert to string and then parse to a uint64
		strV := string(v)
		result, err := strconv.ParseUint(strV, 10, 64)
		if err != nil {
			level.Info(logger).Log("msg", "Could not parse []byte", "err", err)
			return 0, false
		}
		return result, true
	case string:
		result, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			level.Info(logger).Log("msg", "Could not parse string", "err", err)
			return 0, false
		}
		return result, true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case nil:
		return 0, true
	default:
		return 0, false
	}
}

// Convert database.sql to string for Prometheus labels. Null types are mapped to empty strings.
func dbToString(t interface{}) (string, bool) {
	switch v := t.(type) {
	case int64:
		return fmt.Sprintf("%v", v), true
	case float64:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		return fmt.Sprintf("%v", v.Unix()), true
	case nil:
		return "", true
	case []byte:
		// Try and convert to string
		return string(v), true
	case string:
		return v, true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

func parseFingerprint(url string) (string, error) {
	dsn, err := pq.ParseURL(url) //nolint:staticcheck
	if err != nil {
		dsn = url
	}

	kv, err := config.ParseKeyValue(dsn)
	if err != nil {
		return "", fmt.Errorf("malformed dsn %q: %w", dsn, err)
	}

	var fingerprint string

	if host, ok := kv["host"]; ok {
		fingerprint += host
	} else {
		fingerprint += "localhost"
	}

	if port, ok := kv["port"]; ok {
		fingerprint += ":" + port
	} else {
		fingerprint += ":5432"
	}

	return fingerprint, nil
}

// plainTargetName matches a target that cannot be carrying a credential.
//
// An allow list rather than a search for the places a password hides. The
// characters a connection string needs to carry one, which are at least the
// separators, the quote and the space, are all outside this.
var plainTargetName = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// loggableTarget is a probe target with any credential in it removed.
//
// The target comes from the request and need not be a name. A URL carries a
// password in its user info and again in a password query parameter, and a
// keyword string carries one in a password field whose value may be quoted and
// contain an escaped quote. Logging it as given put a password into the log of
// a request that was refused before it reached a server.
//
// The redaction is the connection parser's own, so it cannot understand less
// of the syntax than the thing that builds the connection. A target that
// parser cannot understand is logged as its identity instead, because a string
// nothing could parse may still hold a password.
//
// A name with nothing in it that could carry a credential is logged as it is,
// because an operator reading a refusal wants to know which target it was.
func loggableTarget(target string) string {
	if plainTargetName.MatchString(target) {
		return target
	}
	if redacted, err := config.Loggable(target); err == nil {
		return redacted
	}
	id, _ := targetid.ID(target)
	return "target " + id
}

func loggableDSN(dsn string) string {
	pDSN, err := url.Parse(dsn)
	if err != nil {
		return "could not parse DATA_SOURCE_NAME"
	}
	// Blank user info if not nil
	if pDSN.User != nil {
		pDSN.User = url.UserPassword(pDSN.User.Username(), "PASSWORD_REMOVED")
	}

	return pDSN.String()
}
