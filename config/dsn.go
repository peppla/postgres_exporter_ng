// Copyright 2022 The Prometheus Authors
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

package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// DSN represents a parsed datasource. It contains fields for the individual connection components.
type DSN struct {
	scheme   string
	username string
	password string
	host     string
	path     string
	query    url.Values
}

// String makes a dsn safe to print by excluding any passwords. This allows dsn to be used in
// strings and log messages without needing to call a redaction function first.
func (d DSN) String() string {
	if d.password != "" {
		return fmt.Sprintf("%s://%s:******@%s%s?%s", d.scheme, d.username, d.host, d.path, d.query.Encode())
	}

	if d.username != "" {
		return fmt.Sprintf("%s://%s@%s%s?%s", d.scheme, d.username, d.host, d.path, d.query.Encode())
	}

	return fmt.Sprintf("%s://%s%s?%s", d.scheme, d.host, d.path, d.query.Encode())
}

// GetConnectionString returns the URL to pass to the driver for database connections. This value should not be logged.
func (d DSN) GetConnectionString() string {
	u := url.URL{
		Scheme:   d.scheme,
		Host:     d.host,
		Path:     d.path,
		RawQuery: d.query.Encode(),
	}

	// Username and Password
	if d.username != "" {
		u.User = url.UserPassword(d.username, d.password)
	}

	return u.String()
}

// dsnFromString parses a connection string into a dsn. It will attempt to parse the string as
// a URL and as a set of key=value pairs. If both attempts fail, dsnFromString will return an error.
func dsnFromString(in string) (DSN, error) {
	if strings.HasPrefix(in, "postgresql://") || strings.HasPrefix(in, "postgres://") {
		return dsnFromURL(in)
	}

	// Try to parse as key=value pairs
	d, err := dsnFromKeyValue(in)
	if err == nil {
		return d, nil
	}

	// Parse the string as a URL, with the scheme prefixed
	d, err = dsnFromURL(fmt.Sprintf("postgresql://%s", in))
	if err == nil {
		return d, nil
	}

	return DSN{}, fmt.Errorf("could not understand DSN")
}

// dsnFromURL parses the input as a URL and returns the dsn representation.
func dsnFromURL(in string) (DSN, error) {
	u, err := url.Parse(in)
	if err != nil {
		return DSN{}, err
	}
	pass, _ := u.User.Password()
	user := u.User.Username()

	query := u.Query()

	if queryPass := query.Get("password"); queryPass != "" {
		if pass == "" {
			pass = queryPass
		}
	}
	query.Del("password")

	if queryUser := query.Get("user"); queryUser != "" {
		if user == "" {
			user = queryUser
		}
	}
	query.Del("user")

	d := DSN{
		scheme:   u.Scheme,
		username: user,
		password: pass,
		host:     u.Host,
		path:     u.Path,
		query:    query,
	}

	return d, nil
}

// dsnFromKeyValue parses the input as a set of key=value pairs and returns the dsn representation.
func dsnFromKeyValue(in string) (DSN, error) {
	// Attempt to confirm at least one key=value pair before starting the rune parser
	connstringRe := regexp.MustCompile(`^ *[a-zA-Z0-9]+ *= *[^= ]+`)
	if !connstringRe.MatchString(in) {
		return DSN{}, fmt.Errorf("input is not a key-value DSN")
	}

	// Anything other than known fields should be part of the querystring
	query := url.Values{}

	pairs, err := parseKeyValue(in)
	if err != nil {
		return DSN{}, fmt.Errorf("failed to parse key-value DSN: %v", err)
	}

	// Build the dsn from the key=value pairs
	d := DSN{
		scheme: "postgresql",
	}

	hostname := ""
	port := ""

	for k, v := range pairs {
		switch k {
		case "host":
			hostname = v
		case "port":
			port = v
		case "user":
			d.username = v
		case "password":
			d.password = v
		default:
			query.Set(k, v)
		}
	}

	if hostname == "" {
		hostname = "localhost"
	}

	if port == "" {
		d.host = hostname
	} else {
		d.host = fmt.Sprintf("%s:%s", hostname, port)
	}

	d.query = query

	return d, nil
}

// parseKeyValue is a key=value parser. It loops over each rune to split out keys and values
// and attempting to honor quoted values. parseKeyValue will return an error if it is unable
// to properly parse the input.
func parseKeyValue(in string) (map[string]string, error) {
	out := map[string]string{}

	inPart := false
	inQuote := false
	part := []rune{}
	key := ""
	for _, c := range in {
		switch {
		case unicode.In(c, unicode.Quotation_Mark):
			if inQuote {
				inQuote = false
			} else {
				inQuote = true
			}
		case unicode.In(c, unicode.White_Space):
			if inPart {
				if inQuote {
					part = append(part, c)
				} else {
					// Are we finishing a key=value?
					if key == "" {
						return out, fmt.Errorf("invalid input")
					}
					out[key] = string(part)
					inPart = false
					part = []rune{}
				}
			} else {
				// Are we finishing a key=value?
				if key == "" {
					return out, fmt.Errorf("invalid input")
				}
				out[key] = string(part)
				inPart = false
				part = []rune{}
				// Do something with the value
			}
		case c == '=' && !inQuote:
			// Only outside quotes. libpq's options value is itself a list of
			// settings written as -c name=value, so a quoted value can hold
			// equals signs of its own, and treating one of those as the
			// separator splits the pair in the wrong place and loses the rest.
			if inPart {
				inPart = false
				key = string(part)
				part = []rune{}
			} else {
				return out, fmt.Errorf("invalid input")
			}
		default:
			inPart = true
			part = append(part, c)
		}
	}

	if len(part) > 0 {
		// Text left over that never became a key=value pair. The old
		// per-engine parsers rejected this, and silently returning an empty
		// map instead would turn a malformed connection string into a
		// connection to localhost on the default port.
		if key == "" {
			return out, fmt.Errorf("invalid input")
		}
		out[key] = string(part)
	}

	return out, nil
}

// DSNWithDatabase returns the connection string in, pointed at another
// database on the same server.
//
// Both DSN forms are handled, because an operator can configure either and the
// difference must not decide which databases get monitored. A URL carries the
// database in its path, and a key=value string carries it in dbname, so the
// substitution is not the same edit in both and doing it by hand on a string
// is how one of the two forms quietly stops working.
//
// The result is a URL connection string whichever form went in, since that is
// what DSN serialises to. It carries the password, so it must not be logged.
//
// The dbname query parameter is dropped as well as the path being set. A
// key=value connection string keeps its dbname in the query, and libpq reads
// a dbname keyword there as well as the path, so setting the path alone leaves
// two answers in one string and collection silently stays on the original
// database.
func DSNWithDatabase(in, database string) (string, error) {
	d, err := dsnFromString(in)
	if err != nil {
		return "", err
	}
	d.path = "/" + database
	if d.query != nil {
		d.query.Del("dbname")
	}
	return d.GetConnectionString(), nil
}

// DatabaseFromDSN returns the database a connection string names, and whether
// it names one unambiguously.
//
// A URL can carry the database twice, once in its path and once as a dbname
// parameter, and the two can disagree. There is no useful rule for which wins.
// pq.ParseURL emits both as dbname keywords and sorts its output, and libpq
// takes the last occurrence of a repeated keyword, so the database that is
// actually connected to is whichever of the two names sorts later. Measured
// against PostgreSQL 18 with lib/pq:
//
//	.../postgres?dbname=root  connects to root, which is the query parameter
//	.../root?dbname=postgres  connects to root, which is the path
//
// Reproducing that in a parser would be depending on a sort order inside a
// driver, and reporting either half of it would be wrong half the time. So a
// connection string that names two different databases names none, as far as
// this function is concerned, and the caller is left to ask the server or to
// treat the database as unknown.
//
// An absent name is likewise not an error: libpq then falls back to the user
// name, and guessing that here would put a name on a database nobody
// configured.
func DatabaseFromDSN(in string) (string, bool) {
	d, err := dsnFromString(in)
	if err != nil {
		return "", false
	}
	fromPath := strings.TrimPrefix(d.path, "/")
	fromQuery := ""
	if d.query != nil {
		fromQuery = d.query.Get("dbname")
	}

	switch {
	case fromPath != "" && fromQuery != "" && fromPath != fromQuery:
		return "", false
	case fromPath != "":
		return fromPath, true
	case fromQuery != "":
		return fromQuery, true
	default:
		return "", false
	}
}

// DSNWithConnectTimeout returns the connection string with a connect_timeout
// no longer than d.
//
// Without one, lib/pq waits indefinitely. Its own documentation says zero or
// unspecified means wait forever, and that is not only about the TCP connect:
// a server that accepts a connection and then says nothing holds the attempt
// open through the startup handshake. Measured against a listener that accepts
// and never answers, a scrape with a ten-second deadline did not return at
// all, because the context a caller passes to PingContext or QueryContext does
// not reach pq's dial or its startup. connect_timeout is what pq honours
// there: it bounds the dial and sets a deadline across the handshake.
//
// A connect_timeout already in the string is kept when it is shorter. The
// budget is a ceiling, so an operator may ask for less and may not ask for
// more.
//
// pq reads the value in whole seconds, and zero means wait forever, so a
// budget under a second becomes one second rather than rounding down into no
// timeout at all. That lets a connection outlive a very short budget by under
// a second, which is the price of not reintroducing the unbounded wait.
func DSNWithConnectTimeout(in string, d time.Duration) (string, error) {
	if d <= 0 {
		return in, nil
	}
	parsed, err := dsnFromString(in)
	if err != nil {
		return "", err
	}

	seconds := int(d / time.Second)
	if d%time.Second != 0 || seconds == 0 {
		seconds++
	}

	if parsed.query == nil {
		parsed.query = url.Values{}
	}
	if existing := parsed.query.Get("connect_timeout"); existing != "" {
		if n, err := strconv.Atoi(existing); err == nil && n > 0 && n < seconds {
			return parsed.GetConnectionString(), nil
		}
	}
	parsed.query.Set("connect_timeout", strconv.Itoa(seconds))
	return parsed.GetConnectionString(), nil
}

// DSNWithSessionTimeouts returns the connection string with statement_timeout
// and lock_timeout set on the session, through libpq's options parameter.
//
// These are server-side backstops. The context a scrape runs under already
// cancels a query when its budget runs out, and lib/pq sends a cancel request
// when it does, but that depends on the exporter still being there to send it.
// A session timeout is enforced by the server, so a statement is bounded even
// if the exporter is killed, the network drops, or the cancel itself cannot be
// delivered.
//
// A value the connection string already sets is left alone. Unlike
// connect_timeout, which is a parameter of its own and a number this code can
// compare, options is a free-form argument string, and quietly appending to it
// would override whatever an operator wrote there without being able to say
// whether the result is shorter or longer than what they asked for.
//
// PostgreSQL takes both as milliseconds when given a bare number.
func DSNWithSessionTimeouts(in string, statement, lock time.Duration) (string, error) {
	parsed, err := dsnFromString(in)
	if err != nil {
		return "", err
	}
	if parsed.query == nil {
		parsed.query = url.Values{}
	}

	options := parsed.query.Get("options")
	for _, setting := range []struct {
		name  string
		value time.Duration
	}{
		{"statement_timeout", statement},
		{"lock_timeout", lock},
	} {
		if setting.value <= 0 || strings.Contains(options, setting.name) {
			continue
		}
		ms := setting.value.Milliseconds()
		if ms < 1 {
			// Rounding to zero would disable the timeout rather than shorten
			// it, which is the opposite of what the caller asked for.
			ms = 1
		}
		if options != "" {
			options += " "
		}
		options += fmt.Sprintf("-c %s=%d", setting.name, ms)
	}

	if options == "" {
		return parsed.GetConnectionString(), nil
	}
	parsed.query.Set("options", options)
	return parsed.GetConnectionString(), nil
}

// ParseKeyValue splits a libpq key=value connection string, honouring quoted
// values.
//
// Exported because both engines had their own copy of this, each splitting on
// plain spaces and on the first equals sign. That rejects any connection
// string with a quoted value containing a space, which includes a password
// with a space in it, an application_name with one, and the options string the
// session timeouts are written into.
func ParseKeyValue(in string) (map[string]string, error) {
	return parseKeyValue(in)
}
