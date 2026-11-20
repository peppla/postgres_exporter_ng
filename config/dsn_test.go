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
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// Test_dsn_String is designed to test different dsn combinations for their string representation.
// dsn.String() is designed to be safe to print, redacting any password information and these test
// cases are intended to cover known cases.
func Test_dsn_String(t *testing.T) {
	type fields struct {
		scheme   string
		username string
		password string
		host     string
		path     string
		query    url.Values
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Without Password",
			fields: fields{
				scheme:   "postgresql",
				username: "test",
				host:     "localhost:5432",
				query:    url.Values{},
			},
			want: "postgresql://test@localhost:5432?",
		},
		{
			name: "With Password",
			fields: fields{
				scheme:   "postgresql",
				username: "test",
				password: "supersecret",
				host:     "localhost:5432",
				query:    url.Values{},
			},
			want: "postgresql://test:******@localhost:5432?",
		},
		{
			name: "With Password and Query String",
			fields: fields{
				scheme:   "postgresql",
				username: "test",
				password: "supersecret",
				host:     "localhost:5432",
				query: url.Values{
					"ssldisable": []string{"true"},
				},
			},
			want: "postgresql://test:******@localhost:5432?ssldisable=true",
		},
		{
			name: "With Password, Path, and Query String",
			fields: fields{
				scheme:   "postgresql",
				username: "test",
				password: "supersecret",
				host:     "localhost:5432",
				path:     "/somevalue",
				query: url.Values{
					"ssldisable": []string{"true"},
				},
			},
			want: "postgresql://test:******@localhost:5432/somevalue?ssldisable=true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := DSN{
				scheme:   tt.fields.scheme,
				username: tt.fields.username,
				password: tt.fields.password,
				host:     tt.fields.host,
				path:     tt.fields.path,
				query:    tt.fields.query,
			}
			if got := d.String(); got != tt.want {
				t.Errorf("dsn.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_dsnFromString tests the dsnFromString function with known variations
// of connection string inputs to ensure that it properly parses the input into
// a dsn.
func Test_dsnFromString(t *testing.T) {

	tests := []struct {
		name    string
		input   string
		want    DSN
		wantErr bool
	}{
		{
			name:  "Key value with password",
			input: "host=host.example.com user=postgres port=5432 password=s3cr3t",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				username: "postgres",
				password: "s3cr3t",
				query:    url.Values{},
			},
			wantErr: false,
		},
		{
			name:  "Key value with quoted password and space",
			input: "host=host.example.com user=postgres port=5432 password=\"s3cr 3t\"",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				username: "postgres",
				password: "s3cr 3t",
				query:    url.Values{},
			},
			wantErr: false,
		},
		{
			name:  "Key value with different order",
			input: "password=abcde host=host.example.com user=postgres port=5432",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				username: "postgres",
				password: "abcde",
				query:    url.Values{},
			},
			wantErr: false,
		},
		{
			name:  "Key value with different order, quoted password, duplicate password",
			input: "password=abcde host=host.example.com user=postgres port=5432 password=\"s3cr 3t\"",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				username: "postgres",
				password: "s3cr 3t",
				query:    url.Values{},
			},
			wantErr: false,
		},
		{
			name:  "URL with user in query string",
			input: "postgresql://host.example.com:5432/tsdb?user=postgres",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				path:     "/tsdb",
				query:    url.Values{},
				username: "postgres",
			},
			wantErr: false,
		},
		{
			name:  "URL with user and password",
			input: "postgresql://user:s3cret@host.example.com:5432/tsdb?user=postgres",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				path:     "/tsdb",
				query:    url.Values{},
				username: "user",
				password: "s3cret",
			},
			wantErr: false,
		},
		{
			name:  "Alternative URL prefix",
			input: "postgres://user:s3cret@host.example.com:5432/tsdb?user=postgres",
			want: DSN{
				scheme:   "postgres",
				host:     "host.example.com:5432",
				path:     "/tsdb",
				query:    url.Values{},
				username: "user",
				password: "s3cret",
			},
			wantErr: false,
		},
		{
			name:  "URL with user and password in query string",
			input: "postgresql://host.example.com:5432/tsdb?user=postgres&password=s3cr3t",
			want: DSN{
				scheme:   "postgresql",
				host:     "host.example.com:5432",
				path:     "/tsdb",
				query:    url.Values{},
				username: "postgres",
				password: "s3cr3t",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dsnFromString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("dsnFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dsnFromString() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestDSNWithDatabase covers both connection-string forms, because an operator
// can configure either and a substitution that works on one and not the other
// would silently monitor a different set of databases.
func TestDSNWithDatabase(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		db   string
		want string
	}{
		{
			name: "url with a database",
			in:   "postgresql://user:pass@host:5432/postgres?sslmode=disable",
			db:   "app",
			want: "postgresql://user:pass@host:5432/app?sslmode=disable",
		},
		{
			name: "url with no database",
			in:   "postgresql://user:pass@host:5432?sslmode=disable",
			db:   "app",
			want: "postgresql://user:pass@host:5432/app?sslmode=disable",
		},
		{
			name: "key value form",
			in:   "host=host port=5432 user=user password=pass dbname=postgres sslmode=disable",
			db:   "app",
			want: "postgresql://user:pass@host:5432/app?sslmode=disable",
		},
		{
			name: "key value form with no dbname",
			in:   "host=host port=5432 user=user password=pass sslmode=disable",
			db:   "app",
			want: "postgresql://user:pass@host:5432/app?sslmode=disable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DSNWithDatabase(tc.in, tc.db)
			if err != nil {
				t.Fatalf("DSNWithDatabase: %s", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
			// Round trip: the result must name the database that was asked for.
			if name, ok := DatabaseFromDSN(got); !ok || name != tc.db {
				t.Errorf("rewritten DSN names %q (%v), want %q", name, ok, tc.db)
			}
		})
	}
}

func TestDatabaseFromDSN(t *testing.T) {
	if name, ok := DatabaseFromDSN("postgresql://u:p@h:5432/app?sslmode=disable"); !ok || name != "app" {
		t.Errorf("got %q %v, want app true", name, ok)
	}
	// libpq falls back to the user name when no database is given. Guessing
	// that here would put a name on a database nobody configured.
	if name, ok := DatabaseFromDSN("postgresql://u:p@h:5432?sslmode=disable"); ok {
		t.Errorf("got %q %v, want an unknown database", name, ok)
	}
	if _, ok := DatabaseFromDSN("this is not a dsn"); ok {
		t.Error("an unparseable DSN reported a database name")
	}
}

// TestDSNWithDatabaseClearsStaleDbname is the case that makes the rewrite more
// than setting a field. A key=value connection string keeps its database in
// the dbname query parameter, which libpq reads as well as the path, so a
// rewrite that sets only the path leaves two answers in one string.
func TestDSNWithDatabaseClearsStaleDbname(t *testing.T) {
	got, err := DSNWithDatabase("host=h port=5432 user=u password=p dbname=postgres sslmode=disable", "app")
	if err != nil {
		t.Fatalf("DSNWithDatabase: %s", err)
	}
	if strings.Contains(got, "dbname=postgres") {
		t.Errorf("the original database is still named in %q", got)
	}
	if name, ok := DatabaseFromDSN(got); !ok || name != "app" {
		t.Errorf("rewritten DSN names %q (%v), want app", name, ok)
	}
}

// TestDatabaseFromDSNReadsDbnameQuery covers the other half: a connection
// string whose database is in the query rather than the path still has one.
func TestDatabaseFromDSNReadsDbnameQuery(t *testing.T) {
	if name, ok := DatabaseFromDSN("host=h port=5432 user=u dbname=app"); !ok || name != "app" {
		t.Errorf("got %q %v, want app true", name, ok)
	}
}
