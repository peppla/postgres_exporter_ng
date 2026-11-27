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

package targetid

import (
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestOneIdentityPerConnectionString is the whole contract: the same string
// keeps its identity, and a different one gets a different identity however
// small the difference is.
func TestOneIdentityPerConnectionString(t *testing.T) {
	Clear()
	defer Clear()

	const base = "postgresql://reader:secret@db:5432/app?sslmode=disable"
	first, assigned := ID(base)
	if !assigned {
		t.Error("the first use of a connection string did not assign an identity")
	}
	again, assigned := ID(base)
	if assigned {
		t.Error("the same connection string was assigned a second identity")
	}
	if again != first {
		t.Errorf("the same connection string has identities %q and %q", first, again)
	}

	// Each of these differs from base in one thing, and each is a connection
	// that can read something the others cannot.
	for _, dsn := range []string{
		"postgresql://reader:secret@db:5432/other?sslmode=disable",
		"postgresql://other:secret@db:5432/app?sslmode=disable",
		"postgresql://reader:secret@db:5433/app?sslmode=disable",
		"postgresql://reader:secret@db:5432/app?sslmode=disable&options=-csearch_path%3Dreporting",
	} {
		id, assigned := ID(dsn)
		if !assigned {
			t.Errorf("%s reused an identity", dsn)
		}
		if id == first {
			t.Errorf("%s shares the identity %q with the base connection", dsn, first)
		}
	}
	if Len() != 5 {
		t.Errorf("five connection strings produced %d identities", Len())
	}
}

// TestAnIdentityCarriesNothingFromTheString keeps credentials out of what ends
// up in a metric label.
//
// The parts looked for all contain a character hexadecimal does not use. A
// substring of the hex alphabet is not evidence of anything: "db" turns up in
// about one identity in nine by chance, which failed this on PostgreSQL 19 in
// the matrix at 60cb548 while the same code passed on the other four majors.
// A case that fails one run in nine is a case that says nothing.
//
// The shape is checked as well, because a value that is not a fixed-width hex
// string is a different question from what it carries.
func TestAnIdentityCarriesNothingFromTheString(t *testing.T) {
	Clear()
	defer Clear()

	id, _ := ID("postgresql://reader:hunter2zz@dbhost:5432/appname")
	for _, part := range []string{"hunter2zz", "reader", "dbhost", "appname", "postgresql"} {
		if strings.Contains(id, part) {
			t.Errorf("the identity %q carries %q from the connection string", id, part)
		}
	}
	if len(id) != Width {
		t.Errorf("the identity %q is %d characters, want %d", id, len(id), Width)
	}
	for _, c := range id {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Errorf("the identity %q is not hexadecimal", id)
			break
		}
	}
}

// TestOneCharacterOfTheStringChangesTheWholeIdentity is the property that
// makes a connection context its own.
//
// Two connections that differ in one character are different contexts and must
// not share an identity, and a reader of two identities must not be able to
// tell how close the strings behind them are.
func TestOneCharacterOfTheStringChangesTheWholeIdentity(t *testing.T) {
	Clear()
	defer Clear()

	const base = "postgresql://reader:secret@db:5432/app?sslmode=disable"
	const nearly = "postgresql://reader:secret@db:5432/apq?sslmode=disable"
	if base == nearly || len(base) != len(nearly) {
		t.Fatal("the two fixtures are not one character apart, so this proves nothing")
	}
	first, _ := ID(base)
	second, _ := ID(nearly)

	if first == second {
		t.Fatalf("two connection strings differing in one character share the identity %q", first)
	}
	same := 0
	for i := range first {
		if first[i] == second[i] {
			same++
		}
	}
	// Two unrelated hex strings agree in about one position in sixteen. A
	// derivation that carried the input through would agree in nearly all of
	// them.
	if same > Width/2 {
		t.Errorf("the identities %q and %q agree in %d of %d positions", first, second, same, Width)
	}
}

// TestConcurrentAssignmentIsSafe covers the obvious: several scrapes reach
// this at once.
func TestConcurrentAssignmentIsSafe(t *testing.T) {
	Clear()
	defer Clear()

	var wg sync.WaitGroup
	seen := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _ := ID("postgresql://reader@db:5432/app")
			seen <- id
		}()
	}
	wg.Wait()
	close(seen)

	first := ""
	for id := range seen {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("one connection string was assigned %q and %q", first, id)
		}
	}
	if Len() != 1 {
		t.Errorf("one connection string produced %d identities", Len())
	}
}

// TestTwoProcessesDoNotAgree is what the private key buys.
//
// An identity is published in a label. If it were a plain digest of the
// connection string, anyone reading the label could digest a guessed password
// and compare. Under a key this process reads at startup and never discloses,
// the same connection string gives a different identity in another process,
// so nothing outside it can relate the two.
func TestTwoProcessesDoNotAgree(t *testing.T) {
	Clear()
	defer Clear()

	const dsn = "postgresql://reader:secret@db:5432/app?sslmode=disable"
	before, _ := ID(dsn)

	Rekey()
	defer Rekey()
	after, _ := ID(dsn)

	if before == after {
		t.Errorf("one connection string gave the identity %q under two keys", before)
	}
	if len(before) != Width || len(after) != Width {
		t.Errorf("identities are %d and %d characters, want %d", len(before), len(after), Width)
	}
}

// TestNothingIsKeptPerConnection bounds this package.
//
// It holds no map from connection strings to identities, so the only thing
// that can grow is the record of what has been logged, and that is capped.
// Past the cap the mapping stops being logged and nothing else changes.
func TestNothingIsKeptPerConnection(t *testing.T) {
	Clear()
	defer Clear()

	for i := 0; i < loggedCap*2; i++ {
		dsn := "postgresql://u:p@db:5432/d" + strconv.Itoa(i) + "?sslmode=disable"
		id, first := ID(dsn)
		if len(id) != Width {
			t.Fatalf("identity %q is %d characters, want %d", id, len(id), Width)
		}
		if i < loggedCap && !first {
			t.Fatalf("connection %d was not reported as new", i)
		}
		if i >= loggedCap && first {
			t.Fatalf("connection %d was reported as new past the cap", i)
		}
	}
	if Len() > loggedCap {
		t.Errorf("%d identities remembered, want at most %d", Len(), loggedCap)
	}
}
