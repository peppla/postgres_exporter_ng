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
func TestAnIdentityCarriesNothingFromTheString(t *testing.T) {
	Clear()
	defer Clear()

	id, _ := ID("postgresql://reader:hunter2@db:5432/app")
	for _, part := range []string{"hunter2", "reader", "db", "app"} {
		if strings.Contains(id, part) {
			t.Errorf("the identity %q carries %q from the connection string", id, part)
		}
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
