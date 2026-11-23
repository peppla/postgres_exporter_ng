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
	"context"
	"testing"
)

// TestClosingAnInstanceThatNeverConnected covers the crash on the probe's
// failure path.
//
// setup clears db when it cannot connect, and the deferred cleanup then closed
// it regardless. A probe against a target refusing connections answered the
// client and panicked the process on the way out, which turns one unreachable
// target into an outage of the exporter.
func TestClosingAnInstanceThatNeverConnected(t *testing.T) {
	// A port nothing is listening on, so setup fails and clears the handle.
	i, err := newInstance("postgresql://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("building the instance: %s", err)
	}
	if err := i.setup(context.Background()); err == nil {
		t.Fatal("connecting to a closed port succeeded")
	}
	if i.db != nil {
		t.Fatal("setup left a handle behind after failing, so this test is not covering the case it exists for")
	}

	// The thing that used to panic.
	if err := i.Close(); err != nil {
		t.Errorf("closing an instance that never connected returned %s", err)
	}
	// And twice, because cleanup can run more than once on an error path.
	if err := i.Close(); err != nil {
		t.Errorf("closing twice returned %s", err)
	}
}
