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

// How much the exporter may remember between scrapes, as an operator writes
// it.
//
//	state:
//	  profiles: 64
//	  probe_connections: 256
//	  idle: 24h
//
// Most of what is remembered is bounded by what a server says it has, and a
// complete catalogue read is what removes the rest. Two things have no such
// authority. A collection profile is whatever set of collectors a request asks
// for, and a probe connection is whatever target a request names, so both are
// minted by whoever can reach the endpoint.
//
// The two numbers are how many of each may hold a slot, and idle is how long a
// slot survives without being asked for. A name holding a slot keeps it until
// it goes quiet for that long, and a name arriving when every slot is held is
// refused before any SQL runs.
//
// idle has to be longer than the longest interval anything scrapes this
// exporter at. A job whose interval is longer than the window loses its slot
// between its own requests, every time, and then competes for admission like a
// newcomer.
//
// Every key is optional. With the whole section left out nothing is refused
// and nothing is released, which is what this exporter does today: the three
// values are an operational decision that has not been made, and leaving them
// out is how it behaves until it is.

package config

import (
	"fmt"
	"time"

	"github.com/percona/postgres_exporter/internal/slots"
	"github.com/prometheus/common/model"
)

// State is the state section of the configuration file.
//
// Every field is a pointer, so a key left out and a key written as zero stay
// different. `profiles: 0` is a mistake worth reporting, and with a plain int
// it would be indistinguishable from an omitted key and would quietly mean no
// limit at all, which is the opposite of what writing a zero looks like.
type State struct {
	Profiles         *int            `yaml:"profiles"`
	ProbeConnections *int            `yaml:"probe_connections"`
	Idle             *model.Duration `yaml:"idle"`
}

// Slots is what the file said, as the admission reads it.
//
// There is nothing to overlay onto. Unlike the budget, these have no approved
// defaults to fall back to, so a key left out stays zero and zero is what the
// admission reads as no limit configured.
//
// It does not validate. The caller validates once, by the same rule the
// admission itself is written against.
func (s State) Slots() slots.Limits {
	var out slots.Limits
	if s.Profiles != nil {
		out.Profiles = *s.Profiles
	}
	if s.ProbeConnections != nil {
		out.Contexts = *s.ProbeConnections
	}
	if s.Idle != nil {
		out.Idle = time.Duration(*s.Idle)
	}
	return out
}

// Validate reports what only this form can see.
//
// A number written as zero is a mistake. Read as a limit it means no limit,
// which is the opposite of what writing a zero looks like, and the admission
// cannot tell it from a key left out because by then both are zero. Only the
// pointer knows, so only here can it be said.
//
// Everything else about these values is checked by the admission's own rule,
// so the file and the code that reads it cannot disagree.
func (s State) Validate() error {
	for _, named := range []struct {
		key   string
		value *int
	}{
		{"profiles", s.Profiles},
		{"probe_connections", s.ProbeConnections},
	} {
		if named.value != nil && *named.value == 0 {
			return fmt.Errorf("state.%s is 0: write a number of at least 1, or leave the key out to set no limit", named.key)
		}
	}
	if s.Idle != nil && time.Duration(*s.Idle) == 0 {
		return fmt.Errorf("state.idle is 0: write a duration, or leave the key out to release nothing")
	}
	return s.Slots().Validate()
}

// stateKeyPaths are the keys where writing nothing after the name would
// silently mean something other than what it looks like.
func stateKeyPaths() [][]string {
	return [][]string{
		{"state"},
		{"state", "profiles"},
		{"state", "probe_connections"},
		{"state", "idle"},
	}
}
