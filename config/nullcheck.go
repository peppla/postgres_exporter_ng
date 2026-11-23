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

package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// rejectNullKeys fails a configuration that writes one of these keys with
// nothing after it.
//
// `connections:` with no value and an omitted `connections` are the same thing
// to the typed parse, because yaml does not call a custom unmarshaler for a
// null and decodes it into a nil pointer, which is what an absent key produces
// too. So a key written and left empty silently takes the default, and the
// operator who wrote it reads the file as if it were in force.
//
// This has to be a second pass over the document. The typed parse cannot
// report what it cannot see.
func rejectNullKeys(content []byte, paths ...[]string) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(content, &doc); err != nil {
		// The typed parse reports its own error for a malformed file. This
		// pass looks for one shape and stays quiet about everything else.
		return nil
	}
	root := documentRoot(&doc)
	for _, path := range paths {
		node := root
		for _, key := range path {
			node = childOf(node, key)
			if node == nil {
				break
			}
		}
		if node != nil && node.Tag == "!!null" {
			return fmt.Errorf("%s is present with no value: give it a value or leave the key out",
				strings.Join(path, "."))
		}
	}
	return nil
}

// documentRoot unwraps the document node a whole-file parse produces.
func documentRoot(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		return node.Content[0]
	}
	return node
}

// childOf returns the value node for a key of a mapping, or nil.
func childOf(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}
