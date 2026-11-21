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

// Reading the metrics this package defines, out of the package itself.
//
// The catalogue has to describe every metric the exporter publishes, and a
// catalogue maintained by hand beside the code describes whatever it described
// when someone last remembered. So the mechanical half is read from the source
// with Go's own parser: names, labels and the file each one lives in.
//
// The extractor fails rather than skipping. A descriptor built in a shape it
// cannot resolve is a metric that would otherwise be absent from the inventory
// and therefore absent from everything checked against it, which is the same
// silent gap this exists to close.

package collector

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
)

// metricDefinition is one prometheus.NewDesc in this package.
type metricDefinition struct {
	Name   string
	Help   string
	Labels []string
	File   string
	Line   int
	// Symbol is the package-level variable the descriptor is assigned to, or
	// the map key it is stored under. It is how a value type found at an
	// emission site is attached back to the metric.
	Symbol string
	// Types are the prometheus value types this descriptor is emitted with.
	// More than one is a finding, not a shape to accommodate.
	Types []string
}

// packageSymbols holds the package-level string and []string values the
// descriptor calls refer to, so an identifier can be resolved to what it
// stands for.
type packageSymbols struct {
	strings map[string]string
	slices  map[string][]string
}

// readMetricInventory parses the non-test files of this package and returns
// every metric a prometheus.NewDesc call defines.
func readMetricInventory(dir string) ([]metricDefinition, error) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}

	// Every source file of the package except the tests. Reading the
	// directory rather than using parser.ParseDir, which is deprecated and
	// which this does not need: the question is which descriptors the package
	// defines, not which build configuration selects them.
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, dir+"/"+name, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", name, err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no source files found in %s", dir)
	}

	symbols := collectSymbols(files)

	var out []metricDefinition
	var failures []string
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isSelector(call.Fun, "prometheus", "NewDesc") {
				return true
			}
			pos := fset.Position(call.Pos())
			def := metricDefinition{File: trimDir(pos.Filename), Line: pos.Line}

			if len(call.Args) < 3 {
				failures = append(failures,
					fmt.Sprintf("%s:%d: NewDesc with %d arguments", def.File, def.Line, len(call.Args)))
				return true
			}

			name, err := resolveName(call.Args[0], symbols)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s:%d: %s", def.File, def.Line, err))
				return true
			}
			def.Name = name

			labels, err := resolveStrings(call.Args[2], symbols)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s:%d: labels: %s", def.File, def.Line, err))
				return true
			}
			def.Labels = labels

			if help, err := resolveString(call.Args[1], symbols); err == nil {
				def.Help = help
			}

			out = append(out, def)
			return true
		})
	}

	if len(failures) > 0 {
		return nil, fmt.Errorf("could not read %d descriptor(s):\n  %s",
			len(failures), strings.Join(failures, "\n  "))
	}

	attachSymbols(files, fset, out)
	attachValueTypes(files, out)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].File < out[j].File
	})
	return out, nil
}

// collectSymbols records every package-level string and []string value, so an
// identifier used as a name part or a label list can be resolved.
func collectSymbols(files []*ast.File) packageSymbols {
	sym := packageSymbols{strings: map[string]string{}, slices: map[string][]string{}}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || (gen.Tok != token.CONST && gen.Tok != token.VAR) {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					if s, err := resolveString(vs.Values[i], sym); err == nil {
						sym.strings[name.Name] = s
						continue
					}
					if ss, err := resolveStrings(vs.Values[i], sym); err == nil {
						sym.slices[name.Name] = ss
					}
				}
			}
		}
	}
	return sym
}

// resolveName reads a descriptor's fully qualified name, which is either a
// literal or a BuildFQName of three parts.
func resolveName(expr ast.Expr, sym packageSymbols) (string, error) {
	if call, ok := expr.(*ast.CallExpr); ok && isSelector(call.Fun, "prometheus", "BuildFQName") {
		if len(call.Args) != 3 {
			return "", fmt.Errorf("BuildFQName with %d arguments", len(call.Args))
		}
		parts := make([]string, 0, 3)
		for _, arg := range call.Args {
			s, err := resolveString(arg, sym)
			if err != nil {
				return "", fmt.Errorf("BuildFQName argument: %w", err)
			}
			if s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "_"), nil
	}
	s, err := resolveString(expr, sym)
	if err != nil {
		return "", fmt.Errorf("metric name: %w", err)
	}
	return s, nil
}

// resolveString reads a string literal, a known identifier, or a concatenation
// of those.
func resolveString(expr ast.Expr, sym packageSymbols) (string, error) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", fmt.Errorf("a %s literal where a string was expected", e.Kind)
		}
		return strconv.Unquote(e.Value)
	case *ast.Ident:
		if v, ok := sym.strings[e.Name]; ok {
			return v, nil
		}
		return "", fmt.Errorf("unknown identifier %q", e.Name)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", fmt.Errorf("unsupported operator %s", e.Op)
		}
		left, err := resolveString(e.X, sym)
		if err != nil {
			return "", err
		}
		right, err := resolveString(e.Y, sym)
		if err != nil {
			return "", err
		}
		return left + right, nil
	default:
		return "", fmt.Errorf("unsupported expression %T", expr)
	}
}

// resolveStrings reads a []string literal, nil, or a known identifier.
func resolveStrings(expr ast.Expr, sym packageSymbols) ([]string, error) {
	switch e := expr.(type) {
	case *ast.Ident:
		if e.Name == "nil" {
			return nil, nil
		}
		if v, ok := sym.slices[e.Name]; ok {
			return append([]string{}, v...), nil
		}
		return nil, fmt.Errorf("unknown identifier %q", e.Name)
	case *ast.CompositeLit:
		out := []string{}
		for _, el := range e.Elts {
			s, err := resolveString(el, sym)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported expression %T", expr)
	}
}

func isSelector(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == pkg
}

func trimDir(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

// attachSymbols records which variable or map key each descriptor is stored
// under, by position: the inventory already knows where every NewDesc call is,
// so the enclosing assignment identifies it.
func attachSymbols(files []*ast.File, fset *token.FileSet, defs []metricDefinition) {
	byPos := map[string]int{}
	for i, d := range defs {
		byPos[fmt.Sprintf("%s:%d", d.File, d.Line)] = i
	}
	key := func(n ast.Node) (int, bool) {
		pos := fset.Position(n.Pos())
		i, ok := byPos[fmt.Sprintf("%s:%d", trimDir(pos.Filename), pos.Line)]
		return i, ok
	}

	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.ValueSpec:
				for i, v := range node.Values {
					if i >= len(node.Names) {
						continue
					}
					if j, ok := key(v); ok {
						defs[j].Symbol = node.Names[i].Name
					}
				}
			case *ast.KeyValueExpr:
				if j, ok := key(node.Value); ok {
					if lit, isLit := node.Key.(*ast.BasicLit); isLit {
						if s, err := strconv.Unquote(lit.Value); err == nil {
							defs[j].Symbol = s
						}
					}
				}
			}
			return true
		})
	}
}

// attachValueTypes finds where each descriptor is turned into a metric and
// records the prometheus value type used there.
//
// Three shapes produce a metric in this package: MustNewConstMetric, the emit
// closure the shared-label collectors use, and a sharedLabelsMetric literal.
// All three take the descriptor first or as a named field, so the descriptor's
// symbol is what links an emission back to the inventory.
func attachValueTypes(files []*ast.File, defs []metricDefinition) {
	index := map[string]int{}
	for i, d := range defs {
		if d.Symbol != "" {
			index[d.Symbol] = i
		}
	}
	record := func(symbol, vtype string) {
		if i, ok := index[symbol]; ok && vtype != "" && !containsString(defs[i].Types, vtype) {
			defs[i].Types = append(defs[i].Types, vtype)
		}
	}

	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				name := calleeName(node.Fun)
				if len(node.Args) < 1 {
					return true
				}
				switch name {
				case "MustNewConstMetric", "NewConstMetric", "emit":
					// The value type is the argument after the descriptor.
					if len(node.Args) >= 2 {
						record(descSymbol(node.Args[0]), valueTypeName(node.Args[1]))
					}
				case "MustNewConstHistogram", "NewConstHistogram":
					// A histogram carries no value type; its shape is the type.
					record(descSymbol(node.Args[0]), "Histogram")
				case "MustNewConstSummary", "NewConstSummary":
					record(descSymbol(node.Args[0]), "Summary")
				case "gauge":
					// A local closure in collector.go that wraps
					// MustNewConstMetric with GaugeValue and the server label.
					// Named here rather than followed, because the alternative
					// is a metric with no type at all, which
					// TestEveryMetricHasExactlyOneValueType then reports.
					record(descSymbol(node.Args[0]), "Gauge")
				}
			case *ast.CompositeLit:
				var symbol, vtype string
				for _, el := range node.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					ident, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					switch ident.Name {
					case "desc":
						symbol = descSymbol(kv.Value)
					case "vtype":
						vtype = valueTypeName(kv.Value)
					}
				}
				record(symbol, vtype)
			}
			return true
		})
	}
}

// descSymbol names the descriptor an expression refers to: a bare variable, or
// a map lookup with a literal key.
func descSymbol(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		if lit, ok := e.Index.(*ast.BasicLit); ok {
			if s, err := strconv.Unquote(lit.Value); err == nil {
				return s
			}
		}
	}
	return ""
}

func valueTypeName(expr ast.Expr) string {
	if sel, ok := expr.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "prometheus" {
			return strings.TrimSuffix(sel.Sel.Name, "Value")
		}
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func calleeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.Ident:
		return e.Name
	}
	return ""
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
