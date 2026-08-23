package spec

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
)

// codeLiteral matches a diagnostic code written out anywhere in the package.
var codeLiteral = regexp.MustCompile(`\b(PQ[0-9]{4}|W[0-9]{4})\b`)

// TestEveryCodeInTheSourceIsListed reads the package back and checks that every
// code it spells out is in Codes(). Codes() is what the catalogue behind
// `paceq error` is built from, so a code that raises but is not listed is a
// message that sends the reader to a page that does not exist.
func TestEveryCodeInTheSourceIsListed(t *testing.T) {
	listed := map[string]bool{}
	for _, code := range Codes() {
		if listed[code] {
			t.Errorf("Codes() lists %s twice", code)
		}
		listed[code] = true
	}

	found := map[string]string{}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "testdata" {
			return fs.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range codeLiteral.FindAllString(string(src), -1) {
			found[match] = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the package: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("no codes found in the package source, the check would pass vacuously")
	}

	for code, path := range found {
		if !listed[code] {
			t.Errorf("%s is written in %s but is not in Codes()", code, path)
		}
	}
	for code := range listed {
		if _, exists := found[code]; !exists {
			t.Errorf("Codes() lists %s, but nothing in the package spells it out", code)
		}
	}
}

// TestCodesUseTheSeriesTheyBelongTo. PQ1xxx is parsing and schema, PQ2xxx is
// semantics, W1xxx is a warning (03 section 8.1). A code in the wrong series
// sends a reader looking in the wrong chapter.
func TestCodesUseTheSeriesTheyBelongTo(t *testing.T) {
	semantic := map[string]bool{
		CodeDuplicateStep:   true,
		CodeUnknownNeed:     true,
		CodeUnknownTimezone: true,
	}
	warning := map[string]bool{CodeShell: true, CodeInheritEnv: true}
	// The sensor codes are a dedicated series: the sensor contract is frozen at
	// v0.1, so its rules get number space of their own (PQ4xxx) rather than
	// renting numbers in the shared parsing and schema series.
	sensor := map[string]bool{
		CodeSensorBadName:     true,
		CodeSensorNameTaken:   true,
		CodeSensorKind:        true,
		CodeSensorRun:         true,
		CodeSensorIntervalMin: true,
		CodeSensorMinInterval: true,
		CodeSensorTimeout:     true,
		CodeSensorTriggers:    true,
		CodeSensorWorkdir:     true,
		CodeSensorEnvKey:      true,
	}

	for _, code := range Codes() {
		switch {
		case warning[code]:
			if !strings.HasPrefix(code, "W1") {
				t.Errorf("%s is a warning and should be in the W1xxx series", code)
			}
		case semantic[code]:
			if !strings.HasPrefix(code, "PQ2") {
				t.Errorf("%s is a semantic rule and should be in the PQ2xxx series", code)
			}
		case sensor[code]:
			if !strings.HasPrefix(code, "PQ4") {
				t.Errorf("%s is a sensor rule and should be in the PQ4xxx series", code)
			}
		default:
			if !strings.HasPrefix(code, "PQ1") {
				t.Errorf("%s is a parsing or schema rule and should be in the PQ1xxx series", code)
			}
		}
	}
}

// TestAnAliasThatResolvesToItselfIsRefused proves the guard that the YAML
// parser makes unreachable from a file: it rejects &a *a as a syntax error, so
// the cycle is built here directly. Without the guard the resolver would follow
// the alias until it ran out of budget, and the message would blame the size of
// a file that is three lines long.
func TestAnAliasThatResolvesToItselfIsRefused(t *testing.T) {
	file, err := parser.ParseBytes([]byte("anchor: &a 1\nuse: *a\n"), 0)
	if err != nil {
		t.Fatalf("parse the fixture: %v", err)
	}
	body, ok := file.Docs[0].Body.(*ast.MappingNode)
	if !ok {
		t.Fatalf("the fixture body is %T, want a mapping", file.Docs[0].Body)
	}
	alias, ok := body.Values[1].Value.(*ast.AliasNode)
	if !ok {
		t.Fatalf("the fixture value is %T, want an alias", body.Values[1].Value)
	}

	d := &decoder{
		file:    "cycle.yaml",
		anchors: map[string]ast.Node{"a": alias},
		open:    map[string]bool{},
		budget:  MaxExpandedNodes,
	}

	node, ok := d.resolve(alias, "use")

	if ok || node != nil {
		t.Fatalf("resolving a cycle returned %v, %v", node, ok)
	}
	if len(d.diags) != 1 || d.diags[0].Code != CodeTooManyAliases {
		t.Fatalf("got %v, want one %s", d.diags, CodeTooManyAliases)
	}
	if !strings.Contains(d.diags[0].Message, "itself") {
		t.Errorf("the message does not say the anchor contains itself: %s", d.diags[0].Message)
	}
	if d.budget < MaxExpandedNodes-8 {
		t.Errorf("the cycle spent %d of the budget before it was caught", MaxExpandedNodes-d.budget)
	}
}

// TestTheBudgetStopsAWalkThatWouldNotEnd is the same guard from the other side:
// a decoder with nothing left to spend refuses rather than continuing.
func TestTheBudgetStopsAWalkThatWouldNotEnd(t *testing.T) {
	file, err := parser.ParseBytes([]byte("a: 1\n"), 0)
	if err != nil {
		t.Fatalf("parse the fixture: %v", err)
	}

	d := &decoder{file: "spent.yaml", anchors: map[string]ast.Node{}, open: map[string]bool{}}

	if _, ok := d.resolve(file.Docs[0].Body, "a"); ok {
		t.Fatal("a decoder with no budget left resolved a node")
	}
	if len(d.diags) != 1 || d.diags[0].Code != CodeTooLarge {
		t.Fatalf("got %v, want one %s", d.diags, CodeTooLarge)
	}
}
