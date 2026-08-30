package arch_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// stringMatchers are the package strings entry points that decide something by
// looking at text. Applied to an error's message they turn a sentence into an
// interface: the message is reworded, the branch stops matching, and nothing
// fails until an operator meets the case the branch existed for.
var stringMatchers = map[string]bool{
	"Contains":    true,
	"HasPrefix":   true,
	"HasSuffix":   true,
	"EqualFold":   true,
	"Index":       true,
	"Count":       true,
	"ContainsAny": true,
}

// errTextMatch is one place where a decision reads an error's message.
type errTextMatch struct {
	file string
	line int
	call string
}

func (m errTextMatch) String() string {
	return fmt.Sprintf("%s:%d: %s", m.file, m.line, m.call)
}

// TestErrorsAreMatchedBySentinelNotBySentence: internal/cli decides what to do
// about an error by asking errors.Is or errors.As, never by reading the words
// in it. #214 found the branch this guard exists to stop: preview matched "no
// more occurrences", a sentence cronx has never produced, so the branch was
// dead and an exhausted schedule became an internal error.
//
// Test files are exempt: asserting that a message says something useful is
// exactly what a message test is for.
func TestErrorsAreMatchedBySentinelNotBySentence(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "internal", "cli")
	fset := token.NewFileSet()

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		matches, err := findErrTextMatches(fset, path)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}
		for _, m := range matches {
			m.file = rel(root, m.file)
			t.Errorf("%s: forbidden, an error's message is prose and changes; branch on "+
				"errors.Is with the sentinel the package exports, or errors.As with its type", m)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}

// findErrTextMatches reports every strings.X(...) in one file that is handed
// the result of an Error() call. It matches the local name of the strings
// import, so an aliased import cannot hide the call.
func findErrTextMatches(fset *token.FileSet, path string) ([]errTextMatch, error) {
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}

	names := importNames(file, "strings")
	if len(names) == 0 {
		return nil, nil
	}

	var out []errTextMatch
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !stringMatchers[sel.Sel.Name] {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || !names[ident.Name] || ident.Obj != nil {
			return true
		}
		for _, arg := range call.Args {
			if !isErrorCall(arg) {
				continue
			}
			pos := fset.Position(call.Pos())
			out = append(out, errTextMatch{
				file: pos.Filename,
				line: pos.Line,
				call: ident.Name + "." + sel.Sel.Name + " over an error's message",
			})
			break
		}
		return true
	})
	return out, nil
}

// isErrorCall reports whether an expression is a bare x.Error() call, which is
// the one way an error's message reaches a string.
func isErrorCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Error"
}

// importNames returns the local names one import path is reachable under.
func importNames(file *ast.File, want string) map[string]bool {
	names := map[string]bool{}
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if path != want {
			continue
		}
		if imp.Name == nil {
			names[filepath.Base(want)] = true
			continue
		}
		if imp.Name.Name != "_" && imp.Name.Name != "." {
			names[imp.Name.Name] = true
		}
	}
	return names
}
