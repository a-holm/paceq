package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// folds names the two functions in internal/model that take a run-level failure
// as their last argument. Both answer I10, and both are wrong for a run that
// failed for something no step can express unless that argument is the truth
// about the run in front of the caller.
var folds = map[string]bool{"RunAggregate": true, "RunStateAgrees": true}

// TestRunAggregateCallersComputeTheirSecondInput refuses a fold call whose
// run-level failure argument is written rather than computed.
//
// The argument decides the whole answer: true short-circuits to failed, false
// hands the steps the last word. A caller that spells a literal there has not
// answered the question, it has skipped it, and the literal that costs nothing
// to write is the one that reads a quarantined run as a success. The answer is
// reason.IsRunLevelFailure over the code the row carries, which every caller
// outside internal/model can reach.
//
// Tests are exempt: a table that drives both values of the input is exactly
// what the model's own proofs are made of.
func TestRunAggregateCallersComputeTheirSecondInput(t *testing.T) {
	root := repoRoot(t)
	modelDir := filepath.Join(root, "internal", "model")

	checked := 0
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch {
			case strings.HasPrefix(d.Name(), "."):
				return fs.SkipDir
			case d.Name() == "bin" || d.Name() == "dist" || d.Name() == "testdata":
				return fs.SkipDir
			// internal/model owns the fold and spells the argument in its
			// own doc examples and tables.
			case path == modelDir:
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}

		// modelAliases maps this file's local name for internal/model back to
		// the package, so an import under another name is still caught.
		modelAliases := map[string]bool{"model": true}
		for _, imp := range file.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil || p != "github.com/a-holm/paceq/internal/model" || imp.Name == nil {
				continue
			}
			modelAliases[imp.Name.String()] = imp.Name.String() != "_"
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !folds[sel.Sel.Name] || len(call.Args) == 0 {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || !modelAliases[pkg.Name] {
				return true
			}
			checked++
			last, ok := call.Args[len(call.Args)-1].(*ast.Ident)
			if !ok || (last.Name != "true" && last.Name != "false") {
				return true
			}
			t.Errorf("%s:%d writes %s as model.%s's run-level failure argument: "+
				"compute it with reason.IsRunLevelFailure over the run's reason code, "+
				"or the fold answers a question this caller never asked",
				rel(root, path), fset.Position(last.Pos()).Line, last.Name, sel.Sel.Name)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if checked == 0 {
		t.Fatal("no call to the fold was found outside internal/model: the guard swept nothing and proves nothing")
	}
}
