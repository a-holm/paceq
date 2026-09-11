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

// stepTerms names the guard fields model.TerminalVerdict ranks a run's steps
// by. A literal that spells either one has begun building a verdict.
var stepTerms = []string{"AnyStepFailed", "AnyStepCancelled"}

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
// It sees direct calls and nothing else. A writer that reaches the fold through
// a model.Guards literal spells the term as a field name rather than as an
// argument, and TestTerminalRunGuardsRankTheRunAndNotOnlyItsSteps is the guard
// for that shape.
//
// Tests are exempt: a table that drives both values of the input is exactly
// what the model's own proofs are made of.
func TestRunAggregateCallersComputeTheirSecondInput(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	checked := 0

	forEachFoldSource(t, root, fset, func(path string, file *ast.File) {
		aliases := modelAliasesOf(file)
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
			if !ok || !aliases[pkg.Name] {
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
	})

	if checked == 0 {
		t.Fatal("no call to the fold was found outside internal/model: the guard swept nothing and proves nothing")
	}
}

// TestTerminalRunGuardsRankTheRunAndNotOnlyItsSteps refuses a model.Guards
// literal that ranks a run's steps without ranking the run.
//
// The two shapes fail differently, which is why one guard cannot hold both. The
// fold takes its run-level term positionally, so a caller that forgets it does
// not compile. A model.Guards literal takes the term by name, so a caller that
// forgets it compiles and hands the machine a false: a writer of a terminal run
// state answering "nothing failed above the steps" to a question it never
// asked. Nothing revisits a terminal row afterwards.
//
// The rule is what a walk over syntax can decide on its own. TerminalVerdict
// reads three fields and no others, RunLevelFailure outranking AnyStepFailed
// outranking AnyStepCancelled, so a literal that spells either step term is
// building a verdict and must spell the run term too.
//
// Its reach stops there. It does not prove that a literal it flags reaches
// TerminalVerdict, and it does not see one that ranks a verdict while spelling
// no step term at all. Together with the direct-call guard above it covers
// every shape that reaches the fold in this tree.
func TestTerminalRunGuardsRankTheRunAndNotOnlyItsSteps(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	checked := 0

	forEachFoldSource(t, root, fset, func(path string, file *ast.File) {
		aliases := modelAliasesOf(file)
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok || !isGuardsLiteral(lit, aliases) {
				return true
			}
			fields := keyedFields(lit)
			if !ranksSteps(fields) {
				return true
			}
			checked++
			if _, ok := fields["RunLevelFailure"]; ok {
				return true
			}
			t.Errorf("%s:%d builds model.Guards that rank the run's steps but not the run: "+
				"set RunLevelFailure from reason.IsRunLevelFailure over the run's reason code, "+
				"or the verdict comes from the steps alone and a run that failed for "+
				"something no step can express is recorded as a success",
				rel(root, path), fset.Position(lit.Pos()).Line)
			return true
		})
	})

	if checked == 0 {
		t.Fatal("no model.Guards literal ranking a run's steps was found: the guard swept nothing and proves nothing")
	}
}

// isGuardsLiteral reports whether the literal builds a model.Guards under any
// of the file's names for the package.
func isGuardsLiteral(lit *ast.CompositeLit, aliases map[string]bool) bool {
	sel, ok := lit.Type.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Guards" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && aliases[pkg.Name]
}

// ranksSteps reports whether the literal spells a term the verdict ranks the
// steps by.
func ranksSteps(fields map[string]ast.Expr) bool {
	for _, name := range stepTerms {
		if _, ok := fields[name]; ok {
			return true
		}
	}
	return false
}

// modelAliasesOf maps the file's local names for internal/model back to the
// package, so an import under another name is still caught.
func modelAliasesOf(file *ast.File) map[string]bool {
	aliases := map[string]bool{"model": true}
	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil || p != "github.com/a-holm/paceq/internal/model" || imp.Name == nil {
			continue
		}
		aliases[imp.Name.String()] = imp.Name.String() != "_"
	}
	return aliases
}

// forEachFoldSource parses every non test source outside internal/model and
// hands it to fn. The model owns the fold and spells the run-level term in its
// own doc examples and tables.
func forEachFoldSource(t *testing.T, root string, fset *token.FileSet, fn func(path string, file *ast.File)) {
	t.Helper()

	modelDir := filepath.Join(root, "internal", "model")
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
		fn(path, file)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}
