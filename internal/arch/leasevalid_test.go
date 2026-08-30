package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Issue #202 collapsed four rules about one fact into one. A lease admits a
// write when the run row names this caller at this caller's fencing epoch, and
// store.leaseHeldBy is the only place that says so. The two guards here hold
// that: nothing else may compute the fact, and the fact may not grow a term.
// Both failures are silent at runtime - a second rule refuses verdicts the
// first accepts, and nothing revisits the run afterwards.

// storeFuncName is the one sanctioned source of model.Guards.LeaseValid.
const storeFuncName = "leaseHeldBy"

// TestGuardsLeaseValidComesFromTheOneDefinition walks the store's sources and
// holds every assignment to Guards.LeaseValid to two forms: a call to
// leaseHeldBy, which is a holder's write, or the literal false, which is
// recovery acting on a lease nobody holds. An expression anywhere else is a
// second definition of the fence.
func TestGuardsLeaseValidComesFromTheOneDefinition(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	seen := 0

	forEachStoreFile(t, root, fset, func(path string, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			for _, value := range leaseValidValues(n) {
				seen++
				if sanctionedLeaseValidSource(value) {
					continue
				}
				t.Errorf("%s:%d: Guards.LeaseValid is set from %s; the only sources are %s(run, ref) and the literal false",
					rel(root, path), fset.Position(value.Pos()).Line,
					types.ExprString(value), storeFuncName)
			}
			return true
		})
	})

	if seen == 0 {
		t.Error("the guard found no Guards.LeaseValid at all: it is scanning the wrong tree")
	}
}

// TestLeaseValidityNeverReadsTheDeadline pins the decision itself. The lease
// deadline says when the reaper may take a run, not who owns it, so the one
// definition reads owner and epoch and nothing else. An expiry term here
// refuses a rightful owner's verdict for work that already finished, and the
// reaper then charges the run a crash and hands it to a second executor.
func TestLeaseValidityNeverReadsTheDeadline(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	found := 0

	forEachStoreFile(t, root, fset, func(path string, file *ast.File) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != storeFuncName || fn.Recv != nil {
				continue
			}
			found++
			ast.Inspect(fn, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "LeaseExpiresAt" {
					t.Errorf("%s:%d: %s reads the lease deadline; the deadline is the reaper's trigger, not a term of ownership",
						rel(root, path), fset.Position(sel.Pos()).Line, storeFuncName)
				}
				return true
			})
		}
	})

	if found != 1 {
		t.Errorf("the store declares %s %d times, want exactly 1", storeFuncName, found)
	}
}

// forEachStoreFile parses every non test source of internal/store and hands it
// to fn.
func forEachStoreFile(t *testing.T, root string, fset *token.FileSet, fn func(path string, file *ast.File)) {
	t.Helper()

	dir := filepath.Join(root, "internal", "store")
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
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
		t.Fatalf("walk internal/store: %v", err)
	}
}

// leaseValidValues returns what one node assigns to Guards.LeaseValid, either
// as a field in a composite literal or as a plain assignment to the field.
func leaseValidValues(n ast.Node) []ast.Expr {
	switch v := n.(type) {
	case *ast.KeyValueExpr:
		if key, ok := v.Key.(*ast.Ident); ok && key.Name == "LeaseValid" {
			return []ast.Expr{v.Value}
		}
	case *ast.AssignStmt:
		var out []ast.Expr
		for i, lhs := range v.Lhs {
			sel, ok := lhs.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "LeaseValid" || i >= len(v.Rhs) {
				continue
			}
			out = append(out, v.Rhs[i])
		}
		return out
	}
	return nil
}

// sanctionedLeaseValidSource reports whether the expression is one of the two
// forms the fence allows.
func sanctionedLeaseValidSource(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name == "false"
	case *ast.CallExpr:
		name, ok := v.Fun.(*ast.Ident)
		return ok && name.Name == storeFuncName
	}
	return false
}
