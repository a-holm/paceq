package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Issue #10, AC-4: no automatic code path may take a run from failed to
// queued. The move exists as exactly one store method,
// ReopenTerminalRunByOperator, and its name is the law: only a caller that
// spells the operator's name out may call it. This test walks every call site
// in the module and refuses any that does not sit in the operator surfaces:
// the CLI, which speaks for the person typing, and the daemon's API handlers,
// which speak for the person's HTTP client. A scheduler loop, a reaper or a
// sensor that learned to spell it fails here.
//
// The allowlist is by file, not by package, so a stray call inside an
// otherwise innocent file in an allowed package still trips it.
var operatorReopenCallers = map[string]bool{
	"internal/store/reopen.go":    true, // the definition
	"internal/cli/runs_retry.go":  true, // runs retry <id>
	"internal/daemon/handlers.go": true, // POST /v1/runs/{id}/retry
}

func TestOnlyOperatorSurfacesReopenATerminalRun(t *testing.T) {
	root := repoRoot(t)
	calls := 0

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := rel(root, path)
		if strings.Contains(rel, "testdata") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", rel, err)
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "ReopenTerminalRunByOperator" {
				return true
			}
			calls++
			if !operatorReopenCallers[rel] {
				t.Errorf("%s:%d calls ReopenTerminalRunByOperator: only the operator surfaces (%s) may reopen a terminal run",
					rel, fset.Position(call.Pos()).Line,
					strings.Join(allowlistNames(), ", "))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}

	// The guard is not vacuous while the feature exists: at least one
	// operator surface has to reach the method, or the allowlist guards an
	// empty room.
	if calls == 0 {
		t.Fatal("no caller of ReopenTerminalRunByOperator was found: the operator path is missing")
	}
}

func allowlistNames() []string {
	out := make([]string, 0, len(operatorReopenCallers))
	for name := range operatorReopenCallers {
		out = append(out, name)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
