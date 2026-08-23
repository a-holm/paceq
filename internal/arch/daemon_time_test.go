package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDaemonLoopsTakeNoTimeDirectly is the acceptance criterion from the
// daemon milestone, stated as its own guard: no file in internal/daemon may
// read or wait on the real clock. Every timing decision in the loops comes
// through a clock.Clock, which is what makes the whole topology provable in a
// testing/synctest bubble and keeps the suite free of sleeps.
//
// TestTimeStaysInClock already bans package time everywhere; this one names
// the daemon rule where its readers will look for it, so a future loop cannot
// drift onto the wall clock without a commit that says so.
func TestDaemonLoopsTakeNoTimeDirectly(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "daemon")

	fset := token.NewFileSet()
	checked := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		checked++

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", path, err)
			return nil
		}
		names := timeImportNames(file)
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !forbiddenTimeFuncs[sel.Sel.Name] {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || !names[ident.Name] {
				return true
			}
			pos := fset.Position(sel.Pos())
			t.Errorf("%s:%d: %s.%s: forbidden in the daemon, take a clock.Clock and call its method instead",
				filepath.Base(path), pos.Line, ident.Name, sel.Sel.Name)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if checked < 5 {
		t.Fatalf("only %d production files walked in internal/daemon, the check would pass vacuously", checked)
	}
}
