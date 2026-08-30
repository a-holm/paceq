package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// invariantID matches the catalogue's numbered check names. "reason" is an ID
// too, but the word is ordinary prose and matching it would flag everything.
var invariantID = regexp.MustCompile(`^I[0-9]{1,3}$`)

// TestInvariantIDsStayInStore keeps the severity grade in one place. A package
// that spells an invariant ID is deciding something about that invariant
// outside the catalogue, which is how the boot gate and doctor came to
// disagree about which findings refuse a start: a hardcoded list of three IDs
// duplicated a grade store.Invariants already carries. Ask the catalogue
// through store.CriticalViolations instead.
func TestInvariantIDsStayInStore(t *testing.T) {
	root := repoRoot(t)
	storeDir := filepath.Join(root, "internal", "store")
	skipDir := selfDir(t)

	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch {
			case strings.HasPrefix(d.Name(), "."):
				return fs.SkipDir
			case d.Name() == "bin" || d.Name() == "dist":
				return fs.SkipDir
			case path == storeDir || path == skipDir:
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Errorf("parse %s: %v", rel(root, path), perr)
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, uerr := strconv.Unquote(lit.Value)
			if uerr != nil || !invariantID.MatchString(value) {
				return true
			}
			t.Errorf("%s:%d names invariant %q: forbidden, the catalogue in internal/store grades every check",
				rel(root, path), fset.Position(lit.Pos()).Line, value)
			return false
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}
