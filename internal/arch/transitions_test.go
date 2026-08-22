package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Issue #65 turns two written rules into two mechanical ones.

// stateUpdatePattern matches the writes that move a run or a step between
// states. Every one of them has to live in internal/store/transitions.go,
// beside the machine calls that decide them, so no caller can change what a
// run or a step is without walking through the transition layer.
var stateUpdatePattern = regexp.MustCompile(`UPDATE\s+(runs|steps)\b`)

// transitionsFile is the only file in the module allowed to match it.
const transitionsFile = "internal/store/transitions.go"

func TestStateUpdatesStayInTheTransitionLayer(t *testing.T) {
	root := repoRoot(t)

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel := rel(root, path)
		if strings.Contains(rel, "testdata") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		srcBytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(srcBytes)
		matched := stateUpdatePattern.FindAllStringIndex(src, -1)
		if len(matched) == 0 {
			return nil
		}
		if rel == transitionsFile {
			return nil
		}
		for _, at := range matched {
			line := 1 + strings.Count(src[:at[0]], "\n")
			t.Errorf("%s:%d: a run or step state update outside %s",
				rel, line, transitionsFile)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}
}

// TestEngineCannotRunAProcessInsideATransaction holds the cardinal rule by
// construction rather than by review. Two facts together make an exec inside
// a write transaction impossible to express:
//
//   - The engine imports neither database/sql nor anything that can open one,
//     and it contains no process launcher of its own beyond the runner call,
//     which takes no transaction.
//   - The store's exported surface accepts no function values: the engine can
//     never hand a closure carrying runner.Run INTO a transaction, because no
//     signature exists that would take one.
//
// The handle guard (TestStoreExportsNoDatabaseHandle) already keeps *sql.Tx
// out of exported signatures; this test adds the callback half of the same
// sentence and the engine's own import list.
func TestEngineCannotRunAProcessInsideATransaction(t *testing.T) {
	root := repoRoot(t)

	for _, imp := range directImports(t, filepath.Join(root, "internal", "engine")) {
		switch imp {
		case "os/exec", "database/sql", "modernc.org/sqlite", "syscall":
			t.Errorf("internal/engine must not import %s", imp)
		}
	}

	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, filepath.Join(root, "internal", "store"),
		func(info fs.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, 0)
	if err != nil {
		t.Fatalf("parse internal/store: %v", err)
	}
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 || !fn.Name.IsExported() {
					continue
				}
				if takesFunc(fn) {
					rel := rel(root, fset.Position(fn.Pos()).Filename)
					t.Errorf("%s: exported store method %s accepts a function value"+
						": a callback is a way to run code inside a transaction",
						rel, fn.Name.Name)
				}
			}
		}
	}
}

// takesFunc reports whether the declaration names any func type among its
// parameters or results. The root FuncType itself is deliberately not
// visited: every method has one.
func takesFunc(fn *ast.FuncDecl) bool {
	found := false
	visit := func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncType); ok {
			found = true
			return false
		}
		return true
	}
	if fn.Type.Params != nil {
		ast.Inspect(fn.Type.Params, visit)
	}
	if !found && fn.Type.Results != nil {
		ast.Inspect(fn.Type.Results, visit)
	}
	return found
}

func directImports(t *testing.T, dir string) []string {
	t.Helper()

	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	var seen []string
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				seen = append(seen, path)
			}
		}
	}
	return seen
}
