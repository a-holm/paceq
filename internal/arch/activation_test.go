package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Issue #182 makes the sensor wiring mechanical. internal/daemon built its
// sensor runtime with Source and Sink both nil, so the loop woke on every tick
// interval, asked nobody what was due, and slept. Nothing failed: the evaluator
// had its own tests, the store had its own tests, the runtime had its own
// tests, and no test anywhere looked at the seam between them. It stayed that
// way for a milestone.
//
// This guard reads the construction site rather than the behaviour, so an
// inert loop is a red build on the line that made it inert instead of a silent
// daemon. test/activation is the other half: it proves the wired runtime
// really evaluates an applied sensor.
//
// The check is the nil literal, which is how the seam was left open. A field
// filled from a variable that happens to be a nil interface is past what an
// AST can see, and that is what the end-to-end proof is for.

// sensorPkg declares the Source and Sink seams the daemon must satisfy.
const sensorPkg = modulePath + "/internal/sensor"

// runtimeSeams are the fields of sensor.RuntimeConfig that carry the database.
// A runtime missing either one runs, evaluates nothing, and says nothing.
var runtimeSeams = []string{"Source", "Sink"}

func TestTheDaemonWiresTheSensorRuntime(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "daemon")

	fset := token.NewFileSet()
	built := 0
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

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", path, err)
			return nil
		}
		names := importNamesOf(file, sensorPkg)
		if len(names) == 0 {
			return nil
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !callsSelector(call, names, "NewRuntime") {
				return true
			}
			built++
			where := fset.Position(call.Pos())
			if len(call.Args) != 2 {
				t.Errorf("%s:%d: sensor.NewRuntime takes an evaluator and a config; this call passes %d arguments",
					where.Filename, where.Line, len(call.Args))
				return true
			}
			cfg, ok := call.Args[1].(*ast.CompositeLit)
			if !ok {
				t.Errorf("%s:%d: the runtime config is not written at the call, so the seams cannot be read here",
					where.Filename, where.Line)
				return true
			}
			fields := keyedFields(cfg)
			for _, seam := range runtimeSeams {
				value, named := fields[seam]
				if !named {
					t.Errorf("%s:%d: the sensor runtime names no %s, so the loop wakes and finds nothing",
						where.Filename, where.Line, seam)
					continue
				}
				if isNilLiteral(value) {
					pos := fset.Position(value.Pos())
					t.Errorf("%s:%d: the sensor runtime's %s is nil, which is an inert loop (#182)",
						pos.Filename, pos.Line, seam)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/daemon: %v", err)
	}
	if built == 0 {
		t.Fatal("internal/daemon builds no sensor runtime: the construction site moved and this guard now proves nothing")
	}
}

// importNamesOf reports the local names one import path has in a file, so an
// alias cannot hide a call.
func importNamesOf(file *ast.File, want string) map[string]bool {
	names := map[string]bool{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != want {
			continue
		}
		if imp.Name == nil {
			names[path[strings.LastIndex(path, "/")+1:]] = true
			continue
		}
		if imp.Name.Name != "_" && imp.Name.Name != "." {
			names[imp.Name.Name] = true
		}
	}
	return names
}

// callsSelector reports whether the call is pkg.Name for one of the given
// package names.
func callsSelector(call *ast.CallExpr, pkgNames map[string]bool, name string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && pkgNames[ident.Name] && ident.Obj == nil
}

// keyedFields reads a composite literal's keyed elements by field name.
func keyedFields(lit *ast.CompositeLit) map[string]ast.Expr {
	out := make(map[string]ast.Expr, len(lit.Elts))
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok {
			out[key.Name] = kv.Value
		}
	}
	return out
}

// isNilLiteral reports whether the expression is the bare nil, through any
// number of parentheses.
func isNilLiteral(expr ast.Expr) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "nil"
}
