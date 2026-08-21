package arch_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// handleTypes are the database handles that must never appear in the exported
// surface of internal/store. A caller holding one of them can write outside a
// store method, which is the invariant the whole write model rests on.
//
// A callback parameter counts. `WithTx(ctx, func(*sql.Tx) error)` hands the
// caller a writable handle just as surely as returning one does.
var handleTypes = []string{"sql.DB", "sql.Tx", "sql.Conn", "sql.Stmt", "driver.Conn", "driver.Tx"}

// TestStoreExportsNoDatabaseHandle walks the exported declarations of
// internal/store and fails on any signature or field that mentions a database
// handle.
//
// Two rules from the write model cannot be checked mechanically and stay review
// rules, stated in internal/store/doc.go: no process, file or network I/O
// inside a write transaction, and RETURNING rows consumed before the
// transaction continues.
func TestStoreExportsNoDatabaseHandle(t *testing.T) {
	root := repoRoot(t)
	storeDir := filepath.Join(root, "internal", "store")

	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatalf("read %s: %v", storeDir, err)
	}

	fset := token.NewFileSet()
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++

		path := filepath.Join(storeDir, name)
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", rel(root, path), err)
		}
		for _, decl := range file.Decls {
			for _, found := range exportedHandleUses(fset, decl) {
				t.Errorf("%s: exported %s mentions %s: forbidden, no package outside internal/store "+
					"may hold a writable database handle", rel(root, path), found.what, found.handle)
			}
		}
	}
	if checked == 0 {
		t.Fatalf("no non-test Go files found in %s, the check would pass vacuously", rel(root, storeDir))
	}
}

type handleUse struct {
	what   string
	handle string
}

func exportedHandleUses(fset *token.FileSet, decl ast.Decl) []handleUse {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() || !exportedReceiver(d) {
			return nil
		}
		return uses(fset, "func "+d.Name.Name, d.Type)
	case *ast.GenDecl:
		var found []handleUse
		for _, spec := range d.Specs {
			found = append(found, specUses(fset, spec)...)
		}
		return found
	}
	return nil
}

func specUses(fset *token.FileSet, spec ast.Spec) []handleUse {
	switch s := spec.(type) {
	case *ast.TypeSpec:
		if !s.Name.IsExported() {
			return nil
		}
		if st, ok := s.Type.(*ast.StructType); ok {
			var found []handleUse
			for _, field := range st.Fields.List {
				if !anyExported(field.Names) {
					continue
				}
				found = append(found, uses(fset, "field "+s.Name.Name+"."+fieldName(field), field.Type)...)
			}
			return found
		}
		return uses(fset, "type "+s.Name.Name, s.Type)
	case *ast.ValueSpec:
		if !anyExported(s.Names) || s.Type == nil {
			return nil
		}
		return uses(fset, "value "+s.Names[0].Name, s.Type)
	}
	return nil
}

// exportedReceiver reports whether a method belongs to an exported type. A
// method on an unexported type is unreachable from outside the package however
// it is spelled.
func exportedReceiver(d *ast.FuncDecl) bool {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return true
	}
	name := d.Recv.List[0].Type
	if star, ok := name.(*ast.StarExpr); ok {
		name = star.X
	}
	ident, ok := name.(*ast.Ident)
	return ok && ident.IsExported()
}

func uses(fset *token.FileSet, what string, node ast.Node) []handleUse {
	var rendered strings.Builder
	if err := printer.Fprint(&rendered, fset, node); err != nil {
		return nil
	}
	text := rendered.String()

	var found []handleUse
	for _, handle := range handleTypes {
		if strings.Contains(text, handle) {
			found = append(found, handleUse{what: what, handle: handle})
		}
	}
	return found
}

func anyExported(names []*ast.Ident) bool {
	for _, n := range names {
		if n.IsExported() {
			return true
		}
	}
	return false
}

func fieldName(field *ast.Field) string {
	if len(field.Names) == 0 {
		return "<embedded>"
	}
	return field.Names[0].Name
}
