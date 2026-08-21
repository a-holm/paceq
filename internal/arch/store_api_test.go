package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// handleTypes are the database handles that must never appear in the exported
// surface of internal/store, listed per import path. A caller holding one of
// them can write outside a store method, which is the invariant the whole write
// model rests on.
//
// A callback parameter counts. `WithTx(ctx, func(*sql.Tx) error)` hands the
// caller a writable handle just as surely as returning one does.
var handleTypes = map[string][]string{
	"database/sql":        {"DB", "Tx", "Conn", "Stmt"},
	"database/sql/driver": {"Conn", "Tx", "Stmt"},
}

// TestStoreExportsNoDatabaseHandle walks the exported declarations of
// internal/store and fails on any signature or field that mentions a database
// handle.
//
// Handle names are resolved through each file's own import list rather than
// matched as the text "sql.Tx", so an alias or a dot import names the same type
// and is caught the same way.
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

		filePath := filepath.Join(storeDir, name)
		file, err := parser.ParseFile(fset, filePath, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", rel(root, filePath), err)
		}
		handles := handleNames(file)
		for _, decl := range file.Decls {
			for _, found := range exportedHandleUses(decl, handles) {
				t.Errorf("%s: exported %s mentions %s: forbidden, no package outside internal/store "+
					"may hold a writable database handle", rel(root, filePath), found.what, found.handle)
			}
		}
	}
	if checked == 0 {
		t.Fatalf("no non-test Go files found in %s, the check would pass vacuously", rel(root, storeDir))
	}
}

// TestNoDotImportOfSQLPackages keeps the handle check honest across the whole
// module. A dot import erases the package qualifier from every type name, which
// is the shape a mechanical check reads worst, and it buys nothing anywhere in
// this codebase.
func TestNoDotImportOfSQLPackages(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	self := filepath.Join(selfDir(t), "store_api_test.go")

	checked := 0
	err := filepath.WalkDir(root, func(filePath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "bin" || d.Name() == "dist" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(filePath, ".go") || filePath == self {
			return nil
		}
		checked++

		file, err := parser.ParseFile(fset, filePath, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, filePath), err)
			return nil
		}
		for _, imp := range file.Imports {
			if imp.Name == nil || imp.Name.Name != "." {
				continue
			}
			importPath, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			if _, ok := handleTypes[importPath]; ok {
				t.Errorf("%s dot imports %q: forbidden, it hides the package qualifier the "+
					"exported handle check reads", rel(root, filePath), importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if checked == 0 {
		t.Fatal("no Go files walked, the check would pass vacuously")
	}
}

// handleNames maps the names a file can spell a database handle with onto the
// handle it names. A normal import gives "sql.Tx", an alias gives "db.Tx", a
// dot import gives a bare "Tx", and a blank import gives nothing.
func handleNames(file *ast.File) map[string]string {
	names := map[string]string{}
	for _, imp := range file.Imports {
		importPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		types, ok := handleTypes[importPath]
		if !ok {
			continue
		}

		local := path.Base(importPath)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		if local == "_" {
			continue
		}
		for _, typeName := range types {
			qualified := importPath + "." + typeName
			if local == "." {
				names[typeName] = qualified
				continue
			}
			names[local+"."+typeName] = qualified
		}
	}
	return names
}

type handleUse struct {
	what   string
	handle string
}

func exportedHandleUses(decl ast.Decl, handles map[string]string) []handleUse {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() || !exportedReceiver(d) {
			return nil
		}
		return uses("func "+d.Name.Name, d.Type, handles)
	case *ast.GenDecl:
		var found []handleUse
		for _, spec := range d.Specs {
			found = append(found, specUses(spec, handles)...)
		}
		return found
	}
	return nil
}

func specUses(spec ast.Spec, handles map[string]string) []handleUse {
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
				found = append(found, uses("field "+s.Name.Name+"."+fieldName(field), field.Type, handles)...)
			}
			return found
		}
		return uses("type "+s.Name.Name, s.Type, handles)
	case *ast.ValueSpec:
		if !anyExported(s.Names) || s.Type == nil {
			return nil
		}
		return uses("value "+s.Names[0].Name, s.Type, handles)
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

// uses walks a declaration's type and reports every handle it names, at any
// depth: a return value, a parameter, and a parameter of a callback parameter
// all count.
func uses(what string, node ast.Node, handles map[string]string) []handleUse {
	var found []handleUse
	seen := map[string]bool{}
	record := func(key string) {
		handle, ok := handles[key]
		if !ok || seen[handle] {
			return
		}
		seen[handle] = true
		found = append(found, handleUse{what: what, handle: handle})
	}

	ast.Inspect(node, func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := e.X.(*ast.Ident); ok {
				record(pkg.Name + "." + e.Sel.Name)
			}
			return false
		case *ast.Ident:
			record(e.Name)
		}
		return true
	})
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
