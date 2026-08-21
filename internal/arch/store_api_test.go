package arch_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
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
// The package is read as a whole rather than file by file, because a
// package-local type name launders a handle past any per-file check:
// `type handle = *sql.DB` in one file and `func (s *Store) Writer() handle` in
// another, where the second file imports nothing at all.
//
// Three shapes stay out of reach of a syntax check. The module is covered
// against them elsewhere or not at all:
//
//   - A handle behind an any or an interface{}. A declaration says nothing about
//     what a dynamic value holds, and no checker of any kind rules that out.
//   - A type only a type checker can infer, such as an exported variable
//     initialised from a function call instead of a literal. A literal's type is
//     written out and is read.
//   - A package-local name that reaches a handle through a type declared in
//     another package. Naming a handle needs an import of database/sql, and
//     TestSQLStaysInStore forbids that import in every module package outside
//     internal/store, skipping this directory whole, testdata included. That
//     exemption is why this guard reads the fixture packages itself: this guard
//     is what keeps them honest. A generic type is read from its
//     instantiation, box[*sql.DB], for the same reason: binding a type
//     parameter to a handle means naming the handle somewhere.
//
// Two rules from the write model cannot be checked mechanically and stay review
// rules, stated in internal/store/doc.go: no process, file or network I/O
// inside a write transaction, and RETURNING rows consumed before the
// transaction continues.
func TestStoreExportsNoDatabaseHandle(t *testing.T) {
	root := repoRoot(t)
	storeDir := filepath.Join(root, "internal", "store")

	found, err := packageHandleUses(storeDir, func(p string) string { return rel(root, p) })
	if err != nil {
		t.Fatalf("check %s: %v", rel(root, storeDir), err)
	}
	for _, use := range found {
		t.Errorf("%s: forbidden, no package outside internal/store may hold a writable database handle", use)
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
			if strings.HasPrefix(d.Name(), ".") || d.Name() == "bin" || d.Name() == "dist" {
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

// TestGuardCatchesLaunderedHandles runs the guard against a fixture package that
// spells out every way a package-local type name can carry a database handle
// into an exported signature, alongside the shapes that legitimately hide one.
// The expected set is exact, so a shape that stops being caught and a shape that
// starts being flagged both fail here.
func TestGuardCatchesLaunderedHandles(t *testing.T) {
	dir := filepath.Join(selfDir(t), "testdata", "badhandle")

	found, err := packageHandleUses(dir, filepath.Base)
	if err != nil {
		t.Fatalf("check %s: %v", dir, err)
	}

	// The handles are declared in alias.go and every laundered use sits in
	// launder.go, which imports nothing: the guard has to read the package, not
	// the file, to connect the two. The declaration positions are part of the
	// assertion, so a message that points at the wrong line fails here too.
	want := []string{
		"alias.go: exported func Direct mentions database/sql.DB",
		"alias.go: exported value Direct2 mentions database/sql.DB",
		"alias.go: exported value Pair mentions database/sql.DB",
		"launder.go: exported field Carrier.H mentions aliasHandle, declared at alias.go:29, which resolves to database/sql.DB",
		"launder.go: exported func Callback mentions callbackHandle, declared at alias.go:38, which resolves to database/sql.Tx",
		"launder.go: exported func Chain mentions chainHandle, declared at launder.go:9, which resolves to database/sql.DB",
		"launder.go: exported func Deep mentions deepHandle, declared at alias.go:26, which resolves to database/sql.DB",
		"launder.go: exported func Embed mentions embedHandle, declared at alias.go:32, which resolves to database/sql.DB",
		"launder.go: exported func Field mentions fieldHandle, declared at alias.go:35, which resolves to database/sql.Conn",
		"launder.go: exported func Generic mentions genericHandle, declared at alias.go:52, which resolves to database/sql.DB",
		"launder.go: exported func Iface mentions ifaceHandle, declared at alias.go:44, which resolves to database/sql.Tx",
		"launder.go: exported func List mentions listHandle, declared at alias.go:41, which resolves to database/sql.Stmt",
		"launder.go: exported func Method mentions methodHandle, declared at alias.go:56, whose exported method Unwrap names database/sql.DB",
		"launder.go: exported func Plain mentions aliasHandle, declared at alias.go:29, which resolves to database/sql.DB",
		"launder.go: exported func Promoted mentions promotedHandle, declared at alias.go:64, which resolves to database/sql.DB",
		"launder.go: exported value Laundered mentions aliasHandle, declared at alias.go:29, which resolves to database/sql.DB",
	}
	assertFindings(t, want, found)
}

// TestGuardLeavesHiddenHandlesAlone is the other half of the fixture pair. Every
// declaration in it holds a database handle the way internal/store itself does,
// out of reach of any caller outside the package, and the guard must stay quiet
// about all of them. Without this half, a guard that flagged every mention of a
// handle anywhere would look perfect.
func TestGuardLeavesHiddenHandlesAlone(t *testing.T) {
	dir := filepath.Join(selfDir(t), "testdata", "goodhandle")

	found, err := packageHandleUses(dir, filepath.Base)
	if err != nil {
		t.Fatalf("check %s: %v", dir, err)
	}
	for _, use := range found {
		t.Errorf("%s: false positive, this shape keeps the handle inside the package", use)
	}
}

// TestHandleFixturesCompile builds both fixture packages. They live under
// testdata, so no other target ever compiles them, and a shape the compiler
// rejects is not a laundering shape: it is a fixture that proves nothing.
func TestHandleFixturesCompile(t *testing.T) {
	for _, name := range []string{"badhandle", "goodhandle"} {
		runGo(t, "build", "./internal/arch/testdata/"+name)
	}
}

func assertFindings(t *testing.T, want []string, found []finding) {
	t.Helper()

	got := make([]string, 0, len(found))
	for _, use := range found {
		got = append(got, use.String())
	}
	sort.Strings(got)
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)

	for _, missing := range difference(sorted, got) {
		t.Errorf("guard missed a laundering shape, wanted the failure:\n\t%s", missing)
	}
	for _, extra := range difference(got, sorted) {
		t.Errorf("guard reported a shape the fixture does not expect:\n\t%s", extra)
	}
}

func difference(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, s := range b {
		in[s] = true
	}
	var only []string
	for _, s := range a {
		if !in[s] {
			only = append(only, s)
		}
	}
	return only
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

// laundered is a package-local type name that puts a database handle within
// reach of a caller outside the package. Where it is declared is half the
// message: the signature that trips the check names the local type, and the
// handle it hides is somewhere else, usually in another file.
type laundered struct {
	name   string
	pos    string
	handle string
	// method names the exported method that carries the handle, empty when the
	// type declaration itself carries it.
	method string
}

// finding is one exported declaration that mentions a database handle.
type finding struct {
	file string
	what string
	// handle is the import path qualified handle, "database/sql.Tx".
	handle string
	// via is set when a package-local type name, not the handle itself, is what
	// the declaration spells.
	via laundered
}

func (f finding) String() string {
	switch {
	case f.via.name == "":
		return fmt.Sprintf("%s: exported %s mentions %s", f.file, f.what, f.handle)
	case f.via.method != "":
		return fmt.Sprintf("%s: exported %s mentions %s, declared at %s, whose exported method %s names %s",
			f.file, f.what, f.via.name, f.via.pos, f.via.method, f.handle)
	default:
		return fmt.Sprintf("%s: exported %s mentions %s, declared at %s, which resolves to %s",
			f.file, f.what, f.via.name, f.via.pos, f.handle)
	}
}

// parsedFile is one file of the package under check, kept with the path used in
// failure messages and the handle spellings its own import list allows.
type parsedFile struct {
	display string
	file    *ast.File
	imports map[string]string
}

// packageHandleUses parses every non-test Go file in dir as one package and
// reports each exported declaration that mentions a database handle, whether it
// names the handle itself or a package-local type that carries one. display maps
// an absolute file path to the path failures print.
func packageHandleUses(dir string, display func(string) string) ([]finding, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()
	var files []parsedFile
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		filePath := filepath.Join(dir, name)
		file, err := parser.ParseFile(fset, filePath, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", display(filePath), err)
		}
		files = append(files, parsedFile{display: display(filePath), file: file, imports: handleNames(file)})
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no non-test Go files found, the check would pass vacuously")
	}

	local := launderingTypes(fset, files, display)

	var found []finding
	for _, f := range files {
		for _, decl := range f.file.Decls {
			for _, use := range declHandleUses(decl, f.imports, local) {
				use.file = f.display
				found = append(found, use)
			}
		}
	}
	return found, nil
}

// launderingTypes resolves the package-local type names that carry a database
// handle. It runs over the whole package, because a declaration and the exported
// signature that uses it need not share a file, and to a fixpoint, because an
// alias may point at an alias declared further down or in another file.
//
// Resolution reads the right hand side of a declaration, never its name: a type
// called handle that resolves to nothing is not laundering anything, and one
// called ids that resolves to *sql.DB is. What counts as carrying a handle
// depends on the shape:
//
//   - An alias or a defined type whose right hand side names a handle anywhere:
//     `= *sql.DB`, `= []*sql.Stmt`, `= func(*sql.Tx) error`.
//   - A struct that embeds one, however spelled, because embedding promotes
//     every exported method of the handle into the outer type, or that has an
//     exported field of one.
//   - An interface with an exported method whose signature names one, or that
//     embeds an interface that does.
//   - An unexported type with an exported method whose signature names one. A
//     value of an unexported type is still a value the caller holds, and the
//     method set comes with it. An exported type needs no entry from its
//     methods: they are part of the exported surface and are read directly.
//
// The rule for a package-local type is reachability, not mention: an ordinary
// unexported field is how internal/store's own Store holds both pools, and
// flagging that would be flagging the design the guard exists to protect.
func launderingTypes(fset *token.FileSet, files []parsedFile, display func(string) string) map[string]laundered {
	declPos := map[string]string{}
	for _, f := range files {
		for _, decl := range f.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok {
					declPos[ts.Name.Name] = position(fset, display, ts.Name.Pos())
				}
			}
		}
	}

	local := map[string]laundered{}
	record := func(name, handle, method string) bool {
		if _, done := local[name]; done {
			return false
		}
		local[name] = laundered{name: name, pos: declPos[name], handle: handle, method: method}
		return true
	}

	for changed := true; changed; {
		changed = false
		for _, f := range files {
			for _, decl := range f.file.Decls {
				switch d := decl.(type) {
				case *ast.GenDecl:
					if d.Tok != token.TYPE {
						continue
					}
					for _, spec := range d.Specs {
						ts, ok := spec.(*ast.TypeSpec)
						if !ok {
							continue
						}
						if handle, ok := reachableHandle(ts.Type, f.imports, local); ok {
							changed = record(ts.Name.Name, handle, "") || changed
						}
					}
				case *ast.FuncDecl:
					name, ok := receiverName(d)
					if !ok || !d.Name.IsExported() || ast.IsExported(name) {
						continue
					}
					if handle, ok := firstHandle(d.Type, f.imports, local); ok {
						changed = record(name, handle, d.Name.Name) || changed
					}
				}
			}
		}
	}
	return local
}

// reachableHandle reports the handle a type declaration puts within reach of a
// caller outside the package, resolved through the file's import spellings and
// the package-local names known so far.
func reachableHandle(expr ast.Expr, imports map[string]string, local map[string]laundered) (string, bool) {
	switch t := expr.(type) {
	case *ast.StructType:
		return exposedFieldHandle(t.Fields, imports, local)
	case *ast.InterfaceType:
		return exposedFieldHandle(t.Methods, imports, local)
	}
	return firstHandle(expr, imports, local)
}

// exposedFieldHandle reports the first handle reachable through a field list. A
// field with no name is embedded, and embedding promotes what it embeds; a field
// with an unexported name is reachable only from inside the package.
func exposedFieldHandle(fields *ast.FieldList, imports map[string]string, local map[string]laundered) (string, bool) {
	if fields == nil {
		return "", false
	}
	for _, field := range fields.List {
		if len(field.Names) > 0 && !anyExported(field.Names) {
			continue
		}
		if handle, ok := firstHandle(field.Type, imports, local); ok {
			return handle, true
		}
	}
	return "", false
}

// firstHandle reports the first database handle named anywhere under node, at
// any depth, counting both import spellings and package-local names.
func firstHandle(node ast.Node, imports map[string]string, local map[string]laundered) (string, bool) {
	var handle string
	ast.Inspect(node, func(n ast.Node) bool {
		if handle != "" {
			return false
		}
		switch e := n.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := e.X.(*ast.Ident); ok {
				handle = imports[pkg.Name+"."+e.Sel.Name]
			}
			return false
		case *ast.Ident:
			if h, ok := imports[e.Name]; ok {
				handle = h
				return false
			}
			handle = local[e.Name].handle
		}
		return true
	})
	return handle, handle != ""
}

// receiverName is the name of the type a method is declared on, with the pointer
// and any type parameters stripped.
func receiverName(d *ast.FuncDecl) (string, bool) {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return "", false
	}
	return typeName(d.Recv.List[0].Type)
}

// typeName is the name a type expression carries, with the pointer, the
// parentheses, any type arguments and any package qualifier stripped.
func typeName(expr ast.Expr) (string, bool) {
	for {
		switch t := expr.(type) {
		case *ast.StarExpr:
			expr = t.X
		case *ast.ParenExpr:
			expr = t.X
		case *ast.IndexExpr:
			expr = t.X
		case *ast.IndexListExpr:
			expr = t.X
		case *ast.SelectorExpr:
			return t.Sel.Name, true
		case *ast.Ident:
			return t.Name, true
		default:
			return "", false
		}
	}
}

func declHandleUses(decl ast.Decl, imports map[string]string, local map[string]laundered) []finding {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() || !exportedReceiver(d) {
			return nil
		}
		return uses("func "+d.Name.Name, d.Type, imports, local)
	case *ast.GenDecl:
		var found []finding
		for _, spec := range d.Specs {
			found = append(found, specUses(spec, imports, local)...)
		}
		return found
	}
	return nil
}

func specUses(spec ast.Spec, imports map[string]string, local map[string]laundered) []finding {
	switch s := spec.(type) {
	case *ast.TypeSpec:
		if !s.Name.IsExported() {
			return nil
		}
		if st, ok := s.Type.(*ast.StructType); ok {
			var found []finding
			for _, field := range st.Fields.List {
				if len(field.Names) > 0 && !anyExported(field.Names) {
					continue
				}
				found = append(found, uses("field "+s.Name.Name+"."+fieldName(field), field.Type, imports, local)...)
			}
			return found
		}
		return uses("type "+s.Name.Name, s.Type, imports, local)
	case *ast.ValueSpec:
		if s.Type != nil {
			name, ok := exportedName(s.Names)
			if !ok {
				return nil
			}
			return uses("value "+name, s.Type, imports, local)
		}
		// The type is inferred. A function or composite literal still writes it
		// out, and only that type is read: the body of a literal is ordinary
		// code, and a handle inside it never leaves the package by being there.
		// One value belongs to one name, so each is reported under its own.
		var found []finding
		for i, value := range s.Values {
			if i >= len(s.Names) || !s.Names[i].IsExported() {
				continue
			}
			what := "value " + s.Names[i].Name
			switch v := value.(type) {
			case *ast.FuncLit:
				found = append(found, uses(what, v.Type, imports, local)...)
			case *ast.CompositeLit:
				if v.Type != nil {
					found = append(found, uses(what, v.Type, imports, local)...)
				}
			}
		}
		return found
	}
	return nil
}

// exportedReceiver reports whether a method belongs to an exported type. A
// method on an unexported type is reachable only through a value of that type,
// which is the laundering path launderingTypes resolves instead.
func exportedReceiver(d *ast.FuncDecl) bool {
	name, ok := receiverName(d)
	if !ok {
		return true
	}
	return ast.IsExported(name)
}

// uses walks a declaration's type and reports every handle it names, at any
// depth: a return value, a parameter, and a parameter of a callback parameter
// all count. A package-local type that carries a handle counts as naming it.
func uses(what string, node ast.Node, imports map[string]string, local map[string]laundered) []finding {
	var found []finding
	seen := map[string]bool{}
	record := func(f finding) {
		key := f.via.name + " " + f.handle
		if seen[key] {
			return
		}
		seen[key] = true
		found = append(found, f)
	}

	ast.Inspect(node, func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := e.X.(*ast.Ident); ok {
				if handle, ok := imports[pkg.Name+"."+e.Sel.Name]; ok {
					record(finding{what: what, handle: handle})
				}
			}
			return false
		case *ast.Ident:
			if handle, ok := imports[e.Name]; ok {
				record(finding{what: what, handle: handle})
				return true
			}
			if l, ok := local[e.Name]; ok {
				record(finding{what: what, handle: l.handle, via: l})
			}
		}
		return true
	})
	return found
}

func anyExported(names []*ast.Ident) bool {
	_, ok := exportedName(names)
	return ok
}

// exportedName is the first exported name in a declaration's name list, which is
// the name a failure has to print: `var a, B = ...` is reached from outside the
// package as B.
func exportedName(names []*ast.Ident) (string, bool) {
	for _, n := range names {
		if n.IsExported() {
			return n.Name, true
		}
	}
	return "", false
}

// fieldName is the name a caller reaches a field by. An embedded field is named
// after the type it embeds, which is how the compiler promotes it.
func fieldName(field *ast.Field) string {
	if len(field.Names) > 0 {
		return field.Names[0].Name
	}
	if name, ok := typeName(field.Type); ok {
		return name
	}
	return "<embedded>"
}

func position(fset *token.FileSet, display func(string) string, pos token.Pos) string {
	p := fset.Position(pos)
	return fmt.Sprintf("%s:%d", display(p.Filename), p.Line)
}
