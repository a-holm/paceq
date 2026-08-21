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

// sqlPackages are the import paths only internal/store may use. This is the guard
// that actually holds: no package can execute SQL without one of them.
var sqlPackages = map[string]bool{
	"database/sql":        true,
	"database/sql/driver": true,
}

// sqlPrefixes mark a string literal as a query. Matching is case sensitive on
// purpose: uppercase keywords are the project's SQL convention, and matching
// case insensitively would flag ordinary prose such as "with %d retries".
// Statement keywords carry a trailing space so "updated" cannot match. Runs of
// whitespace in the literal are collapsed first, so "CREATE  TABLE" still matches.
var sqlPrefixes = []string{
	"SELECT ",
	"INSERT ",
	"UPDATE ",
	"DELETE ",
	"REPLACE ",
	"PRAGMA ",
	"ATTACH ",
	"WITH ",
	"BEGIN ",
	"CREATE TABLE",
	"CREATE INDEX",
	"ALTER TABLE",
	"DROP TABLE",
}

func TestSQLStaysInStore(t *testing.T) {
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
			case d.Name() == ".git" || d.Name() == "bin" || d.Name() == "dist":
				return fs.SkipDir
			case path == storeDir || path == skipDir:
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}

		for _, imp := range file.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			if sqlPackages[p] {
				t.Errorf("%s imports %q: forbidden, only internal/store may import %q",
					rel(root, path), p, p)
			}
		}

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			normalised := strings.Join(strings.Fields(value), " ")
			for _, prefix := range sqlPrefixes {
				if strings.HasPrefix(normalised, prefix) {
					t.Errorf("%s holds an SQL literal starting with %q: forbidden, all SQL lives in internal/store",
						rel(root, fset.Position(lit.Pos()).Filename), strings.TrimSpace(prefix))
					return false
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

func rel(root, path string) string {
	r, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return r
}
