package store

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestDSNEscapesWhatEndsAURIFilename pins the rendered connection string. The
// driver hands a file: DSN to SQLite as a URI, so ?, # and % are read there
// rather than passed on, while a space and a non-ASCII byte must survive
// untouched: over-escaping renames a directory that works today.
func TestDSNEscapesWhatEndsAURIFilename(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{"plain", "/srv/paceq/state.db", "file:/srv/paceq/state.db?mode=ro"},
		{"query", "/srv/q?1/state.db", "file:/srv/q%3f1/state.db?mode=ro"},
		{"fragment", "/srv/Sak#42/state.db", "file:/srv/Sak%2342/state.db?mode=ro"},
		{"percent", "/srv/100%42/state.db", "file:/srv/100%2542/state.db?mode=ro"},
		{"space", "/srv/my dir/state.db", "file:/srv/my dir/state.db?mode=ro"},
		{"non ascii", "/srv/Ærlig/state.db", "file:/srv/Ærlig/state.db?mode=ro"},
		{"in memory", ":memory:", "file::memory:?mode=ro"},
		{"authority", "//srv/state.db", "file:%2f/srv/state.db?mode=ro"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dsn(tc.path, "mode=ro", nil)
			if err != nil {
				t.Fatalf("dsn(%q): %v", tc.path, err)
			}
			if got != tc.want {
				t.Errorf("dsn(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

// TestDSNRefusesAPathNoURICanCarry covers the other half of the answer. Every
// byte a filesystem accepts can be escaped, so nothing else needs refusing, but
// these two cannot be expressed at all: an empty filename opens a private
// temporary database, and a NUL ends the string the driver passes on.
func TestDSNRefusesAPathNoURICanCarry(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"empty", ""},
		{"nul", "/srv/pa\x00ceq/state.db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dsn(tc.path, "mode=ro", nil)
			if err == nil {
				t.Fatalf("dsn(%q) = %q, want an error", tc.path, got)
			}
		})
	}
}

// TestOpenUsesTheFileItWasGiven is the end-to-end proof. A state directory whose
// name carries a ? or a # used to end the URI early: SQLite created the
// truncated name, both opens succeeded, and every operation ran against a
// database nobody reads. A % is worse still, because SQLite decodes the two
// bytes after it and lands in a directory that does not exist.
func TestOpenUsesTheFileItWasGiven(t *testing.T) {
	for _, dir := range []string{"jobs?1", "Sak#42", "100%42", "my dir", "Ærlig"} {
		t.Run(dir, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, dir, DatabaseFileName)
			if err := os.MkdirAll(filepath.Dir(path), DirMode); err != nil {
				t.Fatalf("create %s: %v", filepath.Dir(path), err)
			}

			s, err := Open(context.Background(), path, Options{})
			if err != nil {
				t.Fatalf("Open(%q): %v", path, err)
			}
			err = s.withTx(context.Background(), func(tx *sql.Tx) error {
				if _, err := tx.Exec("CREATE TABLE mark (n INTEGER NOT NULL)"); err != nil {
					return err
				}
				_, err := tx.Exec("INSERT INTO mark (n) VALUES (1)")
				return err
			})
			if err != nil {
				t.Fatalf("write through %q: %v", path, err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			ro, err := OpenReadOnly(context.Background(), path, Options{})
			if err != nil {
				t.Fatalf("OpenReadOnly(%q): %v", path, err)
			}
			t.Cleanup(func() { _ = ro.Close() })

			var n int
			err = ro.withRead(context.Background(), func(ctx context.Context, r reader) error {
				return r.QueryRowContext(ctx, "SELECT n FROM mark").Scan(&n)
			})
			if err != nil {
				t.Fatalf("read back from %q: %v", path, err)
			}
			if n != 1 {
				t.Errorf("read back n = %d, want 1", n)
			}

			assertNoStrayDatabase(t, root, path)
		})
	}
}

// assertNoStrayDatabase fails when anything under root was created that nobody
// asked for. This is the assertion the unit tests could not make: a filename cut
// short at ? or # lands beside the intended database and is otherwise invisible,
// because the open succeeds and the empty result looks like a fresh install.
func assertNoStrayDatabase(t *testing.T, root, path string) {
	t.Helper()

	asked := map[string]bool{path: true, path + "-wal": true, path + "-shm": true}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !asked[p] {
			t.Errorf("%s exists but was never asked for: the DSN named a different database", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}
