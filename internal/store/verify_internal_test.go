package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyPoolNamesTheMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open(driverName, "file:"+path+"?_pragma=journal_mode(DELETE)&_pragma=foreign_keys(OFF)")
	if err != nil {
		t.Fatalf("open raw pool: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = verifyPool(context.Background(), db, "writer", path, []pragmaSpec{
		{name: "journal_mode", want: "wal"},
		{name: "foreign_keys", want: "1"},
	})
	if err == nil {
		t.Fatal("verifyPool accepted a pool in journal_mode=DELETE with foreign_keys off, want an error")
	}
	for _, want := range []string{"writer", "journal_mode", "delete", "wal"} {
		if !strings.Contains(strings.ToLower(err.Error()), want) {
			t.Errorf("verifyPool error %q does not mention %q", err, want)
		}
	}
}

func TestVerifyPoolAcceptsAMatchingPool(t *testing.T) {
	s := openTestStore(t, Options{})

	if err := verifyPool(context.Background(), s.w, "writer", s.path, writerSpecs("NORMAL")); err != nil {
		t.Errorf("verifyPool on the writer pool: %v", err)
	}
	if err := verifyPool(context.Background(), s.r, "reader", s.path, readerSpecs("NORMAL")); err != nil {
		t.Errorf("verifyPool on the reader pool: %v", err)
	}
}

// TestVerifyFileRefusesAnotherDatabase is the backstop behind the DSN escaping.
// A connection that opened the wrong file answers every pragma from it just as
// readily, so the file itself has to be checked before anything is read.
func TestVerifyFileRefusesAnotherDatabase(t *testing.T) {
	root := t.TempDir()
	opened := filepath.Join(root, "opened.db")
	asked := filepath.Join(root, "asked.db")

	db, err := sql.Open(driverName, "file:"+opened)
	if err != nil {
		t.Fatalf("open raw pool: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("take connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "CREATE TABLE t (n INTEGER)"); err != nil {
		t.Fatalf("materialise %s: %v", opened, err)
	}
	if err := os.WriteFile(asked, nil, DatabaseMode); err != nil {
		t.Fatalf("create %s: %v", asked, err)
	}

	err = verifyFile(context.Background(), conn, "writer", asked)
	if err == nil {
		t.Fatal("verifyFile accepted a connection holding another database, want an error")
	}
	for _, want := range []string{"writer", asked, opened} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("verifyFile error %q does not mention %q", err, want)
		}
	}

	if err := verifyFile(context.Background(), conn, "writer", opened); err != nil {
		t.Errorf("verifyFile on the file the connection holds: %v", err)
	}
}
