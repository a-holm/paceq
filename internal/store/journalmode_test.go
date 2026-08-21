package store_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// writeDeleteModeDatabase builds a database the way a foreign tool would: no
// WAL, one table, then closed. The driver is already registered by the import
// of internal/store.
func writeDeleteModeDatabase(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(DELETE)")
	if err != nil {
		t.Fatalf("open raw database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE foreign_table (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if mode != "delete" {
		t.Fatalf("fixture journal_mode = %q, want %q", mode, "delete")
	}
}

// TestOpenAgainstAFileAlreadyInJournalModeDelete pins the documented behaviour
// for a database created by something other than paceq: Open converts it to WAL
// and the startup verification then reads WAL back. It does not warn and carry
// on in DELETE mode.
func TestOpenAgainstAFileAlreadyInJournalModeDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	writeDeleteModeDatabase(t, path)

	s, err := store.Open(context.Background(), path, store.Options{})
	if err != nil {
		t.Fatalf("Open against a DELETE-mode file: %v", err)
	}
	defer func() { _ = s.Close() }()

	// A WAL database has a -wal sidecar while a connection is open.
	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Errorf("no WAL sidecar next to %q after Open: %v", path, err)
	}
}

// TestOpenAgainstAnEmptyUnmigratedFile covers the M0 contract: the schema and
// the migration engine land in later issues, so Open must not require a table.
func TestOpenAgainstAnEmptyUnmigratedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create empty file: %v", err)
	}

	s, err := store.Open(context.Background(), path, store.Options{})
	if err != nil {
		t.Fatalf("Open against an empty file: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestOpenRejectsAnUnsupportedSynchronousValue(t *testing.T) {
	s, err := store.Open(context.Background(), tempPath(t), store.Options{Synchronous: "off"})
	if err == nil {
		_ = s.Close()
		t.Fatal("Open accepted synchronous=off, want an error")
	}
	if got := err.Error(); got == "" {
		t.Error("Open returned an empty error message")
	}
}
