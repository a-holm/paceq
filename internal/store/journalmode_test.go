package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeDeleteModeDatabase builds a database the way a foreign tool would: no
// WAL, one table, then closed. The driver is already registered by the import
// of internal/store.
func writeDeleteModeDatabase(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite", testDSN(t, path, "_pragma=journal_mode(DELETE)"))
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

	s, err := Open(context.Background(), path, Options{})
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

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("Open against an empty file: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestOpenRejectsAnUnsupportedSynchronousValue(t *testing.T) {
	s, err := Open(context.Background(), tempPath(t), Options{Synchronous: "off"})
	if err == nil {
		_ = s.Close()
		t.Fatal("Open accepted synchronous=off, want an error")
	}
	if got := err.Error(); got == "" {
		t.Error("Open returned an empty error message")
	}
}

// TestOpenRefusesAnInMemoryDatabase is what makes testutil.TempStore's real
// file mandatory rather than conventional. ":memory:" gives the writer and the
// reader two unrelated databases and no WAL, and the startup verification
// refuses it.
func TestOpenRefusesAnInMemoryDatabase(t *testing.T) {
	for _, path := range []string{":memory:", ":memory:?cache=shared"} {
		t.Run(path, func(t *testing.T) {
			s, err := Open(context.Background(), path, Options{})
			if err == nil {
				_ = s.Close()
				t.Fatalf("Open(%q) succeeded, want a refusal", path)
			}
			t.Logf("Open(%q) = %v", path, err)
		})
	}
}

// TestReturningRowsMustBeConsumed pins the documented consequence of a
// single-connection writer pool: an unread *sql.Rows from RETURNING holds the
// only write connection, and the next statement on that transaction deadlocks
// against the process itself rather than failing.
func TestReturningRowsMustBeConsumed(t *testing.T) {
	s := newStore(t)

	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.Query("UPDATE counter SET n = n + 1 WHERE id = 1 RETURNING n")
		if err != nil {
			return err
		}
		var n int
		for rows.Next() {
			if err := rows.Scan(&n); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if n != 1 {
			t.Errorf("RETURNING gave n = %d, want 1", n)
		}
		_, err = tx.Exec("UPDATE counter SET n = n + 1 WHERE id = 1")
		return err
	})
	if err != nil {
		t.Fatalf("withTx: %v", err)
	}
	if got := readCounter(t, s); got != 2 {
		t.Errorf("counter = %d, want 2", got)
	}
}

// TestUnconsumedReturningIsNotADeadlockToday records what the driver actually
// does when RETURNING rows are left open and the transaction runs another
// statement. It does not deadlock: the result set is materialised before Query
// returns. The discipline stays a rule anyway, because nothing in database/sql
// or in the driver's contract promises that, and the writer pool has exactly
// one connection to lose.
func TestUnconsumedReturningIsNotADeadlockToday(t *testing.T) {
	s := newStore(t)

	done := make(chan error, 1)
	go func() {
		done <- s.withTx(context.Background(), func(tx *sql.Tx) error {
			rows, err := tx.Query("UPDATE counter SET n = n + 1 WHERE id = 1 RETURNING n")
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			_, err = tx.Exec("UPDATE counter SET n = n + 1 WHERE id = 1")
			return err
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("withTx with an unconsumed RETURNING result: %v", err)
		}
		if got := readCounter(t, s); got != 2 {
			t.Errorf("counter = %d, want 2", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an unconsumed RETURNING result deadlocked the single write connection")
	}
}
