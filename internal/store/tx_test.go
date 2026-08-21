package store_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// newStore opens a store on a real file and creates the one table the
// transaction tests write to. The schema itself lands in a later issue, so the
// test owns it.
func newStore(t *testing.T) *store.Store {
	t.Helper()

	s, err := store.Open(context.Background(), tempPath(t), store.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	err = s.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
		if err != nil {
			return err
		}
		_, err = tx.Exec("INSERT INTO counter (id, n) VALUES (1, 0)")
		return err
	})
	if err != nil {
		t.Fatalf("create fixture table: %v", err)
	}
	return s
}

func readCounter(t *testing.T, s *store.Store) int {
	t.Helper()

	var n int
	err := s.WithRead(context.Background(), func(ctx context.Context, r store.Reader) error {
		return r.QueryRowContext(ctx, "SELECT n FROM counter WHERE id = 1").Scan(&n)
	})
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return n
}

func TestWithTxCommitsWhenTheCallbackSucceeds(t *testing.T) {
	s := newStore(t)

	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE counter SET n = 7 WHERE id = 1")
		return err
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	if got := readCounter(t, s); got != 7 {
		t.Errorf("counter = %d, want 7", got)
	}
}

func TestWithTxRollsBackWhenTheCallbackFails(t *testing.T) {
	s := newStore(t)
	sentinel := errors.New("callback refused")

	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE counter SET n = 42 WHERE id = 1"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx error = %v, want %v", err, sentinel)
	}
	if got := readCounter(t, s); got != 0 {
		t.Errorf("counter = %d after a rolled back transaction, want 0", got)
	}
}

// TestWithReadCannotWrite proves query_only is enforced by the engine, not by
// convention: a write through the reader pool is refused even though the
// caller went out of its way to attempt one.
func TestWithReadCannotWrite(t *testing.T) {
	s := newStore(t)

	err := s.WithRead(context.Background(), func(ctx context.Context, r store.Reader) error {
		_, err := r.QueryContext(ctx, "INSERT INTO counter (id, n) VALUES (2, 1)")
		return err
	})
	if err == nil {
		t.Fatal("an INSERT through WithRead succeeded, want a readonly database error")
	}
	if !strings.Contains(err.Error(), "readonly") {
		t.Errorf("WithRead error = %q, want it to mention a readonly database", err)
	}
	if got := readCounter(t, s); got != 0 {
		t.Errorf("counter = %d, want the reader pool to have changed nothing", got)
	}
}
