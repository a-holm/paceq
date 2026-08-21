package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// newStore opens a store on a real file and creates the one table the
// transaction tests write to. The schema itself lands in a later issue, so the
// test owns it.
func newStore(t *testing.T) *Store {
	t.Helper()

	s, err := Open(context.Background(), tempPath(t), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	err = s.withTx(context.Background(), func(tx *sql.Tx) error {
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

func readCounter(t *testing.T, s *Store) int {
	t.Helper()

	var n int
	err := s.withRead(context.Background(), func(ctx context.Context, r reader) error {
		return r.QueryRowContext(ctx, "SELECT n FROM counter WHERE id = 1").Scan(&n)
	})
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return n
}

func TestWithTxCommitsWhenTheCallbackSucceeds(t *testing.T) {
	s := newStore(t)

	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE counter SET n = 7 WHERE id = 1")
		return err
	})
	if err != nil {
		t.Fatalf("withTx: %v", err)
	}
	if got := readCounter(t, s); got != 7 {
		t.Errorf("counter = %d, want 7", got)
	}
}

func TestWithTxRollsBackWhenTheCallbackFails(t *testing.T) {
	s := newStore(t)
	sentinel := errors.New("callback refused")

	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE counter SET n = 42 WHERE id = 1"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("withTx error = %v, want %v", err, sentinel)
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

	err := s.withRead(context.Background(), func(ctx context.Context, r reader) error {
		_, err := r.QueryContext(ctx, "INSERT INTO counter (id, n) VALUES (2, 1)")
		return err
	})
	if err == nil {
		t.Fatal("an INSERT through withRead succeeded, want a readonly database error")
	}
	if !strings.Contains(err.Error(), "readonly") {
		t.Errorf("withRead error = %q, want it to mention a readonly database", err)
	}
	if got := readCounter(t, s); got != 0 {
		t.Errorf("counter = %d, want the reader pool to have changed nothing", got)
	}
}

// TestWithReadAppliesADefaultDeadline covers the WAL hygiene rule: no read may
// hold a snapshot open indefinitely, whatever context the caller passes.
func TestWithReadAppliesADefaultDeadline(t *testing.T) {
	s := newStore(t)

	var deadline time.Time
	var ok bool
	err := s.withRead(context.Background(), func(ctx context.Context, _ reader) error {
		deadline, ok = ctx.Deadline()
		return nil
	})
	if err != nil {
		t.Fatalf("withRead: %v", err)
	}
	if !ok {
		t.Fatal("withRead handed the callback a context with no deadline")
	}
	if left := time.Until(deadline); left <= 0 || left > readDeadline {
		t.Errorf("withRead deadline is %v away, want (0, %v]", left, readDeadline)
	}
}

// TestWithReadShortensButNeverExtendsTheCallerDeadline keeps the cap from
// overriding a caller that wants a tighter one.
func TestWithReadShortensButNeverExtendsTheCallerDeadline(t *testing.T) {
	s := newStore(t)

	caller, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var left time.Duration
	err := s.withRead(caller, func(ctx context.Context, _ reader) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("withRead dropped the caller deadline")
		}
		left = time.Until(deadline)
		return nil
	})
	if err != nil {
		t.Fatalf("withRead: %v", err)
	}
	if left > 50*time.Millisecond {
		t.Errorf("withRead deadline is %v away, want no more than the caller's 50ms", left)
	}
}
