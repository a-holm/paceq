package store

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
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

// waitForFinishedTransaction blocks until database/sql has finished the
// rollback it starts from its own goroutine when the context ends. Without the
// wait a test races that goroutine and only sometimes reaches the state it is
// written to describe.
func waitForFinishedTransaction(t *testing.T, tx *sql.Tx) error {
	t.Helper()

	for range 10000 {
		_, err := tx.Exec("SELECT 1")
		if errors.Is(err, sql.ErrTxDone) {
			return err
		}
		if err != nil {
			t.Fatalf("statement inside the transaction whose context ended: %v", err)
		}
		runtime.Gosched()
	}
	t.Fatal("database/sql never finished the transaction whose context ended")
	return nil
}

// TestWithTxReportsTheContextError pins what a caller sees when its context
// ends inside a write transaction. database/sql rolls the transaction back from
// its own goroutine, so the next statement and the commit both meet a finished
// transaction and report sql.ErrTxDone. The cancellation is what happened, and
// a caller deciding whether to retry has to be able to see it.
func TestWithTxReportsTheContextError(t *testing.T) {
	contexts := []struct {
		name string
		want error
		open func(context.Context) (context.Context, context.CancelFunc)
		end  func(context.CancelFunc)
	}{
		{
			name: "cancelled",
			want: context.Canceled,
			open: context.WithCancel,
			end:  func(cancel context.CancelFunc) { cancel() },
		},
		{
			name: "deadline exceeded",
			want: context.DeadlineExceeded,
			open: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithTimeout(parent, time.Second)
			},
			end: func(context.CancelFunc) {},
		},
	}
	points := []struct {
		name            string
		fromTheCallback bool
	}{
		{name: "the callback meets it", fromTheCallback: true},
		{name: "the commit meets it", fromTheCallback: false},
	}

	for _, ctxCase := range contexts {
		for _, point := range points {
			t.Run(ctxCase.name+"/"+point.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					s := newStore(t)
					ctx, cancel := ctxCase.open(context.Background())
					defer cancel()

					err := s.withTx(ctx, func(tx *sql.Tx) error {
						if _, err := tx.Exec("UPDATE counter SET n = 3 WHERE id = 1"); err != nil {
							return err
						}
						ctxCase.end(cancel)
						<-ctx.Done()
						finished := waitForFinishedTransaction(t, tx)
						if point.fromTheCallback {
							return finished
						}
						return nil
					})

					if !errors.Is(err, ctxCase.want) {
						t.Errorf("withTx error = %v, want it to report %v", err, ctxCase.want)
					}
					if !errors.Is(err, sql.ErrTxDone) {
						t.Errorf("withTx error = %v, want %v to stay inspectable", err, sql.ErrTxDone)
					}
					if got := readCounter(t, s); got != 0 {
						t.Errorf("counter = %d, want the cancelled write to have rolled back", got)
					}
				})
			})
		}
	}
}

// TestWithTxLeavesAFinishedTransactionAlone is the other half of the rule. A
// transaction that is finished for its own reasons, with a live context, keeps
// reporting exactly that. Only a cancellation gets the context error in front.
func TestWithTxLeavesAFinishedTransactionAlone(t *testing.T) {
	s := newStore(t)

	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		if err := tx.Rollback(); err != nil {
			return err
		}
		_, err := tx.Exec("UPDATE counter SET n = 5 WHERE id = 1")
		return err
	})

	if err != sql.ErrTxDone {
		t.Fatalf("withTx error = %v, want exactly %v", err, sql.ErrTxDone)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("withTx error = %v, want no context error while the context is live", err)
	}
}

// TestWithReadReportsTheContextError records why the read path needs no help
// with this. withRead opens no explicit transaction, so there is nothing for
// database/sql to finish behind the caller's back and the context error arrives
// as itself.
func TestWithReadReportsTheContextError(t *testing.T) {
	s := newStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		var n int
		return r.QueryRowContext(ctx, "SELECT n FROM counter WHERE id = 1").Scan(&n)
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("withRead error = %v, want %v", err, context.Canceled)
	}
}
