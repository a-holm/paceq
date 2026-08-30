package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

// codedError stands in for a driver error in tests that need a specific result
// code without arranging the lock contention that produces it.
type codedError struct{ code int }

func (e codedError) Error() string { return fmt.Sprintf("stub sqlite error (%d)", e.code) }
func (e codedError) Code() int     { return e.code }

// busySnapshotStub is a stand-in driver error carrying the SQLITE_BUSY_SNAPSHOT
// result code. The number is written out rather than read from
// busySnapshotCode: a test that takes its expectation from the code under test
// cannot catch that code being wrong. TestIsBusySnapshotOnARealDriverError is
// what anchors 517 against the driver itself.
func busySnapshotStub() codedError { return codedError{code: 517} }

// realBusySnapshot reproduces SQLITE_BUSY_SNAPSHOT the only way it can happen:
// a DEFERRED transaction reads, another connection commits, and the first one
// then tries to write. This anchors the result code against the driver instead
// of against a constant copied from a header.
func realBusySnapshot(t *testing.T) error {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	setup, err := sql.Open(driverName, testDSN(t, path, "_pragma=journal_mode(WAL)"))
	if err != nil {
		t.Fatalf("open setup connection: %v", err)
	}
	defer func() { _ = setup.Close() }()
	if _, err := setup.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := setup.Exec("INSERT INTO t (id, n) VALUES (1, 0)"); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	deferred, err := sql.Open(driverName, testDSN(t, path, "_txlock=deferred&_pragma=busy_timeout(200)"))
	if err != nil {
		t.Fatalf("open deferred connection: %v", err)
	}
	defer func() { _ = deferred.Close() }()
	deferred.SetMaxOpenConns(1)

	tx, err := deferred.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin deferred transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var n int
	if err := tx.QueryRow("SELECT n FROM t WHERE id = 1").Scan(&n); err != nil {
		t.Fatalf("read inside the deferred transaction: %v", err)
	}
	if _, err := setup.Exec("UPDATE t SET n = 99 WHERE id = 1"); err != nil {
		t.Fatalf("competing commit: %v", err)
	}

	_, err = tx.Exec("UPDATE t SET n = 1 WHERE id = 1")
	if err == nil {
		t.Fatal("the lock upgrade succeeded, expected SQLITE_BUSY_SNAPSHOT")
	}
	return err
}

func TestIsBusySnapshotOnARealDriverError(t *testing.T) {
	err := realBusySnapshot(t)

	var coded interface{ Code() int }
	if !errors.As(err, &coded) {
		t.Fatalf("driver error %v does not expose a result code", err)
	}
	if coded.Code() != busySnapshotCode {
		t.Errorf("driver reported result code %d, the retry policy is written for %d",
			coded.Code(), busySnapshotCode)
	}
	if !isBusySnapshot(err) {
		t.Errorf("isBusySnapshot(%v) = false, want true", err)
	}
}

func TestIsBusySnapshotRejectsEverythingElse(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "nil", err: nil},
		{name: "plain error", err: errors.New("boom")},
		{name: "plain busy", err: codedError{code: 5}},
		{name: "busy recovery", err: codedError{code: 261}},
		{name: "constraint", err: codedError{code: 19}},
		{name: "wrapped plain busy", err: fmt.Errorf("commit: %w", codedError{code: 5})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if isBusySnapshot(tc.err) {
				t.Errorf("isBusySnapshot(%v) = true, want false", tc.err)
			}
		})
	}
}

func TestIsBusySnapshotSeesThroughWrapping(t *testing.T) {
	err := fmt.Errorf("commit write transaction: %w", busySnapshotStub())
	if !isBusySnapshot(err) {
		t.Errorf("isBusySnapshot(%v) = false, want true", err)
	}
}

func TestWithTxRetriesBusySnapshotUpToThreeAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := openTestStore(t, Options{})

		attempts := 0
		start := time.Now()

		// The overrun error is deliberately not a busy snapshot, so a retry
		// loop that lost its attempt bound stops here instead of spinning
		// until the package test timeout kills it with a goroutine dump.
		var overrun error
		err := s.withTx(t.Context(), func(*sql.Tx) error {
			attempts++
			if attempts > maxWriteAttempts {
				overrun = fmt.Errorf("withTx ran the callback %d times, want at most %d",
					attempts, maxWriteAttempts)
				return overrun
			}
			return busySnapshotStub()
		})
		if overrun != nil {
			t.Fatal(overrun)
		}

		if !isBusySnapshot(err) {
			t.Errorf("withTx error = %v, want the busy snapshot error to bubble out after the last attempt", err)
		}
		if attempts != maxWriteAttempts {
			t.Errorf("withTx ran the callback %d times, want %d", attempts, maxWriteAttempts)
		}
		if waited := time.Since(start); waited == 0 {
			t.Error("withTx retried without any backoff")
		}
	})
}

func TestWithTxDoesNotRetryOtherErrors(t *testing.T) {
	s := openTestStore(t, Options{})

	sentinel := errors.New("not retryable")
	attempts := 0
	err := s.withTx(context.Background(), func(*sql.Tx) error {
		attempts++
		return sentinel
	})

	if !errors.Is(err, sentinel) {
		t.Errorf("withTx error = %v, want %v", err, sentinel)
	}
	if attempts != 1 {
		t.Errorf("withTx ran the callback %d times, want 1", attempts)
	}
}

func TestWithTxStopsRetryingWhenTheContextIsCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := openTestStore(t, Options{})

		ctx, cancel := context.WithCancel(t.Context())
		attempts := 0
		err := s.withTx(ctx, func(*sql.Tx) error {
			attempts++
			cancel()
			return busySnapshotStub()
		})

		if !errors.Is(err, context.Canceled) {
			t.Errorf("withTx error = %v, want %v", err, context.Canceled)
		}
		if !isBusySnapshot(err) {
			t.Errorf("withTx error = %v, want the error it gave up on to stay inspectable", err)
		}
		if attempts != 1 {
			t.Errorf("withTx ran the callback %d times, want 1 before the context was cancelled", attempts)
		}
	})
}

// TestWithTxLeavesNoOpenTransaction proves the cancelled transaction released
// the single write connection: the next write succeeds instead of blocking.
func TestWithTxLeavesNoOpenTransaction(t *testing.T) {
	s := openTestStore(t, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.withTx(ctx, func(*sql.Tx) error {
		t.Error("the callback ran with an already cancelled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("withTx error = %v, want %v", err, context.Canceled)
	}

	done := make(chan error, 1)
	go func() {
		done <- s.withTx(context.Background(), func(tx *sql.Tx) error {
			_, err := tx.Exec("CREATE TABLE after_cancel (id INTEGER PRIMARY KEY)")
			return err
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after a cancelled transaction: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write after a cancelled transaction blocked: the write connection was never released")
	}
}

// TestWithTxCommitsAfterARetry pins the point of the retry policy. The other
// retry tests only prove withTx gives up correctly; this one proves a
// transaction that lost a snapshot race runs again and its write lands.
func TestWithTxCommitsAfterARetry(t *testing.T) {
	s := newStore(t)

	attempts := 0
	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		attempts++
		if attempts < maxWriteAttempts {
			return busySnapshotStub()
		}
		_, err := tx.Exec("UPDATE counter SET n = 7 WHERE id = 1")
		return err
	})
	if err != nil {
		t.Fatalf("withTx: %v", err)
	}
	if attempts != maxWriteAttempts {
		t.Errorf("withTx ran the callback %d times, want %d", attempts, maxWriteAttempts)
	}
	// Read through the reader pool, so the value has to have been committed to
	// the file rather than left on the writer's own connection.
	if got := readCounter(t, s); got != 7 {
		t.Errorf("counter = %d, want 7: the retried transaction never committed", got)
	}
}

// TestWriterConnectionIsNeverRecycled backs ConnMaxLifetime(0): the pool keeps
// exactly the one connection it opened, however many transactions run.
func TestWriterConnectionIsNeverRecycled(t *testing.T) {
	s := openTestStore(t, Options{})

	if err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
		return err
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := range 200 {
		if err := s.withTx(context.Background(), func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO counter (id, n) VALUES (?, ?)", i, i)
			return err
		}); err != nil {
			t.Fatalf("transaction %d: %v", i, err)
		}
	}

	stats := s.w.Stats()
	if stats.OpenConnections != 1 {
		t.Errorf("writer OpenConnections = %d, want 1", stats.OpenConnections)
	}
	// A configuration smoke check, not a behavioural guard: the database/sql
	// connection cleaner runs at most once a second, so a test this short would
	// not see a recycled connection even if the lifetime were set.
	if stats.MaxLifetimeClosed != 0 {
		t.Errorf("writer MaxLifetimeClosed = %d, want 0", stats.MaxLifetimeClosed)
	}
}
