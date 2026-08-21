package store_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
)

// competingWriter is a second connection to the same file, standing in for the
// sqlite3 shell or a backup tool. Its busy_timeout is short so the test does
// not wait ten seconds for the outcome it expects.
func competingWriter(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate&_pragma=busy_timeout(200)")
	if err != nil {
		t.Fatalf("open competing writer: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestWithTxTakesTheWriteLockAtBegin is the upgrade deadlock test. A
// transaction that begins DEFERRED takes only a read lock, so a competing
// writer can commit underneath it and the later write fails with
// SQLITE_BUSY_SNAPSHOT, which busy_timeout does not retry.
//
// The assertions pin both halves: the competing writer must be locked out for
// the whole transaction, and fn must run exactly once, because a retry would
// paper over a lock upgrade that never should have been possible.
func TestWithTxTakesTheWriteLockAtBegin(t *testing.T) {
	s := newStore(t)
	ext := competingWriter(t, s.Path())

	readDone := make(chan struct{})
	extDone := make(chan error, 1)
	go func() {
		<-readDone
		_, err := ext.Exec("UPDATE counter SET n = n + 100 WHERE id = 1")
		extDone <- err
	}()

	attempts := 0
	var extErr error
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		attempts++
		var n int
		if err := tx.QueryRow("SELECT n FROM counter WHERE id = 1").Scan(&n); err != nil {
			return err
		}
		if attempts == 1 {
			close(readDone)
			extErr = <-extDone
		}
		_, err := tx.Exec("UPDATE counter SET n = ? WHERE id = 1", n+1)
		return err
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	if attempts != 1 {
		t.Errorf("WithTx ran the callback %d times, want 1: the transaction had to be retried, "+
			"so it did not hold the write lock from BEGIN", attempts)
	}
	if extErr == nil {
		t.Error("the competing writer committed while WithTx was open: BEGIN did not take the write lock")
	}
	if got := readCounter(t, s); got != 1 {
		t.Errorf("counter = %d, want 1", got)
	}
}

// TestConcurrentReadThenWriteNeverReportsBusy is the fast smoke version of the
// concurrency gate. The full 32 goroutine version lives in the performance
// issue; this one runs on every package test.
func TestConcurrentReadThenWriteNeverReportsBusy(t *testing.T) {
	const (
		writers    = 2
		iterations = 500
	)

	s := newStore(t)

	var wg sync.WaitGroup
	errs := make(chan error, writers*iterations)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
					var n int
					if err := tx.QueryRow("SELECT n FROM counter WHERE id = 1").Scan(&n); err != nil {
						return err
					}
					_, err := tx.Exec("UPDATE counter SET n = ? WHERE id = 1", n+1)
					return err
				})
				if err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if strings.Contains(strings.ToLower(err.Error()), "busy") {
			t.Fatalf("SQLITE_BUSY from our own writers: %v", err)
		}
		t.Fatalf("WithTx: %v", err)
	}

	if got := readCounter(t, s); got != writers*iterations {
		t.Errorf("counter = %d, want %d: read-compute-write inside one IMMEDIATE transaction lost an update",
			got, writers*iterations)
	}
}
