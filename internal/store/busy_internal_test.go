package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// realPlainBusy reproduces plain SQLITE_BUSY the way an operator meets it: a
// second connection holds the write lock, and this one gives up rather than
// waiting. busy_timeout is short so the test costs milliseconds instead of the
// ten seconds the write pool waits in production.
func realPlainBusy(t *testing.T) error {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	holder, err := sql.Open(driverName, testDSN(t, path, "_txlock=immediate&_pragma=journal_mode(WAL)"))
	if err != nil {
		t.Fatalf("open the holding connection: %v", err)
	}
	defer func() { _ = holder.Close() }()
	holder.SetMaxOpenConns(1)
	if _, err := holder.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := holder.Exec("INSERT INTO t (id, n) VALUES (1, 0)"); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	tx, err := holder.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin the holding transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("UPDATE t SET n = 1 WHERE id = 1"); err != nil {
		t.Fatalf("write inside the holding transaction: %v", err)
	}

	blocked, err := sql.Open(driverName, testDSN(t, path, "_txlock=immediate&_pragma=busy_timeout(200)"))
	if err != nil {
		t.Fatalf("open the blocked connection: %v", err)
	}
	defer func() { _ = blocked.Close() }()
	blocked.SetMaxOpenConns(1)

	_, err = blocked.Exec("UPDATE t SET n = 2 WHERE id = 1")
	if err == nil {
		t.Fatal("the blocked write succeeded, expected SQLITE_BUSY")
	}
	return err
}

// TestIsBusyOnRealDriverErrors anchors the exported predicate against the
// driver rather than against two numbers copied from a header. Both outcomes
// are produced by the lock contention that causes them: an error that only
// looks busy in its message would prove nothing about what paceq will meet.
func TestIsBusyOnRealDriverErrors(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
	}{
		"plain busy":    {err: realPlainBusy(t), want: 5},
		"busy snapshot": {err: realBusySnapshot(t), want: 517},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var coded interface{ Code() int }
			if !errors.As(c.err, &coded) {
				t.Fatalf("driver error %v carries no result code", c.err)
			}
			if coded.Code() != c.want {
				t.Errorf("driver reported result code %d, want %d", coded.Code(), c.want)
			}
			if !IsBusy(c.err) {
				t.Errorf("IsBusy(%v) = false: a contended database would reach the operator as a bug", c.err)
			}
			if !IsBusy(fmt.Errorf("list all schedules: %w", c.err)) {
				t.Errorf("IsBusy sees nothing through the wrapping every store method adds")
			}
		})
	}
}

// TestIsBusyRejectsEverythingElse is the half that keeps the predicate from
// swallowing failures that really are the tool's own.
func TestIsBusyRejectsEverythingElse(t *testing.T) {
	cases := map[string]error{
		"nil":                 nil,
		"a plain error":       errors.New("disk on fire"),
		"a constraint":        codedError{code: 19},
		"a read only handle":  ErrReadOnly,
		"a message that lies": errors.New("database is locked (5) (SQLITE_BUSY)"),
	}

	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			if IsBusy(err) {
				t.Errorf("IsBusy(%v) = true, want false", err)
			}
		})
	}
}
