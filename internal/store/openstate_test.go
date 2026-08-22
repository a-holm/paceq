package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/clock"
)

// TestOpenStateTakesTheLockBeforeTheDatabase pins the startup order. The
// database file is the evidence: if a refused start had opened it, the file
// would be there.
func TestOpenStateTakesTheLockBeforeTheDatabase(t *testing.T) {
	dir := stateDir(t)
	ctx := context.Background()

	held, err := AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("hold the state lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	s, err := OpenState(ctx, dir, Options{})
	if err == nil {
		_ = s.Close()
		t.Fatal("OpenState opened a state directory another holder had locked")
	}
	var locked *LockedError
	if !errors.As(err, &locked) {
		t.Fatalf("error %v is not a *LockedError", err)
	}
	if _, err := os.Stat(filepath.Join(dir, DatabaseFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused start created %s, so the database was opened before the lock was taken", DatabaseFileName)
	}
}

// TestOpenStateNamesTheProcessHoldingTheLock is the operator facing half: the
// second start says who owns the state directory and what to do about it, read
// from the session row rather than guessed from the lock file.
func TestOpenStateNamesTheProcessHoldingTheLock(t *testing.T) {
	dir := stateDir(t)
	ctx := context.Background()

	first, err := OpenState(ctx, dir, Options{Clock: clock.NewFake(sessionOrigin)})
	if err != nil {
		t.Fatalf("open the state directory: %v", err)
	}
	defer func() { _ = first.Close() }()
	if err := first.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	owner, err := first.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("start session: %v", err)
	}

	second, err := OpenState(ctx, dir, Options{})
	if err == nil {
		_ = second.Close()
		t.Fatal("a second process opened a state directory that was already in use")
	}

	message := err.Error()
	for _, want := range []string{
		"PQ5002",
		strconv.Itoa(owner.PID),
		owner.Version,
		sessionOrigin.Format("2006-01-02"),
		"another state directory",
		"kill " + strconv.Itoa(owner.PID),
	} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal does not mention %q, so the operator cannot act on it:\n%s", want, message)
		}
	}
}

// TestOpenStateWithoutASessionRowStillExplains covers the state directory
// locked by something that never wrote a session row: a foreign process, or a
// start that died between the lock and the first write.
func TestOpenStateWithoutASessionRowStillExplains(t *testing.T) {
	dir := stateDir(t)
	ctx := context.Background()

	held, err := AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("hold the state lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	_, err = OpenState(ctx, dir, Options{})
	if err == nil {
		t.Fatal("OpenState opened a locked state directory")
	}
	if !strings.Contains(err.Error(), "PQ5002") || !strings.Contains(err.Error(), "no session row") {
		t.Errorf("the refusal does not say that the owner is unknown:\n%s", err)
	}
}

// TestOpenStateReleasesTheLockOnClose keeps a restart from being blocked by the
// process that just stopped.
func TestOpenStateReleasesTheLockOnClose(t *testing.T) {
	dir := stateDir(t)
	ctx := context.Background()

	s, err := OpenState(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("open the state directory: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again, err := OpenState(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("reopen the state directory after a clean close: %v", err)
	}
	if err := again.Close(); err != nil {
		t.Fatalf("close again: %v", err)
	}
}

// TestOpenStateCloseIsIdempotent covers the shutdown path that runs twice: a
// deferred Close beside an explicit one. The second call has to be a no-op, not
// an attempt to unlock a descriptor that is already gone.
func TestOpenStateCloseIsIdempotent(t *testing.T) {
	dir := stateDir(t)

	s, err := OpenState(context.Background(), dir, Options{})
	if err != nil {
		t.Fatalf("open the state directory: %v", err)
	}
	for call := 1; call <= 2; call++ {
		if err := s.Close(); err != nil {
			t.Errorf("close call %d: %v", call, err)
		}
	}
}

// TestOpenStateKeepsTheDatabasePrivate covers the file paceq creates itself.
// SQLite creates it under the process umask, which is nothing paceq controls,
// so a fresh database is narrowed here and a widened one is refused.
func TestOpenStateKeepsTheDatabasePrivate(t *testing.T) {
	dir := stateDir(t)
	ctx := context.Background()

	s, err := OpenState(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("open the state directory: %v", err)
	}
	dbPath := filepath.Join(dir, DatabaseFileName)
	if got := statMode(t, dbPath); got != 0o600 {
		t.Errorf("database mode is %#o, want %#o", got, 0o600)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := os.Chmod(dbPath, 0o644); err != nil {
		t.Fatalf("chmod %s: %v", dbPath, err)
	}
	again, err := OpenState(ctx, dir, Options{})
	if err == nil {
		_ = again.Close()
		t.Fatal("paceq started on a world readable database")
	}
	if !strings.Contains(err.Error(), dbPath) {
		t.Errorf("the refusal does not name the database:\n%s", err)
	}
}

// TestStateLockDoesNotCoverASharedDatabase documents a deliberate hole. The
// lock is on a state directory, so two processes pointed at the same database
// file through different state directories both start. Nothing here fixes that:
// the role leases in M2-02 are what make it safe, and this test exists so the
// gap is a known one rather than a surprise.
func TestStateLockDoesNotCoverASharedDatabase(t *testing.T) {
	shared := filepath.Join(stateDir(t), "state.db")
	ctx := context.Background()

	for _, dir := range []string{stateDir(t), stateDir(t)} {
		lock, err := AcquireStateLock(dir)
		if err != nil {
			t.Fatalf("take the state lock on %s: %v", dir, err)
		}
		defer func() { _ = lock.Release() }()

		s, err := Open(ctx, shared, Options{})
		if err != nil {
			t.Fatalf("open the shared database from %s: %v", dir, err)
		}
		defer func() { _ = s.Close() }()
	}
}
