package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/id"
)

// sessionOrigin is the wall clock reading the session tests start from. A fixed
// instant keeps every expected timestamp an offset rather than a moving target.
var sessionOrigin = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

// sessionStore is a migrated store on a fake clock, with the boot id reader
// replaced. The reader is injected because /proc/sys/kernel/random/boot_id
// cannot be changed from a test, and a machine restart is exactly what these
// tests have to reproduce.
func sessionStore(t *testing.T, clk clock.Clock, boot func() (string, error)) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store at %q: %v", path, err)
		}
	})
	s.bootID = boot
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func constantBoot(value string) func() (string, error) {
	return func() (string, error) { return value, nil }
}

// TestStartSessionRecordsTheRun covers the row every later gap detection reads:
// who ran, on which boot, from when.
func TestStartSessionRecordsTheRun(t *testing.T) {
	clk := clock.NewFake(sessionOrigin)
	s := sessionStore(t, clk, constantBoot("boot-one"))
	ctx := context.Background()

	sess, err := s.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("start session: %v", err)
	}

	if _, err := id.Parse(sess.ID); err != nil {
		t.Errorf("session id %q is not a ULID: %v", sess.ID, err)
	}
	if sess.PID != os.Getpid() {
		t.Errorf("session pid is %d, want %d", sess.PID, os.Getpid())
	}
	if sess.Version != "1.2.3" {
		t.Errorf("session version is %q, want %q", sess.Version, "1.2.3")
	}
	if !sess.StartedAt.Equal(sessionOrigin) {
		t.Errorf("session started at %s, want %s", sess.StartedAt, sessionOrigin)
	}

	stored := readSession(t, s, sess.ID)
	if stored.BootID != "boot-one" {
		t.Errorf("stored boot id is %q, want %q", stored.BootID, "boot-one")
	}
	if !stored.LastSeenAt.Equal(sessionOrigin) {
		t.Errorf("stored last_seen_at is %s, want %s", stored.LastSeenAt, sessionOrigin)
	}
	if !stored.StoppedAt.IsZero() {
		t.Errorf("a session that just started is already stopped at %s", stored.StoppedAt)
	}
	if s.BootChanged() {
		t.Error("BootChanged is true on a database that had no boot id yet, so the first start looks like a restart")
	}
	if got := metaValue(t, s, "current_boot_id"); got != "boot-one" {
		t.Errorf("meta.current_boot_id is %q, want %q", got, "boot-one")
	}
}

// TestStartSessionMarksTheLastRunCrashed is R1: an open row from an earlier run
// means the process never got to say goodbye.
func TestStartSessionMarksTheLastRunCrashed(t *testing.T) {
	clk := clock.NewFake(sessionOrigin)
	s := sessionStore(t, clk, constantBoot("boot-one"))
	ctx := context.Background()

	crashed, err := s.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("start the first session: %v", err)
	}
	clk.Advance(time.Minute)

	if _, err = s.StartSession(ctx, "1.2.3"); err != nil {
		t.Fatalf("start the second session: %v", err)
	}

	previous := readSession(t, s, crashed.ID)
	if previous.StopReason != "crash" {
		t.Errorf("previous session stop_reason is %q, want %q", previous.StopReason, "crash")
	}
	if want := sessionOrigin.Add(time.Minute); !previous.StoppedAt.Equal(want) {
		t.Errorf("previous session stopped_at is %s, want %s", previous.StoppedAt, want)
	}
	if got := openSessions(t, s); got != 1 {
		t.Errorf("%d open sessions after a restart, want exactly 1", got)
	}
}

// TestStartSessionReportsANewBoot is R0. A changed boot id is the strongest
// evidence in the system: the machine restarted, so nothing paceq started can
// still be running, and reconciliation does not have to wait out a lease.
func TestStartSessionReportsANewBoot(t *testing.T) {
	clk := clock.NewFake(sessionOrigin)
	s := sessionStore(t, clk, constantBoot("boot-one"))
	ctx := context.Background()

	if _, err := s.StartSession(ctx, "1.2.3"); err != nil {
		t.Fatalf("start the first session: %v", err)
	}
	if s.BootChanged() {
		t.Fatal("BootChanged is true for a session started on the same boot")
	}

	s.bootID = constantBoot("boot-two")
	if _, err := s.StartSession(ctx, "1.2.3"); err != nil {
		t.Fatalf("start the session after a restart: %v", err)
	}

	if !s.BootChanged() {
		t.Error("BootChanged is false after the machine restarted, so reconciliation would wait out the lease instead of acting at once")
	}
	if got := metaValue(t, s, "current_boot_id"); got != "boot-two" {
		t.Errorf("meta.current_boot_id is %q, want %q", got, "boot-two")
	}
}

// readSession reads one row back through the reader pool, so the test sees what
// was committed rather than what the caller believes it wrote.
func readSession(t *testing.T, s *Store, sessionID string) Session {
	t.Helper()

	var (
		row                 Session
		boot, reason        *string
		started, lastSeen   int64
		stopped             *int64
		pid                 int
		version, storedByID string
	)
	err := s.r.QueryRowContext(context.Background(),
		`SELECT id, version, boot_id, pid, started_at, last_seen_at, stopped_at, stop_reason
		   FROM daemon_sessions WHERE id = ?`, sessionID).
		Scan(&storedByID, &version, &boot, &pid, &started, &lastSeen, &stopped, &reason)
	if err != nil {
		t.Fatalf("read session %s: %v", sessionID, err)
	}

	row.ID = storedByID
	row.Version = version
	row.PID = pid
	row.StartedAt = time.UnixMilli(started).UTC()
	row.LastSeenAt = time.UnixMilli(lastSeen).UTC()
	if boot != nil {
		row.BootID = *boot
	}
	if reason != nil {
		row.StopReason = *reason
	}
	if stopped != nil {
		row.StoppedAt = time.UnixMilli(*stopped).UTC()
	}
	return row
}

func openSessions(t *testing.T, s *Store) int {
	t.Helper()

	var n int
	err := s.r.QueryRowContext(context.Background(),
		"SELECT count(*) FROM daemon_sessions WHERE stopped_at IS NULL").Scan(&n)
	if err != nil {
		t.Fatalf("count open sessions: %v", err)
	}
	return n
}

// metaValue returns a meta row's value, or the empty string when the key is
// absent.
func metaValue(t *testing.T, s *Store, key string) string {
	t.Helper()

	var value string
	err := s.r.QueryRowContext(context.Background(),
		"SELECT value FROM meta WHERE key = ?", key).Scan(&value)
	if err != nil {
		return ""
	}
	return value
}
