//go:build unix

package store

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// crashEnv carries the database path to the child process. Its presence is what
// turns the helper test into the crashing half of this test.
const crashEnv = "PACEQ_MIGRATE_CRASH_DB"

// TestMigrateCrashLeavesNothingBehind kills a real process between a
// migration's DDL and its commit. A migration that could be half applied would
// show up here as a table without a ledger row, which is the failure mode the
// single transaction exists to make impossible.
func TestMigrateCrashLeavesNothingBehind(t *testing.T) {
	if os.Getenv(crashEnv) != "" {
		t.Skip("child process, driven by the parent test")
	}

	// The recovery below has to take over the dead child's lock at once. With
	// the full budget a broken takeover would still pass, it would just wait
	// half a minute first.
	defer func(previous time.Duration) { migrationLockWait = previous }(migrationLockWait)
	migrationLockWait = 20 * time.Millisecond

	path := filepath.Join(t.TempDir(), "state.db")
	cmd := exec.Command(os.Args[0], "-test.run=TestMigrateCrashChild", "-test.count=1")
	cmd.Env = append(os.Environ(), crashEnv+"="+path)
	out, err := cmd.CombinedOutput()

	var status *exec.ExitError
	if !asExitError(err, &status) || status.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child exited with %v, want death by SIGKILL\n%s", err, out)
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("reopen the crashed database: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	var integrity string
	if err := s.w.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if integrity != "ok" {
		t.Errorf("integrity_check reports %q, want \"ok\"", integrity)
	}

	if tableExists(t, s, "widgets") {
		t.Error("the crashed migration left its table behind, so it was applied in part")
	}
	if got := userVersion(t, s); got != 0 {
		t.Errorf("user_version is %d after the crash, want 0", got)
	}

	// The recovery run is the point of the guarantee: nothing was half done, so
	// the next start simply migrates.
	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("migrate after the crash: %v", err)
	}
	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}
	if len(applied) != 2 {
		t.Errorf("ledger holds %d rows after recovery, want 2", len(applied))
	}
}

// TestMigrateCrashChild is the child half. It runs only under the parent, which
// sets the environment variable holding the database path.
func TestMigrateCrashChild(t *testing.T) {
	path := os.Getenv(crashEnv)
	if path == "" {
		t.Skip("driven by TestMigrateCrashLeavesNothingBehind")
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	beforeCommit = func() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }

	if err := s.migrateFS(context.Background(), twoMigrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Fatal("the child survived its own SIGKILL")
}

func asExitError(err error, target **exec.ExitError) bool {
	exitErr, ok := err.(*exec.ExitError)
	if ok {
		*target = exitErr
	}
	return ok
}
