package daemon

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/janitor"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// TestMaintenanceNeverRunsUnderAForeignLease pins the exclusivity promise of
// #36: with the maintenance lease held by someone else, the wired loop ticks
// but never cycles; once the lease is free, the same loop cycles at once.
func TestMaintenanceNeverRunsUnderAForeignLease(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	dbPath := filepath.Join(stateDir, "state.db")
	s, err := store.Open(ctx, dbPath, store.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	maint := janitor.New(janitor.Config{
		Store:     s,
		Clock:     clock.System(),
		LogRoot:   filepath.Join(stateDir, "logs"),
		BackupDir: filepath.Join(stateDir, "backups"),
		Log:       slog.Default(),
	})

	logger := slog.Default()
	bus := notify.New()
	sts := newStatuses(func() time.Time { return clock.System().Now() })
	d := testLoops(t, logger, bus, sts, clock.System())

	// An intruder holds the lease for far longer than this test runs.
	if _, ok, err := s.AcquireOrRenew(ctx, "maintenance", "intruder", 5*time.Minute); err != nil || !ok {
		t.Fatalf("seed the foreign lease: ok=%v err=%v", ok, err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = maintenanceLoop(loopCtx, d, 20*time.Millisecond, s, maint, 3, "this-daemon")
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	if _, found, err := s.MetaValue(ctx, store.MetaKeyGCCycleLastAt); err != nil || found {
		t.Fatalf("maintenance ran under a foreign lease (meta found=%v err=%v)", found, err)
	}

	// Hand the lease over and the very next tick owes - and runs - a cycle.
	if _, err := s.ReleaseLease(ctx, "maintenance", "intruder"); err != nil {
		t.Fatalf("release the foreign lease: %v", err)
	}

	loopCtx2, cancel2 := context.WithCancel(ctx)
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_ = maintenanceLoop(loopCtx2, d, 20*time.Millisecond, s, maint, 3, "this-daemon")
	}()

	var cycled bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, found, err := s.MetaValue(ctx, store.MetaKeyGCCycleLastAt); err == nil && found {
			cycled = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel2()
	<-done2
	if !cycled {
		t.Fatal("the cycle never ran after the lease became free")
	}
}
