package status

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// TestBuildSurfacesMaintenanceFacts pins the visibility promise of #36: the
// gc_* rows the janitor writes reach the report, so a failed maintenance
// phase is something status shows rather than something nobody notices. A
// database no daemon has maintained yet leaves the block absent instead of
// inventing facts.
func TestBuildSurfacesMaintenanceFacts(t *testing.T) {
	s := testutil.TempStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()

	// Nothing maintained yet: every field stays absent.
	empty, err := Build(ctx, s, Options{})
	if err != nil {
		t.Fatalf("build on a fresh database: %v", err)
	}
	if empty.Maintenance.LastAt != "" || empty.Maintenance.Status != "" {
		t.Fatalf("an unmaintained database invented maintenance facts: %+v", empty.Maintenance)
	}

	lastAt := time.Date(2026, 12, 9, 3, 0, 5, 0, time.UTC)
	if err := s.SetMeta(ctx, map[string]string{
		store.MetaKeyGCCycleLastAt:     lastAt.Format(time.RFC3339),
		store.MetaKeyGCCycleLastStatus: "error",
		store.MetaKeyGCCycleLastError:  "incremental_vacuum: busy",
	}); err != nil {
		t.Fatalf("record a gc cycle: %v", err)
	}
	if err := s.RecordBackup(ctx, lastAt, store.BackupStatusFailed,
		"/backups/state-x.db", "quick_check: garbage", time.Time{}); err != nil {
		t.Fatalf("record a backup attempt: %v", err)
	}

	rep, err := Build(ctx, s, Options{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	m := rep.Maintenance
	if m.Status != "error" {
		t.Fatalf("maintenance status = %q, want error", m.Status)
	}
	if m.Error != "incremental_vacuum: busy" {
		t.Fatalf("maintenance error = %q", m.Error)
	}
	wantAt := lastAt.UTC().Format(time.RFC3339)
	if m.LastAt != wantAt {
		t.Fatalf("maintenance last_at = %q, want %q", m.LastAt, wantAt)
	}
	if m.BackupVerified {
		t.Fatal("a failed backup must not read as verified")
	}
	if m.LastBackup == "" {
		t.Fatal("the last backup attempt is missing from the report")
	}
}
