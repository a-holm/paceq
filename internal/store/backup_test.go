package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestVacuumIntoProducesVerifiedCopy is the happy path: the copy exists, opens
// read-only and passes quick_check. Without this, "backup" means hope.
func TestVacuumIntoProducesVerifiedCopy(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName: "b", SpecHash: "sha256:b", SpecJSON: `{"steps":[]}`,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "state-2026-08-25T03-00-00Z.db")
	if err := s.VacuumInto(ctx, dst); err != nil {
		t.Fatalf("vacuum into: %v", err)
	}
	if err := VerifyDatabaseFile(ctx, dst, false); err != nil {
		t.Fatalf("quick_check on fresh copy: %v", err)
	}
	if err := VerifyDatabaseFile(ctx, dst, true); err != nil {
		t.Fatalf("integrity_check on fresh copy: %v", err)
	}
}

// TestBackupVerificationFailsOnCorruption is test plan item 4: garbage in the
// middle of the copy must fail verification, and a failed backup must never
// be presented as taken.
func TestBackupVerificationFailsOnCorruption(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	dst := filepath.Join(t.TempDir(), "corrupt.db")
	if err := s.VacuumInto(ctx, dst); err != nil {
		t.Fatalf("vacuum into: %v", err)
	}
	f, err := os.OpenFile(dst, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open copy for corruption: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat copy: %v", err)
	}
	// Two whole pages of zeros take out the file header and sqlite_master,
	// which is what real mid-disk rot looks like to quick_check.
	garbage := make([]byte, 2*4096)
	if _, err := f.WriteAt(garbage, 0); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	_ = info
	if err := f.Close(); err != nil {
		t.Fatalf("close copy: %v", err)
	}

	if err := VerifyDatabaseFile(ctx, dst, false); err == nil {
		t.Fatal("verification passed on a corrupted copy, want failure")
	}
}

// TestVerifyMissingCopyFails pins that an absent file is a verification
// failure, not a panic and not a pass.
func TestVerifyMissingCopyFails(t *testing.T) {
	err := VerifyDatabaseFile(context.Background(), filepath.Join(t.TempDir(), "nope.db"), false)
	if err == nil {
		t.Fatal("verified a database file that does not exist")
	}
}

func TestMetaRoundTrip(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	if _, ok, err := s.MetaValue(ctx, MetaKeyBackupLastAt); err != nil || ok {
		t.Fatalf("fresh meta answered (ok=%v, err=%v), want absence without error", ok, err)
	}
	if err := s.SetMeta(ctx, map[string]string{"k1": "v1"}); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	if err := s.SetMeta(ctx, map[string]string{"k1": "v2"}); err != nil {
		t.Fatalf("overwrite meta: %v", err)
	}
	v, ok, err := s.MetaValue(ctx, "k1")
	if err != nil || !ok || v != "v2" {
		t.Fatalf("meta roundtrip gave %q ok=%v err=%v, want v2 true nil", v, ok, err)
	}
}

func TestBackupStatusRoundTripAndAbsence(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	info, err := s.BackupStatus(ctx)
	if err != nil {
		t.Fatalf("backup status on a never-backed-up database: %v", err)
	}
	if info.HasBackup {
		t.Fatal("a fresh database claims it has a backup")
	}

	at := now.Add(-2 * time.Hour)
	deep := at.Add(-23 * time.Hour)
	if err := s.RecordBackup(ctx, at, BackupStatusVerified, "/backups/x.db", "", deep); err != nil {
		t.Fatalf("record backup: %v", err)
	}
	info, err = s.BackupStatus(ctx)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !info.HasBackup || !info.Verified() || info.Path != "/backups/x.db" || info.Error != "" {
		t.Fatalf("backup info came back wrong: %+v", info)
	}
	if d := info.Age(now); d < 119*time.Minute || d > 121*time.Minute {
		t.Fatalf("age = %v, want about two hours", d)
	}
	if got := info.LastDeepCheck; got.Sub(deep) > time.Second || deep.Sub(got) > time.Second {
		t.Fatalf("deep check stamp = %v, want %v", got, deep)
	}

	if err := s.RecordBackup(ctx, now, BackupStatusFailed, "/backups/y.db", "quick_check: boom", time.Time{}); err != nil {
		t.Fatalf("record failed backup: %v", err)
	}
	info, err = s.BackupStatus(ctx)
	if err != nil {
		t.Fatalf("read back after failure: %v", err)
	}
	if info.Verified() || info.Error != "quick_check: boom" {
		t.Fatalf("failed backup not recorded as such: %+v", info)
	}
	if info.LastDeepCheck.IsZero() {
		t.Fatalf("deep-check stamp was clobbered by a plain record: %+v", info)
	}
}

// seedFreelist creates real rows and deletes them again through the retention
// path, so the freelist holds pages incremental_vacuum can give back.
func seedFreelist(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	const batches = 40
	for b := range batches {
		var sb strings.Builder
		sb.WriteString(`INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at,
			last_started_at, finished_at, repeat_count, outcome, reason_code) VALUES `)
		var args []any
		for i := range 250 {
			if i > 0 {
				sb.WriteString(",")
			}
			id := fmt.Sprintf("fl-%04d-%04d", b, i)
			ms := time.Now().AddDate(-1, 0, 0).UnixMilli() + int64(i)
			sb.WriteString("(?, 'sensor', 'freeseed', NULL, ?, ?, ?, 1, 'skipped', 'W')")
			args = append(args, id, ms, ms, ms)
		}
		if _, err := s.w.ExecContext(ctx, sb.String(), args...); err != nil {
			t.Fatalf("seed freelist batch %d: %v", b, err)
		}
	}
	cutoff := time.Now().AddDate(0, 0, -7)
	for {
		n, err := s.PruneSkippedTicksBatch(ctx, cutoff)
		if err != nil {
			t.Fatalf("prune seeded skips: %v", err)
		}
		if n == 0 {
			break
		}
	}
}

func TestIncrementalVacuumReleasesTheFreeList(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	freeBefore := mustFreelist(t, s, ctx)

	seedFreelist(t, s)

	freeAfterDelete := mustFreelist(t, s, ctx)
	if freeAfterDelete <= freeBefore {
		t.Fatalf("freelist did not grow after mass deletion: before=%d after=%d", freeBefore, freeAfterDelete)
	}

	start := time.Now()
	if err := s.IncrementalVacuum(ctx, 2000); err != nil {
		t.Fatalf("incremental_vacuum: %v", err)
	}
	took := time.Since(start)

	freeAfterVacuum := mustFreelist(t, s, ctx)
	released := freeAfterDelete - freeAfterVacuum
	if released <= 0 {
		t.Fatalf("incremental_vacuum released nothing: freelist %d -> %d", freeAfterDelete, freeAfterVacuum)
	}
	pagesPerCall := int64(2000)
	if released > pagesPerCall {
		t.Fatalf("released %d pages in one call, more than the 2000-page cap", released)
	}
	t.Logf("incremental_vacuum(2000): freelist %d -> %d (%d pages, %s)",
		freeAfterDelete, freeAfterVacuum, released, took)

	// The timing gate lives where the clock is honest: the race detector
	// multiplies wall time several-fold, so only the structural asserts above
	// run under -race.
	if !raceEnabled && took > 100*time.Millisecond {
		t.Fatalf("incremental_vacuum took %s, want under 100ms", took)
	}
}

func mustFreelist(t *testing.T, s *Store, ctx context.Context) int64 {
	t.Helper()
	n, err := s.FreelistCount(ctx)
	if err != nil {
		t.Fatalf("freelist_count: %v", err)
	}
	return n
}

func TestWalCheckpointTruncatesWhenIdle(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	active, err := s.ActiveRunsExist(ctx)
	if err != nil || active {
		t.Fatalf("an empty database reports active runs (active=%v, err=%v)", active, err)
	}
	// A write puts frames into the WAL; then the truncate has something to do.
	if err := s.SetMeta(ctx, map[string]string{"waltest": "1"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	walPath := s.path + "-wal"
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected a WAL file after a write: %v", err)
	}
	if err := s.WalCheckpointTruncate(ctx); err != nil {
		t.Fatalf("checkpoint when idle: %v", err)
	}
	if info, err := os.Stat(walPath); err == nil && info.Size() > 0 {
		t.Fatalf("WAL still holds %d bytes after TRUNCATE checkpoint", info.Size())
	}
}

func TestActiveRunsExistSeesARunningRun(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	versionID := mustVersion(t, s, "busy")

	ms := time.Now().UnixMilli()
	if _, err := s.w.ExecContext(ctx, `
INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
VALUES ('r-live', 'busy', ?, 'manual', 'running', ?, ?, ?)`,
		versionID, ms, ms, ms); err != nil {
		t.Fatalf("seed running run: %v", err)
	}
	active, err := s.ActiveRunsExist(ctx)
	if err != nil || !active {
		t.Fatalf("active runs = %v (err=%v), want true with a running row present", active, err)
	}
}

func TestFullVacuumRuns(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	seedFreelist(t, s)

	pagesBefore, err := s.PageCount(ctx)
	if err != nil {
		t.Fatalf("page_count before: %v", err)
	}
	if err := s.FullVacuum(ctx); err != nil {
		t.Fatalf("full vacuum: %v", err)
	}
	pagesAfter, err := s.PageCount(ctx)
	if err != nil {
		t.Fatalf("page_count after: %v", err)
	}
	t.Logf("full vacuum: %d -> %d pages", pagesBefore, pagesAfter)
	if pagesAfter > pagesBefore {
		t.Fatalf("file grew from %d to %d pages across a full vacuum", pagesBefore, pagesAfter)
	}
}
