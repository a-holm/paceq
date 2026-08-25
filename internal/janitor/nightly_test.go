package janitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// scriptedStore answers every janitor question from fields a test sets, so
// the nightly sequence can be driven through states a live database would
// not sit still in.
type scriptedStore struct {
	active      bool
	freelist    int64
	backupInfo  store.BackupInfo
	vacuumErr   error
	checkpointR bool

	meta    map[string]string
	cycles  int
	backups []string
}

func newScriptedStore() *scriptedStore {
	return &scriptedStore{meta: map[string]string{}}
}

func (s *scriptedStore) PruneRunsBatch(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (s *scriptedStore) PruneSkippedTicksBatch(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (s *scriptedStore) PruneTicksBatch(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (s *scriptedStore) PruneRunKeysBatch(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (s *scriptedStore) PruneSessionsBatch(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (s *scriptedStore) EstimateRetention(context.Context, store.Policies, time.Time) (store.RetentionPlan, error) {
	return store.RetentionPlan{}, nil
}
func (s *scriptedStore) IncrementalVacuum(context.Context, int) error { return s.vacuumErr }
func (s *scriptedStore) WalCheckpointTruncate(context.Context) error {
	s.checkpointR = true
	return nil
}

func (s *scriptedStore) ActiveRunsExist(context.Context) (bool, error) {
	return s.active, nil
}

func (s *scriptedStore) FreelistCount(context.Context) (int64, error) {
	if s.freelist > 0 {
		before := s.freelist
		s.freelist = 0
		return before, nil
	}
	return 0, nil
}

func (s *scriptedStore) VacuumInto(_ context.Context, dst string) error {
	s.backups = append(s.backups, dst)
	return os.WriteFile(dst, []byte("SQLite format 3\x00"), 0o600)
}

func (s *scriptedStore) RecordBackup(_ context.Context, at time.Time, status, path, errMsg string, deepAt time.Time) error {
	s.meta[store.MetaKeyBackupLastAt] = at.UTC().Format(time.RFC3339)
	s.meta[store.MetaKeyBackupLastStatus] = status
	s.meta[store.MetaKeyBackupLastPath] = path
	s.meta[store.MetaKeyBackupLastError] = errMsg
	if !deepAt.IsZero() {
		s.meta[store.MetaKeyBackupLastDeepCheck] = deepAt.UTC().Format(time.RFC3339)
	}
	return nil
}

func (s *scriptedStore) BackupStatus(context.Context) (store.BackupInfo, error) {
	info := s.backupInfo
	if raw, ok := s.meta[store.MetaKeyBackupLastAt]; ok {
		info.LastAt, _ = time.Parse(time.RFC3339, raw)
		info.HasBackup = true
	}
	if v, ok := s.meta[store.MetaKeyBackupLastStatus]; ok {
		info.Status = v
	}
	if v, ok := s.meta[store.MetaKeyBackupLastPath]; ok {
		info.Path = v
	}
	if v, ok := s.meta[store.MetaKeyBackupLastError]; ok {
		info.Error = v
	}
	if v, ok := s.meta[store.MetaKeyBackupLastDeepCheck]; ok {
		info.LastDeepCheck, _ = time.Parse(time.RFC3339, v)
	}
	return info, nil
}

func (s *scriptedStore) MetaValue(_ context.Context, key string) (string, bool, error) {
	v, ok := s.meta[key]
	return v, ok, nil
}

func (s *scriptedStore) SetMeta(_ context.Context, kv map[string]string) error {
	for k, v := range kv {
		s.meta[k] = v
	}
	s.cycles++
	return nil
}

var errScripted = errors.New("scripted")

// fixedNow is a clock that only answers Now, which is all the nightly
// sequence reads.
type fixedNow struct{ t time.Time }

func (f fixedNow) Now() time.Time                 { return f.t }
func (f fixedNow) Mark() clock.Mono               { return clock.Mono{} }
func (f fixedNow) Since(clock.Mono) time.Duration { return 0 }
func (f fixedNow) NewTimer(d time.Duration) *time.Timer {
	return time.NewTimer(d)
}
func (f fixedNow) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// TestDeepCheckRunsWeeklyPinsTheCadence pins the verification ladder:
// quick_check on an ordinary night, integrity_check once the week has rolled
// since the last deep pass, and never two deep checks inside the week. It
// runs against a real store because verification must pass on a real copy.
func TestDeepCheckRunsWeeklyPinsTheCadence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)

	// run plants the recorded deep-check stamp, takes one backup night, and
	// reports whether tonight's copy earned a fresh deep-check stamp. The
	// planted row survives a quick-only night (SetMeta upserts, never
	// deletes), so "deep ran" means the stamp moved to tonight.
	run := func(t *testing.T, lastDeep time.Time) string {
		s := migratedTempStore(t)
		bdir := t.TempDir()
		if err := s.RecordBackup(ctx, now.Add(-time.Hour), store.BackupStatusVerified,
			filepath.Join(bdir, "previous.db"), "", lastDeep); err != nil {
			t.Fatalf("plant the previous backup record: %v", err)
		}
		j := New(Config{Store: s, Clock: fixedNow{t: now}, BackupDir: bdir})
		out := j.backup(ctx)
		if out.Status != store.BackupStatusVerified {
			t.Fatalf("backup did not verify: %+v", out)
		}
		raw, found, err := s.MetaValue(ctx, store.MetaKeyBackupLastDeepCheck)
		if err != nil {
			t.Fatalf("read the deep stamp: %v", err)
		}
		if !found {
			return "quick_check"
		}
		got, parseErr := time.Parse(time.RFC3339, raw)
		if parseErr == nil && got.Equal(now.Truncate(time.Minute)) {
			return "integrity_check"
		}
		return "quick_check"
	}

	t.Run("six days in, quick check", func(t *testing.T) {
		if got := run(t, now.Add(-6*24*time.Hour)); got != "quick_check" {
			t.Fatalf("six days into the week the copy got %s, want quick_check", got)
		}
	})
	t.Run("eight days in, deep check", func(t *testing.T) {
		if got := run(t, now.Add(-8*24*time.Hour)); got != "integrity_check" {
			t.Fatalf("eight days since the last deep pass it got %s, want integrity_check", got)
		}
	})
	t.Run("no stamp yet, deep check", func(t *testing.T) {
		if got := run(t, time.Time{}); got != "integrity_check" {
			t.Fatalf("a database with no recorded deep pass got %s, want the deep one", got)
		}
	})
}

// TestCheckpointWaitsForQuietPinsTheGate pins the WAL rule: the truncate
// checkpoint only runs when no run is active, and an active run makes the
// night skip it without recording a failure.
func TestCheckpointWaitsForQuiet(t *testing.T) {
	ctx := context.Background()
	bdir := t.TempDir()
	logRoot := filepath.Join(bdir, "logs")
	if err := os.MkdirAll(logRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	build := func(active bool) (*Janitor, *scriptedStore) {
		st := newScriptedStore()
		st.active = active
		j := New(Config{
			Store:     st,
			Clock:     fixedNow{t: time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)},
			LogRoot:   logRoot,
			BackupDir: filepath.Join(bdir, "backups"),
		})
		return j, st
	}

	jBusy, stBusy := build(true)
	res, err := jBusy.Cycle(ctx)
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if res.Checkpoint != "skipped_active_runs" {
		t.Fatalf("checkpoint during active runs = %q", res.Checkpoint)
	}
	if stBusy.checkpointR {
		t.Fatal("the truncate ran while a run was active")
	}
	if len(res.Failures) != 0 {
		t.Fatalf("a skipped checkpoint is correct behaviour, not a failure: %v", res.Failures)
	}

	jIdle, stIdle := build(false)
	res, err = jIdle.Cycle(ctx)
	if err != nil {
		t.Fatalf("idle cycle: %v", err)
	}
	if res.Checkpoint != "ran" || !stIdle.checkpointR {
		t.Fatalf("an idle database must truncate: checkpoint=%q ran=%v",
			res.Checkpoint, stIdle.checkpointR)
	}
}

// TestFailedPhasesLandInMetaAndInTheResult pins the no-silent-maintenance
// rule: a phase error shows up in the result, in the log-worthy failures
// list, and in the gc status row doctor and status read.
func TestFailedPhasesLandInMetaAndInTheResult(t *testing.T) {
	ctx := context.Background()
	st := newScriptedStore()
	st.freelist = 5 // the vacuum only runs when there is something to free
	st.vacuumErr = errScripted
	j := New(Config{
		Store:     st,
		Clock:     fixedNow{t: time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)},
		BackupDir: "",
	})
	res, err := j.Cycle(ctx)
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	found := false
	for _, f := range res.Failures {
		if contains(f, "incremental_vacuum") && contains(f, "scripted") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the vacuum failure never reached the result: %v", res.Failures)
	}
	if st.meta[store.MetaKeyGCCycleLastStatus] != "error" {
		t.Fatalf("gc status meta = %q, want error", st.meta[store.MetaKeyGCCycleLastStatus])
	}
	if !contains(st.meta[store.MetaKeyGCCycleLastError], "scripted") {
		t.Fatalf("gc error meta = %q", st.meta[store.MetaKeyGCCycleLastError])
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
