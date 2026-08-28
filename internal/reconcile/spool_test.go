package reconcile

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spool"
	"github.com/a-holm/paceq/internal/store"
)

// The spool consumer's routing tests (issue #39). Every kind of file a crash
// can leave in the attempts directory has exactly one fate, and a second pass
// over a settled directory writes nothing.

// spoolWorld is a world plus a seeded running step whose lease has expired,
// the shape a crashed executor leaves, and the directory its shim would have
// written into.
type spoolWorld struct {
	*world
	spoolDir string
	runID    string
}

func newSpoolWorld(t *testing.T) *spoolWorld {
	w := newWorld(t, "boot-1")
	spec := `{"schema":"paceq.job.v1","name":"spooled","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false}]}`
	if _, _, err := w.store.UpsertJobVersion(w.ctx, store.JobVersionInput{
		JobName:  "spooled",
		SpecHash: "sha256:spooled",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := w.store.MaterializeManualTrigger(w.ctx, store.ManualTriggerInput{
		JobName: "spooled", Actor: "test",
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if _, _, err := w.store.ClaimRun(w.ctx, res.Run.ID,
		store.LeaseInput{Owner: "exec-doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := w.store.StartStep(w.ctx, res.Run.ID, "only",
		store.LeaseRef{Owner: "exec-doomed", Epoch: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}
	w.clk.Advance(2 * time.Minute) // the executor is gone; the lease expired

	spoolDir := filepath.Join(t.TempDir(), "spool", "attempts")
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		t.Fatal(err)
	}
	return &spoolWorld{world: w, spoolDir: spoolDir, runID: res.Run.ID}
}

func (w *spoolWorld) write(t *testing.T, r spool.Result) string {
	t.Helper()
	// Identity defaults to this world's attempt; a test that plants a
	// foreign result sets the fields itself and they stay.
	if r.RunID == "" {
		r.RunID, r.Step, r.Attempt = w.runID, "only", 1
	}
	if r.ClaimEpoch == 0 {
		r.ClaimEpoch = 1
	}
	r.V = spool.Version
	if r.EndedAt == 0 {
		r.EndedAt = w.clk.Now().UnixMilli()
	}
	if err := spool.WriteResult(w.spoolDir, r); err != nil {
		t.Fatalf("write the spool result: %v", err)
	}
	return filepath.Join(w.spoolDir, spool.FileName(r.RunID, r.Step, r.Attempt))
}

func aSpoolResult() spool.Result {
	return spool.Result{Outcome: spool.OutcomeSucceeded, ExitCode: 0}
}

func TestConsumeSpoolCommitsWhatTheShimWrote(t *testing.T) {
	w := newSpoolWorld(t)
	path := w.write(t, aSpoolResult())

	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	detail, err := w.store.GetRun(w.ctx, w.runID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Steps[0].State != "succeeded" {
		t.Fatalf("state = %s, want succeeded", detail.Steps[0].State)
	}
	if detail.Steps[0].OutcomeSource != "spool" {
		t.Fatalf("outcome_source = %q, want spool", detail.Steps[0].OutcomeSource)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the consumed file is still in the attempts directory: %v", err)
	}
}

func TestConsumeSpoolArchivesAStaleEpoch(t *testing.T) {
	w := newSpoolWorld(t)
	stale := aSpoolResult()
	stale.ClaimEpoch = 998
	path := w.write(t, stale)

	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := os.Stat(filepath.Join(spool.UnknownDir(w.spoolDir), filepath.Base(path))); err != nil {
		t.Fatalf("the stale result was not archived: %v", err)
	}
	events, err := w.store.ExplainRunEvents(w.ctx, w.runID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasKind(events, "run.spool_archived") {
		t.Fatal("no warn event for the archived stale result")
	}
	detail, err := w.store.GetRun(w.ctx, w.runID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Steps[0].State != "running" {
		t.Fatalf("state = %s, want still running: fencing holds", detail.Steps[0].State)
	}
}

func TestConsumeSpoolArchivesAnUnknownAttempt(t *testing.T) {
	w := newSpoolWorld(t)
	foreign := aSpoolResult()
	foreign.RunID, foreign.Step, foreign.Attempt, foreign.ClaimEpoch, foreign.BootID = "01J0NOBODY", "only", 1, 1, ""
	path := w.write(t, foreign)

	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := os.Stat(filepath.Join(spool.UnknownDir(w.spoolDir), filepath.Base(path))); err != nil {
		t.Fatalf("the foreign result was not archived: %v", err)
	}
	// A file naming a run this database never knew has no run to hang the
	// event on; the archive move is the whole story.
}

func TestConsumeSpoolArchivesUntrustworthyFiles(t *testing.T) {
	for _, tc := range []struct {
		name  string
		plant func(t *testing.T, dir string) string
	}{
		{"garbage", func(t *testing.T, dir string) string {
			p := filepath.Join(dir, "01J0-x-1.json")
			if err := os.WriteFile(p, []byte("{oops"), 0o600); err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{"wrong version", func(t *testing.T, dir string) string {
			p := filepath.Join(dir, "01J0-y-1.json")
			b, _ := json.Marshal(spool.Result{V: 42, RunID: "01J0-y", Step: "x", Attempt: 1, Outcome: spool.OutcomeSucceeded})
			if err := os.WriteFile(p, b, 0o600); err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{"unknown outcome word", func(t *testing.T, dir string) string {
			p := filepath.Join(dir, "01J0-z-1.json")
			b, _ := json.Marshal(spool.Result{V: spool.Version, RunID: "01J0-z", Step: "x", Attempt: 1, Outcome: "mystery"})
			if err := os.WriteFile(p, b, 0o600); err != nil {
				t.Fatal(err)
			}
			return p
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newSpoolWorld(t)
			path := tc.plant(t, w.spoolDir)
			if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
				t.Fatalf("consume: %v", err)
			}
			if _, err := os.Stat(filepath.Join(spool.UnknownDir(w.spoolDir), filepath.Base(path))); err != nil {
				t.Fatalf("the untrustworthy file was not archived: %v", err)
			}
		})
	}
}

func TestConsumeSpoolLeavesAFileWhoseExecutorMayLive(t *testing.T) {
	w := newSpoolWorld(t)
	// Rewind to inside the lease: the executor may still be alive, the
	// file is its own to consume after the wait.
	w.clk.Advance(-90 * time.Second)
	path := w.write(t, aSpoolResult())

	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the live executor's result was taken: %v", err)
	}
}

func TestConsumeSpoolIgnoresTempFiles(t *testing.T) {
	w := newSpoolWorld(t)
	tmp := filepath.Join(w.spoolDir, ".interrupted.json.tmp")
	if err := os.WriteFile(tmp, []byte("{half"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("the interrupted write was touched: %v", err)
	}
}

// The pass is idempotent: a second one over a settled directory writes
// nothing anywhere, and the database tells the same story twice.
func TestConsumeSpoolTwiceIsBitIdentical(t *testing.T) {
	w := newSpoolWorld(t)
	w.write(t, aSpoolResult())
	stale := aSpoolResult()
	stale.ClaimEpoch = 5
	w.write(t, stale)

	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	eventsFirst, err := w.store.ExplainRunEvents(w.ctx, w.runID)
	if err != nil {
		t.Fatal(err)
	}
	unknownFirst := listDir(t, spool.UnknownDir(w.spoolDir))

	if err := ConsumeSpoolResults(w.ctx, w.store, w.spoolDir, nil); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	eventsSecond, err := w.store.ExplainRunEvents(w.ctx, w.runID)
	if err != nil {
		t.Fatal(err)
	}
	unknownSecond := listDir(t, spool.UnknownDir(w.spoolDir))

	if len(eventsFirst) != len(eventsSecond) {
		t.Fatalf("the second pass wrote %d events over %d", len(eventsSecond), len(eventsFirst))
	}
	if strings.Join(unknownFirst, ",") != strings.Join(unknownSecond, ",") {
		t.Fatalf("the unknown directory changed between passes: %v then %v", unknownFirst, unknownSecond)
	}
}

// The whole thing is wired into the startup sequence, before the run
// handback: the file's epoch is still the one on the row at that point.
func TestOnStartupConsumesTheSpool(t *testing.T) {
	w := newSpoolWorld(t)
	w.write(t, aSpoolResult())

	opts := w.opts(time.Time{}, "")
	opts.SpoolDir = w.spoolDir
	if err := OnStartup(w.ctx, w.store, opts); err != nil {
		t.Fatalf("startup reconciliation: %v", err)
	}
	detail, err := w.store.GetRun(w.ctx, w.runID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Steps[0].OutcomeSource != "spool" {
		t.Fatalf("outcome_source = %q after startup, want spool", detail.Steps[0].OutcomeSource)
	}
}

// An empty SpoolDir means the installation never shims: the pass is skipped
// entirely and the lease-based handback stays the whole recovery story.
func TestConsumeSpoolWithoutADirectoryDoesNothing(t *testing.T) {
	w := newSpoolWorld(t)
	if err := ConsumeSpoolResults(w.ctx, w.store, "", nil); err != nil {
		t.Fatalf("an empty directory is not an error: %v", err)
	}
	consumeSpool(w.ctx, w.store, &Options{})
}

func hasKind(events []store.RunEvent, kind string) bool {
	for _, e := range events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

var _ = reason.STEPSucceeded
