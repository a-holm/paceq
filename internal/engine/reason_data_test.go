package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// #191: the finish path is the one writer of a run's verdict that lives above
// the store, so the two run level codes only it can produce are held against
// the catalogue here. The store's own writers are covered by
// TestEveryRunLevelPromiseIsKeptOnItsRow in internal/store.
//
// Both cases drive a real process group to its end and read the committed row
// back, because a payload the engine builds and nobody stores would prove
// nothing.
func TestFinishReasonCarriesThePromisedKeys(t *testing.T) {
	t.Run("a failed step names the step and the attempt it spent", func(t *testing.T) {
		ctx := context.Background()
		f := newFixture(t)
		runID := f.aQueuedRun(t, []string{"first", "second"},
			[]string{"exit 0", "exit 7"}, nil, 60000)

		if state := f.mustFinish(t, runID); state != "failed" {
			t.Fatalf("run ended %s, want failed", state)
		}
		assertRowKeepsItsPromise(t, ctx, f, runID, reason.RUNFailedStep)
	})

	t.Run("a spent run deadline names the deadline", func(t *testing.T) {
		ctx := context.Background()
		f := newFixture(t)

		if _, _, err := f.Store.UpsertJobVersion(ctx, store.JobVersionInput{
			JobName:  "slowjob",
			SpecHash: "sha256:slowjob-promise",
			SpecJSON: fmt.Sprintf(`{"name":"slowjob","schema":"paceq.job.v1",
"steps":[{"name":"hangs","run":[%s],"shell":false}],
"timeout_ms":250}`, runArgv(f.fakeCmd(t), "sleep 30s")),
		}); err != nil {
			t.Fatalf("record the slow job: %v", err)
		}
		out, err := f.Store.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "slowjob"})
		if err != nil {
			t.Fatalf("materialise: %v", err)
		}
		if state := f.mustFinish(t, out.Run.ID); state != "failed" {
			t.Fatalf("run ended %s, want failed", state)
		}
		assertRowKeepsItsPromise(t, ctx, f, out.Run.ID, reason.RUNTimedOut)
	})
}

func assertRowKeepsItsPromise(t *testing.T, ctx context.Context, f *engineFixture,
	runID string, want reason.Code,
) {
	t.Helper()

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run %s: %v", runID, err)
	}
	if detail.ReasonCode != string(want) {
		t.Fatalf("reason_code = %q, want %q", detail.ReasonCode, want)
	}
	if missing := reason.MissingDataKeys(want, detail.ReasonData); len(missing) > 0 {
		t.Errorf("run %s carries %s with reason_data %q, which is missing the promised %v",
			runID, want, detail.ReasonData, missing)
	}
}
