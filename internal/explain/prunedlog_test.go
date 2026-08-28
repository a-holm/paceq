package explain

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The pruned-log guarantee (#44): the byte cap may remove the files, but the
// report must still say what an attempt printed last, and it must say that
// the file is gone rather than letting the absence read as "never logged".

func TestThePrunedLogStillExplainsItself(t *testing.T) {
	ctx := context.Background()
	st := fixtureStore(t)
	if _, _, serr := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "pruned",
		SpecHash: "sha256:pruned",
		SpecJSON: `{"schema":"paceq.job.v1","name":"pruned","max_concurrent":1,"steps":[{"name":"build","run":["/bin/true"]}]}`,
	}); serr != nil {
		t.Fatalf("seed the job: %v", serr)
	}
	queued, err := st.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "pruned", Actor: "test"})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	token, epoch, err := st.ClaimRun(ctx, queued.Run.ID, store.LeaseInput{Owner: "exec-prune", TTL: store.DefaultRunLeaseTTL})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: "exec-prune", Epoch: epoch}
	if token == "" {
		t.Fatal("the claim minted no token")
	}
	if err := st.StartStep(ctx, queued.Run.ID, "build", ref); err != nil {
		t.Fatalf("start the step: %v", err)
	}
	if err := st.RecordStepOutcome(ctx, queued.Run.ID, "build", store.StepOutcome{
		Event:      string(model.EvStepFailed),
		ReasonCode: reason.STEPFailedNonzeroExit,
		FinishedAt: frozenNow,
		LogMeta: store.LogMeta{
			RelPath:   "2026-08-19/" + queued.Run.ID + "/build.1.ndjson",
			Bytes:     4096,
			Truncated: true,
			ErrorTail: "panic: the disk ate the build",
		},
	}, ref); err != nil {
		t.Fatalf("fail the step: %v", err)
	}
	if _, err := st.FinishRun(ctx, queued.Run.ID, ref, store.FinishReason{Code: reason.RUNFailedStep}); err != nil {
		t.Fatalf("finish the run: %v", err)
	}

	// The step row names a file in the 2026-08-19 shard; the shard goes.
	if _, err := st.MarkLogShardPruned(ctx, "2026-08-19"); err != nil {
		t.Fatalf("prune the shard: %v", err)
	}
	detail, err := st.GetRun(ctx, queued.Run.ID)
	if err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	if detail.Steps[0].LogPath != "" {
		t.Fatalf("the step still names %q after its shard was removed", detail.Steps[0].LogPath)
	}
	if detail.Steps[0].ErrorTail != "panic: the disk ate the build" {
		t.Fatalf("the pruning lost the error tail: %q", detail.Steps[0].ErrorTail)
	}

	res, err := Resolve(ctx, st, "run/"+queued.Run.ID)
	if err != nil {
		t.Fatalf("resolve the run: %v", err)
	}
	report, err := Build(ctx, st, res, Options{Since: frozenNow.Add(-48 * time.Hour), Clock: clock.NewFake(frozenNow)})
	if err != nil {
		t.Fatalf("build the report: %v", err)
	}
	run := report.Entries[0]
	var step *Entry
	for i := range run.Children {
		if run.Children[i].Kind == "step" {
			step = &run.Children[i]
			break
		}
	}
	if step == nil {
		t.Fatalf("the report has no step child: %+v", run)
	}
	if step.ReasonData["log_pruned"] != true {
		t.Errorf("the step does not say its log was pruned: %v", step.ReasonData)
	}
	if step.ReasonData["error_tail"] != "panic: the disk ate the build" {
		t.Errorf("the report lost the error tail: %v", step.ReasonData["error_tail"])
	}
	if got := step.ReasonData["log_bytes"]; got == nil {
		t.Errorf("the report lost the log size: %v", step.ReasonData)
	}

	var out bytes.Buffer
	RenderText(&out, report, Style{})
	text := out.String()
	if !strings.Contains(text, "the log file was pruned away") {
		t.Errorf("the text report does not name the pruning:\n%s", text)
	}
	if !strings.Contains(text, "panic: the disk ate the build") {
		t.Errorf("the text report lost the error tail:\n%s", text)
	}
}
