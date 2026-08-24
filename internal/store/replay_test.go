package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// M4-04: replay. One command makes a NEW run out of an old one: a new id,
// replay_of pointing back, no run_key of its own, and the same frozen
// job_version_id the source ran, so an apply that landed in between cannot
// change what the replay does. Steps named by --from reuse their upstream
// closure from the frozen graph as succeeded references with
// STEP_SKIPPED_REPLAY_REUSED and copied artifact rows; --failed reuses every
// step that succeeded in the source. Everything else runs again.

func TestReplayMakesANewRunFromTheFrozenVersion(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "replaychain", retryChainSpec)
	driveChain(t, ctx, s, clk, srcID, "c")
	src := mustGetRun(t, ctx, s, srcID)

	out, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{Actor: "cli:1000"})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if out.NewRunID == "" || out.NewRunID == srcID {
		t.Fatalf("new run id = %q, want a fresh id beside %q", out.NewRunID, srcID)
	}

	replay := mustGetRun(t, ctx, s, out.NewRunID)
	if replay.ReplayOf != srcID {
		t.Errorf("replay_of = %q, want %q", replay.ReplayOf, srcID)
	}
	if replay.RunKey != "" {
		t.Errorf("run_key = %q, want none: a replay is never deduped against anything", replay.RunKey)
	}
	if replay.JobVersionID != src.JobVersionID {
		t.Errorf("job_version_id = %q, want the source's frozen %q",
			replay.JobVersionID, src.JobVersionID)
	}
	if replay.State != "queued" {
		t.Errorf("state = %s, want queued so the claim loop takes it", replay.State)
	}
	if replay.Origin != "replay" {
		t.Errorf("origin = %s, want replay", replay.Origin)
	}
	if replay.JobName != src.JobName {
		t.Errorf("job = %q, want %q", replay.JobName, src.JobName)
	}
}

// TestReplayUsesTheFrozenGraphNotTheCurrentOne is AC-7 at the record level:
// the job's definition changes after the source ran, and the replay still
// carries exactly the source's steps and edges, byte for byte.
func TestReplayUsesTheFrozenGraphNotTheCurrentOne(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "frozen", retryChainSpec)
	driveChain(t, ctx, s, clk, srcID, "c")

	srcDeps, err := s.StepDeps(ctx, srcID)
	if err != nil {
		t.Fatalf("read the source edges: %v", err)
	}

	// A new version of the same job lands: different steps, different edges.
	aCanonicalJob(t, s, "frozen", isolatedSkipSpec)

	out, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{})
	if err != nil {
		t.Fatalf("replay past an apply: %v", err)
	}

	replay := mustGetRun(t, ctx, s, out.NewRunID)
	src := mustGetRun(t, ctx, s, srcID)
	if replay.JobVersionID != src.JobVersionID {
		t.Fatalf("the replay took version %q, want the frozen %q",
			replay.JobVersionID, src.JobVersionID)
	}
	replayDeps, err := s.StepDeps(ctx, out.NewRunID)
	if err != nil {
		t.Fatalf("read the replay edges: %v", err)
	}
	if len(replayDeps) != len(srcDeps) {
		t.Fatalf("the replay froze %d edges, want the source's %d",
			len(replayDeps), len(srcDeps))
	}
	for i, d := range srcDeps {
		r := replayDeps[i]
		if r.StepName != d.StepName || r.DependsOn != d.DependsOn {
			t.Errorf("edge %d = %s <- %s, want the source's %s <- %s",
				i, r.StepName, r.DependsOn, d.StepName, d.DependsOn)
		}
	}

	names := map[string]bool{}
	for _, st := range mustGetRun(t, ctx, s, out.NewRunID).Steps {
		names[st.Name] = true
	}
	for _, want := range []string{"a", "b", "c", "d", "e"} {
		if !names[want] {
			t.Errorf("the replay has no step %s: the current spec leaked in", want)
		}
	}
	if names["p"] || names["q"] {
		t.Error("steps from the newer spec appear in a replay of the older one")
	}
}

// TestReplayFromMaterializesUpstreamAsSucceeded is AC-8 on a diamond:
// replay --from w reuses x, y and z (everything w sits on top of), each as
// succeeded with STEP_SKIPPED_REPLAY_REUSED, and copies the artifact rows the
// reused steps produced. w itself reruns.
func TestReplayFromMaterializesUpstreamAsSucceeded(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "diamond", diamondSkipSpec)

	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}
	if _, _, err := s.ClaimRun(ctx, srcID, store.LeaseInput{Owner: testOwner, TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, step := range []string{"x", "y", "z"} {
		if err := s.StartStep(ctx, srcID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		if err := s.RecordStepOutcome(ctx, srcID, step, store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: ptr(0), FinishedAt: clk.Now(),
		}, ref); err != nil {
			t.Fatalf("succeed %s: %v", step, err)
		}
	}
	if err := s.InjectArtifact(ctx, srcID, "y", "report.csv", "file:///out/report.csv", "sha256:abc"); err != nil {
		t.Fatalf("plant an artifact: %v", err)
	}
	if err := s.StartStep(ctx, srcID, "w", ref); err != nil {
		t.Fatalf("start w: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, srcID, "w", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode: ptr(9), FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("fail w: %v", err)
	}
	if _, err := s.FinishRun(ctx, srcID, ref, store.FinishReason{
		Code: reason.RUNFailedStep, Data: `{"step":"w"}`,
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	from := "w"
	out, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{From: &from})
	if err != nil {
		t.Fatalf("replay --from w: %v", err)
	}
	if got := out.Reused; len(got) != 3 || got[0] != "x" || got[1] != "y" || got[2] != "z" {
		t.Errorf("reused = %v, want exactly [x y z]", got)
	}
	if got := out.Rerun; len(got) != 1 || got[0] != "w" {
		t.Errorf("rerun = %v, want exactly [w]", got)
	}

	states := stepStates(t, ctx, s, out.NewRunID)
	for _, name := range []string{"x", "y", "z"} {
		if states[name].state != "succeeded" {
			t.Errorf("%s = %s, want materialized succeeded", name, states[name].state)
		}
	}
	if states["w"].state != "pending" {
		t.Errorf("w = %s, want pending for its real attempt", states["w"].state)
	}
	step := mustStep(t, ctx, s, out.NewRunID, "y")
	if step.ReasonCode != string(reason.STEPSkippedReplayReused) {
		t.Errorf("y reason = %q, want %s", step.ReasonCode, reason.STEPSkippedReplayReused)
	}

	arts, err := s.ArtifactsOf(ctx, out.NewRunID)
	if err != nil {
		t.Fatalf("read the replay's artifacts: %v", err)
	}
	if len(arts) != 1 {
		t.Fatalf("the replay carries %d artifacts, want the copied one", len(arts))
	}
	a := arts[0]
	if a.StepName != "y" || a.Name != "report.csv" ||
		a.URI != "file:///out/report.csv" || a.Checksum != "sha256:abc" {
		t.Errorf("copied artifact = %+v, want y/report.csv with the source's uri and checksum", a)
	}
}

// TestReplayFromTakesOnlyTheUpstreamDirection pins which way the closure
// walks: --from x, a root step, spares nothing, because nothing sits above
// it. Everything runs again. Had the closure walked downstream instead, y, z
// or w would have turned up spared.
func TestReplayFromTakesOnlyTheUpstreamDirection(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "diamond", diamondSkipSpec)
	driveChainDiamond(t, ctx, s, clk, srcID)

	from := "x"
	out, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{From: &from})
	if err != nil {
		t.Fatalf("replay --from x: %v", err)
	}
	if got := out.Reused; len(got) != 0 {
		t.Errorf("reused = %v, want none: a root step has no upstream", got)
	}
	if got := out.Rerun; len(got) != 4 || got[0] != "x" || got[1] != "y" || got[2] != "z" || got[3] != "w" {
		t.Errorf("rerun = %v, want exactly [x y z w]", got)
	}
}

// TestReplayFailedReusesEverySucceededStep is AC-9: --failed reuses all and
// only the steps that succeeded in the source, whatever their position.
func TestReplayFailedReusesEverySucceededStep(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "replaychain", retryChainSpec)
	driveChain(t, ctx, s, clk, srcID, "c")

	out, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{FailedOnly: true})
	if err != nil {
		t.Fatalf("replay --failed: %v", err)
	}
	if got := out.Reused; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("reused = %v, want exactly [a b]", got)
	}
	if got := out.Rerun; len(got) != 3 || got[0] != "c" || got[1] != "d" || got[2] != "e" {
		t.Errorf("rerun = %v, want exactly [c d e]", got)
	}

	states := stepStates(t, ctx, s, out.NewRunID)
	if states["a"].state != "succeeded" || states["b"].state != "succeeded" {
		t.Errorf("reused steps are %s/%s, want both succeeded",
			states["a"].state, states["b"].state)
	}
	for _, name := range []string{"c", "d", "e"} {
		if states[name].state != "pending" {
			t.Errorf("%s = %s, want pending", name, states[name].state)
		}
	}
}

// TestReplayNeverTouchesRunKeysOrCopiesTheKey is AC-13: a source that carries
// a run_key replays fine, the copy has none, and the run_keys table is
// untouched row for row.
func TestReplayNeverTouchesRunKeysOrCopiesTheKey(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "deduped", retryChainSpec)
	driveChain(t, ctx, s, clk, srcID, "")

	if err := s.InjectRunKey(ctx, srcID, "sensor-dropzone", "rk-2026-08-24"); err != nil {
		t.Fatalf("plant a run key: %v", err)
	}
	before, err := s.RunKeysSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot run_keys: %v", err)
	}

	out, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{})
	if err != nil {
		t.Fatalf("replay of a deduped run: %v", err)
	}
	if got := mustGetRun(t, ctx, s, out.NewRunID).RunKey; got != "" {
		t.Errorf("the replay carries run_key %q, want none", got)
	}
	after, err := s.RunKeysSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot run_keys: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("run_keys went from %d rows to %d across a replay",
			len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("run_keys row %d changed: %q -> %q", i, before[i], after[i])
		}
	}
}

// TestReplayRefusesAnUnknownFromStep names a step the source never had.
func TestReplayRefusesAnUnknownFromStep(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "diamond", diamondSkipSpec)
	driveChainDiamond(t, ctx, s, clk, srcID)

	from := "nosuch"
	if _, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{From: &from}); !errors.Is(err, store.ErrStepNotInThisRun) {
		t.Fatalf("replay --from nosuch = %v, want ErrStepNotInThisRun", err)
	}
}

// TestReplayedRunSweepsClean is AC-12's replay half: fsck holds right after
// the materialization, while the new run waits to be claimed.
func TestReplayedRunSweepsClean(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	srcID := aDagRun(t, s, "replaychain", retryChainSpec)
	driveChain(t, ctx, s, clk, srcID, "c")

	if _, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("fsck after a replay: %+v", violations)
	}
}

// driveChainDiamond succeeds every step of the diamond spec, ending with a
// terminal succeeded run.
func driveChainDiamond(t *testing.T, ctx context.Context, s *store.Store,
	clk interface{ Now() time.Time }, runID string,
) {
	t.Helper()

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}
	for _, step := range []string{"x", "y", "z", "w"} {
		if err := s.StartStep(ctx, runID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		if err := s.RecordStepOutcome(ctx, runID, step, store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: ptr(0),
		}, ref); err != nil {
			t.Fatalf("succeed %s: %v", step, err)
		}
	}
	if _, err := s.FinishRun(ctx, runID, ref, store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish: %v", err)
	}
}
