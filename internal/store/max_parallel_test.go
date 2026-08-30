package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// max_parallel is the per-run step budget the operator writes in the job
// file. The claim predicate reads it off the run row, so the run row has to
// carry what the file said; every test here drives the real materialisation
// and the real gate, never a run row steered by hand.

// rootsSpecWithCap builds a canonical job of n independent steps. cap is the
// declared max_parallel; zero leaves the key out, which is what a file that
// says nothing produces.
func rootsSpecWithCap(name string, n, cap int) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"max_concurrent":1,`)
	if cap > 0 {
		fmt.Fprintf(&b, `"max_parallel":%d,`, cap)
	}
	fmt.Fprintf(&b, `"name":%q,"schema":"paceq.job.v1","timeout_ms":3600000,"steps":[`, name)
	for i := range n {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"name":"r%d","run":["/bin/true"],"shell":false}`, i)
	}
	b.WriteString(`]}`)
	return b.String()
}

// aRunOfCappedJob seeds a job of n independent steps declaring cap, then
// materialises and claims one run of it.
func aRunOfCappedJob(t *testing.T, s *store.Store, name string, n, cap int) (string, int64) {
	t.Helper()
	ctx := context.Background()
	aCanonicalJob(t, s, name, rootsSpecWithCap(name, n, cap))
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: name})
	if err != nil {
		t.Fatalf("materialise a run of %s: %v", name, err)
	}
	state, epoch, err := s.ClaimRun(ctx, out.Run.ID, store.LeaseInput{Owner: testOwner})
	if err != nil || state != "running" {
		t.Fatalf("claim the run of %s: state=%q err=%v", name, state, err)
	}
	return out.Run.ID, epoch
}

// peakRunningSteps claims as long as the gate admits anything and reports the
// highest number of steps in state running it ever saw at once. Nothing is
// ever finished, so the count only grows: the peak is what the gate let in.
func peakRunningSteps(t *testing.T, s *store.Store, runID string, epoch int64, tries int) int {
	t.Helper()
	ctx := context.Background()
	peak := 0
	for range tries {
		c, err := s.ClaimNextStep(ctx, runID, store.LeaseRef{Owner: testOwner, Epoch: epoch})
		if err != nil {
			t.Fatalf("claim a step of %s: %v", runID, err)
		}
		if c == nil {
			break
		}
		detail, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("read run %s: %v", runID, err)
		}
		running := 0
		for _, st := range detail.Steps {
			if st.State == "running" {
				running++
			}
		}
		if running > peak {
			peak = running
		}
	}
	return peak
}

// TestDeclaredMaxParallelBoundsTheRunningSteps is the whole point of the
// field: what the file declares is what the gate admits. The cap is never set
// by hand here, so the run row has to have been written from the frozen spec.
func TestDeclaredMaxParallelBoundsTheRunningSteps(t *testing.T) {
	cases := []struct {
		name  string
		steps int
		cap   int
	}{
		{"serial", 3, 1},
		{"pairs", 4, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := coreStore(t)
			runID, epoch := aRunOfCappedJob(t, s, tc.name, tc.steps, tc.cap)
			peak := peakRunningSteps(t, s, runID, epoch, tc.steps+1)
			if peak != tc.cap {
				t.Fatalf("peak running steps = %d, want exactly %d (max_parallel: %d)",
					peak, tc.cap, tc.cap)
			}
		})
	}
}

// TestMaterialisedRunCarriesTheDeclaredMaxParallel reads the cap back off the
// claim, which is where the executor reads it: the floor, a wide fan-out, and
// the default a job that says nothing gets.
func TestMaterialisedRunCarriesTheDeclaredMaxParallel(t *testing.T) {
	cases := []struct {
		name     string
		declared int
		want     int
	}{
		{"floor", 1, 1},
		{"wide", 16, 16},
		{"unsaid", 0, spec.DefaultMaxParallel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := coreStore(t)
			runID, epoch := aRunOfCappedJob(t, s, tc.name, 1, tc.declared)
			c, err := s.ClaimNextStep(context.Background(), runID,
				store.LeaseRef{Owner: testOwner, Epoch: epoch})
			if err != nil || c == nil {
				t.Fatalf("claim the only step = %v err=%v", c, err)
			}
			if c.MaxParallel != tc.want {
				t.Fatalf("the run's parallel cap = %d, want %d", c.MaxParallel, tc.want)
			}
		})
	}
}

// TestReplayCarriesTheParallelCapOfItsFrozenVersion: a replay reproduces the
// run it was made from, so its budget is the one the source froze, never the
// one the job file carries now.
func TestReplayCarriesTheParallelCapOfItsFrozenVersion(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)

	aCanonicalJob(t, s, "replaycap", rootsSpecWithCap("replaycap", 1, 2))
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "replaycap"})
	if err != nil {
		t.Fatalf("materialise the source: %v", err)
	}
	srcID := out.Run.ID
	if _, _, err := s.ClaimRun(ctx, srcID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim the source: %v", err)
	}
	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}
	if err := s.StartStep(ctx, srcID, "r0", ref); err != nil {
		t.Fatalf("start the only step: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, srcID, "r0", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
		ExitCode: ptr(0), FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("succeed the only step: %v", err)
	}
	if _, err := s.FinishRun(ctx, srcID, ref,
		store.FinishReason{Code: reason.RUNSucceeded, Data: "{}"}); err != nil {
		t.Fatalf("finish the source: %v", err)
	}

	// The file widens after the source ran. The replay must not notice.
	if _, created, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       "replaycap",
		SpecHash:      "sha256:replaycap-wider",
		SpecJSON:      rootsSpecWithCap("replaycap", 1, 8),
		MaxConcurrent: 1,
	}); err != nil || !created {
		t.Fatalf("apply the wider spec: created=%v err=%v", created, err)
	}

	replay, err := s.MaterializeReplay(ctx, srcID, store.ReplayOpts{})
	if err != nil {
		t.Fatalf("replay the source: %v", err)
	}
	_, epoch, err := s.ClaimRun(ctx, replay.NewRunID, store.LeaseInput{Owner: testOwner})
	if err != nil {
		t.Fatalf("claim the replay: %v", err)
	}
	c, err := s.ClaimNextStep(ctx, replay.NewRunID, store.LeaseRef{Owner: testOwner, Epoch: epoch})
	if err != nil || c == nil {
		t.Fatalf("claim the replayed step = %v err=%v", c, err)
	}
	if c.MaxParallel != 2 {
		t.Fatalf("the replay's parallel cap = %d, want the frozen 2", c.MaxParallel)
	}
}
