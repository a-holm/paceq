package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The claim predicate's own guarantees that need the writer handle: the retry
// gate the claim reads from the step row, and the transaction that keeps the
// step flip and its event from ever diverging. These two are why a claim is a
// reservation rather than a suggestion.

const internalDiamond = `{"max_concurrent":1,"name":"diamond","schema":"paceq.job.v1","timeout_ms":3600000,` +
	`"steps":[` +
	`{"name":"extract","run":["/bin/true"],"shell":false},` +
	`{"name":"transform","needs":["extract"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load-warehouse","needs":["transform"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load-cache","needs":["transform"],"run":["/bin/true"],"shell":false}` +
	`]}`

const internalOwner = "exec-i"

// claimableRun opens a migrated store, seeds the diamond job, materialises one
// manual run and claims it, returning the store, the run id and the epoch the
// claim is fenced on.
func claimableRun(t *testing.T) (*Store, string, int64) {
	t.Helper()
	s := testStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	version, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:       "diamond",
		SpecHash:      "sha256:diamond",
		SpecJSON:      internalDiamond,
		MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("seed the diamond job: %v", err)
	}
	_ = version
	out, err := s.MaterializeManualTrigger(context.Background(), ManualTriggerInput{JobName: "diamond"})
	if err != nil {
		t.Fatalf("materialise the diamond: %v", err)
	}
	state, epoch, err := s.ClaimRun(context.Background(), out.Run.ID, LeaseInput{Owner: internalOwner, TTL: time.Minute})
	if err != nil || state != "running" {
		t.Fatalf("claim the run: state=%q err=%v", state, err)
	}
	return s, out.Run.ID, epoch
}

// TestRetryGateIsInsideTheClaim: a pending step whose next_attempt_at lies in
// the future is not claimable; the claim reads the gate from the step row, so
// the retry scheduler lives in the predicate and nowhere else.
func TestRetryGateIsInsideTheClaim(t *testing.T) {
	s, runID, epoch := claimableRun(t)
	now := s.clk.Now().UTC()
	ref := LeaseRef{Owner: internalOwner, Epoch: epoch}

	if _, err := s.w.Exec(`UPDATE steps SET next_attempt_at = ? WHERE run_id = ? AND name = 'extract'`,
		now.Add(5*time.Minute).UnixMilli(), runID); err != nil {
		t.Fatalf("park extract: %v", err)
	}
	if c, err := s.ClaimNextStep(context.Background(), runID, ref); err != nil || c != nil {
		t.Fatalf("a future retry gate admitted a claim: %v %v", c, err)
	}

	if _, err := s.w.Exec(`UPDATE steps SET next_attempt_at = ? WHERE run_id = ? AND name = 'extract'`,
		now.Add(-1*time.Second).UnixMilli(), runID); err != nil {
		t.Fatalf("unpark extract: %v", err)
	}
	if c, err := s.ClaimNextStep(context.Background(), runID, ref); err != nil || c == nil || c.Name != "extract" {
		t.Fatalf("past the gate the claim admitted nothing: %v %v", c, err)
	}
}

// TestClaimIsAllOrNothing: the step flip and its step.started event share one
// BEGIN IMMEDIATE transaction. When the event write refuses, the flip must
// roll back with it, so a claim can never leave a running step whose start
// nobody recorded. The injection is the same trigger technique the transition
// tests use.
func TestClaimIsAllOrNothing(t *testing.T) {
	s, runID, epoch := claimableRun(t)
	ref := LeaseRef{Owner: internalOwner, Epoch: epoch}

	if _, err := s.w.Exec(abortEvents); err != nil {
		t.Fatalf("install the event-refusing trigger: %v", err)
	}
	defer dropTestTriggers(t, s)

	_, err := s.ClaimNextStep(context.Background(), runID, ref)
	if err == nil {
		t.Fatal("the claim went through despite the event write refusing")
	}
	if !strings.Contains(err.Error(), "injected") {
		t.Fatalf("error = %v, want the injected refusal named", err)
	}

	// Nothing survived: the step is still pending and the failed claim left
	// no started event, because the whole transaction rolled back.
	var state string
	if err := s.w.QueryRow(`SELECT state FROM steps WHERE run_id = ? AND name = 'extract'`, runID).Scan(&state); err != nil {
		t.Fatalf("read the step state: %v", err)
	}
	if state != "pending" {
		t.Fatalf("step state after a rolled-back claim = %q, want pending", state)
	}
	var started int
	if err := s.w.QueryRow(`SELECT count(*) FROM run_events WHERE run_id = ? AND kind = 'step.started'`, runID).Scan(&started); err != nil {
		t.Fatalf("count the step.started events: %v", err)
	}
	if started != 0 {
		t.Fatalf("step.started events after a rolled-back claim = %d, want none", started)
	}

	dropTestTriggers(t, s)
	if c, err := s.ClaimNextStep(context.Background(), runID, ref); err != nil || c == nil || c.Name != "extract" {
		t.Fatalf("claim with the trigger gone = %v %v, want extract", c, err)
	}
}
