package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The publication contract (#13): rows land in the SAME transaction as the
// step verdict, a failed or refused verdict publishes nothing, and a name
// collision between steps of one run resolves to the highest index.

type publishFixture struct {
	s       *store.Store
	ctx     context.Context
	runID   string
	ref     store.LeaseRef
	steps   []string
	version store.JobVersion
}

func newPublishFixture(t *testing.T, steps ...store.NewStep) *publishFixture {
	t.Helper()
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		Steps:        steps,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	_, epoch, err := s.ClaimRun(ctx, run.ID, store.LeaseInput{Owner: "tester", TTL: time.Minute})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	names := make([]string, 0, len(steps))
	for _, st := range steps {
		names = append(names, st.Name)
	}
	return &publishFixture{
		s: s, ctx: ctx, runID: run.ID,
		ref:     store.LeaseRef{Owner: "tester", Epoch: epoch},
		steps:   names,
		version: version,
	}
}

func (f *publishFixture) start(name string) {
	if err := f.s.StartStep(f.ctx, f.runID, name, f.ref); err != nil {
		panic(err)
	}
}

func aRef(name, uri string) store.Artifact {
	return store.Artifact{StepName: "irrelevant", Name: name, URI: uri}
}

func TestSucceededStepPublishesItsArtifacts(t *testing.T) {
	f := newPublishFixture(t, twoSteps()...)
	f.start("build")

	size := int64(4096)
	err := f.s.RecordStepOutcome(f.ctx, f.runID, "build", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		FinishedAt: time.UnixMilli(1000),
		Artifacts: []store.Artifact{
			{
				Name:      "raw",
				URI:       "file:///data/raw.parquet",
				SizeBytes: &size,
				Checksum:  "sha256:abc",
				MediaType: "application/x-parquet",
			},
		},
	}, f.ref)
	if err != nil {
		t.Fatalf("record: %v", err)
	}

	detail, err := f.s.GetRun(f.ctx, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	var got []store.Artifact
	for _, st := range detail.Steps {
		if st.Name == "build" {
			got = st.Artifacts
		}
	}
	if len(got) != 1 {
		t.Fatalf("build published %d refs, want 1", len(got))
	}
	a := got[0]
	if a.StepName != "build" || a.Name != "raw" || a.URI != "file:///data/raw.parquet" {
		t.Errorf("ref = %+v", a)
	}
	if a.SizeBytes == nil || *a.SizeBytes != 4096 || a.Checksum != "sha256:abc" {
		t.Errorf("facts = %+v", a)
	}
	if a.MediaType != "application/x-parquet" {
		t.Errorf("media_type = %q, want it carried out of meta_json", a.MediaType)
	}
	if a.CreatedAt.IsZero() {
		t.Error("created_at was never set")
	}
}

func TestRunsArtifactsListsEveryPublishedReference(t *testing.T) {
	f := newPublishFixture(t, twoSteps()...)
	for _, step := range []string{"build", "test"} {
		f.start(step)
		out := store.StepOutcome{
			Event:      "step_succeeded",
			ReasonCode: reason.STEPSucceeded,
			FinishedAt: time.UnixMilli(1000),
			Artifacts:  []store.Artifact{aRef(step+"-out", "/"+step+".bin")},
		}
		if err := f.s.RecordStepOutcome(f.ctx, f.runID, step, out, f.ref); err != nil {
			t.Fatalf("record %s: %v", step, err)
		}
	}
	rows, err := f.s.RunsArtifacts(f.ctx, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// Spec order first, name within the step: the listing is deterministic.
	if rows[0].StepName != "build" || rows[0].Name != "build-out" ||
		rows[1].StepName != "test" || rows[1].Name != "test-out" {
		t.Errorf("rows = %+v", rows)
	}
}

func TestArtifactRowsShareTheVerdictTransaction(t *testing.T) {
	f := newPublishFixture(t, twoSteps()...)
	f.start("build")

	stranger := store.LeaseRef{Owner: "someone-else", Epoch: 99}
	err := f.s.RecordStepOutcome(f.ctx, f.runID, "build", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		Artifacts:  []store.Artifact{aRef("raw", "/x")},
	}, stranger)
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
	rows, err := f.s.RunsArtifacts(f.ctx, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("a refused verdict left %d artifact rows behind; they did not share its transaction", len(rows))
	}
}

func TestAFailedStepNeverPublishes(t *testing.T) {
	f := newPublishFixture(t, twoSteps()...)
	f.start("build")

	err := f.s.RecordStepOutcome(f.ctx, f.runID, "build", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   ptrInt(1),
		Artifacts:  []store.Artifact{aRef("raw", "/x")},
	}, f.ref)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	rows, err := f.s.RunsArtifacts(f.ctx, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("a failed step published %d rows; only success publishes", len(rows))
	}
}

func TestHighestIndexStepWinsANameCollision(t *testing.T) {
	f := newPublishFixture(t, twoSteps()...)

	// The later step (higher idx) publishes first; the earlier one then
	// tries to claim the same name. The stored row must stay the later
	// step's either way.
	f.start("test")
	if err := f.s.RecordStepOutcome(f.ctx, f.runID, "test", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		Artifacts:  []store.Artifact{aRef("shared", "/from-test")},
	}, f.ref); err != nil {
		t.Fatal(err)
	}
	f.start("build")
	if err := f.s.RecordStepOutcome(f.ctx, f.runID, "build", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		Artifacts:  []store.Artifact{aRef("shared", "/from-build")},
	}, f.ref); err != nil {
		t.Fatal(err)
	}

	rows, err := f.s.RunsArtifacts(f.ctx, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want exactly one survivor", len(rows))
	}
	if rows[0].StepName != "test" || rows[0].URI != "/from-test" {
		t.Errorf("survivor = %+v, want the highest-index publisher", rows[0])
	}
}

func ptrInt(i int) *int { return &i }
