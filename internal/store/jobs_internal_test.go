package store

import (
	"context"
	"testing"
)

// TestUpsertJobVersionLeavesAPausedJobPaused reads the column directly, because
// pausing is an operator action that no method in this milestone performs and
// the invariant is worth pinning before one does.
//
// Reloading a spec must not resume a job somebody paused. The pause is a
// statement about the world, not about the file, and a reload that undid it
// would start jobs an operator deliberately stopped.
func TestUpsertJobVersionLeavesAPausedJobPaused(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	in := JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:aa",
		SpecJSON: `{"steps":[]}`,
	}
	if _, _, err := s.UpsertJobVersion(ctx, in); err != nil {
		t.Fatalf("first load: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, "UPDATE jobs SET paused = 1 WHERE name = 'nightly'"); err != nil {
		t.Fatalf("pause the job: %v", err)
	}

	in.SpecHash = "sha256:bb"
	in.SpecJSON = `{"steps":[{"name":"build"}]}`
	if _, _, err := s.UpsertJobVersion(ctx, in); err != nil {
		t.Fatalf("load a changed spec: %v", err)
	}

	var paused int
	if err := s.w.QueryRowContext(ctx,
		"SELECT paused FROM jobs WHERE name = 'nightly'").Scan(&paused); err != nil {
		t.Fatalf("read the job back: %v", err)
	}
	if paused != 1 {
		t.Error("reloading the spec resumed a paused job")
	}
}

// TestCreateRunWithStepsWritesItsEventAndEdges reads the tables the public API
// does not expose yet: the frozen dependency edges M4-02 will claim against,
// and the queued event explain is built on. Both are written in the same
// transaction as the run, and nothing outside this package can see them.
func TestCreateRunWithStepsWritesItsEventAndEdges(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	version, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName: "nightly", SpecHash: "sha256:aa", SpecJSON: `{"steps":[]}`,
	})
	if err != nil {
		t.Fatalf("record the job: %v", err)
	}

	run, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "cli:1000",
		Steps: []NewStep{
			{Name: "build"},
			{Name: "test", DependsOn: []string{"build"}},
			{Name: "deploy", DependsOn: []string{"build", "test"}},
		},
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	var edges int
	if err := s.w.QueryRowContext(ctx,
		"SELECT count(*) FROM step_deps WHERE run_id = ?", run.ID).Scan(&edges); err != nil {
		t.Fatalf("count the frozen edges: %v", err)
	}
	if edges != 3 {
		t.Errorf("the run froze %d edges, want 3", edges)
	}

	var kind, toState, actor string
	if err := s.w.QueryRowContext(ctx,
		"SELECT kind, to_state, actor FROM run_events WHERE run_id = ? ORDER BY id", run.ID).
		Scan(&kind, &toState, &actor); err != nil {
		t.Fatalf("read the queued event: %v", err)
	}
	if kind != "run.queued" || toState != "queued" || actor != "cli:1000" {
		t.Errorf("the queued event is (%s, %s, %s), want (run.queued, queued, cli:1000)",
			kind, toState, actor)
	}
}
