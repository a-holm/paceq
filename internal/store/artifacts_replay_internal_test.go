package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// AC-13's side of the #13 work: replaying a run copies artifact REFERENCES
// into the new run and touches no content. The writer is a transaction-level
// helper because the copy must share the new run's creation transaction,
// which MaterializeReplay (issue #10, M4-04) owns. Until #10 lands this has
// exactly one caller: this file. That is the seam holding the place, not a
// parallel replay.

func seedPublishedRefs(t *testing.T, s *Store, runID string, arts []Artifact) {
	t.Helper()
	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		return insertArtifactsTx(tx, runID, "build", time.UnixMilli(1000), arts)
	})
	if err != nil {
		t.Fatalf("seed the references of %s: %v", runID, err)
	}
}

func TestCopyArtifactRefsCarriesTheReferencesAndNothingElse(t *testing.T) {
	s := internalStore(t)
	ctx := context.Background()
	version := aJobInternal(t, s, "nightly")

	steps := []NewStep{{Name: "build"}}
	orig, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual", Steps: steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual",
		ReplayOf: orig.ID, Steps: steps,
	})
	if err != nil {
		t.Fatal(err)
	}

	size := int64(4096)
	seedPublishedRefs(t, s, orig.ID, []Artifact{{
		Name: "raw", URI: "s3://bucket/raw.parquet", SizeBytes: &size,
		Checksum: "sha256:abc", MediaType: "application/x-parquet",
	}, {
		Name: "log", URI: "/tmp/run.log",
	}})

	at := time.UnixMilli(2000)
	var copied int
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		copied, err = CopyArtifactRefsTx(tx, orig.ID, replayed.ID, at)
		return err
	}); err != nil {
		t.Fatalf("copy the references: %v", err)
	}
	if copied != 2 {
		t.Fatalf("copied %d references, want 2", copied)
	}

	rows, err := s.RunsArtifacts(ctx, replayed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("the replayed run holds %d references, want 2", len(rows))
	}
	want := map[string]Artifact{
		"raw": {
			StepName: "build", Name: "raw", URI: "s3://bucket/raw.parquet",
			SizeBytes: &size, Checksum: "sha256:abc", MediaType: "application/x-parquet",
		},
		"log": {StepName: "build", Name: "log", URI: "/tmp/run.log"},
	}
	for _, r := range rows {
		w := want[r.Name]
		if r.URI != w.URI || r.StepName != w.StepName || r.MediaType != w.MediaType ||
			r.Checksum != w.Checksum {
			t.Errorf("reference %+v, want %+v", r, w)
		}
		if (r.SizeBytes == nil) != (w.SizeBytes == nil) ||
			(r.SizeBytes != nil && *r.SizeBytes != *w.SizeBytes) {
			t.Errorf("reference %s size = %v, want %v", r.Name, r.SizeBytes, w.SizeBytes)
		}
		if !r.CreatedAt.Equal(at) {
			t.Errorf("reference %s stamped %v, want the replay's stamp %v", r.Name, r.CreatedAt, at)
		}
	}

	origRows, err := s.RunsArtifacts(ctx, orig.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(origRows) != 2 {
		t.Errorf("the original run holds %d references after the copy, want 2 untouched", len(origRows))
	}
}

func TestCopyArtifactRefsCopiesNothingFromAnEmptyRun(t *testing.T) {
	s := internalStore(t)
	ctx := context.Background()
	version := aJobInternal(t, s, "nightly")

	steps := []NewStep{{Name: "build"}}
	orig, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual", Steps: steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual",
		ReplayOf: orig.ID, Steps: steps,
	})
	if err != nil {
		t.Fatal(err)
	}

	var copied int
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		copied, err = CopyArtifactRefsTx(tx, orig.ID, replayed.ID, time.UnixMilli(2000))
		return err
	}); err != nil {
		t.Fatalf("copy the references: %v", err)
	}
	if copied != 0 {
		t.Errorf("copied %d references out of an empty run", copied)
	}
}
