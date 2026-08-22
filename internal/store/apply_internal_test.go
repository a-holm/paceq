package store

import (
	"context"
	"sync"
	"testing"
)

// applyInput is a small spec with the given hash, so a test can say "the same
// file" and "a changed file" without building a real one.
func applyInput(name, hash string) JobVersionInput {
	return JobVersionInput{
		JobName:  name,
		SpecHash: "sha256:" + hash,
		SpecJSON: `{"job":"` + name + `","hash":"` + hash + `"}`,
	}
}

// versionCount reads how many versions of a job the database holds.
func versionCount(t *testing.T, s *Store, job string) int {
	t.Helper()
	var n int
	if err := s.r.QueryRow("SELECT count(*) FROM job_versions WHERE job_name = ?",
		job).Scan(&n); err != nil {
		t.Fatalf("count the versions of %s: %v", job, err)
	}
	return n
}

// pointerID reads the version a job currently points at.
func pointerID(t *testing.T, s *Store, job string) string {
	t.Helper()
	var id string
	if err := s.r.QueryRow(
		"SELECT current_version_id FROM jobs WHERE name = ?", job).Scan(&id); err != nil {
		t.Fatalf("read the pointer of %s: %v", job, err)
	}
	return id
}

// versionIDByNumber reads the id of one version by its number, so a test can
// name "version 1" instead of a ULID it never chose.
func versionIDByNumber(t *testing.T, s *Store, job string, version int) string {
	t.Helper()
	var id string
	if err := s.r.QueryRow(
		"SELECT id FROM job_versions WHERE job_name = ? AND version = ?", job, version).Scan(&id); err != nil {
		t.Fatalf("read version %d of %s: %v", version, job, err)
	}
	return id
}

// TestApplyJobsTwiceWritesOneVersion is the whole point of the command: the
// second apply of an unchanged file changes nothing. Row counts stay still and
// the result says the version was already there.
func TestApplyJobsTwiceWritesOneVersion(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	first, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "aa")})
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first apply reported %d results, want 1", len(first))
	}
	if !first[0].Created || first[0].Version != 1 {
		t.Errorf("first apply: created=%v version=%d, want created=true version=1",
			first[0].Created, first[0].Version)
	}

	second, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "aa")})
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("second apply reported %d results, want 1", len(second))
	}
	if second[0].Created {
		t.Error("second apply of the same file reported a new version")
	}
	if second[0].Version != 1 || second[0].VersionID != first[0].VersionID {
		t.Errorf("second apply moved the version: id=%s version=%d, want id=%s version=1",
			second[0].VersionID, second[0].Version, first[0].VersionID)
	}
	if got := versionCount(t, s, "nightly"); got != 1 {
		t.Errorf("two applies left %d version rows, want 1", got)
	}
	if pointerID(t, s, "nightly") != first[0].VersionID {
		t.Error("the job does not point at the one version it has")
	}
}

// TestApplyJobsChangedSpecIsExactlyOneNewVersion: a changed hash lands as one
// new row with the next version number, and the pointer moves to it.
func TestApplyJobsChangedSpecIsExactlyOneNewVersion(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	if _, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "aa")}); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	results, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "bb")})
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(results) != 1 || !results[0].Created || results[0].Version != 2 {
		t.Fatalf("changed spec: %+v, want one result, created, version 2", results)
	}
	if got := versionCount(t, s, "nightly"); got != 2 {
		t.Errorf("two specs left %d version rows, want 2", got)
	}
	if pointerID(t, s, "nightly") != results[0].VersionID {
		t.Error("the job does not point at the newest version")
	}
}

// TestApplyJobsRollbackToAnOldSpecReusesItsVersion: apply A, then B, then A
// again. Three applies, two versions, and the pointer walks back to version 1.
// Re-loading an old file must reuse the row it already owns, never invent a
// third copy of A.
func TestApplyJobsRollbackToAnOldSpecReusesItsVersion(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	if _, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "aa")}); err != nil {
		t.Fatalf("apply A: %v", err)
	}
	if _, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "bb")}); err != nil {
		t.Fatalf("apply B: %v", err)
	}
	again, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "aa")})
	if err != nil {
		t.Fatalf("apply A again: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("apply A again reported %d results, want 1", len(again))
	}
	if again[0].Created {
		t.Error("re-applying the first spec created a version")
	}
	if got := versionCount(t, s, "nightly"); got != 2 {
		t.Errorf("A, B, A left %d version rows, want 2", got)
	}
	if pointerID(t, s, "nightly") != versionIDByNumber(t, s, "nightly", 1) {
		t.Error("after rolling back to A the pointer does not name version 1")
	}
}

// TestApplyJobsIsAllOrNothing: one bad input in the batch must leave nothing
// behind, not even the jobs that parsed fine. A partial load is the one state
// apply exists to make impossible.
//
// max_concurrent -1 is refused by the CHECK on the jobs table, which is a
// deterministic failure in the middle of the batch.
func TestApplyJobsIsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	bad := applyInput("broken", "cc")
	bad.MaxConcurrent = -1
	if _, err := s.ApplyJobs(ctx, []JobVersionInput{
		applyInput("healthy", "aa"),
		bad,
	}); err == nil {
		t.Fatal("a batch with a bad job applied without an error")
	}

	if got := versionCount(t, s, "healthy"); got != 0 {
		t.Errorf("the failed batch left %d version rows for the healthy job, want 0", got)
	}
	var jobs int
	if err := s.r.QueryRow("SELECT count(*) FROM jobs").Scan(&jobs); err != nil {
		t.Fatalf("count the jobs: %v", err)
	}
	if jobs != 0 {
		t.Errorf("the failed batch left %d job rows, want 0", jobs)
	}
}

// TestApplyJobsRacingTheSameFileLandsInOneState: two applies of the same file
// at the same time must end in one version row and one pointer, whichever
// commit lands first. The UNIQUE key decides, not the schedule.
func TestApplyJobsRacingTheSameFileLandsInOneState(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	const racers = 2
	results := make([][]JobApplyResult, racers)
	errs := make([]error, racers)
	var ready sync.WaitGroup
	ready.Add(racers)
	var goAhead sync.WaitGroup
	goAhead.Add(1)
	// done gives the main goroutine a happens-before edge on every racer's
	// writes to its results/errs slot. Reading those slots before the racers
	// have returned is a data race, even though each racer touches only its
	// own index.
	var done sync.WaitGroup
	done.Add(racers)
	for i := 0; i < racers; i++ {
		go func(i int) {
			defer done.Done()
			ready.Done()
			goAhead.Wait()
			results[i], errs[i] = s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "aa")})
		}(i)
	}
	ready.Wait()
	goAhead.Done()
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("racing apply %d: %v", i, err)
		}
	}
	if got := versionCount(t, s, "nightly"); got != 1 {
		t.Errorf("%d racing applies left %d version rows, want 1", racers, got)
	}
	pointer := pointerID(t, s, "nightly")
	created := 0
	for i, res := range results {
		if len(res) != 1 {
			t.Fatalf("racing apply %d reported %d results, want 1", i, len(res))
		}
		if res[0].Created {
			created++
		}
		if res[0].Version != 1 || res[0].VersionID != pointer {
			t.Errorf("racing apply %d reported id=%s version=%d, want id=%s version=1",
				i, res[0].VersionID, res[0].Version, pointer)
		}
	}
	if created != 1 {
		t.Errorf("%d racing applies reported %d created versions, want exactly 1", racers, created)
	}
}

// TestListJobVersionsReadsNewestFirst pins the read the CLI reports and a
// future history view stand on: every version of the named job, newest first,
// and nothing from any other job.
func TestListJobVersionsReadsNewestFirst(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	if _, err := s.ApplyJobs(ctx, []JobVersionInput{
		applyInput("nightly", "aa"),
		applyInput("other", "zz"),
	}); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if _, err := s.ApplyJobs(ctx, []JobVersionInput{applyInput("nightly", "bb")}); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	versions, err := s.ListJobVersions(ctx, "nightly")
	if err != nil {
		t.Fatalf("list the versions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("the list holds %d versions, want 2", len(versions))
	}
	if versions[0].Version != 2 || versions[1].Version != 1 {
		t.Errorf("the list is ordered [%d, %d], want [2, 1]",
			versions[0].Version, versions[1].Version)
	}
	if versions[0].SpecHash != "sha256:bb" || versions[1].SpecHash != "sha256:aa" {
		t.Errorf("the hashes came back as %s and %s, want bb then aa",
			versions[0].SpecHash, versions[1].SpecHash)
	}
}

// TestApplyJobsLeavesEveryPointerOnARealVersion: after any successful apply,
// every job points at a version row that exists. The foreign key is deferred,
// so the invariant is checked here rather than trusted from the schema.
func TestApplyJobsLeavesEveryPointerOnARealVersion(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	inputs := []JobVersionInput{applyInput("one", "aa"), applyInput("two", "bb")}
	for round := 0; round < 2; round++ {
		if _, err := s.ApplyJobs(ctx, inputs); err != nil {
			t.Fatalf("apply round %d: %v", round, err)
		}
		var dangling int
		if err := s.r.QueryRow(`SELECT count(*) FROM jobs
WHERE current_version_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM job_versions v WHERE v.id = jobs.current_version_id)`).
			Scan(&dangling); err != nil {
			t.Fatalf("look for dangling pointers: %v", err)
		}
		if dangling != 0 {
			t.Fatalf("round %d left %d jobs pointing at a version that does not exist", round, dangling)
		}
	}
}
