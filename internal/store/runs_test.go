package store_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// coreStore is a migrated store on a real file, driven by a fake clock so every
// timestamp in a test is a stated one. The clock also makes ids share a
// timestamp prefix, which is what the prefix lookup cases need.
func coreStore(t *testing.T) (*store.Store, *clock.Fake) {
	t.Helper()

	clk := testutil.NewClock(t)
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(context.Background(), path, store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store at %q: %v", path, err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, clk
}

// aJob records one job with one version and returns that version.
func aJob(t *testing.T, s *store.Store, name string) store.JobVersion {
	t.Helper()

	version, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:     name,
		Description: "the " + name + " job",
		SourcePath:  "jobs/" + name + ".yaml",
		SpecHash:    "sha256:" + name,
		SpecJSON:    `{"steps":[{"name":"build"}]}`,
	})
	if err != nil {
		t.Fatalf("record job %s: %v", name, err)
	}
	return version
}

// twoSteps is the step list most cases use: one step feeding another.
func twoSteps() []store.NewStep {
	return []store.NewStep{
		{Name: "build"},
		{Name: "test", DependsOn: []string{"build"}},
	}
}

func TestUpsertJobVersionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)

	first, created, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "nightly", SpecHash: "sha256:aa", SpecJSON: `{"steps":[]}`,
	})
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	if !created {
		t.Error("the first load did not report a new version")
	}
	if first.Version != 1 {
		t.Errorf("the first version is %d, want 1", first.Version)
	}

	again, created, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "nightly", SpecHash: "sha256:aa", SpecJSON: `{"steps":[]}`,
	})
	if err != nil {
		t.Fatalf("second load of the same spec: %v", err)
	}
	if created {
		t.Error("loading the same spec twice invented a version")
	}
	if again.ID != first.ID || again.Version != first.Version {
		t.Errorf("the same spec came back as version %d (%s), want %d (%s)",
			again.Version, again.ID, first.Version, first.ID)
	}

	changed, created, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "nightly", SpecHash: "sha256:bb", SpecJSON: `{"steps":[{"name":"build"}]}`,
	})
	if err != nil {
		t.Fatalf("load of a changed spec: %v", err)
	}
	if !created || changed.Version != 2 {
		t.Errorf("the changed spec is version %d (new=%v), want 2 (new=true)", changed.Version, created)
	}
}

func TestCreateRunWithStepsWritesTheWholeRun(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "cli:1000",
		Steps:        twoSteps(),
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if run.State != "queued" {
		t.Errorf("the new run is %q, want queued", run.State)
	}

	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	if len(detail.Steps) != 2 {
		t.Fatalf("the run has %d steps, want 2", len(detail.Steps))
	}
	if detail.Steps[0].Name != "build" || detail.Steps[1].Name != "test" {
		t.Errorf("the steps came back as %q and %q, want build and test",
			detail.Steps[0].Name, detail.Steps[1].Name)
	}
	if detail.Steps[1].Index != 1 {
		t.Errorf("the second step is at index %d, want 1", detail.Steps[1].Index)
	}
	for _, step := range detail.Steps {
		if step.State != "pending" {
			t.Errorf("step %s starts as %q, want pending", step.Name, step.State)
		}
	}
}

// TestCreateRunWithStepsRollsBackWholesale is the atomicity criterion. A
// failure after the run row is written has to take the run row with it, or a
// caller can find a run that has no steps and never will.
//
// The failure is injected rather than crashed into: that one transaction is one
// commit is what the migration crash test already proves against a real SIGKILL,
// and what is under test here is that this write is one transaction.
func TestCreateRunWithStepsRollsBackWholesale(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	_, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps: []store.NewStep{
			{Name: "build"},
			{Name: "build"},
		},
	})
	if err == nil {
		t.Fatal("a run with two steps of one name was created")
	}

	runs, err := s.ListRuns(ctx, store.RunFilter{})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("the failed run left %d rows behind, want none", len(runs))
	}
}

func TestCreateRunWithStepsRefusesASecondActiveConcurrencyKey(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	held := store.NewRun{
		JobName:        "nightly",
		JobVersionID:   version.ID,
		Origin:         "schedule",
		ConcurrencyKey: "nightly",
		Steps:          twoSteps(),
	}
	if _, err := s.CreateRunWithSteps(ctx, held); err != nil {
		t.Fatalf("create the first run: %v", err)
	}

	_, err := s.CreateRunWithSteps(ctx, held)
	if !errors.Is(err, store.ErrConcurrencyKeyHeld) {
		t.Fatalf("the second run failed with %v, want ErrConcurrencyKeyHeld", err)
	}
}

// TestTwoWritersRaceForOneConcurrencyKey is the enforcement under contention.
// Both goroutines are released at once and both insert; the partial unique
// index decides, so exactly one wins and the loser is told why rather than
// being handed a duplicate.
//
// Nothing sleeps and nothing polls. The barrier releases both writers, and the
// assertions run after both have returned.
func TestTwoWritersRaceForOneConcurrencyKey(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	results := make([]error, 2)
	for i := range results {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			_, results[i] = s.CreateRunWithSteps(ctx, store.NewRun{
				JobName:        "nightly",
				JobVersionID:   version.ID,
				Origin:         "sensor",
				ConcurrencyKey: "nightly",
				Steps:          twoSteps(),
			})
		}()
	}
	start.Done()
	done.Wait()

	won, lost := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			won++
		case errors.Is(err, store.ErrConcurrencyKeyHeld):
			lost++
		default:
			t.Errorf("a writer failed with something other than a held key: %v", err)
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("%d writers won and %d lost the key, want exactly one of each", won, lost)
	}

	runs, err := s.ListRuns(ctx, store.RunFilter{})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("the race left %d runs, want 1", len(runs))
	}
}

func TestDeferredRunsSayWhy(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	version := aJob(t, s, "nightly")

	held := store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "retry",
		AvailableAt:  clk.Now().Add(time.Minute),
		Steps:        twoSteps(),
	}
	if _, err := s.CreateRunWithSteps(ctx, held); err == nil {
		t.Fatal("a run held back without a reason was accepted")
	}

	held.DeferReason = "STEP_RETRY_SCHEDULED"
	run, err := s.CreateRunWithSteps(ctx, held)
	if err != nil {
		t.Fatalf("create a run held back with a reason: %v", err)
	}
	if !run.AvailableAt.Equal(clk.Now().Add(time.Minute)) {
		t.Errorf("the run is available at %s, want a minute from now", run.AvailableAt)
	}
}

func TestGetRunTakesAPrefixAndRefusesAnAmbiguousOne(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	first, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual", Steps: twoSteps(),
	})
	if err != nil {
		t.Fatalf("create the first run: %v", err)
	}

	// A prefix long enough to be unique. Ids are ULIDs and the clock has not
	// moved, so the two runs share their timestamp half and differ after it.
	detail, err := s.GetRun(ctx, first.ID[:20])
	if err != nil {
		t.Fatalf("look up the run by a unique prefix: %v", err)
	}
	if detail.ID != first.ID {
		t.Errorf("the prefix found %s, want %s", detail.ID, first.ID)
	}

	second, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual", Steps: twoSteps(),
	})
	if err != nil {
		t.Fatalf("create the second run: %v", err)
	}
	if first.ID[:10] != second.ID[:10] {
		t.Fatalf("the two runs do not share a timestamp prefix (%s, %s), so the case proves nothing",
			first.ID, second.ID)
	}
	if _, err := s.GetRun(ctx, first.ID[:10]); !errors.Is(err, store.ErrAmbiguousRunID) {
		t.Fatalf("the shared prefix returned %v, want ErrAmbiguousRunID", err)
	}

	if _, err := s.GetRun(ctx, "0000000000000000000000000Z"); !errors.Is(err, store.ErrRunNotFound) {
		t.Fatalf("an id nobody used returned %v, want ErrRunNotFound", err)
	}
	if _, err := s.GetRun(ctx, "not an id"); err == nil {
		t.Fatal("a lookup of something that is not an id succeeded")
	}
}

func TestListRunsPagesNewestFirstWithoutOffset(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	const total = 7
	created := make([]string, 0, total)
	for range total {
		run, err := s.CreateRunWithSteps(ctx, store.NewRun{
			JobName: "nightly", JobVersionID: version.ID, Origin: "manual", Steps: twoSteps(),
		})
		if err != nil {
			t.Fatalf("create run: %v", err)
		}
		created = append(created, run.ID)
	}

	var walked []string
	cursor := ""
	for range total + 1 {
		page, err := s.ListRuns(ctx, store.RunFilter{Limit: 3, Before: cursor})
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, run := range page {
			walked = append(walked, run.ID)
		}
		cursor = page[len(page)-1].ID
	}

	if len(walked) != total {
		t.Fatalf("paging walked %d runs, want %d", len(walked), total)
	}
	for i, gotID := range walked {
		wantID := created[total-1-i]
		if gotID != wantID {
			t.Fatalf("page position %d is %s, want %s: the listing is not newest first",
				i, gotID, wantID)
		}
	}
}

func TestListRunsFiltersByJobAndState(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	nightly := aJob(t, s, "nightly")
	hourly := aJob(t, s, "hourly")

	for _, version := range []store.JobVersion{nightly, hourly} {
		if _, err := s.CreateRunWithSteps(ctx, store.NewRun{
			JobName: version.JobName, JobVersionID: version.ID, Origin: "manual", Steps: twoSteps(),
		}); err != nil {
			t.Fatalf("create a run of %s: %v", version.JobName, err)
		}
	}

	byJob, err := s.ListRuns(ctx, store.RunFilter{JobName: "hourly"})
	if err != nil {
		t.Fatalf("list by job: %v", err)
	}
	if len(byJob) != 1 || byJob[0].JobName != "hourly" {
		t.Fatalf("the job filter returned %d rows, want one hourly run", len(byJob))
	}

	queued, err := s.ListRuns(ctx, store.RunFilter{States: []string{"queued"}})
	if err != nil {
		t.Fatalf("list by state: %v", err)
	}
	if len(queued) != 2 {
		t.Errorf("the state filter returned %d queued runs, want 2", len(queued))
	}

	none, err := s.ListRuns(ctx, store.RunFilter{States: []string{"succeeded", "failed"}})
	if err != nil {
		t.Fatalf("list by terminal states: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("the terminal filter returned %d rows, want none", len(none))
	}
}

func TestAppendRunEventRecordsATransitionOfItsOwn(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	version := aJob(t, s, "nightly")

	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual", Steps: twoSteps(),
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	clk.Advance(time.Second)
	if err := s.AppendRunEvent(ctx, store.RunEvent{
		RunID:      run.ID,
		StepName:   "build",
		Kind:       "step.started",
		FromState:  "pending",
		ToState:    "running",
		Actor:      "system",
		DetailJSON: `{"attempt":1}`,
	}); err != nil {
		t.Fatalf("append an event: %v", err)
	}

	if err := s.AppendRunEvent(ctx, store.RunEvent{RunID: "no-such-run", Kind: "run.queued"}); err == nil {
		t.Fatal("an event on a run that does not exist was accepted")
	}
}
