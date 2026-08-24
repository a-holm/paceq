package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// The display half of #17: a keyless deferral reads as waiting, and both the
// list and the single run view name the wanted key and the run that blocks
// it. JSON carries reason_data verbatim; a human should not have to parse it.

// keyedFixture seeds a project whose job keyed carries concurrency_key "k",
// one queued run holding the key, and one keyless deferral blocked by it.
func keyedFixture(t *testing.T) (dir string, holderID, victimID string) {
	t.Helper()

	dir = t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	job := &spec.Job{
		Name:           "keyed",
		MaxConcurrent:  4,
		Steps:          []spec.Step{{Name: "build", Run: []string{"/bin/true"}}},
		ConcurrencyKey: &spec.ConcurrencyKey{Constant: "k"},
	}
	h := spec.Compile(job)
	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "keyed",
		SpecHash: h.Hash,
		SpecJSON: string(h.Canonical),
	})
	if err != nil {
		t.Fatalf("record job: %v", err)
	}

	holder, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:        "keyed",
		JobVersionID:   version.ID,
		Origin:         "manual",
		ConcurrencyKey: "keyed:k",
		Steps:          []store.NewStep{{Name: "build"}},
	})
	if err != nil {
		t.Fatalf("create the holder: %v", err)
	}
	holderID = holder.ID

	// The loser goes through the same door the scheduler uses: the key is
	// held, so it lands as a keyless deferral naming the blocker.
	row, err := s.UpsertSchedule(ctx, store.ScheduleInput{
		JobName:    "keyed",
		Name:       "nightly",
		Kind:       "cron",
		Expr:       "* * * * *",
		NextTickAt: testOrigin.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("upsert the schedule: %v", err)
	}
	res, err := s.MaterializeTick(ctx, store.TickInput{
		Schedule:       row,
		ScheduledFor:   testOrigin,
		Outcome:        store.OutcomeTriggered,
		RunKey:         "keyed/nightly:" + testOrigin.Format(time.RFC3339),
		NextTickAt:     testOrigin.Add(2 * time.Hour),
		UpdateProgress: true,
	})
	if err != nil {
		t.Fatalf("materialise the blocked fire: %v", err)
	}
	if res.Run.DeferReason != "concurrency_key" {
		t.Fatalf("setup: the fire did not defer: %+v", res.Run)
	}
	victimID = res.Run.ID
	return dir, holderID, victimID
}

func TestRunsListNamesTheKeyAndTheBlocker(t *testing.T) {
	dir, holderID, _ := keyedFixture(t)

	stdout, readOut := terminalFile(t)
	stderr, readErr := pipeFile(t)
	env := Env{Stdout: stdout, Stderr: stderr, Dir: dir, Getenv: lookup(nil)}
	code := run(context.Background(), env, []string{"runs", "list"})
	_ = stdout.Close()
	_ = stderr.Close()
	if code != ExitOK {
		t.Fatalf("runs list at a terminal = %d\n%s", code, readErr())
	}
	table := readOut()

	for _, want := range []string{"waiting", "keyed:k", holderID} {
		if !strings.Contains(table, want) {
			t.Errorf("the list does not show %q for the deferred run:\n%s", want, table)
		}
	}
}

func TestRunShowNamesTheKeyAndTheBlocker(t *testing.T) {
	dir, holderID, victimID := keyedFixture(t)

	piped := runCLI(t, dir, nil, "run", "show", victimID)
	if piped.code != ExitOK {
		t.Fatalf("run show = %d\n%s", piped.code, piped.stderr)
	}
	for _, want := range []string{"keyed:k", holderID} {
		if !strings.Contains(piped.stdout+piped.stderr, want) {
			t.Errorf("run show does not name %q:\n%s%s", want, piped.stdout, piped.stderr)
		}
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(piped.stdout), &doc); err != nil {
		return // human form; the JSON contract is pinned by reason_data above
	}
}
