package demo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/worker"
)

// dagPaceqOnce guards the one build of the real binary this suite needs;
// the CLI subprocesses and the state they write must all come from one
// build, mirroring the test/serve harness pattern.
var (
	dagPaceqOnce sync.Once
	dagPaceqPath string
	dagPaceqErr  error
)

func buildPaceq(t *testing.T) string {
	t.Helper()
	dagPaceqOnce.Do(func() {
		dir, err := os.MkdirTemp("", "paceq-demo-bin")
		if err != nil {
			dagPaceqErr = err
			return
		}
		root, err := moduleRoot()
		if err != nil {
			dagPaceqErr = err
			return
		}
		path := filepath.Join(dir, "paceq")
		build := exec.Command("go", "build", "-o", path, "./cmd/paceq")
		build.Dir = root
		if out, buildErr := build.CombinedOutput(); buildErr != nil {
			dagPaceqErr = fmt.Errorf("%v\n%s", buildErr, out)
			return
		}
		dagPaceqPath = path
	})
	if dagPaceqErr != nil {
		t.Fatalf("could not build paceq: %v", dagPaceqErr)
	}
	return dagPaceqPath
}

// moduleRoot walks up to the directory holding go.mod.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}

// TestDiamondBranchWindowsOverlap is the parallel half of the M4 exit
// criterion, verified as time intervals. The diamond job's two branches are
// driven through the production claim gate and the production worker pool:
// the run is claimed through the store, steps are admitted by the M4-02
// claim predicate under its max_parallel budget, each attempt runs as its
// own process, and every verdict lands through RecordStepOutcome. The row
// then reads the recorded windows back and demands that load-warehouse and
// load-cache actually intersect in time - the later start strictly before
// the earlier end - which "both succeeded" can never prove.
//
// The foreground engine executes one step at a time today (internal/engine's
// drive loop is serial), so this proof drives the pool exactly the way the
// claim predicate was built to be driven. When the engine wires the pool
// into its own loop, this row keeps passing unchanged; it asserts on the
// recorded windows, never on who drove them.
func TestDiamondBranchWindowsOverlap(t *testing.T) {
	ctx := context.Background()
	work := t.TempDir()
	if err := os.MkdirAll(filepath.Join(work, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(work, "effects"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(work, "marks"), 0o755); err != nil {
		t.Fatal(err)
	}
	jobYAML, err := os.ReadFile(filepath.Join("..", "..", "examples", "dag", "diamond.yaml"))
	if err != nil {
		t.Fatalf("the diamond example is missing: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "jobs", "diamond.yaml"), jobYAML, 0o644); err != nil {
		t.Fatal(err)
	}

	paceq := buildPaceq(t)
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(paceq, args...)
		cmd.Dir = work
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("paceq %v failed: %v\n%s", args, err, out)
		}
		return string(out)
	}

	run("init")
	run("apply")

	s, err := store.OpenState(ctx, filepath.Join(work, ".paceq"), store.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer func() { _ = s.Close() }()

	queued, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
		JobName: "diamond",
		Actor:   "test:m4overlap",
	})
	if err != nil {
		t.Fatalf("queue the run: %v", err)
	}
	const owner = "m4-overlap"
	_, epoch, err := s.ClaimRun(ctx, queued.Run.ID, store.LeaseInput{Owner: owner, TTL: 5 * time.Minute})
	if err != nil {
		t.Fatalf("claim run %s: %v", queued.Run.ID, err)
	}
	ref := store.LeaseRef{Owner: owner, Epoch: epoch}

	detail, err := s.GetRun(ctx, queued.Run.ID)
	if err != nil {
		t.Fatalf("read back the run: %v", err)
	}
	version, err := s.JobVersionByID(ctx, detail.Run.JobVersionID)
	if err != nil {
		t.Fatalf("read the frozen spec: %v", err)
	}
	job, err := spec.FromIR([]byte(version.SpecJSON))
	if err != nil {
		t.Fatalf("decode the frozen spec of version %s: %v", version.ID, err)
	}
	stepsByName := make(map[string]spec.Step, len(job.Steps))
	for _, st := range job.Steps {
		stepsByName[st.Name] = st
	}

	execute := func(ctx context.Context, claimed *store.ClaimedStep) error {
		st := stepsByName[claimed.Name]
		timeout := st.Timeout
		if timeout <= 0 {
			timeout = time.Hour
		}
		res, err := runnerRun(ctx, runnerSpecInput{
			Argv:    st.Run,
			Workdir: work,
			Timeout: timeout,
			RunID:   claimed.RunID,
			Job:     job.Name,
			Step:    claimed.Name,
			Attempt: claimed.Attempt,
		})
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		switch res.Outcome {
		case runner.Succeeded:
			zero := 0
			return s.RecordStepOutcome(ctx, claimed.RunID, claimed.Name, store.StepOutcome{
				Event:      string(model.EvStepSucceeded),
				ReasonCode: reason.STEPSucceeded,
				ExitCode:   &zero,
				FinishedAt: now,
			}, ref)
		case runner.Failed:
			code := res.ExitCode
			return s.RecordStepOutcome(ctx, claimed.RunID, claimed.Name, store.StepOutcome{
				Event:      string(model.EvStepFailed),
				ReasonCode: reason.STEPFailedNonzeroExit,
				ExitCode:   &code,
				FinishedAt: now,
				DetailJSON: fmt.Sprintf(`{"exit_code":%d}`, code),
			}, ref)
		default:
			return fmt.Errorf("step %s ended %s (%s)", claimed.Name, res.Outcome, res.ReasonCode)
		}
	}

	pool := worker.New(s, nil, execute, clock.System(), 4)
	if err := pool.Run(ctx, queued.Run.ID, ref); err != nil {
		t.Fatalf("the worker pool could not drive run %s: %v", queued.Run.ID, err)
	}
	state, err := s.FinishRun(ctx, queued.Run.ID, ref, store.FinishReason{Code: reason.RUNSucceeded})
	if err != nil {
		t.Fatalf("finish run %s: %v", queued.Run.ID, err)
	}
	if state != "succeeded" {
		after, _ := s.GetRun(ctx, queued.Run.ID)
		var rows []string
		for _, st := range after.Steps {
			rows = append(rows, fmt.Sprintf("  %s %s reason=%s exit=%d", st.Name, st.State, st.ReasonCode, st.ExitCode))
		}
		t.Fatalf("the pooled run ended %s, want succeeded:\n%s", state, joinLines(rows))
	}

	windows := map[string][2]time.Time{}
	after, err := s.GetRun(ctx, queued.Run.ID)
	if err != nil {
		t.Fatalf("read back the finished run: %v", err)
	}
	for _, st := range after.Steps {
		windows[st.Name] = [2]time.Time{st.StartedAt, st.FinishedAt}
	}

	transformEnd := windows["transform"][1]
	for _, branch := range []string{"load-warehouse", "load-cache"} {
		start := windows[branch][0]
		if start.Before(transformEnd) {
			t.Fatalf("%s started at %v, before transform finished at %v", branch, start, transformEnd)
		}
	}
	aStart, aEnd := windows["load-warehouse"][0], windows["load-warehouse"][1]
	bStart, bEnd := windows["load-cache"][0], windows["load-cache"][1]
	overlapStart := maxTime(aStart, bStart)
	overlapEnd := minTime(aEnd, bEnd)
	if !overlapEnd.After(overlapStart) {
		t.Fatalf("the branches do not overlap:\nload-warehouse %v .. %v\nload-cache    %v .. %v",
			aStart, aEnd, bStart, bEnd)
	}
	t.Logf("%s and %s overlap by %dms", "load-warehouse", "load-cache",
		overlapEnd.Sub(overlapStart).Milliseconds())

	for _, step := range []string{"extract", "transform", "load-warehouse", "publish", "report", "load-cache", "notify"} {
		data, err := os.ReadFile(filepath.Join(work, "effects", fmt.Sprintf("%s.%s.txt", queued.Run.ID, step)))
		got := 0
		if err == nil {
			for _, line := range splitLines(string(data)) {
				if line != "" {
					got++
				}
			}
		}
		if got != 1 {
			t.Errorf("%s wrote %d effects, want 1", step, got)
		}
	}
}

// The steps run through internal/runner, the same process executor the
// engine uses: real argv, own process group, the verdict taxonomy the store
// records.
func runnerRun(ctx context.Context, in runnerSpecInput) (runner.Result, error) {
	return runner.Run(ctx, runner.Spec{
		Argv:    in.Argv,
		Workdir: in.Workdir,
		Timeout: in.Timeout,
		Ctx: runner.RunContext{
			RunID:   in.RunID,
			Job:     in.Job,
			Step:    in.Step,
			Attempt: in.Attempt,
		},
	})
}

type runnerSpecInput struct {
	Argv    []string
	Workdir string
	Timeout time.Duration
	RunID   string
	Job     string
	Step    string
	Attempt int
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func joinLines(rows []string) string {
	out := ""
	for i, row := range rows {
		if i > 0 {
			out += "\n"
		}
		out += row
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
