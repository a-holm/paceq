package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Issue #10, M4-04, review focus 7: retry and replay must answer with the
// same JSON whichever way the write travelled. This suite builds the shipped
// binary, drives it twice over the same scenario, once against a daemon
// serving a unix socket and once with nothing between the command and the
// database, and demands byte identical answers once ids are masked. The
// event history is read afterwards to prove each write really took the path
// its scenario names.

// parityULID matches the identifiers the store mints, for masking.
var parityULID = regexp.MustCompile(`[0-9][0-9ABCDEFGHJKMNPQRSTVWXYZ]{25}`)

// buildPaceq compiles the shipped binary once per test. The parity question
// is about what a user's command line does, so the answer must come from the
// binary itself and not from a test double of it.
func buildPaceq(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "paceq")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/a-holm/paceq/cmd/paceq")
	cmd.Env = append(os.Environ(), "GOFLAGS=-buildvcs=false")
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build ./cmd/paceq: %v\n%s", err, combined)
	}
	return bin
}

// paceqRun runs one command of the built binary inside the project
// directory, under an environment that carries none of this machine's
// noise, and returns what the command wrote to stdout.
func paceqRun(t *testing.T, bin, dir string, args ...string) []byte {
	t.Helper()
	var out, errBuf bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + dir,
		"LC_ALL=C",
		"NO_COLOR=1",
	}
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("paceq %v exited %d:\nstderr:\n%s", args, exit.ExitCode(), errBuf.String())
		}
		t.Fatalf("paceq %v could not start: %v", args, err)
	}
	return out.Bytes()
}

// parityDoc canonicalizes one JSON answer (sorted keys, stable indent) and
// masks every run id into first-seen placeholders, so two answers count as
// the same document when they say the same things about different rows.
func parityDoc(t *testing.T, raw []byte) string {
	t.Helper()
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("the answer is not JSON:\n%s\n%v", raw, err)
	}
	canon, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatalf("could not re-encode the answer: %v", err)
	}
	seen := map[string]string{}
	return string(parityULID.ReplaceAllFunc(canon, func(match []byte) []byte {
		id := string(match)
		name, ok := seen[id]
		if !ok {
			name = fmt.Sprintf("<RUN%d>", len(seen)+1)
			seen[id] = name
		}
		return []byte(name)
	}))
}

// plantParityChain records the job, queues one run of it, and drives it the
// way an executor would: a and b succeed, c dies on its exit code. It
// returns the failed run's id.
func plantParityChain(t *testing.T, ctx context.Context, s *store.Store) string {
	t.Helper()
	const spec = `{"name":"parity","max_concurrent":1,"timeout_ms":3600000,` +
		`"schema":"paceq.job.v1","steps":[` +
		`{"name":"a","run":["/bin/true"],"shell":false},` +
		`{"name":"b","needs":["a"],"run":["/bin/true"],"shell":false},` +
		`{"name":"c","needs":["b"],"run":["/bin/true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "parity", SpecHash: "sha256:parity", SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	queued, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "parity"})
	if err != nil {
		t.Fatalf("queue the run: %v", err)
	}
	runID := queued.Run.ID
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "parity:test", TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: "parity:test", Epoch: 1}
	for _, step := range []string{"a", "b"} {
		if err := s.StartStep(ctx, runID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		if err := s.RecordStepOutcome(ctx, runID, step, store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: new(int), FinishedAt: time.Now(),
		}, ref); err != nil {
			t.Fatalf("record %s: %v", step, err)
		}
	}
	if err := s.StartStep(ctx, runID, "c", ref); err != nil {
		t.Fatalf("start c: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "c", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode: new(int), FinishedAt: time.Now(),
	}, ref); err != nil {
		t.Fatalf("record c: %v", err)
	}
	if _, err := s.FinishRun(ctx, runID, ref, store.FinishReason{
		Code: reason.RUNFailedStep, Data: `{"step":"c"}`,
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	return runID
}

// runParityScenario drives one retry and one replay over the requested
// transport and returns both answers, plus the actor history recorded for
// the reopen: the honest mark of which path carried the write.
func runParityScenario(t *testing.T, withDaemon bool) (retryDoc, replayDoc, reopenActor string) {
	t.Helper()
	ctx := context.Background()
	bin := buildPaceq(t)
	dir := t.TempDir()

	paceqRun(t, bin, dir, "init")

	path := filepath.Join(dir, ".paceq", store.DatabaseFileName)
	s, err := store.Open(ctx, path, store.Options{})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = s.Close() }()

	srcID := plantParityChain(t, ctx, s)

	if withDaemon {
		cfg := Config{
			StateDir:   filepath.Join(dir, ".paceq"),
			Version:    "parity",
			SocketPath: filepath.Join(dir, ".paceq", "paceq.sock"),
			Logger:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
		}
		sts := newStatuses(func() time.Time { return time.Unix(0, 0).UTC() })
		stop := startHealthEndpoint(cfg, sts, cfg.Logger, s)
		if stop == nil {
			t.Fatal("a configured socket did not start the endpoints")
		}
		defer func() { stop(context.Background()) }()
	}

	// Replay first: it reads the finished failed run as its source and mints
	// a new run. Retry second: it reopens the same original run. Doing it this
	// order keeps the original run a valid source for both, because after a
	// retry the original is a reopened (queued) run and replay would refuse it.
	replayDoc = parityDoc(t, paceqRun(t, bin, dir,
		"runs", "replay", srcID, "--failed", "-o", "json"))
	retryDoc = parityDoc(t, paceqRun(t, bin, dir,
		"runs", "retry", srcID, "-o", "json"))

	events, err := s.RunEvents(ctx, srcID)
	if err != nil {
		t.Fatalf("read the events: %v", err)
	}
	for _, e := range events {
		if e.Kind == "operator_reopen" {
			reopenActor = e.Actor
		}
	}
	if reopenActor == "" {
		t.Fatal("no operator_reopen event was written")
	}
	return retryDoc, replayDoc, reopenActor
}

// TestRetryAndReplaySpeakTheSameJSONOverBothTransports is the parity proof:
// the answers match byte for byte across the daemon socket and the direct
// path, and the event history says honestly which one ran.
func TestRetryAndReplaySpeakTheSameJSONOverBothTransports(t *testing.T) {
	sockRetry, sockReplay, sockActor := runParityScenario(t, true)
	directRetry, directReplay, directActor := runParityScenario(t, false)

	if sockRetry != directRetry {
		t.Errorf("the retry answer depends on the transport\nover socket:\n%s\ndirect:\n%s",
			sockRetry, directRetry)
	}
	if sockReplay != directReplay {
		t.Errorf("the replay answer depends on the transport\nover socket:\n%s\ndirect:\n%s",
			sockReplay, directReplay)
	}
	if sockActor != "api" {
		t.Errorf("the socket reopen recorded actor %q, want api", sockActor)
	}
	if directActor != fmt.Sprintf("cli:%d", os.Getuid()) {
		t.Errorf("the direct reopen recorded actor %q, want the cli user", directActor)
	}

	// The shared shape itself, once, so a future format change that keeps
	// the two transports equal but drops a field still fails here.
	var retry map[string]any
	if err := json.Unmarshal([]byte(sockRetry), &retry); err != nil {
		t.Fatalf("the masked retry answer is not JSON: %v", err)
	}
	if got := retry["new_epoch"]; got != float64(2) {
		t.Errorf("new_epoch = %v, want 2", got)
	}
	reopened, _ := retry["reopened"].([]any)
	if len(reopened) != 1 || reopened[0] != "c" {
		t.Errorf("reopened = %v, want [c]", reopened)
	}
	var replay struct {
		ReplayOf string   `json:"replay_of"`
		Reused   []string `json:"reused"`
		Rerun    []string `json:"rerun"`
		RunID    string   `json:"run_id"`
	}
	if err := json.Unmarshal([]byte(sockReplay), &replay); err != nil {
		t.Fatalf("the masked replay answer is not JSON: %v", err)
	}
	if len(replay.Reused) != 2 || replay.Reused[0] != "a" || replay.Reused[1] != "b" {
		t.Errorf("reused = %v, want [a b]", replay.Reused)
	}
	if len(replay.Rerun) != 1 || replay.Rerun[0] != "c" {
		t.Errorf("rerun = %v, want [c]", replay.Rerun)
	}
	if replay.RunID == "" || replay.RunID == replay.ReplayOf {
		t.Errorf("run_id %q beside replay_of %q is not a new run", replay.RunID, replay.ReplayOf)
	}
}
