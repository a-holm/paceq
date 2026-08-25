package chaos

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// syscallTerm keeps the proofs free of a second syscall import spelling.
func syscallTerm() syscall.Signal { return syscall.SIGTERM }

// The battery is only worth something once it has been seen catching the
// exact row each check exists for (issue #20, following the fault injection
// pattern of internal/store/inject.go). Every proof here plants a violation
// against a database that saw real execution, requires the matching check to
// name it, and also requires the same check to stay silent next to it.
//
// One note on scope: internal/store's own tests prove the doubled completed
// run key checker both ways on hand made rows. The live daemon cannot plant
// this shape - serve refuses to start while one run_key names more than one
// run - so the proof below drives one real run to succeeded behind its key
// and then lets the store injector twin that completion, exactly the partial
// write shape the checker exists for.

func openBatteryStore(t *testing.T, ws *chaosWorkspace) *store.Store {
	t.Helper()
	return openStore(t, ws)
}

// waitUntilTerminal blocks until the seeded run ends in the wanted state.
func waitUntilTerminal(t *testing.T, ws *chaosWorkspace, id, want string) {
	t.Helper()

	s := openBatteryStore(t, ws)
	defer closeStore(t, s)
	ctx := context.Background()
	deadline := time.Now().Add(90 * time.Second)
	for {
		detail, err := s.GetRun(ctx, id)
		if err != nil {
			t.Fatalf("read run %s: %v", id, err)
		}
		if detail.Run.State == want {
			return
		}
		if terminalRunStates[detail.Run.State] && detail.Run.State != want {
			var steps []string
			for _, st := range detail.Steps {
				steps = append(steps, fmt.Sprintf("%s=%s(%s)", st.Name, st.State, st.ReasonCode))
			}
			t.Fatalf("run %s ended %s (%s), want %s; steps: %s",
				id, detail.Run.State, detail.Run.ReasonCode, want, strings.Join(steps, " "))
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reached %s; last state %s", id, want, detail.Run.State)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// driveOneSucceededRun seeds one solo run, lets the real daemon finish it,
// and stops the daemon cleanly. The proofs then plant violations beside a
// genuinely produced terminal row.
func driveOneSucceededRun(t *testing.T, ws *chaosWorkspace, jobPrefix string) string {
	t.Helper()

	shapes := chaosShapes(t, ws)
	ids := seedRuns(t, ws, shapes[:1], 1, jobPrefix)

	p := startDaemon(t, ws, 0)
	p.waitReady(t)
	waitUntilTerminal(t, ws, ids[0], "succeeded")
	p.signal(t, syscallTerm())
	if code := p.waitExit(t, daemonExitWait); code != 0 {
		t.Fatalf("the daemon exited %d after its stop, want 0", code)
	}
	return ids[0]
}

// seedOneKeyedRun materialises one manual run carrying the given run_key.
// Nothing in the schema refuses a second run behind the same key, which is
// exactly why a counting checker must exist.
func seedOneKeyedRun(t *testing.T, ws *chaosWorkspace, jobPrefix, key string) string {
	t.Helper()

	shape := chaosShapes(t, ws)[0]
	ctx := context.Background()
	s := openStore(t, ws)
	defer closeStore(t, s)

	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  jobNameForShape(jobPrefix, shape.name),
		SpecHash: "sha256:" + shape.name + "-keyed-" + strconv.Itoa(os.Getpid()),
		SpecJSON: shape.spec(jobNameForShape(jobPrefix, shape.name)),
	})
	if err != nil {
		t.Fatalf("apply the keyed shape: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      jobNameForShape(jobPrefix, shape.name),
		JobVersionID: version.ID,
		Origin:       "manual",
		RunKey:       key,
		Actor:        "chaos",
		Steps:        shape.steps,
	})
	if err != nil {
		t.Fatalf("materialise the keyed run: %v", err)
	}
	return run.ID
}

// driveOneKeyedRun lets the real daemon complete one keyed run. The proof
// then twins that completion through the store injector: serve's startup
// gate refuses a second run behind a live key, so the doubled state is by
// construction a write that skipped the law, never an execution.
func driveOneKeyedRun(t *testing.T, ws *chaosWorkspace, jobPrefix, key string) {
	t.Helper()

	id := seedOneKeyedRun(t, ws, jobPrefix, key)
	p := startDaemon(t, ws, 0)
	p.waitReady(t)
	waitUntilTerminal(t, ws, id, "succeeded")
	p.signal(t, syscallTerm())
	if code := p.waitExit(t, daemonExitWait); code != 0 {
		t.Fatalf("the daemon exited %d after its stop, want 0", code)
	}
}

func TestFsckCheckSilentAfterRealExecutionAndLoudOnPlantedI10(t *testing.T) {
	ctx := context.Background()

	ws := newChaosWorkspace(t)
	driveOneSucceededRun(t, ws, "fsckclean")

	st := openBatteryStore(t, ws)
	defer closeStore(t, st)
	if problems := checkFsckFindings(ctx, st); len(problems) != 0 {
		t.Fatalf("the clean database raised %v, want nothing", problems)
	}

	subject, err := st.InjectFailedStepUnderSucceededRun(ctx)
	if err != nil {
		t.Fatalf("plant an I10 violation: %v", err)
	}
	problems := checkFsckFindings(ctx, st)
	var named bool
	for _, p := range problems {
		if strings.Contains(p, "I10") && strings.Contains(p, subject) {
			named = true
		}
	}
	if !named {
		t.Fatalf("the planted row %q was not named by I10; got %v", subject, problems)
	}
}

func TestFsckCheckNamesPlantedPendingStepUnderTerminalRun(t *testing.T) {
	ctx := context.Background()

	ws := newChaosWorkspace(t)
	driveOneSucceededRun(t, ws, "fsckpending")

	st := openBatteryStore(t, ws)
	defer closeStore(t, st)
	subject, err := st.InjectTerminalStepPending(ctx)
	if err != nil {
		t.Fatalf("plant an I2 violation: %v", err)
	}
	// The subject names the step; I10 speaks about the run it sits under.
	// One planted row therefore has to light two checks whose subjects
	// differ, and the proof demands both by their own subjects.
	runID := strings.Fields(subject)[1]
	problems := checkFsckFindings(ctx, st)
	var i2, i10 bool
	for _, p := range problems {
		if strings.Contains(p, "I2") && strings.Contains(p, subject) {
			i2 = true
		}
		if strings.Contains(p, "I10") && strings.Contains(p, runID) {
			i10 = true
		}
	}
	if !i2 || !i10 {
		t.Fatalf("the planted row %q was not named by I2 and I10; got %v", subject, problems)
	}
}

func TestRunKeyCheckSilentThenLoudBehindOneSharedKey(t *testing.T) {
	ctx := context.Background()

	ws := newChaosWorkspace(t)
	driveOneSucceededRun(t, ws, "keyclean")

	st := openBatteryStore(t, ws)
	defer closeStore(t, st)
	if keys := checkDoubleCompletedRunKeys(ctx, st); len(keys) != 0 {
		t.Fatalf("the clean database holds doubled keys %v, want none", keys)
	}

	ws2 := newChaosWorkspace(t)
	driveOneKeyedRun(t, ws2, "keydouble", "proof/double-key")
	st2 := openBatteryStore(t, ws2)
	defer closeStore(t, st2)

	planted, err := st2.InjectDoubleCompletedRunKey(ctx)
	if err != nil {
		t.Fatalf("twin the completed run: %v", err)
	}
	keys := checkDoubleCompletedRunKeys(ctx, st2)
	if len(keys) != 1 {
		t.Fatalf("the planted pair came back as %v, want exactly one key", keys)
	}
	if !strings.Contains(keys[0], planted) {
		t.Fatalf("the finding says %q, want it to name the shared key %q", keys[0], planted)
	}
}

// A live queued row may sit without a reason; a terminal row may not. The
// unexplained terminal injector clears a real run's reason, which is the
// exact shape AC-8 forbids.
func TestReasonScanFlagsTerminalRowsWithoutReason(t *testing.T) {
	ctx := context.Background()

	ws := newChaosWorkspace(t)
	id := driveOneSucceededRun(t, ws, "reasonscan")

	st := openBatteryStore(t, ws)
	defer closeStore(t, st)
	if problems := checkTerminalReasons(ctx, st, []string{id}); len(problems) != 0 {
		t.Fatalf("a freshly succeeded run carries its reason, yet got %v", problems)
	}

	subject, err := st.InjectUnexplainedTerminal(ctx)
	if err != nil {
		t.Fatalf("clear a terminal reason: %v", err)
	}
	problems := checkTerminalReasons(ctx, st, []string{id})
	if len(problems) == 0 {
		t.Fatalf("the cleared row %q went unseen", subject)
	}
	for _, p := range problems {
		if !strings.Contains(p, "no reason_code") {
			t.Errorf("the finding says %q, want it to speak about the missing reason", p)
		}
	}
}

func TestEffectBoundsAllowCrashesAndFlagRealDuplication(t *testing.T) {
	path := t.TempDir() + "/effects.txt"
	const kills = 2
	oneGood := "key-one\t1\t100\n"
	tooMany := "key-two\t1\t200\nkey-two\t2\t201\nkey-two\t3\t202\nkey-two\t4\t203\n"
	if err := os.WriteFile(path, []byte(oneGood+tooMany), 0o600); err != nil {
		t.Fatalf("write effects: %v", err)
	}

	problems := checkEffectBounds(path, kills)
	if len(problems) != 1 {
		t.Fatalf("got %v, want exactly the over-bound key named once", problems)
	}
	if !strings.Contains(problems[0], "key-two") {
		t.Fatalf("the finding says %q, want it to name key-two", problems[0])
	}
	if strings.Contains(strings.Join(problems, " "), "key-one") {
		t.Fatalf("the single clean effect was flagged: %v", problems)
	}
}

func TestOrphanScanFindsLiveCarrierAndStaysQuietAfterItDies(t *testing.T) {
	runID := "orphan-proof-" + strconv.Itoa(os.Getpid())
	cmd := exec.Command("sleep", "30")
	cmd.Env = append(os.Environ(), "PACEQ_RUN_ID="+runID)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the carrier process: %v", err)
	}
	dead := false
	t.Cleanup(func() {
		if !dead {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})

	problems := checkNoOrphans([]string{runID}, 500*time.Millisecond)
	if len(problems) != 1 {
		t.Fatalf("got %v, want exactly the live carrier named", problems)
	}
	if !strings.Contains(problems[0], runID) {
		t.Fatalf("the finding says %q, want it to name %s", problems[0], runID)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill the carrier: %v", err)
	}
	_, _ = cmd.Process.Wait()
	dead = true

	if problems := checkNoOrphans([]string{runID}, 2*time.Second); len(problems) != 0 {
		t.Fatalf("after the carrier died the scan still says %v", problems)
	}
}
