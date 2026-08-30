package cli

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/diff"
	"github.com/rogpeppe/go-internal/testscript"
	"github.com/rogpeppe/go-internal/txtar"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/runner"
)

// updateGoldens regenerates the golden expectations embedded in the scripts
// under testdata/script: run
//
//	go test ./internal/cli -run TestScripts -update
//
// and the failing cmp and cmpjson commands rewrite their expected side in
// the txtar files. One pass is enough, and it rewrites every golden in a
// script, text and JSON alike. Getting there takes care: testscript's own
// cmp serialises the script's archive over the file as the run ends, and a
// write made during the run does not survive that, so cmpjson queues its
// rewrites and applies them afterwards. TestGoldenUpdateIsIdempotent keeps
// the promise honest. Regeneration is explicit and local, never automatic,
// and forbidden in CI: a golden there is compared, not replaced.
var updateGoldens = flag.Bool("update", false,
	"rewrite the golden expectations inside the test scripts")

// scriptDir holds the golden scripts, one file per user flow.
const scriptDir = "testdata/script"

// TestMain makes the test binary runnable as paceq, so a script can call the
// command line the way a user does: as its own process, with real pipes and
// a real exit code. The fake clock is read here rather than in cli.Main, so
// the shipped binary never freezes time because an environment variable told
// it to; only this harness can.
func TestMain(m *testing.M) {
	// The daemon launches this binary as the exec shim (issue #39) with
	// `exec` as the subcommand. A suite's own image must answer the same
	// way the shipped binary does, so the scripts drive the real chain —
	// on the harness's frozen clock, so the shim's stamps agree with the
	// daemon's.
	if len(os.Args) > 1 && os.Args[1] == "exec" {
		os.Exit(runner.ExecMain(os.Args[1:], harnessClock()))
	}
	testscript.Main(m, map[string]func(){
		"paceq": func() {
			os.Exit(MainEnv(context.Background(), Env{
				Stdout: os.Stdout,
				Stderr: os.Stderr,
				Getenv: os.Getenv,
				Clk:    harnessClock(),
			}, os.Args[1:]))
		},
	})
}

// harnessClock is the clock a paceq process runs on inside the suite. An
// unset or unreadable variable means the system clock, like production.
func harnessClock() clock.Clock {
	value := os.Getenv("PULSEQ_FAKE_CLOCK")
	if value == "" {
		return nil
	}
	stamp, err := time.Parse(time.RFC3339, value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "paceq: PULSEQ_FAKE_CLOCK %q is not an RFC3339 time: %v\n", value, err)
		os.Exit(ExitUsage)
	}
	return clock.NewFake(stamp)
}

// TestScripts runs every script in testdata/script. One file per user flow,
// each readable as documentation of what the command line promises.
func TestScripts(t *testing.T) {
	if *updateGoldens && os.Getenv("CI") != "" {
		t.Fatal("golden updates are forbidden in CI; run -update locally and commit the result")
	}
	testscript.Run(t, scriptParams(scriptDir, *updateGoldens))
}

// scriptParams is the configuration every golden script runs under. The
// directory and the update mode are arguments because the regeneration proof
// runs the same suite over a copy of one script.
func scriptParams(dir string, update bool) testscript.Params {
	return testscript.Params{
		Dir: dir,
		Cmds: map[string]func(*testscript.TestScript, bool, []string){
			"wantexit":          cmdWantExit,
			"maskfile":          cmdMaskFile,
			"cmpjson":           cmdCmpJSON,
			"writeid":           cmdWriteID,
			"startrun":          cmdStartRun,
			"sigwait":           cmdSigWait,
			"holdstate":         cmdHoldState,
			"releasehold":       cmdReleaseHold,
			"writegarbage":      cmdWriteGarbage,
			"plantrun":          cmdPlantRun,
			"plantsensor":       cmdPlantSensor,
			"plantsensortick":   cmdPlantSensorTick,
			"plantschedule":     cmdPlantSchedule,
			"plantscheduletick": cmdPlantScheduleTick,
			"plantoutage":       cmdPlantOutage,
			"plantsensorskip":   cmdPlantSensorSkip,
			"plantshadow":       cmdPlantShadow,
			"plantobs":          cmdPlantObs,
			"ttyrun":            cmdTtyRun,
		},
		Setup: func(e *testscript.Env) error {
			e.Values[goldenStateKey{}] = &goldenState{dir: dir, update: update, t: e.T()}
			return setupScriptEnv(e)
		},
		UpdateScripts:       update,
		RequireExplicitExec: true,
	}
}

// idempotenceScript carries both a text golden and a JSON golden, the pair
// that used to need two -update passes.
const idempotenceScript = "explain_sensor"

// TestGoldenUpdateIsIdempotent proves what the regeneration comment claims.
// It stales both goldens of a copied script, so one -update pass has to
// rewrite both kinds, then demands that a second pass changes nothing and
// that the script compares green without -update. A pass that regenerates
// only half the goldens fails here, naming what is still stale, instead of
// leaving a red suite for the next reader to blame on their own change.
func TestGoldenUpdateIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, idempotenceScript+".txtar")
	source, err := os.ReadFile(filepath.Join(scriptDir, idempotenceScript+".txtar"))
	if err != nil {
		t.Fatalf("the script to regenerate is missing: %v", err)
	}
	stale := staleGoldens(t, source)
	if err := os.WriteFile(scriptPath, stale, 0o644); err != nil {
		t.Fatalf("could not stage the stale script: %v", err)
	}
	pass := func(name string, update bool) []byte {
		if !t.Run(name, func(t *testing.T) { testscript.Run(t, scriptParams(dir, update)) }) {
			t.FailNow()
		}
		data, err := os.ReadFile(scriptPath)
		if err != nil {
			t.Fatalf("the script is unreadable after the %s pass: %v", name, err)
		}
		return data
	}
	first := pass("update", true)
	if bytes.Equal(first, stale) {
		t.Fatal("the -update pass rewrote nothing, so it proves nothing")
	}
	second := pass("update-again", true)
	if !bytes.Equal(first, second) {
		t.Fatalf("a second -update pass changed the script, so the first one was incomplete:\n%s",
			diff.Diff("first-pass", first, "second-pass", second))
	}
	pass("compare", false)
}

// staleGoldens returns the script with its two goldens replaced by content
// no run can produce, so a regeneration pass has to rewrite both.
func staleGoldens(t *testing.T, source []byte) []byte {
	t.Helper()
	archive := txtar.Parse(source)
	staled := 0
	for i := range archive.Files {
		switch archive.Files[i].Name {
		case "expected_sensor.txt":
			archive.Files[i].Data = []byte("stale\n")
			staled++
		case "expected_sensor.json":
			archive.Files[i].Data = []byte("{\"stale\": true}\n")
			staled++
		}
	}
	if staled != 2 {
		t.Fatalf("%s no longer carries both a text and a JSON golden", idempotenceScript)
	}
	return txtar.Format(archive)
}
