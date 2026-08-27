package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/paceq/internal/clock"
)

// updateGoldens regenerates the golden expectations embedded in the scripts
// under testdata/script: run
//
//	go test ./internal/cli -run TestScripts -update
//
// and the failing cmp and cmpjson commands rewrite their expected side in
// the txtar files. Regeneration is explicit and local, never automatic, and
// forbidden in CI: a golden there is compared, not replaced.
var updateGoldens = flag.Bool("update", false,
	"rewrite the golden expectations inside the test scripts")

// TestMain makes the test binary runnable as paceq, so a script can call the
// command line the way a user does: as its own process, with real pipes and
// a real exit code. The fake clock is read here rather than in cli.Main, so
// the shipped binary never freezes time because an environment variable told
// it to; only this harness can.
func TestMain(m *testing.M) {
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
	testscript.Run(t, testscript.Params{
		Dir: "testdata/script",
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
		Setup:               setupScriptEnv,
		UpdateScripts:       *updateGoldens,
		RequireExplicitExec: true,
	})
}
