package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/paceq/internal/clock"
)

// updateGoldens regenerates the golden expectations embedded in the scripts
// under testdata/script: run
//
//	go test ./internal/cli -run TestScriptBothModes -update
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

// scriptCmds are the harness commands the golden scripts use.
func scriptCmds() map[string]func(*testscript.TestScript, bool, []string) {
	return map[string]func(*testscript.TestScript, bool, []string){
		"wantexit":     cmdWantExit,
		"maskfile":     cmdMaskFile,
		"cmpjson":      cmdCmpJSON,
		"writeid":      cmdWriteID,
		"startrun":     cmdStartRun,
		"sigwait":      cmdSigWait,
		"holdstate":    cmdHoldState,
		"releasehold":  cmdReleaseHold,
		"writegarbage": cmdWriteGarbage,
		"plantrun":     cmdPlantRun,
		"ttyrun":       cmdTtyRun,
	}
}

// TestScriptBothModes runs every script in testdata/script twice, which is
// the M2-08 contract test (criterion 1): the whole M1 suite produces byte
// identical output whatever the transport axis says.
//
//	mode=default  PACEQ_SOCKET unset       the state directory's socket
//	mode=env      PACEQ_SOCKET=<work>.sock an explicitly named silent socket
//
// Both passes face a daemon that is down, so every read marks it and every
// write falls back to the flock-guarded direct store; if that fallback
// changed so much as one byte between the default path and an explicitly
// named one, the shared goldens would catch it here. The daemon-up half of
// dual mode has its own proofs in socketmode_test.go; running this whole
// suite against a live daemon is impossible by design, because several
// scripts assert direct-mode semantics (busy.txtar's exit 6 against a held
// lock) that a live daemon takes away.
func TestScriptBothModes(t *testing.T) {
	if *updateGoldens && os.Getenv("CI") != "" {
		t.Fatal("golden updates are forbidden in CI; run -update locally and commit the result")
	}
	modes := []struct {
		name     string
		explicit bool
	}{
		{name: "default"},
		{name: "env", explicit: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			testscript.Run(t, testscript.Params{
				Dir:  "testdata/script",
				Cmds: scriptCmds(),
				Setup: func(e *testscript.Env) error {
					if err := setupScriptEnv(e); err != nil {
						return err
					}
					if mode.explicit {
						e.Setenv("PACEQ_SOCKET", filepath.Join(e.WorkDir, "daemon.sock"))
					} else {
						e.Setenv("PACEQ_SOCKET", "")
					}
					return nil
				},
				UpdateScripts:       *updateGoldens,
				RequireExplicitExec: true,
			})
		})
	}
}
