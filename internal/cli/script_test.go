package cli

import (
	"context"
	"os"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

// TestMain makes the test binary runnable as paceq, so a script can call the
// command line the way a user does: as a process, with real pipes and a real
// exit code. It is what the golden output suite in M1-11 builds on.
func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"paceq": func() { os.Exit(Main(context.Background(), os.Args[1:])) },
	})
}

// TestScripts runs every script in testdata/script. A script is close to what a
// user types, which is the point: the assertions are about the command line as
// a whole, not about the functions behind it.
func TestScripts(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir:                 "testdata/script",
		RequireExplicitExec: true,
	})
}
