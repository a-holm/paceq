//go:build !unix

package cli

import (
	"github.com/rogpeppe/go-internal/testscript"
)

// cmdTtyRun is the stub for platforms without the /dev/ptmx terminal pair.
// Scripts guard their tty rows with [unix], so this body only answers a
// script that reached for a terminal without asking whether one exists.
func cmdTtyRun(ts *testscript.TestScript, neg bool, args []string) {
	ts.Fatalf("ttyrun needs a pseudo terminal, which this platform has no way to give")
}
