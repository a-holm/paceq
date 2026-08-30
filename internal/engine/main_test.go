package engine

import (
	"os"
	"testing"

	"github.com/a-holm/paceq/internal/runner"
)

// TestMain answers the exec shim's entry (issue #39) before the test
// machinery: a row that drives the real exec chain names this binary as the
// shim, so a spawned `engine.test exec ...` must run the shim exactly like
// the shipped binary's hidden subcommand, and never fall through to the
// suite.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "exec" {
		os.Exit(runner.ExecMain(os.Args[1:], nil))
	}
	os.Exit(m.Run())
}
