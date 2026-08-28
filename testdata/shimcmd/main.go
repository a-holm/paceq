// Command shimcmd is the crash-harness-style fixture for the daemon side of
// the exec shim (issue #39): a minimal main that parses the same flag set the
// hidden `paceq exec` command owns and then runs runner.ShimMain. It exists
// so the runner's tests can spawn a real shim subprocess without building the
// whole paceq binary; the real command's flag parsing is pinned by the cli
// package's own tests.
package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/runner"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "exec" {
		args = args[1:]
	}

	var cfg runner.ShimConfig
	cfg.WatchFD = -1
	cfg.BaseFD = -1
	var argv []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			argv = args[i+1:]
			break
		}
		name, val := arg, ""
		hasVal := false
		if eq := strings.IndexByte(arg, '='); eq >= 0 {
			name, val, hasVal = arg[:eq], arg[eq+1:], true
		}
		take := func() string {
			if hasVal {
				return val
			}
			i++
			return args[i]
		}
		switch name {
		case "--run-id":
			cfg.RunID = take()
		case "--step":
			cfg.Step = take()
		case "--attempt":
			n, _ := strconv.Atoi(take())
			cfg.Attempt = n
		case "--claim-epoch":
			n, _ := strconv.ParseInt(take(), 10, 64)
			cfg.ClaimEpoch = n
		case "--spool-dir":
			cfg.SpoolDir = take()
		case "--workdir":
			cfg.Workdir = take()
		case "--timeout":
			d, _ := time.ParseDuration(take())
			cfg.Timeout = d
		case "--kill-grace":
			d, _ := time.ParseDuration(take())
			cfg.KillGrace = d
		case "--watch-fd":
			n, _ := strconv.Atoi(take())
			cfg.WatchFD = n
		case "--base-fd":
			n, _ := strconv.Atoi(take())
			cfg.BaseFD = n
		}
	}
	cfg.Argv = argv
	if dir := os.Getenv("SHIMCMD_SPOOL_DIR"); dir != "" {
		cfg.SpoolDir = dir
	}
	if os.Getenv("SHIMCMD_NO_SPOOL") != "" {
		cfg.SpoolDir = ""
	}

	os.Exit(runner.ShimMain(context.Background(), cfg))
}
