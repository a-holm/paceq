package cli

import (
	"context"
	"fmt"
	"io"
	"os"
)

const usage = `paceq - schedules, sensors and small DAGs, in one static binary.

Usage:
  paceq <command> [flags]

Commands:
  help      Show this help text
  version   Show the version

The command set is not implemented yet.
`

// Main runs the command line and returns the process exit code.
func Main(ctx context.Context, args []string) int {
	return run(ctx, args, os.Stdout, os.Stderr)
}

func run(_ context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stdout, usage)
		return 0
	}
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	case "version", "--version":
		fmt.Fprintln(stdout, "paceq (development build)")
		return 0
	default:
		fmt.Fprintf(stderr, "paceq: unknown command %q\n\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
	}
}
