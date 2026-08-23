// Command paceq-scratch is the payload of the scratch container proof: from
// inside a FROM scratch image with no /usr/share/zoneinfo, no shell and no
// network it must resolve Europe/Oslo through the tzdata embedded in the
// binary and compute the correct next tick for a documented schedule.
//
// This is a test artifact, not a shipped command; make test-scratch builds
// and runs it locally and in CI. Every input is fixed, so the expected output
// is a constant and the check can never flake.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
)

const want = "2026-01-01T01:00:00Z" // 02:00 Europe/Oslo on a +01:00 winter day

func main() {
	zone, err := cronx.LoadZone("Europe/Oslo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "LoadZone: %v\n", err)
		os.Exit(1)
	}
	schedule, err := cronx.Parse("0 2 * * *")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse: %v\n", err)
		os.Exit(1)
	}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := schedule.Next(from, zone, cronx.Policy{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Next: %v\n", err)
		os.Exit(1)
	}
	got := next.At.UTC().Format(time.RFC3339)
	if got != want {
		fmt.Fprintf(os.Stderr, "next tick = %s, want %s\n", got, want)
		os.Exit(1)
	}
	fmt.Println(got)
}
