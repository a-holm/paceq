//go:build !race

package scheduler_test

// See race_on.go: this half reports that no race detector is slowing
// everything down, so the wall-clock budgets from the issue bind.
const raceDetector = false
