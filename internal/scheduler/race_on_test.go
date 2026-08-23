//go:build race

package scheduler_test

// raceDetector is true when the test binary is built with -race. The race
// instrumentation slows the pure Go SQLite driver by an order of magnitude,
// which turns wall-clock budgets into statements about the detector rather
// than about paceq. Timing assertions consult this; correctness assertions
// never do.
const raceDetector = true
