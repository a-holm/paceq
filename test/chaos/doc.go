// Package chaos holds the seeded SIGKILL chaos suite for issue #20.
//
// The suite drives one real `paceq serve` subprocess over a real state
// directory, seeds N manual runs over a mix of DAG shapes, and SIGKILLs the
// daemon at kill points counted in completed runs. The kill schedule is a
// pure function of a logged seed, so a failing sweep can be replayed by
// naming that seed again. After every convergence the full invariant battery
// runs: fsck (I2, I8, I10, I12, I14 and the reason catalogue rule), no
// doubled completed run key, terminal rows carry reasons, no orphan process
// still carries one of our run ids, and every effect sits inside its
// crash budget. Final states are asserted as sets and invariants, never as
// one exact outcome, because which runs were mid-flight at a kill varies
// with machine timing.
//
// Two budgets share this machinery:
//
//   - The smoke (chaos_smoke_test.go, no build tag) runs small N under a
//     fixed seed inside the ordinary PR suite.
//
//   - The full sweep (chaos_test.go, //go:build chaos) runs 500 runs in the
//     nightly workflow and locally with:
//
//     go test -tags chaos -race -count=1 ./test/chaos
//
// A failed sweep archives state.db, the step logs, every daemon generation's
// stderr and the effect file into one artifact directory (AC-11). A seed that
// found a product bug is checked into regressions.txt so the nightly replays
// it forever after.
package chaos
