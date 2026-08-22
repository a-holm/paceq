// Package daemon is the long lived process behind paceq serve: one process,
// one state directory, and a fixed family of loops that share a database.
//
// The rules the package lives by:
//
//   - There is no second execution path. Work runs through engine.ExecuteRun,
//     the same code paceq run uses in its own process.
//   - Every timing decision comes from a clock.Clock. No loop reads the wall
//     clock directly, which the architecture guard enforces.
//   - The notify bus makes waking fast, never correct. Every loop keeps its
//     ticker, so a lost wake costs latency and nothing else (05 section 3.2).
//   - A clean stop hands work back instead of inventing verdicts for it. Steps
//     cut short by a stop go back to pending with their attempt restored, and
//     claimed runs go back to the queue without counting a crash.
package daemon
