// Package reason is the catalogue of reason codes: one closed, stable string
// enum that says why every decision, trigger, run and step ended the way it
// did. It is the mechanism that keeps `paceq explain` from degenerating into
// a wall of UNKNOWN (06 section 2.1).
//
// Three rules shape everything here.
//
//  1. The catalogue is closed. A code outside it is a test failure, and there
//     is no UNKNOWN code to reach for. Codes are added, never redefined: a
//     code means the same thing in 2027 as in 2026.
//
//  2. Every code carries its full anatomy in one table: level, short text,
//     long explanation, remediation hint and the reason_data keys it promises.
//     Nothing that prints a code has to know its semantics.
//
//  3. The package is pure. No I/O, no clock, no state. The one generated
//     artifact, docs/reference/reason-codes.md, is produced from the table by
//     Render and kept fresh by a test, so the page cannot drift from the code.
//
// # Migration path for the runner constants
//
// The step outcome codes STEP_SUCCEEDED, STEP_FAILED_NONZERO_EXIT,
// STEP_FAILED_TIMEOUT, STEP_FAILED_SPAWN and STEP_FAILED_SIGNAL are defined
// here and nowhere else. The runner (issue #61) carries the same five values
// as package constants; when the two branches meet, those constants become
// references into this package, for example
//
//	ReasonSucceeded = string(reason.STEPSucceeded)
//
// and the static guard in internal/arch then refuses any new literal outside
// this catalogue. Until that landing happens, this table is the single home
// of the five values, and their string values match the runner's byte for
// byte. The runner's SpawnFailed result currently sets no reason_data; the
// keys promised here (argv0, workdir, errno) are what it should grow.
package reason
