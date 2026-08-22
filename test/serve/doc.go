// Package serve drives the built paceq binary as a real subprocess and holds
// it to the shutdown promises acceptance puts in writing: one stop signal
// drains what runs and leaves no process group behind; a second one kills
// everything at once and exits 130.
//
// The tests here are the process boundary layer. The wiring proofs that stay
// inside one process live in internal/daemon; these rows exist because signals,
// process groups and /proc do not exist inside a unit test.
package serve
