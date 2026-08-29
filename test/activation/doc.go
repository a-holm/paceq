// Package activation is the proof that a job file reaches the daemon. It
// applies a real job file with the real binary, starts a real `paceq serve`,
// and waits for the state the daemon alone can write: a sensor tick with a
// moved cursor, and a schedule tick with the run it fired.
//
// Every other test in the tree drives the pieces directly. That is why the
// daemon could ship for a whole milestone with the sensor runtime holding a
// nil source and a nil sink, and with apply writing no schedule row at all
// (#182): each piece worked, and nothing asserted that they were joined. The
// rows here write nothing through the store seam, so the only thing that can
// make them pass is apply and the daemon together.
//
// Layout:
//
//	harness_test.go   the binary, the fixture, the workspace, apply, serve
//	sensor_test.go    the sensor half: a tick and a cursor
//	schedule_test.go  the schedule half: a tick and a run
package activation
