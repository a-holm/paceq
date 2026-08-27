//go:build !unix

package notify

import "testing"

// The group-kill proof needs /proc and process groups; on this platform the
// escalation degrades to killing the leader, which the timeout branch of the
// Send error path already covers.
func runUnixGroupKill(t *testing.T) {
	t.Skip("process-group kill is a unix contract; covered by the unix builder")
}
