//go:build !unix

package daemon

import "syscall"

// killProcessGroup has nothing to deliver where process groups are not
// addressable. The scan finds nothing there either, so this is never reached.
func killProcessGroup(int, syscall.Signal) error { return nil }
