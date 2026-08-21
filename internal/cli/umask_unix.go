//go:build unix

package cli

import "syscall"

// setUmask makes every file paceq creates private by default (08 section 3.9).
// It is set once, at the start of Main, because the modes that matter are
// decided by whatever writes first.
func setUmask() { syscall.Umask(0o077) }
