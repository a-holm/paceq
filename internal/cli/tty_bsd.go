//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package cli

import "syscall"

// terminalRequest reads a terminal's settings. The BSDs spell it TIOCGETA.
const terminalRequest = syscall.TIOCGETA
