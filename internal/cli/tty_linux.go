//go:build linux

package cli

import "syscall"

// terminalRequest reads a terminal's settings. Linux spells it TCGETS.
const terminalRequest = syscall.TCGETS
