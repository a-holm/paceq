package arch_test

import (
	"strings"
	"testing"
)

// AC of M2-05: the tick transaction contains no process or network I/O. The
// honest architectural enforcement is at the import level: internal/store
// must not import os/exec, net or net/http in its production files, so no
// write path CAN exec a process or open a connection while it holds the
// write lock. Test files are exempt on purpose: the crash harness spawns
// child processes from tests, and that is exactly where process spawning
// belongs.
func TestTheStoreCannotExecOrDial(t *testing.T) {
	out := runGo(t, "list", "-f", "{{.ImportPath}}|{{join .Imports \" \"}}|{{join .TestImports \" \"}}", "./internal/store")

	forbidden := []string{"os/exec", "net", "net/http", "os/signal"}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.SplitN(line, "|", 3)
		if len(parts) != 3 {
			t.Fatalf("go list printed %q, want path|imports|test-imports", line)
		}
		for _, imp := range strings.Fields(parts[1]) {
			for _, bad := range forbidden {
				if imp == bad {
					t.Errorf("internal/store imports %s: the store must not be able to execute processes or open connections, or nothing can promise the tick transaction stays I/O free", imp)
				}
			}
		}
	}
}
