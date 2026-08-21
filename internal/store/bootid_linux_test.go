//go:build linux

package store

import (
	"os"
	"strings"
	"testing"
)

// TestReadBootIDReadsTheKernelsBootID pins both the source and the value. The
// path is spelled out here rather than taken from the constant, because reading
// the wrong file is exactly the mistake this test exists to catch: /proc has
// neighbouring files that look like an identifier and are not one.
func TestReadBootIDReadsTheKernelsBootID(t *testing.T) {
	raw, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Skipf("this kernel does not expose a boot id: %v", err)
	}
	want := strings.TrimSpace(string(raw))

	got, err := readBootID()
	if err != nil {
		t.Fatalf("read the boot id: %v", err)
	}
	if got != want {
		t.Errorf("boot id is %q, want the kernel's %q", got, want)
	}
	if len(got) != len("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx") {
		t.Errorf("boot id %q is not a UUID", got)
	}

	// The value has to be stable for as long as the machine is up. A source
	// that gives a fresh value on every read, /proc/sys/kernel/random/uuid
	// among them, would make every start look like a restart.
	again, err := readBootID()
	if err != nil {
		t.Fatalf("read the boot id again: %v", err)
	}
	if again != got {
		t.Errorf("two reads gave %q and %q: the source is not a boot identifier", got, again)
	}
}
