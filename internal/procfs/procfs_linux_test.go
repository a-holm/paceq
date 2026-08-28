package procfs

import (
	"os"
	"testing"
)

// The parse is the subtle part: a command name like "x () [] y" pushes the
// numeric fields behind the LAST closing parenthesis, and a naive split on
// spaces would read the wrong field. The line below mirrors what
// /proc/<pid>/stat holds for a process named to break both a first-paren
// parser and a fields split.
func TestParseStartTicksReadsField22AfterTheCommandName(t *testing.T) {
	// Fields after the name start at 3 (state), so start time, field 22
	// overall, is index 19 of what follows the parenthesis. 99887766 sits
	// exactly there, with decoys on both sides.
	line := []byte("4242 (x () [] y) R 1 1 1 0 -1 4194560 1 0 0 0 7 5 0 0 20 0 1 0 99887766 1 1 18446744073709551615 1 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0")
	ticks, err := parseStartTicks(line)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ticks != 99887766 {
		t.Fatalf("start ticks = %d, want 99887766", ticks)
	}
}

func TestParseStartTicksRefusesGarbage(t *testing.T) {
	for _, tc := range []struct {
		name string
		line []byte
	}{
		{"no command name", []byte("4242 R 1 2 3")},
		{"too few fields", []byte("4242 (sh) R 1 2")},
		{"not a number", []byte("4242 (sh) R 1 1 1 0 -1 4194560 1 0 0 0 7 5 0 0 20 0 1 0 later 1 1")},
	} {
		if _, err := parseStartTicks(tc.line); err == nil {
			t.Fatalf("%s: garbage was parsed", tc.name)
		}
	}
}

// The live read answers for a process that certainly exists: ourselves.
func TestProcStartTicksAnswersForSelf(t *testing.T) {
	ticks, ok := ProcStartTicks(os.Getpid())
	if !ok || ticks <= 0 {
		t.Fatalf("self start ticks = %d, %v; the kernel file is readable on Linux", ticks, ok)
	}
}

func TestBootIDIsNotEmpty(t *testing.T) {
	if BootID() == "" {
		t.Fatal("boot id is empty on Linux")
	}
}
