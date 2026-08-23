package cronx

import (
	"os/exec"
	"strings"
	"testing"
)

// TestEmbeddedTzdataShipsInTheBuildGraph proves the binary carries its own
// zone database: time/tzdata is in the package import graph on the default
// build and drops out again under -tags notzdata. This is what makes
// Europe/Oslo resolvable inside a FROM scratch container with no
// /usr/share/zoneinfo, without needing a container runtime to prove it.
func TestEmbeddedTzdataShipsInTheBuildGraph(t *testing.T) {
	if !goListImports(t, "").Contains("time/tzdata") {
		t.Error("time/tzdata is not in the default build's import graph: a scratch container would not resolve Europe/Oslo")
	}
	if goListImports(t, "-tags notzdata").Contains("time/tzdata") {
		t.Error("time/tzdata is still linked under -tags notzdata: the escape hatch is broken")
	}
}

type importsList []string

func (l importsList) Contains(want string) bool {
	for _, v := range l {
		if v == want {
			return true
		}
	}
	return false
}

func goListImports(t *testing.T, tags string) importsList {
	t.Helper()
	args := []string{"list", "-f", "{{join .Imports \"\\n\"}}"}
	if tags != "" {
		args = append(args, "-tags", tags)
	}
	args = append(args, ".")
	out, err := exec.Command("go", args...).Output()
	if err != nil {
		t.Fatalf("go list -tags %q: %v", tags, err)
	}
	var result importsList
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	return result
}

func TestTzdataVersionHelpersReportChangeHonestly(t *testing.T) {
	cur := TzdataVersion()
	if cur == "" {
		t.Fatal("TzdataVersion() is empty, want a stamp")
	}
	if TzdataChanged(cur) {
		t.Errorf("TzdataChanged(%q) = true against itself, want false", cur)
	}
	if !TzdataChanged("") {
		t.Error(`TzdataChanged("") = false, want true: nothing stored yet means changed`)
	}
	if !TzdataChanged("go1.20.1") {
		t.Error(`TzdataChanged("go1.20.1") = false, want true against the current stamp`)
	}
}
