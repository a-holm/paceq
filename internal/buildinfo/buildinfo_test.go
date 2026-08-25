package buildinfo

import (
	"encoding/json"
	"runtime"
	"testing"
)

// TestGetCarriesTheStampedValues proves that -ldflags -X reaches the report.
// The linker writes the package variables; Get copies them into the struct, so
// an injection that misses either half never shows up for a user.
func TestGetCarriesTheStampedValues(t *testing.T) {
	Version = "0.1.0"
	Commit = "7cfc8f0a4a33b43a3705249e9012bab5337da68a"
	Date = "2026-08-25T10:00:00Z"
	defer func() {
		Version = "dev"
		Commit = "unknown"
		Date = "unknown"
	}()

	got := Get()
	switch {
	case got.Version != "0.1.0":
		t.Errorf("Version = %q, want the stamped 0.1.0", got.Version)
	case got.Commit != "7cfc8f0a4a33b43a3705249e9012bab5337da68a":
		t.Errorf("Commit = %q, want the stamped full hash", got.Commit)
	case got.Date != "2026-08-25T10:00:00Z":
		t.Errorf("Date = %q, want the stamped commit date", got.Date)
	}
}

// TestGetCarriesTheRuntimeFacts: go version, os and arch describe the binary
// itself, so they come from the runtime and never from a stamp that could lie.
func TestGetCarriesTheRuntimeFacts(t *testing.T) {
	got := Get()
	if got.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
	}
	if got.OS != runtime.GOOS {
		t.Errorf("OS = %q, want %q", got.OS, runtime.GOOS)
	}
	if got.Arch != runtime.GOARCH {
		t.Errorf("Arch = %q, want %q", got.Arch, runtime.GOARCH)
	}
}

// TestInfoJSONCarriesEveryFrozenField: these names ship in v0.1 archives and
// the smoke job parses them by name, so a renamed or missing field is a
// contract break, not a refactor. Asserted both ways: every name present, and
// nothing extra.
func TestInfoJSONCarriesEveryFrozenField(t *testing.T) {
	raw, err := json.Marshal(Get())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	want := []string{"version", "commit", "date", "go_version", "os", "arch"}
	if len(doc) != len(want) {
		t.Errorf("the document has %d fields, want %d: %v", len(doc), len(want), doc)
	}
	for _, name := range want {
		if _, ok := doc[name]; !ok {
			t.Errorf("the document does not carry %q: %s", name, raw)
		}
	}
}

// TestDefaultsAreHonest keeps the unstamped story truthful: a developer build
// says dev, not a version it was not built from.
func TestDefaultsAreHonest(t *testing.T) {
	if Version != "dev" {
		t.Errorf("Version default = %q, want dev", Version)
	}
	if Commit != "unknown" {
		t.Errorf("Commit default = %q, want unknown", Commit)
	}
	if Date != "unknown" {
		t.Errorf("Date default = %q, want unknown", Date)
	}
}
