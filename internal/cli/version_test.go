package cli

import (
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// TestVersionJSONCarriesEveryField is what a bug report is written from, and
// what an upgrade check reads. Every field is asserted by name, because a
// missing one is only noticed the day somebody needs it.
func TestVersionJSONCarriesEveryField(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "version")

	if got.code != ExitOK {
		t.Fatalf("paceq version = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	doc := got.json(t)

	known, err := store.KnownSchemaVersion()
	if err != nil {
		t.Fatalf("read the known schema version: %v", err)
	}
	want := map[string]any{
		"version":        version,
		"commit":         commit,
		"built":          buildTime,
		"go":             runtime.Version(),
		"platform":       runtime.GOOS + "/" + runtime.GOARCH,
		"schema_version": float64(known),
	}
	for field, value := range want {
		if doc[field] != value {
			t.Errorf("%s = %v, want %v", field, doc[field], value)
		}
	}
	if len(doc) != len(want) {
		t.Errorf("the document has %d fields, want %d: %v", len(doc), len(want), doc)
	}
}

// TestVersionTextNamesTheSameFacts. The two modes are one command, and a text
// report that leaves out the commit sends people to the JSON.
func TestVersionTextNamesTheSameFacts(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "version", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq version -o text = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	known, err := store.KnownSchemaVersion()
	if err != nil {
		t.Fatalf("read the known schema version: %v", err)
	}
	for _, want := range []string{
		version, commit, buildTime, runtime.Version(),
		runtime.GOOS + "/" + runtime.GOARCH, "schema",
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the text report does not name %q:\n%s", want, got.stdout)
		}
	}
	if !strings.Contains(got.stdout, strconv.Itoa(known)) {
		t.Errorf("the text report does not name schema version %d:\n%s", known, got.stdout)
	}
}

// TestVersionJSONFlagMatchesTheOutputFlag keeps `paceq version --json`, which
// is what people type, doing the same as the global flag.
func TestVersionJSONFlagMatchesTheOutputFlag(t *testing.T) {
	short := runCLI(t, t.TempDir(), nil, "version", "--json")
	long := runCLI(t, t.TempDir(), nil, "version", "-o", "json")

	if short.code != ExitOK {
		t.Fatalf("paceq version --json = %d, want %d\n%s", short.code, ExitOK, short.stderr)
	}
	if short.stdout != long.stdout {
		t.Errorf("--json and -o json disagree:\n%s\n%s", short.stdout, long.stdout)
	}
}

// TestVersionFlagMatchesTheCommand keeps the flag people reach for first and
// the command that documents it from drifting apart.
func TestVersionFlagMatchesTheCommand(t *testing.T) {
	for _, mode := range []string{"text", "json"} {
		flag := runCLI(t, t.TempDir(), nil, "--version", "-o", mode)
		command := runCLI(t, t.TempDir(), nil, "version", "-o", mode)

		if flag.code != ExitOK {
			t.Fatalf("paceq --version -o %s = %d, want %d\n%s", mode, flag.code, ExitOK, flag.stderr)
		}
		if flag.stdout != command.stdout {
			t.Errorf("--version and version disagree in %s:\n%s\n%s", mode, flag.stdout, command.stdout)
		}
	}
}
