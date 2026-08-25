package cli

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/store"
)

// TestVersionJSONCarriesEveryField is what a bug report is written from, and
// what an upgrade check reads. Every field is asserted by name against the one
// source it comes from, because a missing one is only noticed the day somebody
// needs it. This is also the parity half of issue #43: the report and the
// release pipeline read the same internal/buildinfo values, so they cannot
// diverge.
func TestVersionJSONCarriesEveryField(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "version")

	if got.code != ExitOK {
		t.Fatalf("paceq version = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	doc := got.json(t)

	stamped := buildinfo.Get()
	want := map[string]any{
		"version":        stamped.Version,
		"commit":         stamped.Commit,
		"date":           stamped.Date,
		"go_version":     stamped.GoVersion,
		"os":             stamped.OS,
		"arch":           stamped.Arch,
		"schema_version": float64(knownSchema(t)),
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

// TestVersionJSONParsesAsTheFrozenDocument runs the exact contract of the
// release smoke job: `paceq version --json` must parse as JSON carrying the
// six frozen buildinfo fields (plan for #43, test 6).
func TestVersionJSONParsesAsTheFrozenDocument(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "version", "--json")

	if got.code != ExitOK {
		t.Fatalf("paceq version --json = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("paceq version --json is not valid JSON: %v\n%s", err, got.stdout)
	}
	for _, field := range []string{"version", "commit", "date", "go_version", "os", "arch"} {
		if _, ok := doc[field]; !ok {
			t.Errorf("the document does not carry %q: %s", field, got.stdout)
		}
	}
}

// TestVersionTextNamesTheSameFacts. The two modes are one command, and a text
// report that leaves out the commit sends people to the JSON.
func TestVersionTextNamesTheSameFacts(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "version", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq version -o text = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	stamped := buildinfo.Get()
	known := knownSchema(t)
	for _, want := range []string{
		stamped.Version, stamped.Commit, stamped.Date, stamped.GoVersion,
		stamped.OS + "/" + stamped.Arch, "schema",
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

func knownSchema(t *testing.T) int {
	t.Helper()

	known, err := store.KnownSchemaVersion()
	if err != nil {
		t.Fatalf("read the known schema version: %v", err)
	}
	return known
}
