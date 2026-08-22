package spec_test

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

var update = flag.Bool("update", false, "rewrite the golden files from what the code produces now")

// TestOkFilesMatchTheirIR is the table driven pass over testdata/ok. Every file
// parses, canonicalises, and is compared byte for byte against the IR checked in
// beside it. The golden is the contract: a change to the canonical form has to
// be a change to a file somebody can read in a diff.
func TestOkFilesMatchTheirIR(t *testing.T) {
	for _, path := range jobFiles(t, "testdata/ok") {
		t.Run(filepath.Base(path), func(t *testing.T) {
			job, diags := spec.Parse(path, read(t, path))
			if diags.HasErrors() {
				t.Fatalf("%s does not parse:\n%s", path, render(t, diags))
			}
			if job == nil {
				t.Fatalf("%s parsed without errors but produced no job", path)
			}

			got := spec.Canonical(job)
			golden := strings.TrimSuffix(path, filepath.Ext(path)) + ".ir.json"
			if *update {
				writeGolden(t, golden, append(got, '\n'))
				return
			}
			want := bytes.TrimSuffix(read(t, golden), []byte("\n"))
			if !bytes.Equal(got, want) {
				t.Errorf("canonical IR for %s does not match %s\ngot:  %s\nwant: %s", path, golden, got, want)
			}
		})
	}
}

// TestCanonicalIsDeterministic is the guard against the classic bug in a
// canonicaliser: an output that depends on Go map iteration order. A thousand
// rounds of parse and canonicalise over a file with several maps in it produce
// one set of bytes, or the hash this project stores means nothing.
func TestCanonicalIsDeterministic(t *testing.T) {
	path := "testdata/ok/full.yaml"
	src := read(t, path)

	first, diags := spec.Parse(path, src)
	if diags.HasErrors() {
		t.Fatalf("%s does not parse:\n%s", path, render(t, diags))
	}
	want := spec.Canonical(first)

	for i := range 1000 {
		job, diags := spec.Parse(path, src)
		if diags.HasErrors() {
			t.Fatalf("round %d: %s stopped parsing", i, path)
		}
		if got := spec.Canonical(job); !bytes.Equal(got, want) {
			t.Fatalf("round %d produced different bytes:\ngot:  %s\nwant: %s", i, got, want)
		}
	}
}

// TestEnvOrderNeverReachesTheOutput canonicalises the same job many times from
// one parse. Parsing again each round would hide the bug this covers: a map
// built once and ranged over per round is exactly where iteration order leaks.
func TestEnvOrderNeverReachesTheOutput(t *testing.T) {
	job, diags := spec.Parse("env.yaml", []byte(`
name: many-keys
env:
  ALPHA: "1"
  BRAVO: "2"
  CHARLIE: "3"
  DELTA: "4"
  ECHO: "5"
  FOXTROT: "6"
  GOLF: "7"
  HOTEL: "8"
steps:
  - name: only
    run: ["/bin/true"]
`))
	if diags.HasErrors() {
		t.Fatalf("the fixture does not parse:\n%s", render(t, diags))
	}

	want := spec.Canonical(job)
	for i := range 1000 {
		if got := spec.Canonical(job); !bytes.Equal(got, want) {
			t.Fatalf("round %d produced different bytes:\ngot:  %s\nwant: %s", i, got, want)
		}
	}
	if !bytes.Contains(want, []byte(`"env":{"ALPHA":"1","BRAVO":"2","CHARLIE":"3"`)) {
		t.Errorf("env keys are not in sorted order: %s", want)
	}
}

// TestEquivalentFilesShareAHash is the acceptance criterion the whole IR exists
// for. Four ways of writing the same job, and one hash between them.
func TestEquivalentFilesShareAHash(t *testing.T) {
	groups := map[string][]string{
		"key order, comments and duration spelling": {
			"testdata/ok/full.yaml",
			"testdata/ok/reordered.yaml",
		},
		"a default left out and written out": {
			"testdata/ok/defaults.yaml",
			"testdata/ok/explicit-defaults.yaml",
		},
	}
	for name, paths := range groups {
		t.Run(name, func(t *testing.T) {
			hashes := map[string]string{}
			for _, path := range paths {
				hashes[path] = hashOf(t, path)
			}
			first := hashes[paths[0]]
			for _, path := range paths {
				if hashes[path] != first {
					t.Errorf("%s hashes to %s, %s to %s", paths[0], first, path, hashes[path])
				}
			}
			if !strings.HasPrefix(first, "sha256:") {
				t.Errorf("hash %q does not name the algorithm that produced it", first)
			}
			if len(first) != len("sha256:")+64 {
				t.Errorf("hash %q is not a sha256 in hex", first)
			}
		})
	}
}

// TestSetFieldsIgnoreOrder pins the other half of that decision: needs and
// inherit_env are sets, so writing them in another order is the same job. It is
// deliberate, and a test says so, because the mutation test below would
// otherwise be free to assume the opposite.
func TestSetFieldsIgnoreOrder(t *testing.T) {
	one := hashOfSource(t, `
name: sets
inherit_env: [ALPHA, BRAVO]
steps:
  - name: first
    run: ["/bin/true"]
  - name: second
    run: ["/bin/true"]
    needs: [first]
`)
	other := hashOfSource(t, `
name: sets
inherit_env: [BRAVO, ALPHA]
steps:
  - name: first
    run: ["/bin/true"]
  - name: second
    run: ["/bin/true"]
    needs: [first]
`)
	if one != other {
		t.Errorf("reordering a set changed the hash: %s and %s", one, other)
	}
}

// TestEveryMeaningfulChangeChangesTheHash is the other direction, and the one
// that would catch a hash taken over too little: twenty small edits, every one
// of them a different job, and twenty different hashes.
func TestEveryMeaningfulChangeChangesTheHash(t *testing.T) {
	const base = `name: report
description: A description.
env:
  PGHOST: db.internal
env_file: .env
inherit_env: [SSH_AUTH_SOCK]
workdir: /srv
timeout: 45m
max_concurrent: 2
steps:
  - name: extract
    run: ["/bin/extract", "--out", "/srv/data"]
    timeout: 20m
    workdir: /srv/work
    retry:
      max: 3
      backoff: exponential
      initial: 10s
      max_delay: 5m
      jitter: full
  - name: transform
    run: ["/bin/transform"]
    needs: [extract]
schedules:
  - name: nightly
    cron: "0 3 * * *"
    timezone: Europe/Oslo
sensors:
  - name: dropzone
    type: file
    interval: 15s
`
	mutations := []struct {
		name string
		from string
		to   string
	}{
		{"job name", "name: report", "name: reports"},
		{"description", "A description.", "Another description."},
		{"env value", "PGHOST: db.internal", "PGHOST: db.external"},
		{"env key", "PGHOST: db.internal", "PGHOSTS: db.internal"},
		{"env file", "env_file: .env", "env_file: .env.prod"},
		{"inherited variable", "inherit_env: [SSH_AUTH_SOCK]", "inherit_env: [SSH_AGENT_PID]"},
		{"job workdir", "workdir: /srv\n", "workdir: /srv2\n"},
		{"job timeout", "timeout: 45m", "timeout: 46m"},
		{"max concurrent", "max_concurrent: 2", "max_concurrent: 3"},
		{"step name", "name: transform", "name: transforms"},
		{"argv program", "/bin/extract", "/bin/extract2"},
		{"argv flag", `"--out"`, `"--output"`},
		{"argv order", `["/bin/extract", "--out", "/srv/data"]`, `["/bin/extract", "/srv/data", "--out"]`},
		{"an argv element added", `["/bin/transform"]`, `["/bin/transform", "-v"]`},
		{"step timeout", "timeout: 20m", "timeout: 21m"},
		{"step workdir", "workdir: /srv/work", "workdir: /srv/work2"},
		{"retry max", "max: 3", "max: 4"},
		{"retry backoff", "backoff: exponential", "backoff: fixed"},
		{"retry initial", "initial: 10s", "initial: 11s"},
		{"retry max delay", "max_delay: 5m", "max_delay: 6m"},
		{"retry jitter", "jitter: full", "jitter: none"},
		{"shell turned on", "run: [\"/bin/transform\"]", "run: [\"/bin/transform\"]\n    shell: true"},
		{"a needs edge removed", "needs: [extract]", "needs: []"},
		{"a step added", "  - name: transform", "  - name: load\n    run: [\"/bin/load\"]\n  - name: transform"},
		{"cron expression", `cron: "0 3 * * *"`, `cron: "0 4 * * *"`},
		{"schedule timezone", "timezone: Europe/Oslo", "timezone: Europe/Stockholm"},
		{"schedule name", "name: nightly", "name: nightlies"},
		{"sensor interval", "interval: 15s", "interval: 16s"},
		{"sensor type", "type: file", "type: http"},
	}

	seen := map[string]string{hashOfSource(t, base): "the file as written"}
	for _, mutation := range mutations {
		mutated := strings.Replace(base, mutation.from, mutation.to, 1)
		if mutated == base {
			t.Fatalf("the mutation %q did not change the source: %q is not in it", mutation.name, mutation.from)
		}
		hash := hashOfSource(t, mutated)
		if previous, taken := seen[hash]; taken {
			t.Errorf("%q hashes the same as %q: %s", mutation.name, previous, hash)
			continue
		}
		seen[hash] = mutation.name
	}
	if len(seen) != len(mutations)+1 {
		t.Errorf("%d mutations produced %d distinct hashes, want %d", len(mutations), len(seen)-1, len(mutations))
	}
}

// TestDefaultsAreMaterialisedBeforeHashing is the trap the issue names by name.
// A job that leaves max_concurrent out has to carry the value 1 in its IR, or
// M1-03 records a new version every time somebody tidies a file up.
func TestDefaultsAreMaterialisedBeforeHashing(t *testing.T) {
	job, diags := spec.Parse("defaults.yaml", read(t, "testdata/ok/defaults.yaml"))
	if diags.HasErrors() {
		t.Fatalf("the fixture does not parse:\n%s", render(t, diags))
	}

	ir := string(spec.Canonical(job))
	for _, want := range []string{
		`"schema":"paceq.job.v1"`,
		`"max_concurrent":1`,
		`"timeout_ms":3600000`,
		`"shell":false`,
	} {
		if !strings.Contains(ir, want) {
			t.Errorf("the IR does not carry %s:\n%s", want, ir)
		}
	}
	if job.Timeout != spec.DefaultTimeout {
		t.Errorf("timeout is %v, want the system default %v", job.Timeout, spec.DefaultTimeout)
	}
	if job.MaxConcurrent != spec.DefaultMaxConcurrent {
		t.Errorf("max_concurrent is %d, want %d", job.MaxConcurrent, spec.DefaultMaxConcurrent)
	}
}

// TestEmptyCollectionsAreLeftOut is the other rule that keeps two equivalent
// files on one hash: an empty list and a missing list are the same job.
func TestEmptyCollectionsAreLeftOut(t *testing.T) {
	written := hashOfSource(t, `
name: empty-collections
env: {}
inherit_env: []
schedules: []
sensors: []
steps:
  - name: only
    run: ["/bin/true"]
    needs: []
`)
	omitted := hashOfSource(t, `
name: empty-collections
steps:
  - name: only
    run: ["/bin/true"]
`)
	if written != omitted {
		t.Errorf("empty collections changed the hash: %s with them, %s without", written, omitted)
	}

	job, _ := spec.Parse("x.yaml", []byte("name: empty-collections\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))
	for _, absent := range []string{`"env"`, `"inherit_env"`, `"schedules"`, `"sensors"`, `"needs"`, `"retry"`} {
		if strings.Contains(string(spec.Canonical(job)), absent) {
			t.Errorf("the IR carries %s for a job that has none", absent)
		}
	}
}

// TestScalarsKeepTheTextTheyWereWrittenAs is the Norway problem. A YAML 1.1
// reader turns the country code NO into false; a job file is not a place where
// that can be allowed to happen quietly.
func TestScalarsKeepTheTextTheyWereWrittenAs(t *testing.T) {
	job, diags := spec.Parse("scalars.yaml", read(t, "testdata/ok/scalars.yaml"))
	if diags.HasErrors() {
		t.Fatalf("the fixture does not parse:\n%s", render(t, diags))
	}

	want := map[string]string{
		"COUNTRY": "no",
		"ANSWER":  "yes",
		"SWITCH":  "on",
		"VERSION": "1.20",
		"PORT":    "5432",
	}
	for name, value := range want {
		if job.Env[name] != value {
			t.Errorf("env %s is %q, want %q", name, job.Env[name], value)
		}
	}
	if got := strings.Join(job.InheritEnv, ","); got != "NO,YES,ON,OFF" {
		t.Errorf("inherit_env is %q, want the four names as written", got)
	}
}

// TestStepTimeoutIsNotMaterialised keeps the one default that deliberately is
// not one: a step without a timeout is bounded by the job's, and writing the
// job's value onto every step would claim a ceiling the job never asked for.
func TestStepTimeoutIsNotMaterialised(t *testing.T) {
	job, _ := spec.Parse("x.yaml", []byte("name: x\ntimeout: 20m\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))
	ir := string(spec.Canonical(job))

	if strings.Count(ir, `"timeout_ms"`) != 1 {
		t.Errorf("the IR has a per step timeout for a step that has none:\n%s", ir)
	}
	if job.Steps[0].Timeout != 0 {
		t.Errorf("step timeout is %v, want zero for a step bounded by the job", job.Steps[0].Timeout)
	}
}

// TestCompileCarriesTheBytesItHashed. A hash on its own cannot be checked.
func TestCompileCarriesTheBytesItHashed(t *testing.T) {
	job, _ := spec.Parse("x.yaml", read(t, "testdata/ok/minimal.yaml"))

	compiled := spec.Compile(job)

	if compiled.Hash != spec.Hash(compiled.Canonical) {
		t.Errorf("Compile returned %s for bytes that hash to %s", compiled.Hash, spec.Hash(compiled.Canonical))
	}
	if compiled.Job != job {
		t.Error("Compile did not carry the job it was given")
	}
}

func hashOf(t *testing.T, path string) string {
	t.Helper()

	job, diags := spec.Parse(path, read(t, path))
	if diags.HasErrors() {
		t.Fatalf("%s does not parse:\n%s", path, render(t, diags))
	}
	return spec.Compile(job).Hash
}

func hashOfSource(t *testing.T, src string) string {
	t.Helper()

	job, diags := spec.Parse("source.yaml", []byte(src))
	if diags.HasErrors() {
		t.Fatalf("the source does not parse:\n%s\n%s", src, render(t, diags))
	}
	return spec.Compile(job).Hash
}

func jobFiles(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".yaml" {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	if len(files) == 0 {
		t.Fatalf("no job files in %s, the table would be empty", dir)
	}
	return files
}

func read(t *testing.T, path string) []byte {
	t.Helper()

	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return src
}

func writeGolden(t *testing.T, path string, content []byte) {
	t.Helper()

	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s", path)
}

// readIfPossible is read for a path that may not exist, used when a failure
// message wants the source behind a diagnostic and the diagnostic came from a
// string rather than from a file.
func readIfPossible(path string) ([]byte, error) {
	return os.ReadFile(path)
}
