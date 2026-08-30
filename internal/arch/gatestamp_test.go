//go:build unix

package arch_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The gate stamp mechanism (#176) lets `make ci` skip a target that this exact
// tree has already passed. Every skip is a claim that running the target again
// would change nothing, and a claim that is even slightly too generous turns the
// gate advisory. So the tests below do two jobs.
//
// The first job is the ordinary one: name every input a stamp has to cover and
// prove the key moves when it does, and name every way a stamp can be missing,
// stale or damaged and prove the target runs anyway.
//
// The second job is the one #177 taught. A guard that passes against a broken
// mechanism guards nothing, and a guard nobody has ever seen go red is
// indistinguishable from one. TestGateStampGuardsCanSeeABrokenMechanism breaks
// the scripts in the three ways that would matter - stamp before the target
// runs, honour a stamp whose key is for another tree, keep the stamps where
// every worktree can read them - and fails if the scenarios above still pass.

// stampWorld is a throwaway repository the gate mechanism runs against for real,
// with make and go stubbed on PATH so no gate target is ever paid for.
type stampWorld struct {
	t       *testing.T
	dir     string // the working tree
	bin     string // stub tools, first on PATH
	calls   string // one line per make invocation
	scripts string // the gate-run.sh and gate-stamp.sh under test
	env     map[string]string
}

// gateRun is what one scripts/gate-run.sh invocation did.
type gateRun struct {
	exitCode  int
	stdout    string
	stderr    string
	makeCalls []string
}

func (r gateRun) ranTarget(target string) bool {
	for _, call := range r.makeCalls {
		if strings.HasSuffix(call, " "+target) {
			return true
		}
	}
	return false
}

const (
	// What the stub go reports for every go env key, so a test can move the
	// toolchain without having a second one installed.
	stubGoDefault = "go1.27.0-stub"
	// A tracked file every scenario can edit, and the bytes it starts with.
	stampSubject     = "internal/store/runs.go"
	stampSubjectBody = "package store\n\nfunc Runs() {}\n"
)

func newStampWorld(t *testing.T, scripts string) *stampWorld {
	t.Helper()

	root := t.TempDir()
	w := &stampWorld{
		t:       t,
		dir:     filepath.Join(root, "tree"),
		bin:     filepath.Join(root, "bin"),
		calls:   filepath.Join(root, "calls"),
		scripts: scripts,
		env: map[string]string{
			"PACEQ_STUB_GO":         stubGoDefault,
			"PACEQ_GATE_VARS":       "GO=go FUZZTIME=60s",
			"PACEQ_STUB_EXIT":       "0",
			"PACEQ_STUB_CALLS":      "",
			"PACEQ_STUB_DIRTY":      "",
			"PACEQ_STUB_DIRTY_FILE": "",
		},
	}
	for _, d := range []string{w.dir, w.bin} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
	}
	w.env["PACEQ_STUB_CALLS"] = w.calls

	// make records what it was asked for and obeys PACEQ_STUB_EXIT. The gate
	// mechanism is what is under test; the gate itself is not.
	// PACEQ_STUB_DIRTY names the target that writes into the working tree, so
	// a test can prove what happens to the targets after it.
	w.writeStub("make", "#!/bin/sh\n"+
		"printf '%s\\n' \"$*\" >>\"$PACEQ_STUB_CALLS\"\n"+
		"printf 'stub make: %s\\n' \"$*\"\n"+
		"if [ -n \"$PACEQ_STUB_DIRTY\" ]; then\n"+
		"\tcase \"$*\" in\n"+
		"\t*\" $PACEQ_STUB_DIRTY\") printf 'dirt\\n' >>\"$PACEQ_STUB_DIRTY_FILE\" ;;\n"+
		"\tesac\n"+
		"fi\n"+
		"exit \"$PACEQ_STUB_EXIT\"\n")
	// go answers `go env <keys>` with one line per key, all carrying the same
	// stub toolchain identity, so a test can change the toolchain by changing
	// one environment variable.
	w.writeStub("go", "#!/bin/sh\n"+
		"if [ \"$1\" = env ]; then\n"+
		"\tshift\n"+
		"\tfor v do printf '%s=%s\\n' \"$v\" \"$PACEQ_STUB_GO\"; done\n"+
		"\texit 0\n"+
		"fi\n"+
		"printf 'go version %s stub/stub\\n' \"$PACEQ_STUB_GO\"\n")

	w.git("init", "-q", ".")
	w.git("config", "user.email", "gate@example.invalid")
	w.git("config", "user.name", "gate")
	w.write(".gitignore", "bin/\n")
	w.write(stampSubject, stampSubjectBody)
	w.write("scripts/tool.sh", "#!/bin/sh\necho tool\n")
	w.git("add", "-A")
	w.git("commit", "-qm", "init")

	return w
}

func (w *stampWorld) writeStub(name, body string) {
	w.t.Helper()

	if err := os.WriteFile(filepath.Join(w.bin, name), []byte(body), 0o700); err != nil {
		w.t.Fatalf("write the %s stub: %v", name, err)
	}
}

func (w *stampWorld) write(name, content string) {
	w.t.Helper()

	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		w.t.Fatalf("create the directory for %s: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		w.t.Fatalf("write %s: %v", name, err)
	}
}

// command builds a command in the working tree with the stubs first on PATH.
func (w *stampWorld) command(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Dir = w.dir
	cmd.Env = append(os.Environ(), "PATH="+w.bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for k, v := range w.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

func (w *stampWorld) git(args ...string) string {
	w.t.Helper()

	out, err := w.command("git", args...).CombinedOutput()
	if err != nil {
		w.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func (w *stampWorld) stamp(args ...string) (string, error) {
	w.t.Helper()

	out, err := w.command("sh", append([]string{filepath.Join(w.scripts, "gate-stamp.sh")}, args...)...).Output()
	return strings.TrimSpace(string(out)), err
}

// key is the content key for the world as it stands. A failure here is a
// failure of the test setup, not a finding.
func (w *stampWorld) key() string {
	w.t.Helper()

	out, err := w.stamp("key")
	if err != nil {
		w.t.Fatalf("gate-stamp.sh key: %v", err)
	}
	if len(out) != 64 {
		w.t.Fatalf("gate-stamp.sh key printed %q, want a sha256", out)
	}
	return out
}

// stampFile is where this working tree keeps its stamps.
func (w *stampWorld) stampFile() string {
	w.t.Helper()

	out, err := w.stamp("file")
	if err != nil {
		w.t.Fatalf("gate-stamp.sh file: %v", err)
	}
	if filepath.IsAbs(out) {
		return out
	}
	return filepath.Join(w.dir, out)
}

// run drives scripts/gate-run.sh over the given targets and reports what the
// make stub was asked for.
func (w *stampWorld) run(targets ...string) gateRun {
	w.t.Helper()

	if err := os.WriteFile(w.calls, nil, 0o600); err != nil {
		w.t.Fatalf("reset the call log: %v", err)
	}

	cmd := w.command("sh", append([]string{filepath.Join(w.scripts, "gate-run.sh")}, targets...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
	default:
		w.t.Fatalf("run gate-run.sh: %v", err)
	}

	return gateRun{
		exitCode:  cmd.ProcessState.ExitCode(),
		stdout:    stdout.String(),
		stderr:    stderr.String(),
		makeCalls: readLines(w.t, w.calls),
	}
}

// TestGateStampKeyCoversEveryInputTheGateReads names every class of input that
// has to move the key, and the one class that deliberately does not. A stamp is
// only as honest as this list.
func TestGateStampKeyCoversEveryInputTheGateReads(t *testing.T) {
	scripts := filepath.Join(repoRoot(t), "scripts")

	cases := []struct {
		name string
		// wantMove is whether the key has to change. A false entry is a
		// documented hole, not an oversight.
		wantMove bool
		apply    func(w *stampWorld)
	}{
		{"nothing at all", false, func(*stampWorld) {}},
		{"the content of a tracked file", true, func(w *stampWorld) {
			w.write(stampSubject, "package store\n\nfunc Runs() { _ = 1 }\n")
		}},
		{"the path of a tracked file, content untouched", true, func(w *stampWorld) {
			w.git("mv", stampSubject, "internal/store/runs2.go")
		}},
		{"the executable bit of a tracked file", true, func(w *stampWorld) {
			if err := os.Chmod(filepath.Join(w.dir, "scripts/tool.sh"), 0o700); err != nil {
				w.t.Fatalf("chmod: %v", err)
			}
		}},
		{"a tracked file taken out of the working tree", true, func(w *stampWorld) {
			if err := os.Remove(filepath.Join(w.dir, stampSubject)); err != nil {
				w.t.Fatalf("remove: %v", err)
			}
		}},
		{"an untracked file git would keep", true, func(w *stampWorld) {
			w.write("internal/store/scratch.go", "package store\n")
		}},
		{"the Go toolchain", true, func(w *stampWorld) {
			w.env["PACEQ_STUB_GO"] = "go1.28.0-stub"
		}},
		{"a make variable the environment can override", true, func(w *stampWorld) {
			w.env["PACEQ_GATE_VARS"] = "GO=go FUZZTIME=1s"
		}},
		{"an optional tool arriving on PATH", true, func(w *stampWorld) {
			w.writeStub("shellcheck", "#!/bin/sh\necho 'ShellCheck - stub'\n")
		}},
		// The hole, stated rather than hidden: an ignored file is not in
		// the key, so a build that reads one can go stale behind a stamp.
		{"a file git ignores", false, func(w *stampWorld) {
			w.write("bin/artifact", "whatever\n")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newStampWorld(t, scripts)
			before := w.key()
			tc.apply(w)
			after := w.key()

			if moved := before != after; moved != tc.wantMove {
				verb := "moved"
				if !moved {
					verb = "stayed put"
				}
				t.Fatalf("changing %s: the key %s (%s -> %s), want moved = %v",
					tc.name, verb, before[:12], after[:12], tc.wantMove)
			}
		})
	}
}

// TestGateStampKeyIsStableAndReal proves the key is a function of the tree and
// not of the clock, and that the real toolchain on this machine produces one at
// all. Everything else here runs against stubs; this case does not.
func TestGateStampKeyIsStableAndReal(t *testing.T) {
	root := repoRoot(t)

	cmd := exec.Command("sh", filepath.Join(root, "scripts", "gate-stamp.sh"), "key")
	cmd.Dir = root
	first, err := cmd.Output()
	if err != nil {
		t.Fatalf("gate-stamp.sh key in the real tree: %v", err)
	}
	cmd = exec.Command("sh", filepath.Join(root, "scripts", "gate-stamp.sh"), "key")
	cmd.Dir = root
	second, err := cmd.Output()
	if err != nil {
		t.Fatalf("gate-stamp.sh key in the real tree, second time: %v", err)
	}

	got := strings.TrimSpace(string(first))
	if len(got) != 64 {
		t.Fatalf("key = %q, want a sha256", got)
	}
	if want := strings.TrimSpace(string(second)); got != want {
		t.Fatalf("the key is not a function of the tree: %s then %s", got, want)
	}
}

// TestGateRunSkipsOnlyWhatThisExactTreeProved is the whole point of the
// mechanism, and every case but the first is a way it has to fail closed.
func TestGateRunSkipsOnlyWhatThisExactTreeProved(t *testing.T) {
	scripts := filepath.Join(repoRoot(t), "scripts")

	cases := []struct {
		name    string
		wantRun bool
		before  func(w *stampWorld)
	}{
		{"the same tree, nothing touched", false, func(*stampWorld) {}},
		{"one byte of one tracked file", true, func(w *stampWorld) {
			w.write(stampSubject, "package store\n\nfunc Runs() {}\n\n")
		}},
		{"a new untracked file", true, func(w *stampWorld) {
			w.write("internal/store/extra.go", "package store\n")
		}},
		{"a newer toolchain", true, func(w *stampWorld) {
			w.env["PACEQ_STUB_GO"] = "go1.28.0-stub"
		}},
		{"no stamp file", true, func(w *stampWorld) {
			if err := os.Remove(w.stampFile()); err != nil {
				w.t.Fatalf("remove the stamp file: %v", err)
			}
		}},
		{"a stamp file of nonsense", true, func(w *stampWorld) {
			if err := os.WriteFile(w.stampFile(), []byte("not a stamp\n\x00\x01\n"), 0o600); err != nil {
				w.t.Fatalf("corrupt the stamp file: %v", err)
			}
		}},
		{"a stamp file that is not a file", true, func(w *stampWorld) {
			path := w.stampFile()
			if err := os.Remove(path); err != nil {
				w.t.Fatalf("remove the stamp file: %v", err)
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				w.t.Fatalf("put a directory in its place: %v", err)
			}
		}},
		{"a stamp older than its window", true, func(w *stampWorld) {
			ageOutStamps(w)
		}},
		{"PACEQ_GATE_STAMP=0", true, func(w *stampWorld) {
			w.env["PACEQ_GATE_STAMP"] = "0"
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newStampWorld(t, scripts)
			if cold := w.run("vet"); !cold.ranTarget("vet") {
				t.Fatalf("the cold run did not run vet, calls = %q", cold.makeCalls)
			}

			tc.before(w)
			got := w.run("vet")

			if got.exitCode != 0 {
				t.Fatalf("gate-run exited %d\nstdout: %s\nstderr: %s", got.exitCode, got.stdout, got.stderr)
			}
			if ran := got.ranTarget("vet"); ran != tc.wantRun {
				t.Fatalf("after %s, vet ran = %v, want %v\nstdout: %s", tc.name, ran, tc.wantRun, got.stdout)
			}
			// Condition 6: a skip is never silent.
			if !tc.wantRun && !strings.Contains(got.stdout, "vet skipped, this exact tree passed it at") {
				t.Fatalf("a skipped target has to say so, stdout = %q", got.stdout)
			}
		})
	}
}

// TestGateRunStampsNothingItDidNotSee covers the other half of the claim: the
// gate is the only writer, and it writes only after make exited zero.
func TestGateRunStampsNothingItDidNotSee(t *testing.T) {
	w := newStampWorld(t, filepath.Join(repoRoot(t), "scripts"))

	w.env["PACEQ_STUB_EXIT"] = "1"
	failed := w.run("vet")
	if failed.exitCode == 0 {
		t.Fatalf("a failing target has to fail the gate, exit code = 0\nstdout: %s", failed.stdout)
	}
	if _, err := os.Stat(w.stampFile()); !os.IsNotExist(err) {
		t.Fatalf("a failing target wrote a stamp file: %v\n%s", err, readFile(t, w.stampFile()))
	}

	w.env["PACEQ_STUB_EXIT"] = "0"
	if retry := w.run("vet"); !retry.ranTarget("vet") {
		t.Fatalf("the retry after a red target skipped it, stdout: %s", retry.stdout)
	}
}

// TestGateRunKeepsGoingTargetByTarget is what makes a retry after one red
// target cheap: everything green before it stays proven, so the retry starts
// where the failure was.
func TestGateRunKeepsGoingTargetByTarget(t *testing.T) {
	w := newStampWorld(t, filepath.Join(repoRoot(t), "scripts"))

	if first := w.run("fmt-check", "vet"); !first.ranTarget("vet") {
		t.Fatalf("the cold run did not reach vet, calls = %q", first.makeCalls)
	}

	second := w.run("fmt-check", "vet", "staticcheck")
	if second.ranTarget("fmt-check") || second.ranTarget("vet") {
		t.Fatalf("the proven targets ran again, calls = %q", second.makeCalls)
	}
	if !second.ranTarget("staticcheck") {
		t.Fatalf("the new target did not run, calls = %q", second.makeCalls)
	}
	if !strings.Contains(second.stdout, "gate-summary: 2 of 3 targets skipped") {
		t.Fatalf("the summary has to count the skips, stdout = %q", second.stdout)
	}
	if !strings.Contains(second.stdout, "PACEQ_GATE_STAMP=0") {
		t.Fatalf("a run that skipped has to name the way to run everything, stdout = %q", second.stdout)
	}
}

// TestGateRunSaysWhenATargetChangedTheTree keeps the rekeying visible. A target
// that writes into the tree hands the next one different bytes, and the run says
// so rather than letting the change pass as a detail of the log.
func TestGateRunSaysWhenATargetChangedTheTree(t *testing.T) {
	w := newStampWorld(t, filepath.Join(repoRoot(t), "scripts"))
	w.env["PACEQ_STUB_DIRTY"] = "fmt-check"
	w.env["PACEQ_STUB_DIRTY_FILE"] = stampSubject

	got := w.run("fmt-check", "vet")

	if got.exitCode != 0 {
		t.Fatalf("gate-run exited %d\nstdout: %s\nstderr: %s", got.exitCode, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stdout, "fmt-check changed the tree") {
		t.Fatalf("a target that wrote into the tree has to say so, stdout = %q", got.stdout)
	}
}

// TestGateStampNeverReachesTheIndex proves the stamp cannot travel: it lives
// under the git directory, where nothing can stage it.
func TestGateStampNeverReachesTheIndex(t *testing.T) {
	w := newStampWorld(t, filepath.Join(repoRoot(t), "scripts"))
	w.run("vet")

	if status := w.git("status", "--porcelain"); status != "" {
		t.Fatalf("a stamped run left the working tree dirty:\n%s", status)
	}
	gitDir := w.git("rev-parse", "--absolute-git-dir")
	file := w.stampFile()
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("the stamped run wrote no stamp file: %v", err)
	}
	if !strings.HasPrefix(file, gitDir+string(os.PathSeparator)) {
		t.Fatalf("the stamp file is at %s, want it under the git directory %s", file, gitDir)
	}
}

// TestGateStampIsPerWorktree matters because every issue in this repository is
// worked in a linked worktree, where .git is a file pointing into the shared
// repository. One worktree must never vouch for another.
func TestGateStampIsPerWorktree(t *testing.T) {
	w := newStampWorld(t, filepath.Join(repoRoot(t), "scripts"))
	if cold := w.run("vet"); !cold.ranTarget("vet") {
		t.Fatalf("the cold run did not run vet, calls = %q", cold.makeCalls)
	}
	if warm := w.run("vet"); warm.ranTarget("vet") {
		t.Fatalf("the warm run in the same worktree ran vet, stdout: %s", warm.stdout)
	}

	other := linkedWorktree(w, "other")
	if got := other.run("vet"); !got.ranTarget("vet") {
		t.Fatalf("a linked worktree honoured another worktree's stamp, stdout: %s", got.stdout)
	}
	if other.stampFile() == w.stampFile() {
		t.Fatalf("both worktrees stamp into %s", w.stampFile())
	}
}

// gateStampScenario is a claim about the mechanism that the honest scripts keep
// and a broken one does not. It reports what went wrong, or nothing.
type gateStampScenario struct {
	name  string
	check func(t *testing.T, scripts string) string
}

var gateStampScenarios = []gateStampScenario{
	{
		name: "a target that failed is not proven",
		check: func(t *testing.T, scripts string) string {
			w := newStampWorld(t, scripts)
			w.env["PACEQ_STUB_EXIT"] = "1"
			w.run("vet")
			w.env["PACEQ_STUB_EXIT"] = "0"
			if got := w.run("vet"); !got.ranTarget("vet") {
				return "vet was skipped after a run in which it failed"
			}
			return ""
		},
	},
	{
		name: "a stamp for another tree is not honoured",
		check: func(t *testing.T, scripts string) string {
			w := newStampWorld(t, scripts)
			w.run("vet")
			w.write(stampSubject, "package store\n\nfunc Runs() { _ = 2 }\n")
			if got := w.run("vet"); !got.ranTarget("vet") {
				return "vet was skipped for a tree it was never proven on"
			}
			return ""
		},
	},
	{
		name: "a target that changed the tree does not vouch for the next one",
		check: func(t *testing.T, scripts string) string {
			w := newStampWorld(t, scripts)
			w.env["PACEQ_STUB_DIRTY"] = "fmt-check"
			w.env["PACEQ_STUB_DIRTY_FILE"] = stampSubject

			w.run("fmt-check", "vet")
			// Put the tree back exactly as fmt-check found it. vet never
			// ran against this tree; it ran against the one fmt-check
			// left behind.
			w.write(stampSubject, stampSubjectBody)

			if got := w.run("fmt-check", "vet"); !got.ranTarget("vet") {
				return "vet was skipped for the tree fmt-check was handed, not the one vet ran against"
			}
			return ""
		},
	},
	{
		name: "one worktree does not vouch for another",
		check: func(t *testing.T, scripts string) string {
			w := newStampWorld(t, scripts)
			w.run("vet")
			other := linkedWorktree(w, "other")
			if got := other.run("vet"); !got.ranTarget("vet") {
				return "a linked worktree skipped vet on another worktree's stamp"
			}
			return ""
		},
	},
}

// TestGateStampScenariosHold runs the claims against the scripts that ship.
func TestGateStampScenariosHold(t *testing.T) {
	scripts := filepath.Join(repoRoot(t), "scripts")
	for _, s := range gateStampScenarios {
		t.Run(s.name, func(t *testing.T) {
			if got := s.check(t, scripts); got != "" {
				t.Fatalf("%s", got)
			}
		})
	}
}

// TestGateStampGuardsCanSeeABrokenMechanism is the #177 lesson applied to this
// issue. Each mutation below is a plausible way to write the mechanism wrong,
// and each is exactly the kind of wrong that would let a push skip a gate it
// never passed. If the scenario above it still holds against a mutated script,
// the guard proves nothing and this test says so.
func TestGateStampGuardsCanSeeABrokenMechanism(t *testing.T) {
	mutations := []struct {
		name     string
		file     string
		old      string
		with     string
		scenario string
	}{
		{
			name:     "stamp the target before it has run",
			file:     "gate-run.sh",
			old:      "\tif ! make --no-print-directory \"$target\"; then",
			with:     "\tif ! make --no-print-directory \"$target\"; false; then",
			scenario: "a target that failed is not proven",
		},
		{
			name:     "match a stamp on the target and ignore the key",
			file:     "gate-stamp.sh",
			old:      "now - $3 < ttl && $1 == key && $2 == target { proven = $4 }",
			with:     "now - $3 < ttl && $2 == target { proven = $4 }",
			scenario: "a stamp for another tree is not honoured",
		},
		{
			name:     "keep the first key for every target in the run",
			file:     "gate-run.sh",
			old:      "\tafter=$(compute_key)",
			with:     "\tafter=$key",
			scenario: "a target that changed the tree does not vouch for the next one",
		},
		{
			name:     "keep the stamps where every worktree can read them",
			file:     "gate-stamp.sh",
			old:      "\tgit rev-parse --git-path \"$STAMP_NAME\"",
			with:     "\tprintf '%s/%s\\n' \"$(git rev-parse --git-common-dir)\" \"$STAMP_NAME\"",
			scenario: "one worktree does not vouch for another",
		},
	}

	real := filepath.Join(repoRoot(t), "scripts")
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			scripts := mutatedScripts(t, real, m.file, m.old, m.with)

			var scenario gateStampScenario
			for _, s := range gateStampScenarios {
				if s.name == m.scenario {
					scenario = s
				}
			}
			if scenario.check == nil {
				t.Fatalf("no scenario named %q, the mutation guards nothing", m.scenario)
			}

			if got := scenario.check(t, scripts); got == "" {
				t.Fatalf("the mechanism was broken (%s) and %q still passed. "+
					"The guard cannot tell a working stamp from a forged one, "+
					"which is the #177 failure mode.", m.name, scenario.name)
			}
		})
	}
}

// mutatedScripts copies the gate scripts and applies one edit, refusing to
// return a copy where the edit changed nothing: a mutation that silently misses
// would make the guard pass for the wrong reason.
func mutatedScripts(t *testing.T, from, file, old, with string) string {
	t.Helper()

	dir := t.TempDir()
	for _, name := range []string{"gate-run.sh", "gate-stamp.sh"} {
		body, err := os.ReadFile(filepath.Join(from, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(body)
		if name == file {
			if n := strings.Count(text, old); n != 1 {
				t.Fatalf("%s holds %d copies of %q, want exactly 1: the mutation has drifted from the script", name, n, old)
			}
			text = strings.Replace(text, old, with, 1)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o700); err != nil {
			t.Fatalf("write the mutated %s: %v", name, err)
		}
	}
	return dir
}

// linkedWorktree adds a second worktree of the same commit and returns a world
// that runs in it, sharing the stubs and the call log.
func linkedWorktree(w *stampWorld, name string) *stampWorld {
	w.t.Helper()

	dir := filepath.Join(filepath.Dir(w.dir), name)
	w.git("worktree", "add", "-q", "-b", name, dir)

	other := *w
	other.dir = dir
	other.env = make(map[string]string, len(w.env))
	for k, v := range w.env {
		other.env[k] = v
	}
	return &other
}

// ageOutStamps rewrites every stamp's epoch to the start of 1970, so the whole
// file is older than any window.
func ageOutStamps(w *stampWorld) {
	w.t.Helper()

	path := w.stampFile()
	var aged []string
	for _, line := range readLines(w.t, path) {
		fields := strings.Fields(line)
		if len(fields) != 5 {
			w.t.Fatalf("stamp line %q has %d fields, want 5", line, len(fields))
		}
		if _, err := strconv.Atoi(fields[2]); err != nil {
			w.t.Fatalf("stamp line %q does not carry an epoch: %v", line, err)
		}
		fields[2] = "1"
		aged = append(aged, strings.Join(fields, " "))
	}
	if len(aged) == 0 {
		w.t.Fatal("no stamps to age out")
	}
	if err := os.WriteFile(path, []byte(strings.Join(aged, "\n")+"\n"), 0o600); err != nil {
		w.t.Fatalf("age out the stamps: %v", err)
	}
}

// TestGateTargetsAreTheOnesTheGateRuns keeps the Makefile's list and the runner
// in step: `make ci` hands gate-run.sh CI_TARGETS, so a target that leaves the
// list leaves the gate, and one that joins it is stamped by being on it.
func TestGateTargetsAreTheOnesTheGateRuns(t *testing.T) {
	root := repoRoot(t)

	cmd := exec.Command("make", "-s", "print-ci-targets")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("make print-ci-targets: %v", err)
	}
	targets := strings.Fields(string(out))
	if len(targets) < 15 {
		t.Fatalf("CI_TARGETS holds %d targets: %v, the gate has shrunk", len(targets), targets)
	}

	makefile := readFile(t, filepath.Join(root, "Makefile"))
	if !strings.Contains(makefile, "scripts/gate-run.sh $(CI_TARGETS)") {
		t.Fatal("the ci target no longer hands CI_TARGETS to gate-run.sh, so nothing is stamped")
	}
	for _, target := range targets {
		if !strings.Contains(makefile, "\n"+target+":") {
			t.Errorf("CI_TARGETS names %q, which the Makefile does not define", target)
		}
	}
}
