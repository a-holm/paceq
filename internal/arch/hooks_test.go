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

// zeroSHA is what git writes in a pre-push ref line for the side of the push that
// does not exist. It sits in the local sha field of a deletion and in the remote
// sha field of a branch that is being created, so the field position is what tells
// the two apart.
const zeroSHA = "0000000000000000000000000000000000000000"

const (
	oldSHA = "085575c291de420c5f31b28bd1509fa9253bcc7c"
	newSHA = "6b4f2ad1c0e94d8a7f3b5c2e1d0a9f8e7c6b5a49"
)

// hookRun is what one pre-push run did: how it exited, what it printed, and how it
// called make.
type hookRun struct {
	exitCode  int
	stdout    string
	stderr    string
	makeCalls []string
	makeStdin string
	gateLog   string
}

// gateRan reports whether the hook handed the push to `make ci`.
func (r hookRun) gateRan() bool {
	for _, call := range r.makeCalls {
		if call == "ci" {
			return true
		}
	}
	return false
}

func TestPrePushRunsTheGateUnlessThePushMovesNoCode(t *testing.T) {
	deletion := "(delete) " + zeroSHA + " refs/heads/feature " + oldSHA + "\n"
	update := "refs/heads/feature " + newSHA + " refs/heads/feature " + oldSHA + "\n"
	creation := "HEAD " + newSHA + " refs/heads/feature " + zeroSHA + "\n"

	cases := []struct {
		name     string
		refLines string
		wantGate bool
		// wantNote is the reason a skipped push has to print. Every push
		// that skips the gate says which kind of nothing it is.
		wantNote string
	}{
		{"one deletion", deletion, false, "deletion only"},
		{"two deletions", deletion + "(delete) " + zeroSHA + " refs/heads/other " + newSHA + "\n", false, "deletion only"},
		// Deleting a ref that never existed on either side is still a
		// deletion: git offers it with both shas at zero (observed on
		// git 2.43 for `git push origin :refs/heads/ghost`).
		{"a ghost ref deletion", "(delete) " + zeroSHA + " refs/heads/ghost " + zeroSHA + "\n", false, "deletion only"},
		{"an update", update, true, ""},
		// The zero sha is in the remote sha field here. Reading the wrong field
		// turns every new branch into a skipped gate.
		{"a new branch", creation, true, ""},
		{"an update and a deletion", update + deletion, true, ""},
		{"a deletion and an update", deletion + update, true, ""},
		// git always ends its ref lines with a newline. A reader that drops an
		// unterminated last line would skip the gate on this push.
		{"an update without a trailing newline", deletion + strings.TrimSuffix(update, "\n"), true, ""},
		// git fires this hook even when there is nothing to push: a clone
		// where everything is up to date answers "Everything up-to-date"
		// and hands the hook zero ref lines (git 2.43, plain push, --all,
		// --tags and --mirror all do it). Nothing moves then either, so
		// this is the same kind of nothing as a deletion-only push.
		{"no ref lines", "", false, "nothing to push"},
		{"a line that is not a ref line", "unexpected\n", true, ""},
	}

	// The hook gets the remote name as its first argument. None of this may depend
	// on the remote being called origin.
	for _, remote := range []string{"origin", "upstream"} {
		for _, tc := range cases {
			t.Run(remote+"/"+tc.name, func(t *testing.T) {
				run := runPrePush(t, remote, tc.refLines, 0)

				if run.exitCode != 0 {
					t.Fatalf("exit code = %d, want 0\nstderr: %s", run.exitCode, run.stderr)
				}
				if got := run.gateRan(); got != tc.wantGate {
					t.Fatalf("gate ran = %v, want %v\nmake calls: %q\nstdout: %s", got, tc.wantGate, run.makeCalls, run.stdout)
				}
				if !tc.wantGate && !strings.Contains(run.stdout, tc.wantNote) {
					t.Fatalf("a skipped push has to say %q, stdout = %q", tc.wantNote, run.stdout)
				}
			})
		}
	}
}

// The hook reads stdin to classify the push, so it has to hand the same bytes on:
// git offers the ref lines once, and no other reader has seen them.
func TestPrePushHandsTheRefLinesToTheGate(t *testing.T) {
	refLines := "refs/heads/feature " + newSHA + " refs/heads/feature " + oldSHA + "\n" +
		"(delete) " + zeroSHA + " refs/heads/other " + oldSHA + "\n"

	run := runPrePush(t, "origin", refLines, 0)

	if !run.gateRan() {
		t.Fatalf("gate did not run, make calls: %q", run.makeCalls)
	}
	if run.makeStdin != refLines {
		t.Fatalf("the gate read %q on stdin, want %q", run.makeStdin, refLines)
	}
}

func TestPrePushFailsWhenTheGateFails(t *testing.T) {
	refLines := "refs/heads/feature " + newSHA + " refs/heads/feature " + oldSHA + "\n"

	run := runPrePush(t, "origin", refLines, 1)

	if run.exitCode == 0 {
		t.Fatalf("a failed gate has to stop the push, exit code = 0\nstdout: %s", run.stdout)
	}
	if !strings.Contains(run.stderr, "make ci") {
		t.Fatalf("a failed gate has to name what failed, stderr = %q", run.stderr)
	}
	if !strings.Contains(run.stderr, "gate") || !strings.Contains(run.stderr, "lines") {
		t.Errorf("a failed gate has to point at the whole log, stderr = %q", run.stderr)
	}
	if !strings.Contains(run.gateLog, "gate output for ci") {
		t.Errorf("the gate log has to hold what the gate printed, log = %q", run.gateLog)
	}
}

// TestPrePushKeepsTheGateOutOfGitsChannel is the fix for #178. git reads a hook's
// output itself and stops reading once it has what it wants. A gate that streams
// tens of thousands of bytes takes SIGPIPE on a later write, dies, and git aborts
// the push reporting the hook's status: a green gate, no transfer, and no message
// saying why. The gate belongs in a file, and a summary belongs on the channel.
func TestPrePushKeepsTheGateOutOfGitsChannel(t *testing.T) {
	refLines := "refs/heads/feature " + newSHA + " refs/heads/feature " + oldSHA + "\n"

	run := runPrePush(t, "origin", refLines, 0)

	if run.exitCode != 0 {
		t.Fatalf("a green gate has to let the push through, exit code = %d\nstderr: %s",
			run.exitCode, run.stderr)
	}
	if !run.gateRan() {
		t.Fatalf("the gate has to run, make calls = %v", run.makeCalls)
	}
	if !strings.Contains(run.gateLog, "gate output for ci") {
		t.Fatalf("the gate output has to reach the log, log = %q", run.gateLog)
	}
	if strings.Contains(run.stdout, "gate output for") {
		t.Errorf("the gate is being streamed to git again, stdout = %q", run.stdout)
	}
	if got := len(strings.Split(strings.TrimSpace(run.stdout), "\n")); got > 3 {
		t.Errorf("the hook printed %d lines to git, want a summary of at most 3\nstdout: %s",
			got, run.stdout)
	}
}

// gateSummaryLine is what scripts/gate-run.sh writes into the gate log once it
// has walked its targets.
const gateSummaryLine = "gate-summary: 6 of 20 targets skipped as already proven for this tree, 14 run in 1180s"

// TestPrePushSaysWhatTheGateSkipped is the visible half of #176. A skip nobody
// is told about is how a gate stops guarding without anyone noticing, so the
// count reaches the person doing the push on every one, whether the gate went
// green or red.
func TestPrePushSaysWhatTheGateSkipped(t *testing.T) {
	refLines := "refs/heads/feature " + newSHA + " refs/heads/feature " + oldSHA + "\n"
	want := "6 of 20 targets skipped"

	green := runPrePush(t, "origin", refLines, 0)
	if !strings.Contains(green.stdout, want) {
		t.Errorf("a green push has to say what it skipped, stdout = %q", green.stdout)
	}

	red := runPrePush(t, "origin", refLines, 1)
	if !strings.Contains(red.stderr, want) {
		t.Errorf("a red push has to say what it skipped too, stderr = %q", red.stderr)
	}
}

// runPrePush runs .githooks/pre-push the way git does: the remote name and url as
// arguments, the ref lines on stdin. make is a stub on PATH, so the test observes
// the gate without paying for it.
func runPrePush(t *testing.T, remote, refLines string, makeExit int) hookRun {
	t.Helper()

	root := repoRoot(t)
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatalf("create the stub directory: %v", err)
	}

	callsFile := filepath.Join(dir, "calls")
	stdinFile := filepath.Join(dir, "stdin")
	logFile := filepath.Join(dir, "gate.log")
	// The stub also writes to stdout, the way make does. That is what the hook
	// has to keep off git's channel, so the tests need something to look for.
	// The summary line is the one the gate writes when it has skipped targets
	// that this exact tree already passed (#176). The hook has to lift it onto
	// git's channel, green or red, so no push is silent about a skip.
	stub := "#!/bin/sh\n" +
		"cat >\"$PACEQ_STUB_STDIN\"\n" +
		"printf '%s\\n' \"$*\" >>\"$PACEQ_STUB_CALLS\"\n" +
		"printf 'gate output for %s\\n' \"$*\"\n" +
		"printf '%s\\n' \"$PACEQ_STUB_SUMMARY\"\n" +
		"exit \"$PACEQ_STUB_EXIT\"\n"
	if err := os.WriteFile(filepath.Join(binDir, "make"), []byte(stub), 0o700); err != nil {
		t.Fatalf("write the make stub: %v", err)
	}

	cmd := exec.Command("sh", filepath.Join(root, ".githooks", "pre-push"), remote, "https://example.invalid/paceq.git")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PACEQ_STUB_CALLS="+callsFile,
		"PACEQ_STUB_STDIN="+stdinFile,
		"PACEQ_STUB_EXIT="+strconv.Itoa(makeExit),
		"PACEQ_STUB_SUMMARY="+gateSummaryLine,
		"PACEQ_GATE_LOG="+logFile,
	)
	cmd.Stdin = strings.NewReader(refLines)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
	default:
		t.Fatalf("run the hook: %v", err)
	}

	return hookRun{
		exitCode:  cmd.ProcessState.ExitCode(),
		stdout:    stdout.String(),
		stderr:    stderr.String(),
		makeCalls: readLines(t, callsFile),
		makeStdin: readFile(t, stdinFile),
		gateLog:   readFile(t, logFile),
	}
}

// readLines returns the non-empty lines of a file the stub may never have created.
func readLines(t *testing.T, path string) []string {
	t.Helper()

	content := readFile(t, path)
	if content == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(content, "\n"), "\n")
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}
