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

func TestPrePushRunsTheGateUnlessThePushOnlyDeletes(t *testing.T) {
	deletion := "(delete) " + zeroSHA + " refs/heads/feature " + oldSHA + "\n"
	update := "refs/heads/feature " + newSHA + " refs/heads/feature " + oldSHA + "\n"
	creation := "HEAD " + newSHA + " refs/heads/feature " + zeroSHA + "\n"

	cases := []struct {
		name     string
		refLines string
		wantGate bool
	}{
		{"one deletion", deletion, false},
		{"two deletions", deletion + "(delete) " + zeroSHA + " refs/heads/other " + newSHA + "\n", false},
		{"an update", update, true},
		// The zero sha is in the remote sha field here. Reading the wrong field
		// turns every new branch into a skipped gate.
		{"a new branch", creation, true},
		{"an update and a deletion", update + deletion, true},
		{"a deletion and an update", deletion + update, true},
		// git always ends its ref lines with a newline. A reader that drops an
		// unterminated last line would skip the gate on this push.
		{"an update without a trailing newline", deletion + strings.TrimSuffix(update, "\n"), true},
		// Nothing to classify is not a deletion. The gate is the safe answer.
		{"no ref lines", "", true},
		{"a line that is not a ref line", "unexpected\n", true},
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
				if !tc.wantGate && !strings.Contains(run.stdout, "deletion only") {
					t.Fatalf("a skipped push has to say so, stdout = %q", run.stdout)
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
	stub := "#!/bin/sh\n" +
		"cat >\"$PACEQ_STUB_STDIN\"\n" +
		"printf '%s\\n' \"$*\" >>\"$PACEQ_STUB_CALLS\"\n" +
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
