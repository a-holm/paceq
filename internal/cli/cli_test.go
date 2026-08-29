package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/reconcile"
)

// result is one command run: what it wrote where, and what the shell would see.
type result struct {
	code   int
	stdout string
	stderr string
}

// runCLI runs the command line with buffers for both streams, which is the pipe
// case: neither stream is a terminal.
func runCLI(t *testing.T, dir string, environ map[string]string, args ...string) result {
	t.Helper()

	return runCLIContext(t, context.Background(), dir, environ, args...)
}

func runCLIContext(t *testing.T, ctx context.Context, dir string, environ map[string]string, args ...string) result {
	t.Helper()

	var stdout, stderr bytes.Buffer
	env := Env{
		Stdout: &stdout,
		Stderr: &stderr,
		Dir:    dir,
		Getenv: lookup(environ),
	}
	code := run(ctx, env, args)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

// lookup is an environment that holds only what a test puts in it, so a
// variable set on the developer's machine cannot change a result.
func lookup(environ map[string]string) func(string) string {
	return func(name string) string { return environ[name] }
}

// runCLIWithDoctorEdges runs the command line with doctor's two machine
// reading edges planted, so a report answers independently of the machine the
// test runs on: its sandbox configuration, and whatever else on the box
// happens to carry PACEQ_RUN_ID. A nil edge reads the machine.
func runCLIWithDoctorEdges(t *testing.T, dir string, status doctor.StatusReader, procs doctor.ProcLister, args ...string) result {
	t.Helper()

	var stdout, stderr bytes.Buffer
	env := Env{
		Stdout: &stdout,
		Stderr: &stderr,
		Dir:    dir,
		Getenv: lookup(nil),
		Status: status,
		Procs:  procs,
	}
	code := run(context.Background(), env, args)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

// sandboxedCLIStatus is a process under the hardened systemd unit, the sandbox
// doctor approves of.
func sandboxedCLIStatus() doctor.StatusReader {
	return func() (doctor.ProcessStatus, error) {
		return doctor.ProcessStatus{NoNewPrivs: 1, Seccomp: 2, CapEff: "0000000000000000"}, nil
	}
}

// otherInstallationsJob is a live job process a second paceq on this machine
// started: it carries PACEQ_RUN_ID, and this installation's attempt baselines
// have never heard of it.
func otherInstallationsJob() doctor.ProcLister {
	return func() ([]reconcile.Process, error) {
		return []reconcile.Process{
			{PID: 2107018, PGID: 2107018, RunID: "01M17NNQ5Y3EXTKHX9TFCNBZ2J", StartTicks: 900, TicksOK: true},
		}, nil
	}
}

func (r result) json(t *testing.T) map[string]any {
	t.Helper()

	var doc map[string]any
	if err := json.Unmarshal([]byte(r.stdout), &doc); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, r.stdout)
	}
	return doc
}

// TestExitCodesAreTheDocumentedContract pins the table from 03 section 7.2. The
// numbers are a public interface: a script written against exit 5 keeps working
// only if 5 never becomes something else.
func TestExitCodesAreTheDocumentedContract(t *testing.T) {
	want := map[string]int{
		"ExitOK":          0,
		"ExitInternal":    1,
		"ExitUsage":       2,
		"ExitNotFound":    3,
		"ExitValidation":  4,
		"ExitRunFailed":   5,
		"ExitBusy":        6,
		"ExitTimeout":     7,
		"ExitInterrupted": 8,
	}
	got := map[string]int{
		"ExitOK":          ExitOK,
		"ExitInternal":    ExitInternal,
		"ExitUsage":       ExitUsage,
		"ExitNotFound":    ExitNotFound,
		"ExitValidation":  ExitValidation,
		"ExitRunFailed":   ExitRunFailed,
		"ExitBusy":        ExitBusy,
		"ExitTimeout":     ExitTimeout,
		"ExitInterrupted": ExitInterrupted,
	}
	for name, code := range want {
		if got[name] != code {
			t.Errorf("%s = %d, want %d", name, got[name], code)
		}
	}
}

// TestHelpDocumentsEveryExitCode keeps the contract where a user finds it. A
// table only the source knows is not a contract.
func TestHelpDocumentsEveryExitCode(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "--help")

	if got.code != ExitOK {
		t.Fatalf("paceq --help = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if got.stderr != "" {
		t.Errorf("help wrote to stderr: %q", got.stderr)
	}
	for _, code := range exitCodes {
		line := strings.TrimSpace(code.Meaning)
		if !strings.Contains(got.stdout, line) {
			t.Errorf("help does not explain exit %d (%q):\n%s", code.Code, line, got.stdout)
		}
	}
}

// TestNoArgumentsPrintsHelp keeps the first thing a new user types useful.
func TestNoArgumentsPrintsHelp(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil)

	if got.code != ExitOK {
		t.Errorf("paceq = %d, want %d", got.code, ExitOK)
	}
	if !strings.Contains(got.stdout, "Usage:") {
		t.Errorf("paceq printed no usage text:\n%s", got.stdout)
	}
}

// TestUsageErrorsExitTwo covers the ways a command line can be wrong. Every one
// of them is exit 2, and every message says what to run instead.
func TestUsageErrorsExitTwo(t *testing.T) {
	cases := map[string][]string{
		"unknown command":                       {"nope"},
		"unknown flag":                          {"version", "--nope"},
		"unknown output format":                 {"version", "-o", "yaml"},
		"argument to a command that takes none": {"version", "extra"},
		"error without a code":                  {"error"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			got := runCLI(t, t.TempDir(), nil, args...)

			if got.code != ExitUsage {
				t.Errorf("paceq %s = %d, want %d\n%s%s", strings.Join(args, " "), got.code, ExitUsage, got.stdout, got.stderr)
			}
			if got.stdout != "" {
				t.Errorf("a failed command wrote to stdout: %q", got.stdout)
			}
			if !strings.Contains(got.stderr, "\n  ") {
				t.Errorf("the message has no indented next step:\n%s", got.stderr)
			}
		})
	}
}

// TestUnknownErrorCodeIsNotFound is the exit 3 path: the resource named on the
// command line does not exist.
func TestUnknownErrorCodeIsNotFound(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "PQ9999")

	if got.code != ExitNotFound {
		t.Fatalf("paceq error PQ9999 = %d, want %d\n%s", got.code, ExitNotFound, got.stderr)
	}
	if !strings.Contains(got.stderr, "PQ9999") {
		t.Errorf("the message does not name the code asked for:\n%s", got.stderr)
	}
}

// TestKnownErrorCodeExplainsItself. The catalogue grows in M1-05; what is
// established here is that the command answers from it.
func TestKnownErrorCodeExplainsItself(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "PQ5001", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq error PQ1002 = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	for _, want := range []string{"PQ5001", "chmod"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the explanation does not mention %q:\n%s", want, got.stdout)
		}
	}
}

// TestInterruptedRunExitsEight. A run stopped by SIGINT is not a failure of the
// tool, and a script that retries on 1 must not retry on it.
func TestInterruptedRunExitsEight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := runCLIContext(t, ctx, t.TempDir(), nil, "init")

	if got.code != ExitInterrupted {
		t.Fatalf("an interrupted init = %d, want %d\n%s", got.code, ExitInterrupted, got.stderr)
	}
	if !strings.Contains(got.stderr, "\n  ") {
		t.Errorf("the message has no indented next step:\n%s", got.stderr)
	}
}

// captureStdout redirects os.Stdout to a file for the length of a test and
// returns what was written to it. It is what makes Main itself testable rather
// than only the function it delegates to.
func captureStdout(t *testing.T) func() string {
	t.Helper()

	path := t.TempDir() + "/stdout"
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	saved := os.Stdout
	os.Stdout = file
	t.Cleanup(func() {
		os.Stdout = saved
		_ = file.Close()
	})

	return func() string {
		if err := file.Sync(); err != nil {
			t.Fatalf("sync %s: %v", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
}
