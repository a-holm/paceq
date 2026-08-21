package cli

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

// runFile runs the command line with stdout on a real file rather than a
// buffer, which is what makes the terminal question answerable at all: the
// check asks the kernel about a descriptor, and a buffer has none.
func runFile(t *testing.T, stdout *os.File, dir string, environ map[string]string, args ...string) result {
	t.Helper()

	stderr, readErr := pipeFile(t)
	env := Env{Stdout: stdout, Stderr: stderr, Dir: dir, Getenv: lookup(environ)}
	code := run(context.Background(), env, args)
	if err := stderr.Close(); err != nil {
		t.Fatalf("close the stderr pipe: %v", err)
	}
	return result{code: code, stderr: readErr()}
}

// pipeFile is a real pipe, drained in the background so a command that writes
// more than the pipe buffer cannot block the test.
func pipeFile(t *testing.T) (*os.File, func() string) {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create a pipe: %v", err)
	}
	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(r)
		done <- string(data)
	}()
	t.Cleanup(func() { _ = r.Close() })

	return w, func() string { return <-done }
}

// tempFile is stdout redirected to a file, which is what a shell does for
// `paceq version > file`.
func tempFile(t *testing.T) (*os.File, func() string) {
	t.Helper()

	path := t.TempDir() + "/stdout"
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	return f, func() string {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
}

// TestOutputModeFollowsTheStream is 03 section 7.1: a human at a terminal gets
// text, a pipe gets JSON, and -o overrides both. It is proven against real
// descriptors, a terminal and a pipe, because that is the only thing the
// autodetection actually reads.
func TestOutputModeFollowsTheStream(t *testing.T) {
	cases := []struct {
		name     string
		terminal bool
		args     []string
		wantJSON bool
	}{
		{name: "pipe defaults to json", args: []string{"version"}, wantJSON: true},
		{name: "terminal defaults to text", terminal: true, args: []string{"version"}},
		{name: "-o json overrides a terminal", terminal: true, args: []string{"version", "-o", "json"}, wantJSON: true},
		{name: "-o text overrides a pipe", args: []string{"version", "-o", "text"}},
		{name: "--output json overrides a terminal", terminal: true, args: []string{"version", "--output", "json"}, wantJSON: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stdout *os.File
			var read func() string
			if c.terminal {
				stdout, read = terminalFile(t)
			} else {
				stdout, read = pipeFile(t)
			}

			got := runFile(t, stdout, t.TempDir(), nil, c.args...)
			if err := stdout.Close(); err != nil {
				t.Fatalf("close stdout: %v", err)
			}
			written := read()

			if got.code != ExitOK {
				t.Fatalf("paceq %s = %d, want %d\n%s", strings.Join(c.args, " "), got.code, ExitOK, got.stderr)
			}
			isJSON := json.Valid([]byte(strings.TrimSpace(written)))
			if isJSON != c.wantJSON {
				t.Errorf("json output = %v, want %v:\n%s", isJSON, c.wantJSON, written)
			}
		})
	}
}

// TestRedirectionToAFileIsJSON is the same rule for `paceq version > file`. A
// file is not a terminal, and a character device such as /dev/null is not one
// either, which is what a mode bit check alone would get wrong.
func TestRedirectionToAFileIsJSON(t *testing.T) {
	stdout, read := tempFile(t)

	got := runFile(t, stdout, t.TempDir(), nil, "version")

	if got.code != ExitOK {
		t.Fatalf("paceq version = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if !json.Valid([]byte(strings.TrimSpace(read()))) {
		t.Errorf("output to a file is not JSON:\n%s", read())
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()
	if isTerminal(devNull) {
		t.Errorf("%s is reported as a terminal, so redirection to it would print text", os.DevNull)
	}
}

// TestColourFollowsTheTerminalAndTheEnvironment covers the four ways colour is
// decided. The escape sequence is the evidence: a report that carries one into
// a pipe breaks every grep written against it.
func TestColourFollowsTheTerminalAndTheEnvironment(t *testing.T) {
	cases := []struct {
		name      string
		terminal  bool
		environ   map[string]string
		args      []string
		wantColor bool
	}{
		{name: "terminal", terminal: true, args: []string{"doctor"}, wantColor: true},
		{name: "pipe", args: []string{"doctor", "-o", "text"}},
		{name: "NO_COLOR", terminal: true, environ: map[string]string{"NO_COLOR": "1"}, args: []string{"doctor"}},
		{name: "--no-color", terminal: true, args: []string{"doctor", "--no-color"}},
		{name: "CLICOLOR_FORCE", environ: map[string]string{"CLICOLOR_FORCE": "1"}, args: []string{"doctor", "-o", "text"}, wantColor: true},
		{
			name:     "NO_COLOR wins over CLICOLOR_FORCE",
			terminal: true,
			environ:  map[string]string{"NO_COLOR": "1", "CLICOLOR_FORCE": "1"},
			args:     []string{"doctor"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stdout *os.File
			var read func() string
			if c.terminal {
				stdout, read = terminalFile(t)
			} else {
				stdout, read = pipeFile(t)
			}

			runFile(t, stdout, t.TempDir(), c.environ, c.args...)
			if err := stdout.Close(); err != nil {
				t.Fatalf("close stdout: %v", err)
			}
			written := read()

			if got := strings.Contains(written, "\x1b["); got != c.wantColor {
				t.Errorf("output carries colour = %v, want %v:\n%q", got, c.wantColor, written)
			}
		})
	}
}

// TestSymbolsFallBackToASCII is 03 section 7.1 for a terminal that cannot show
// the symbols: a report full of question marks is worse than one in ASCII.
func TestSymbolsFallBackToASCII(t *testing.T) {
	utf8 := runCLI(t, t.TempDir(), map[string]string{"LANG": "nb_NO.UTF-8"}, "doctor", "-o", "text")
	ascii := runCLI(t, t.TempDir(), map[string]string{"LANG": "C"}, "doctor", "-o", "text")

	if !strings.Contains(utf8.stdout, "✓") {
		t.Errorf("a UTF-8 locale gets no symbols:\n%s", utf8.stdout)
	}
	for _, symbol := range []string{"✓", "⚠", "✗", "→"} {
		if strings.Contains(ascii.stdout, symbol) {
			t.Errorf("a non UTF-8 locale still gets %q:\n%s", symbol, ascii.stdout)
		}
	}
	if !strings.Contains(ascii.stdout, "OK") {
		t.Errorf("the ASCII fallback does not mark a passing check:\n%s", ascii.stdout)
	}
}

// TestDataOnStdoutNotesOnStderr is what makes `paceq doctor -o json | jq`
// work while -v is on: the document has to be the only thing on stdout.
func TestDataOnStdoutNotesOnStderr(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	quiet := runCLI(t, dir, nil, "doctor")
	loud := runCLI(t, dir, nil, "doctor", "-vv")

	quiet.json(t)
	loud.json(t)
	if quiet.stdout != loud.stdout {
		t.Errorf("-vv changed the data on stdout:\n%s\n%s", quiet.stdout, loud.stdout)
	}
	if quiet.stderr != "" {
		t.Errorf("a run without -v wrote notes to stderr:\n%s", quiet.stderr)
	}
	if loud.stderr == "" {
		t.Error("-vv wrote no notes to stderr")
	}
}

// TestQuietDropsEverythingButTheFindingsThatMatter. A report that is silent
// when everything is well can stand in a motd or a login script.
func TestQuietDropsEverythingButTheFindingsThatMatter(t *testing.T) {
	dir := t.TempDir()

	created := runCLI(t, dir, nil, "init", "-o", "text", "-q")
	if created.code != ExitOK {
		t.Fatalf("paceq init -q = %d, want %d\n%s", created.code, ExitOK, created.stderr)
	}
	if created.stdout != "" {
		t.Errorf("init -q wrote to stdout:\n%s", created.stdout)
	}

	report := runCLI(t, dir, nil, "doctor", "-o", "text", "-q")
	if report.code != ExitOK {
		t.Fatalf("paceq doctor -q = %d, want %d\n%s", report.code, ExitOK, report.stderr)
	}
	if strings.Contains(report.stdout, "journal mode") {
		t.Errorf("doctor -q printed a passing check:\n%s", report.stdout)
	}
}
