package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// fileSink stands in for the log sink the engine will hand to the runner. The
// property under test is the shape, not the package: an io.Writer that is not
// an *os.File. exec cannot pass a Go writer to a child directly, so it builds
// an os.Pipe and copies from one end into the writer.
type fileSink struct{ f *os.File }

func (s *fileSink) Write(p []byte) (int, error) { return s.f.Write(p) }

// TestTheJobSeesPipesNeverItsOwnLogFile is the T17 rule from the security
// plan: the job process gets a pipe to its log, never a handle on the log
// file itself. With a handle, a job could seek it, truncate it or link it
// somewhere; through a pipe it can only write bytes that end up in the file
// the parent owns.
//
// The proof reads the child's descriptor table from /proc/self/fd while the
// child reports it, so nothing is inferred from behaviour.
func TestTheJobSeesPipesNeverItsOwnLogFile(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("no /proc on this platform: the job's descriptor table is read through /proc")
	}

	logPath := filepath.Join(t.TempDir(), "extract.1.ndjson")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open the log file: %v", err)
	}
	sink := &fileSink{f: f}

	spec := baseSpec(t, fakecmd(t), "fds")
	spec.Stdout = sink
	spec.Stderr = sink

	res, err := runBounded(t, time.Minute, context.Background(), spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != Succeeded {
		t.Fatalf("outcome = %s, want Succeeded", res.Outcome)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close the log: %v", err)
	}

	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read what the job wrote: %v", err)
	}
	var fds map[string]string
	if err := json.Unmarshal(raw, &fds); err != nil {
		t.Fatalf("the job's output is not the fd report: %v\n%s", err, raw)
	}
	if len(fds) < 3 {
		t.Fatalf("the fd report holds %d entries, want at least stdin, stdout and stderr",
			len(fds))
	}

	for fd, target := range fds {
		if strings.Contains(target, filepath.Base(logPath)) || target == logPath {
			t.Errorf("fd %s points at the log file %s: the job holds its own log", fd, target)
		}
	}
	for _, fd := range []string{"1", "2"} {
		target, ok := fds[fd]
		if !ok {
			t.Errorf("fd %s is not in the report", fd)
			continue
		}
		if !strings.HasPrefix(target, "pipe:") {
			t.Errorf("fd %s is %s, want a pipe", fd, target)
		}
	}
}
