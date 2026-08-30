package cli

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/logsink"
)

// What an operator reads about a log the quota did not cut. Two reading paths
// answer the same question from two records: paceq logs renders the marker
// inside the file, and paceq explain prints log_truncated from the column the
// sink's own verdict fills. A log with every line on disk must read as
// complete on both, because an operator who believes output is missing stops
// looking at the evidence in front of them.
func TestACompleteLogReadsAsCompleteOnBothPaths(t *testing.T) {
	// The sink's own small quota: 4 KiB total, 1 KiB head. Eight 128-byte
	// lines fill the head exactly, so these two counts sit on the boundary.
	const quota = 4096
	for _, tc := range []struct {
		name  string
		lines int
	}{
		{"the head fills exactly", 8},
		{"one line past the head", 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewFake(time.Date(2026, 9, 17, 3, 0, 1, 123000000, time.UTC))
			root := logsink.NewRoot(t.TempDir())
			s, err := logsink.Open(root, "01K5ZQ8V3M7X", "spew", 1,
				logsink.Options{Clock: clk, Quota: quota})
			if err != nil {
				t.Fatalf("open the sink: %v", err)
			}
			w := s.Writer(logsink.StreamStdout)
			for i := 0; i < tc.lines; i++ {
				if _, err := fmt.Fprintf(w, "%04d-%s\n", i, strings.Repeat("y", 66)); err != nil {
					t.Fatalf("write line %d: %v", i, err)
				}
			}
			rel := s.RelPath()
			_, _, truncated, err := s.Finish()
			if err != nil {
				t.Fatalf("finish: %v", err)
			}

			// The explain path. steps.log_truncated is this bool verbatim,
			// and explain prints log_truncated: true whenever it is set.
			if truncated {
				t.Errorf("paceq explain reports log_truncated for a log with all %d lines on disk", tc.lines)
			}

			// The logs path, through the renderer the command itself uses.
			abs, err := root.Abs(rel)
			if err != nil {
				t.Fatalf("resolve the log path: %v", err)
			}
			var out bytes.Buffer
			u := &ui{out: &out, err: io.Discard, mode: modeText, symbols: symbols(false)}
			r := &logRenderer{u: u, text: true}
			if _, err := logsink.ReadFrom(abs, 0, false, func(l logsink.Line) error {
				return r.emit("spew", 1, l)
			}); err != nil {
				t.Fatalf("read the log back: %v", err)
			}
			text := out.String()
			if strings.Contains(text, "log truncated") {
				t.Errorf("paceq logs announces truncation for a log with all %d lines on disk:\n%s", tc.lines, text)
			}
			if strings.Contains(text, "lines missing") {
				t.Errorf("paceq logs reports missing lines for a complete log:\n%s", text)
			}
			for i := 0; i < tc.lines; i++ {
				want := fmt.Sprintf("%04d-%s", i, strings.Repeat("y", 66))
				if !strings.Contains(text, want) {
					t.Errorf("line %d is not in what the operator reads:\n%s", i, text)
					break
				}
			}
		})
	}
}
