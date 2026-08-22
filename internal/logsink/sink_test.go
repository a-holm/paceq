package logsink

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// smallQuota keeps the truncation cases fast: 4 KiB total, 1 KiB head, 3 KiB
// tail. The rules under test are the same ones the 16 MiB default enforces.
const smallQuota = 4096

func testRoot(t *testing.T) Root {
	t.Helper()
	return NewRoot(t.TempDir())
}

func fakeClk(t *testing.T, at time.Time) *clock.Fake {
	t.Helper()
	clk := clock.NewFake(at)
	t.Cleanup(func() { clk = nil })
	return clk
}

func openSink(t *testing.T, root Root, runID, step string, attempt int, clk clock.Clock, quota int64) *Sink {
	t.Helper()
	s, err := Open(root, runID, step, attempt, Options{Clock: clk, Quota: quota})
	if err != nil {
		t.Fatalf("open the sink: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// readLines parses a finished log file whole. Every test that asserts on
// content goes through it, so a line that is not valid NDJSON fails here
// before any assertion can see it.
func readLines(t *testing.T, path string) []Line {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return parseLines(t, string(raw))
}

func parseLines(t *testing.T, raw string) []Line {
	t.Helper()
	var out []Line
	sc := bufio.NewScanner(strings.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		text := sc.Text()
		if text == "" {
			continue
		}
		var line Line
		if err := json.Unmarshal([]byte(text), &line); err != nil {
			t.Fatalf("a log line is not valid JSON: %v\n%s", err, text)
		}
		out = append(out, line)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan the log: %v", err)
	}
	return out
}

func sinkPath(t *testing.T, root Root, runID, step string, attempt int, clk clock.Clock) string {
	t.Helper()
	rel := root.RelFor(clk.Now(), runID, step, attempt)
	if rel == "" {
		t.Fatal("RelFor refused the test names")
	}
	abs, err := root.Abs(rel)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	return abs
}

// TestGoldenLinesWithAFrozenClock is the format contract. The bytes are pinned
// exactly: field order, the millisecond timestamp and nothing else on the
// line. Anything that changes what a reader sees has to change this test on
// purpose.
func TestGoldenLinesWithAFrozenClock(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "extract", 1, clk, 0)

	if _, err := io.WriteString(s.Writer(StreamStdout), "connecting to warehouse\n"); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	clk.Advance(333 * time.Millisecond)
	if _, err := io.WriteString(s.Writer(StreamStderr), "warning: slow response\n"); err != nil {
		t.Fatalf("write stderr: %v", err)
	}
	tail, bytes, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if tail == "" {
		t.Fatal("Finish returned an empty error tail for a log with content")
	}
	if truncated {
		t.Fatal("a two line log reported itself truncated")
	}

	want := fmt.Sprintf(
		`{"ts":%d,"stream":"stdout","seq":1,"line":"connecting to warehouse"}`+"\n"+
			`{"ts":%d,"stream":"stderr","seq":2,"line":"warning: slow response"}`+"\n",
		frozen.UnixMilli(), frozen.Add(333*time.Millisecond).UnixMilli())

	raw, err := os.ReadFile(sinkPath(t, root, "01K5ZQ8V3M7X", "extract", 1, clk))
	if err != nil {
		t.Fatalf("read the log: %v", err)
	}
	if string(raw) != want {
		t.Fatalf("log bytes:\n%s\nwant:\n%s", raw, want)
	}
	if bytes != int64(len(want)) {
		t.Fatalf("Finish reported %d bytes, want %d", bytes, len(want))
	}
}

// seq is per attempt: attempt 2 does not continue attempt 1's numbering, and
// neither file may start anywhere but 1.
func TestSeqStartsAtOneForEachAttempt(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)

	first := openSink(t, root, "01K5ZQ8V3M7X", "extract", 1, clk, 0)
	_, err := io.WriteString(first.Writer(StreamStdout), "one\ntwo\nthree\n")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, _, err := first.Finish(); err != nil {
		t.Fatalf("finish attempt 1: %v", err)
	}

	second := openSink(t, root, "01K5ZQ8V3M7X", "extract", 2, clk, 0)
	_, err = io.WriteString(second.Writer(StreamStdout), "four\nfive\n")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, _, err := second.Finish(); err != nil {
		t.Fatalf("finish attempt 2: %v", err)
	}

	for _, tc := range []struct {
		attempt int
		want    []int64
	}{
		{1, []int64{1, 2, 3}},
		{2, []int64{1, 2}},
	} {
		lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "extract", tc.attempt, clk))
		if len(lines) != len(tc.want) {
			t.Fatalf("attempt %d has %d lines, want %d", tc.attempt, len(lines), len(tc.want))
		}
		for i, line := range lines {
			if line.Seq != tc.want[i] {
				t.Errorf("attempt %d line %d has seq %d, want %d", tc.attempt, i, line.Seq, tc.want[i])
			}
			if line.TS == 0 {
				t.Errorf("attempt %d line %d has no timestamp", tc.attempt, i)
			}
			if line.Stream != StreamStdout {
				t.Errorf("attempt %d line %d has stream %q", tc.attempt, i, line.Stream)
			}
		}
	}
}

// The pulseq stream is how the system speaks in the same file as the job. Its
// lines carry an event name and never a line of job output.
func TestEmitWritesPulseqEvents(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "extract", 1, clk, 0)
	if err := s.Emit("started", map[string]any{"pid": 4242}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if _, _, _, err := s.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}

	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "extract", 1, clk))
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	line := lines[0]
	if line.Stream != StreamPulseq || line.Event != "started" {
		t.Fatalf("event line is stream %q event %q, want pulseq/started", line.Stream, line.Event)
	}
	if line.Line != "" {
		t.Fatalf("an event line carries job text %q", line.Line)
	}
	if line.Seq != 1 {
		t.Fatalf("event seq is %d, want 1", line.Seq)
	}
}

// The whole truncation story in one case: the quota cuts a stream, the head
// and the tail survive, the marker names the dropped bytes, and the seq gap
// makes the loss visible to any reader.
func TestQuotaKeepsHeadAndTailAndMarksTheGap(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)

	const total = 200
	w := s.Writer(StreamStdout)
	for i := 0; i < total; i++ {
		if _, err := fmt.Fprintf(w, "%03d %s\n", i, strings.Repeat("x", 60)); err != nil {
			t.Fatalf("write line %d: %v", i, err)
		}
	}
	tail, bytes, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !truncated {
		t.Fatal("a log past the quota did not report itself truncated")
	}
	if tail == "" {
		t.Fatal("the error tail is empty for a log with stderr content")
	}

	path := sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() > smallQuota+256 {
		t.Fatalf("the file is %d bytes, want at most the %d quota plus one line", info.Size(), smallQuota)
	}
	if bytes != info.Size() {
		t.Fatalf("Finish reported %d bytes, the file holds %d", bytes, info.Size())
	}

	lines := readLines(t, path)
	markerAt := -1
	for i, line := range lines {
		if line.Event == "truncated" {
			markerAt = i
			break
		}
	}
	if markerAt < 0 {
		t.Fatal("no truncated marker line in the file")
	}
	marker := lines[markerAt]
	if marker.Stream != StreamPulseq {
		t.Fatalf("the marker is on stream %q, want pulseq", marker.Stream)
	}
	if marker.DroppedBytes <= 0 {
		t.Fatalf("the marker says %d dropped bytes, want more than 0", marker.DroppedBytes)
	}

	// Head: the first line the job wrote survives.
	if lines[0].Line != "000 "+strings.Repeat("x", 60) {
		t.Fatalf("the head lost the first line: %q", lines[0].Line)
	}
	// Tail: the LAST line the job wrote survives, after the marker.
	last := lines[len(lines)-1]
	wantLast := fmt.Sprintf("%03d %s", total-1, strings.Repeat("x", 60))
	if last.Line != wantLast {
		t.Fatalf("the tail lost the last line: %q, want %q", last.Line, wantLast)
	}
	// The tail is the tail: everything after the marker is in job write order
	// and ends at the newest line.
	for i := markerAt + 2; i < len(lines); i++ {
		if lines[i].Seq <= lines[i-1].Seq {
			t.Fatalf("tail seqs go backwards at line %d: %d then %d", i, lines[i-1].Seq, lines[i].Seq)
		}
	}

	// Any reader can see the loss from the seq numbers alone.
	var walk SeqCheck
	sawGap := false
	for _, line := range lines {
		if gap, ok := walk.Next(line.Seq); ok && gap > 0 {
			sawGap = true
		}
	}
	if !sawGap {
		t.Fatal("no seq gap in a truncated log: the loss is undetectable")
	}
}

// The boundary is the line that crosses the head limit. Under it nothing is
// dropped; one byte past it the next line starts the tail.
func TestQuotaBoundaryAtTheHeadLimit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lines     int
		wantTrunc bool
	}{
		{"under the head limit", 7, false},
		{"exactly at the head limit", 8, false},
		{"one line past the head limit", 9, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := fakeClk(t, frozen)
			root := testRoot(t)
			s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)
			w := s.Writer(StreamStdout)
			// Each encoded line is exactly 128 bytes with a single digit
			// seq: the fixed fields are 57 bytes and the content is 71.
			// Eight of them fill the 1 KiB head precisely.
			for i := 0; i < tc.lines; i++ {
				if _, err := fmt.Fprintf(w, "%04d-%s\n", i, strings.Repeat("y", 66)); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			_, _, truncated, err := s.Finish()
			if err != nil {
				t.Fatalf("finish: %v", err)
			}
			if truncated != tc.wantTrunc {
				t.Fatalf("truncated = %v, want %v", truncated, tc.wantTrunc)
			}
		})
	}
}

// A step that pours out 64 MiB ends up with at most the 16 MiB quota plus one
// line of overshoot and the marker, and the newest lines are the ones kept.
func TestSixtyFourMiBStaysInsideTheQuota(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, 0)

	const totalMiB = 64
	const totalChunks = totalMiB * 16
	w := s.Writer(StreamStdout)
	for i := 0; i < totalChunks; i++ {
		// Every chunk names itself at the END, so the assertions can tell
		// the newest output from any other even when the error tail only
		// keeps the last few kilobytes of a 64 KiB line.
		chunk := []byte(strings.Repeat("a", 64*1024-8) + fmt.Sprintf("|%06d\n", i))
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
	}
	tail, bytes, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !truncated {
		t.Fatal("64 MiB of output did not report itself truncated")
	}

	path := sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// One 64 KiB fragment of overshoot plus the marker line is the slack the
	// whole-line write rule allows.
	if info.Size() > (16<<20)+128*1024 {
		t.Fatalf("the file is %d bytes, want at most 16 MiB plus one line", info.Size())
	}
	if bytes != info.Size() {
		t.Fatalf("Finish reported %d bytes, the file holds %d", bytes, info.Size())
	}

	lines := readLines(t, path)
	if len(lines) == 0 {
		t.Fatal("the log is empty")
	}
	newest := fmt.Sprintf("|%06d", totalChunks-1)
	if !strings.HasSuffix(lines[0].Line, "|000000") {
		t.Fatalf("the head is gone: first line ends %q", truncate(lines[0].Line))
	}
	last := lines[len(lines)-1]
	if !strings.HasSuffix(last.Line, newest) {
		t.Fatalf("the tail lost the newest chunk: last line ends %q", truncate(last.Line))
	}
	if !strings.Contains(tail, newest) {
		t.Fatal("the error tail does not carry the newest output")
	}
}

// truncate shortens a long log line for an error message.
func truncate(line string) string {
	if len(line) > 24 {
		return line[:24] + "..."
	}
	return line
}

// One hundred megabytes in one Write call with no newline anywhere must not
// buffer: the sink hard splits at 64 KiB and the process stays inside its
// memory budget.
func TestHundredMiBWithoutNewlineHardSplits(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, 0)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	const totalMiB = 100
	w := s.Writer(StreamStdout)
	chunk := bytes.Repeat([]byte{0xAB}, 1<<20)
	var written int
	for i := 0; i < totalMiB; i++ {
		n, err := w.Write(chunk)
		if err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
		written += n
	}
	if written != totalMiB<<20 {
		t.Fatalf("Write reported %d bytes, want %d", written, totalMiB<<20)
	}
	_, _, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !truncated {
		t.Fatal("100 MiB in one line did not report itself truncated")
	}

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	// What a bounded sink retains is the 12 MiB tail ring, the 4 KiB tails
	// and change. A sink that buffered the whole burst keeps over 100 MiB
	// alive even after a collection. The delta is signed: a collection that
	// frees more than the feed allocated is a pass, not an underflow.
	if grown := int64(after.HeapAlloc) - int64(before.HeapAlloc); grown > 48<<20 {
		t.Fatalf("feeding 100 MiB retained %d MiB on the heap, want a bounded sink", grown>>20)
	}

	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))
	splits := 0
	for _, line := range lines {
		if line.Split {
			splits++
		}
	}
	if splits == 0 {
		t.Fatal("no fragment carries the split flag")
	}
	if lines[0].Line == "" {
		t.Fatal("the first fragment is empty")
	}
}

// Raw bytes that are not UTF-8 still have to come out as valid JSON. The
// reader must never hit a parse error, and the content is never interpreted.
func TestInvalidUTF8StillYieldsValidJSON(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "raw", 1, clk, 0)
	w := s.Writer(StreamStdout)
	if _, err := w.Write([]byte{'b', 0xFF, 'a', 0xFE, 'd', '\n'}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, _, err := s.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "raw", 1, clk))
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	// Each invalid byte became one replacement character.
	if lines[0].Line != "b\ufffda\ufffdd" {
		t.Fatalf("line = %q, want the sanitised form", lines[0].Line)
	}
}

// The error tail prefers stderr: the error is almost always there, and 4 KiB
// of stdout progress bars are worthless in explain.
func TestErrorTailPrefersStderr(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "mix", 1, clk, 0)
	if _, err := io.WriteString(s.Writer(StreamStdout), "progress 1%\nprogress 2%\n"); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	if _, err := io.WriteString(s.Writer(StreamStderr), "error: warehouse refused\n"); err != nil {
		t.Fatalf("write stderr: %v", err)
	}
	tail, _, _, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !strings.Contains(tail, "error: warehouse refused") {
		t.Fatalf("the tail lost the stderr error: %q", tail)
	}
	if strings.Contains(tail, "progress") {
		t.Fatalf("the tail carries stdout noise: %q", tail)
	}
}

// With nothing on stderr the tail falls back to the combined stream, so a job
// that only talks on stdout still explains itself.
func TestErrorTailFallsBackToCombined(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "mix", 1, clk, 0)
	if _, err := io.WriteString(s.Writer(StreamStdout), "the only output\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	tail, _, _, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !strings.Contains(tail, "the only output") {
		t.Fatalf("the tail lost the only output there was: %q", tail)
	}
}

// The tail is bounded at about 4 KiB and carries the NEWEST bytes: a mutant
// that keeps the first 4 KiB instead of the last one fails here.
func TestErrorTailIsTheLastFourKiB(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "chatty", 1, clk, 0)
	w := s.Writer(StreamStderr)
	for i := 0; i < 500; i++ {
		if _, err := fmt.Fprintf(w, "line %03d %s\n", i, strings.Repeat("e", 60)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	tail, _, _, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if len(tail) > 4200 {
		t.Fatalf("the tail is %d bytes, want about 4 KiB", len(tail))
	}
	if !strings.Contains(tail, "line 499") {
		t.Fatalf("the tail lost the newest stderr line: %q", tail[:80])
	}
	if strings.Contains(tail, "line 000") {
		t.Fatal("the tail carries the oldest line instead of the newest")
	}
}

// The file order after truncation is head, marker, tail: the marker explains
// the gap before the surviving tail starts.
func TestTruncationWritesMarkerBeforeTail(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)
	w := s.Writer(StreamStdout)
	for i := 0; i < 200; i++ {
		if _, err := fmt.Fprintf(w, "%03d %s\n", i, strings.Repeat("x", 60)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if _, _, _, err := s.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))

	markerAt := -1
	for i, line := range lines {
		if line.Event == "truncated" {
			markerAt = i
			break
		}
	}
	if markerAt <= 0 {
		t.Fatalf("marker at index %d, want after at least one head line", markerAt)
	}
	if lines[markerAt-1].Event == "truncated" {
		t.Fatal("two markers")
	}
	// Before the marker: head lines in write order.
	if lines[0].Line == "" || !strings.HasPrefix(lines[0].Line, "000 ") {
		t.Fatalf("the head does not start with the first line: %q", lines[0].Line)
	}
	// After the marker: job lines again, ending with the newest.
	if lines[len(lines)-1].Line != fmt.Sprintf("199 %s", strings.Repeat("x", 60)) {
		t.Fatalf("the tail does not end with the newest line: %q", lines[len(lines)-1].Line)
	}
}

// Two streams feed one sink from two goroutines once the engine wires exec's
// copiers. No line may be lost, duplicated or torn.
func TestTwoStreamsFeedOneSinkConcurrently(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "race", 1, clk, 1<<20)

	const perStream = 200
	var wg sync.WaitGroup
	wg.Add(2)
	for _, stream := range []string{StreamStdout, StreamStderr} {
		go func(stream string) {
			defer wg.Done()
			w := s.Writer(stream)
			for i := 0; i < perStream; i++ {
				if _, err := fmt.Fprintf(w, "%s %03d\n", stream, i); err != nil {
					t.Errorf("write %s: %v", stream, err)
					return
				}
			}
		}(stream)
	}
	wg.Wait()
	_, _, _, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}

	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "race", 1, clk))
	if len(lines) != perStream*2 {
		t.Fatalf("got %d lines, want %d", len(lines), perStream*2)
	}
	seen := map[int64]bool{}
	for _, line := range lines {
		if seen[line.Seq] {
			t.Fatalf("seq %d appears twice", line.Seq)
		}
		seen[line.Seq] = true
	}
}

// A sink abandoned without Finish closes the file and writes no marker: an
// attempt that never got a verdict is not declared truncated.
func TestCloseWithoutFinishWritesNoMarker(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)
	if _, err := io.WriteString(s.Writer(StreamStdout), "only a head\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))
	if len(lines) != 1 || lines[0].Line != "only a head" {
		t.Fatalf("unexpected lines: %+v", lines)
	}
}

// RelPath is what the database stores: relative to the root, so the state
// directory can move.
func TestRelPathIsRelativeToTheRoot(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "extract", 1, clk, 0)
	rel := s.RelPath()
	if filepath.IsAbs(rel) {
		t.Fatalf("RelPath %q is absolute", rel)
	}
	want := root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1)
	if rel != want {
		t.Fatalf("RelPath = %q, want %q", rel, want)
	}
	abs, err := root.Abs(rel)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	if abs != sinkPath(t, root, "01K5ZQ8V3M7X", "extract", 1, clk) {
		t.Fatalf("Abs(RelPath) = %q, want the sink's own file", abs)
	}
}
