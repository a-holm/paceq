package logsink

// The endless-line cases: a job that never prints a newline. os/exec feeds a
// non-file writer through a pipe in 32 KiB chunks, so the assembler must cut
// an unterminated line by its accumulated length, not by how much any single
// Write happens to carry. These tests pin both delivery shapes and the
// guarantees that survive the cut: quota accounting while the job runs, the
// head/marker/tail order, and the newest bytes living to the end.

import (
	"bytes"
	"os"
	"runtime"
	"strings"
	"testing"
)

// A single Write larger than the whole default quota, with no newline
// anywhere. The cap must hold, the marker must sit between head and tail,
// and the newest bytes must survive.
func TestR63SingleHugeWriteStaysBounded(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, 0)

	total := 17 << 20
	p := make([]byte, total)
	for i := range p {
		p[i] = 'a'
	}
	copy(p[total-64:], strings.Repeat("b", 64))
	n, err := s.Writer(StreamStdout).Write(p)
	if err != nil {
		t.Fatalf("single huge write: %v", err)
	}
	if n != total {
		t.Fatalf("Write reported %d, want %d", n, total)
	}
	_, bytes, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !truncated {
		t.Fatal("17 MiB in one call did not report itself truncated")
	}

	path := sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() > DefaultQuota+128<<10 {
		t.Fatalf("file is %d bytes, want at most the %d quota plus slack", info.Size(), DefaultQuota)
	}
	if bytes != info.Size() {
		t.Fatalf("Finish reported %d bytes, file holds %d", bytes, info.Size())
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
		t.Fatal("no truncated marker after one huge write")
	}
	if markerAt == 0 {
		t.Fatal("marker before any head line")
	}
	if !lines[0].Split || len(lines[0].Line) != maxFragment {
		t.Fatalf("first fragment not a hard split piece: split=%v len=%d", lines[0].Split, len(lines[0].Line))
	}
	last := lines[len(lines)-1]
	if !strings.HasSuffix(last.Line, strings.Repeat("b", 64)) {
		t.Fatalf("the newest bytes did not survive: suffix %q", truncate(last.Line))
	}
	if lines[markerAt].DroppedBytes <= 0 {
		t.Fatalf("marker dropped_bytes = %d", lines[markerAt].DroppedBytes)
	}
	t.Logf("one %d MiB write -> file %d bytes, %d lines, marker at index %d",
		total>>20, info.Size(), len(lines), markerAt)
}

// The same endless line delivered the way exec actually delivers output to a
// non-file writer: pipe-sized chunks of 32 KiB, below the 64 KiB split size.
// The hard split must fire during the writes, so the file grows and the
// quota checks run while the job is still going, and neither the file nor
// the heap may hold the line whole.
func TestR63SmallChunksWithoutNewlineStayBounded(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, 0)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	w := s.Writer(StreamStdout)
	chunk := bytes.Repeat([]byte{0xAB}, 32<<10) // io.Copy's buffer class
	const totalMiB = 32
	checkpointDone := false
	for i := 0; i < totalMiB*32; i++ {
		if _, err := w.Write(chunk); err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
		if !checkpointDone && i == totalMiB*8 { // 8 MiB fed
			checkpointDone = true
			info, statErr := os.Stat(sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))
			if statErr != nil {
				t.Fatalf("stat mid-feed: %v", statErr)
			}
			if info.Size() == 0 {
				t.Fatalf("after 8 MiB fed in pipe-sized chunks with no newline, "+
					"the log file holds 0 bytes: no quota check has run while "+
					"%d MiB sits in memory", 8)
			}
			t.Logf("mid-feed: 8 MiB fed, file already holds %d bytes", info.Size())
		}
	}

	// White-box bound on the defect itself: whatever the heap numbers say,
	// the unterminated remainder of the line must sit inside the fragment
	// window once the write returns.
	s.mu.Lock()
	partial := len(s.assemblers[0].partial)
	s.mu.Unlock()
	if partial >= maxFragment {
		t.Fatalf("the partial-line buffer held %d bytes after %d MiB fed: "+
			"the cumulative cut is not bounding it", partial, totalMiB)
	}

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	grown := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	// The defect this bounds is a whole-line buffered on the heap (~totalMiB).
	// The threshold sits clearly below that and above the measured stream
	// retention, which the runtime's allocator can swing by a few MiB between
	// Go releases (go1.27 measured ~18 MiB). 24 MiB still fails a real
	// whole-line buffer while tolerating allocator noise.
	if grown > 24<<20 {
		t.Fatalf("feeding %d MiB in pipe-sized chunks retained %d MiB on the heap: "+
			"an endless line delivered the way exec delivers it is buffered whole",
			totalMiB, grown>>20)
	}
	t.Logf("%d MiB fed chunk-wise retained %d KiB on the heap (tail ring holds up to %d MiB by design)",
		totalMiB, grown>>10, (DefaultQuota-DefaultQuota/4)>>20)
	if _, _, _, err := s.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
}
