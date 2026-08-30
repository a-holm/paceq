package logsink

import (
	"fmt"
	"strings"
	"testing"
)

// headLine is one job line whose encoded form is exactly 128 bytes: 57 bytes
// of fixed fields and 71 of content, with a seq that stays a single digit for
// the first nine. Eight of them fill the 1 KiB head of smallQuota precisely,
// which puts the boundary cases on a whole number of lines.
func headLine(i int) string {
	return fmt.Sprintf("%04d-%s\n", i, strings.Repeat("y", 66))
}

// seqSet is the seq numbers a finished log carries, and whether any number
// between the lowest and the highest is missing from it. File order is not
// the numbers' order: the marker takes the seq after the last job line and is
// written before the surviving tail, so the set is what carries the meaning.
func seqSet(t *testing.T, lines []Line) (missing []int64, lowest int64) {
	t.Helper()
	if len(lines) == 0 {
		return nil, 0
	}
	seen := map[int64]bool{}
	lowest, highest := lines[0].Seq, lines[0].Seq
	for _, l := range lines {
		if seen[l.Seq] {
			t.Fatalf("seq %d appears twice in the file", l.Seq)
		}
		seen[l.Seq] = true
		if l.Seq < lowest {
			lowest = l.Seq
		}
		if l.Seq > highest {
			highest = l.Seq
		}
	}
	for n := lowest; n <= highest; n++ {
		if !seen[n] {
			missing = append(missing, n)
		}
	}
	return missing, lowest
}

// assertSeqAgrees holds the file's own proof against the verdict the sink
// reported. A gap in the seq numbers is what makes loss detectable from the
// file alone, so a log that says it lost output must have one and a log that
// says it is complete must not.
func assertSeqAgrees(t *testing.T, lines []Line, truncated bool) {
	t.Helper()
	missing, lowest := seqSet(t, lines)
	if len(lines) > 0 && lowest != 1 {
		t.Errorf("the file starts at seq %d: the head lost its first line", lowest)
	}
	switch {
	case truncated && len(missing) == 0:
		t.Errorf("the log reports itself truncated and every seq it handed out is on disk: nothing was lost")
	case !truncated && len(missing) > 0:
		t.Errorf("the log reports itself complete and seq %v never reached the file", missing)
	}
}

// markerOf finds the truncated marker, and fails if there is more than one.
func markerOf(t *testing.T, lines []Line) (Line, bool) {
	t.Helper()
	var found Line
	n := 0
	for _, l := range lines {
		if l.Event == "truncated" {
			found = l
			n++
		}
	}
	if n > 1 {
		t.Fatalf("%d truncated markers in one file, want at most one", n)
	}
	return found, n == 1
}

// The flag the database stores, the marker inside the file and the marker's
// dropped_bytes answer one question: did the quota lose any of this attempt's
// output. They are written by three different pieces of code, so this table
// walks the head boundary and holds all three against each other and against
// the lines that are actually on disk.
func TestTruncationRecordsAgreeAtTheHeadBoundary(t *testing.T) {
	for _, tc := range []struct {
		name          string
		lines         int
		wantTruncated bool
	}{
		{"under the head limit", 7, false},
		{"the head fills exactly", 8, false},
		{"one line past the head", 9, false},
		{"the tail holds without evicting", 20, false},
		{"the ring evicts", 40, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := fakeClk(t, frozen)
			root := testRoot(t)
			s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)
			w := s.Writer(StreamStdout)
			for i := 0; i < tc.lines; i++ {
				if _, err := fmt.Fprint(w, headLine(i)); err != nil {
					t.Fatalf("write line %d: %v", i, err)
				}
			}
			_, _, truncated, err := s.Finish()
			if err != nil {
				t.Fatalf("finish: %v", err)
			}
			lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))
			marker, hasMarker := markerOf(t, lines)

			if truncated != tc.wantTruncated {
				t.Errorf("log_truncated = %v, want %v", truncated, tc.wantTruncated)
			}
			if hasMarker != tc.wantTruncated {
				t.Errorf("the file carries a truncated marker = %v, want %v", hasMarker, tc.wantTruncated)
			}
			if hasMarker && marker.DroppedBytes <= 0 {
				t.Errorf("the marker reports %d dropped bytes: a marker that names no loss cannot be read",
					marker.DroppedBytes)
			}
			assertSeqAgrees(t, lines, truncated)

			if !tc.wantTruncated {
				var got []string
				for _, l := range lines {
					if l.Event == "" {
						got = append(got, l.Line)
					}
				}
				if len(got) != tc.lines {
					t.Errorf("%d job lines on disk, want all %d", len(got), tc.lines)
				}
				for i, line := range got {
					if want := strings.TrimSuffix(headLine(i), "\n"); line != want {
						t.Errorf("line %d on disk is %q, want %q", i, line, want)
						break
					}
				}
			}
		})
	}
}

// A single line bigger than the whole ring is dropped outright, and the ring
// stays empty. The bytes are gone and the seq they consumed is missing, so the
// marker has to be there to explain the hole even though there is no tail to
// write behind it.
func TestAnOversizedLineIsTruncatedWithAnEmptyRing(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)
	w := s.Writer(StreamStdout)
	for i := 0; i < 8; i++ {
		if _, err := fmt.Fprint(w, headLine(i)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	// smallQuota leaves a 3 KiB ring; 4000 bytes cannot be kept in it.
	if _, err := fmt.Fprintf(w, "%s\n", strings.Repeat("z", 4000)); err != nil {
		t.Fatalf("write the oversized line: %v", err)
	}
	_, _, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !truncated {
		t.Fatal("a line the ring could not hold was dropped and the log does not say so")
	}
	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))
	marker, hasMarker := markerOf(t, lines)
	if !hasMarker {
		t.Fatal("no marker explains the seq the dropped line consumed")
	}
	if marker.DroppedBytes != 4000 {
		t.Errorf("the marker reports %d dropped bytes, want the 4000 the line held", marker.DroppedBytes)
	}
	assertSeqAgrees(t, lines, truncated)
}

// An unterminated final line reaches the sink from flushPartial at Finish, and
// it can be the write that evicts. The verdict has to be taken after that, not
// before, or the marker and the flag describe the log as it was one line ago.
func TestAnUnterminatedFinalLineCountsTowardsTheVerdict(t *testing.T) {
	clk := fakeClk(t, frozen)
	root := testRoot(t)
	s := openSink(t, root, "01K5ZQ8V3M7X", "spew", 1, clk, smallQuota)
	w := s.Writer(StreamStdout)
	for i := 0; i < 8; i++ {
		if _, err := fmt.Fprint(w, headLine(i)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	// No newline: this fragment waits in the assembler until Finish.
	if _, err := fmt.Fprint(w, strings.Repeat("z", 4000)); err != nil {
		t.Fatalf("write the fragment: %v", err)
	}
	_, _, truncated, err := s.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !truncated {
		t.Fatal("the remainder flushed at Finish was dropped and the log does not say so")
	}
	lines := readLines(t, sinkPath(t, root, "01K5ZQ8V3M7X", "spew", 1, clk))
	marker, hasMarker := markerOf(t, lines)
	if !hasMarker {
		t.Fatal("no marker for a remainder the ring could not hold")
	}
	if marker.DroppedBytes <= 0 {
		t.Errorf("the marker reports %d dropped bytes, want the fragment's size", marker.DroppedBytes)
	}
	assertSeqAgrees(t, lines, truncated)
}
