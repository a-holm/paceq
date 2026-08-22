package logsink

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// collect is a fn for ReadFrom that gathers lines into a slice.
func collect(sink *[]Line) func(Line) error {
	return func(line Line) error {
		*sink = append(*sink, line)
		return nil
	}
}

func TestReadFromWalksWholeLinesAndResumes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.ndjson")
	want := []string{`{"ts":1,"stream":"stdout","seq":1,"line":"a"}`, `{"ts":2,"stream":"stderr","seq":2,"line":"b"}`}
	if err := os.WriteFile(path, []byte(strings.Join(want, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got []Line
	resume, err := ReadFrom(path, 0, false, collect(&got))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 || got[0].Line != "a" || got[1].Line != "b" {
		t.Fatalf("got %+v, want two lines", got)
	}
	rawLen := int64(len(strings.Join(want, "\n")) + 1)
	if resume != rawLen {
		t.Fatalf("resume offset %d, want %d", resume, rawLen)
	}

	// A writer appends one more line. Reading again yields exactly that.
	appended := `{"ts":3,"stream":"stdout","seq":3,"line":"c"}`
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := f.WriteString(appended + "\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	got = nil
	resume2, err := ReadFrom(path, resume, false, collect(&got))
	if err != nil {
		t.Fatalf("reread: %v", err)
	}
	if len(got) != 1 || got[0].Line != "c" {
		t.Fatalf("reread got %+v, want only the new line", got)
	}
	if resume2 != resume+int64(len(appended)+1) {
		t.Fatalf("second resume %d, want %d", resume2, resume+int64(len(appended)+1))
	}
}

// A torn final line waits: following a live log must never emit half a line
// and then the other half as if it were two lines. Only a final drain may
// emit it, marked as split.
func TestReadFromHoldsBackATornFinalLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.ndjson")
	first := `{"ts":1,"stream":"stdout","seq":1,"line":"whole"}`
	partial := `{"ts":2,"stream":"stdout","seq":2,"lin`
	if err := os.WriteFile(path, []byte(first+"\n"+partial), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got []Line
	resume, err := ReadFrom(path, 0, false, collect(&got))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 || got[0].Line != "whole" {
		t.Fatalf("got %+v, want only the whole line", got)
	}
	wantResume := int64(len(first) + 1)
	if resume != wantResume {
		t.Fatalf("resume %d, want %d: the torn line must stay unread", resume, wantResume)
	}

	got = nil
	if _, err := ReadFrom(path, resume, true, collect(&got)); err != nil {
		t.Fatalf("final drain: %v", err)
	}
	if len(got) != 1 || !got[0].Split || !strings.Contains(got[0].Line, "seq\":2") {
		t.Fatalf("the final drain got %+v, want the torn fragment marked split", got)
	}
}

func TestReadFromAcceptsAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-yet.ndjson")
	var got []Line
	resume, err := ReadFrom(path, 128, false, collect(&got))
	if err != nil {
		t.Fatalf("read of a missing file: %v", err)
	}
	if resume != 128 || len(got) != 0 {
		t.Fatalf("missing file returned offset %d and %d lines", resume, len(got))
	}
}

func TestSeqCheckFindsGapsAndRestarts(t *testing.T) {
	var walk SeqCheck
	steps := []struct {
		seq    int64
		gap    int64
		missed bool
	}{
		{1, 0, false},
		{2, 0, false},
		{3, 0, false},
		// 4, 5 and 6 were consumed elsewhere: the marker explains them.
		{7, 3, true},
		{8, 0, false},
		// After the marker the surviving tail continues below it.
		{4, 0, false},
		{5, 0, false},
	}
	for _, step := range steps {
		gap, missed := walk.Next(step.seq)
		if gap != step.gap || missed != step.missed {
			t.Errorf("Next(%d) = (%d, %v), want (%d, %v)", step.seq, gap, missed, step.gap, step.missed)
		}
	}
}
