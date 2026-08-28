package spool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func aResult() Result {
	return Result{
		V:             Version,
		RunID:         "01JQ9F0R7K3M5N7P9R1T3V5X7Z",
		Step:          "dump",
		Attempt:       2,
		ClaimEpoch:    42,
		BootID:        "boot-1",
		PID:           12345,
		PIDStartTicks: 998877,
		StartedAt:     1767512340000,
		EndedAt:       1767512400000,
		Outcome:       OutcomeFailed,
		ExitCode:      1,
		KilledBy:      "",
	}
}

// The atomic order is the whole guarantee: a visible .json is a complete one.
func TestWriteThenReadRoundTripsEveryFact(t *testing.T) {
	dir := t.TempDir()
	if err := WriteResult(dir, aResult()); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadResult(filepath.Join(dir, FileName(aResult().RunID, aResult().Step, aResult().Attempt)))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got.RunID != aResult().RunID || got.Step != aResult().Step || got.Attempt != aResult().Attempt ||
		got.ClaimEpoch != aResult().ClaimEpoch || got.BootID != aResult().BootID ||
		got.PID != aResult().PID || got.PIDStartTicks != aResult().PIDStartTicks ||
		got.StartedAt != aResult().StartedAt || got.EndedAt != aResult().EndedAt ||
		got.Outcome != aResult().Outcome || got.ExitCode != aResult().ExitCode ||
		got.KilledBy != aResult().KilledBy {
		t.Fatalf("round trip lost facts:\n got %+v\nwant %+v", got, aResult())
	}
}

func TestWriteLeavesNoTempFileBehind(t *testing.T) {
	dir := t.TempDir()
	if err := WriteResult(dir, aResult()); err != nil {
		t.Fatalf("write: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the spool dir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != FileName(aResult().RunID, aResult().Step, aResult().Attempt) {
			t.Fatalf("write left %q behind; a spool dir carries only finished results", e.Name())
		}
	}
}

// An interrupted write is the case the hidden temp name exists for: the
// consumer must never read a .tmp as a known outcome.
func TestListIgnoresTempFilesAndDirectories(t *testing.T) {
	dir := t.TempDir()
	if err := WriteResult(dir, aResult()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".partial.json.tmp"), []byte("{"), 0o600); err != nil {
		t.Fatalf("plant a temp file: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "unknown"), 0o700); err != nil {
		t.Fatalf("plant a directory: %v", err)
	}
	paths, err := List(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(paths) != 1 || filepath.Base(paths[0]) != FileName(aResult().RunID, "dump", 2) {
		t.Fatalf("list returned %v, want exactly the one finished result", paths)
	}
}

func TestReadRejectsWrongVersionAndGarbage(t *testing.T) {
	dir := t.TempDir()
	name := FileName(aResult().RunID, "dump", 2)

	stale := aResult()
	stale.V = 999
	b, _ := json.Marshal(stale)
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadResult(filepath.Join(dir, name)); err == nil {
		t.Fatal("a result from another generation was accepted")
	} else if !IsFormatErr(err) {
		t.Fatalf("wrong version is a format error, got: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, name), []byte("{half"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadResult(filepath.Join(dir, name)); !IsFormatErr(err) {
		t.Fatalf("half-written JSON is a format error, got: %v", err)
	}
}

// IsFormatErr unwraps the admission error the consumers route on. It lives in
// the test file's package so the assertion and the definition cannot drift.
func IsFormatErr(err error) bool { return err != nil && errors.Is(err, ErrFormat) }

func TestReadRejectsIncompleteIdentity(t *testing.T) {
	dir := t.TempDir()
	name := FileName("somerun", "dump", 2)
	for _, tc := range []struct {
		name   string
		mutate func(*Result)
	}{
		{"no run id", func(r *Result) { r.RunID = "" }},
		{"no step", func(r *Result) { r.Step = "" }},
		{"no attempt", func(r *Result) { r.Attempt = 0 }},
		{"no outcome", func(r *Result) { r.Outcome = "" }},
	} {
		r := aResult()
		r.RunID, r.Step = "somerun", "dump"
		tc.mutate(&r)
		b, _ := json.Marshal(r)
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadResult(filepath.Join(dir, name)); !IsFormatErr(err) {
			t.Fatalf("%s: expected a format error, got %v", tc.name, err)
		}
	}
}

func TestArchiveMovesAsideAndKeepsTheName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName(aResult().RunID, "dump", 2))
	if err := WriteResult(dir, aResult()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := Archive(dir, path); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the file is still in the attempts directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(UnknownDir(dir), filepath.Base(path))); err != nil {
		t.Fatalf("the file did not land in the unknown directory: %v", err)
	}
	// Idempotent: the second archive of the same file reads as done.
	if err := Archive(dir, path); err != nil {
		t.Fatalf("second archive: %v", err)
	}
}

func TestRemoveIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName(aResult().RunID, "dump", 2))
	if err := WriteResult(dir, aResult()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := Remove(path); err != nil {
		t.Fatalf("second remove: %v", err)
	}
}

func TestBacklogCountsAndFindsTheOldest(t *testing.T) {
	dir := t.TempDir()
	if count, _ := Backlog(dir); count != 0 {
		t.Fatalf("an empty spool has no backlog, got %d", count)
	}
	first := aResult()
	if err := WriteResult(dir, first); err != nil {
		t.Fatalf("write: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	second := aResult()
	second.Attempt = 3
	if err := WriteResult(dir, second); err != nil {
		t.Fatalf("write: %v", err)
	}
	count, oldest := Backlog(dir)
	if count != 2 {
		t.Fatalf("backlog count = %d, want 2", count)
	}
	info, err := os.Stat(filepath.Join(dir, FileName(first.RunID, "dump", 2)))
	if err != nil {
		t.Fatal(err)
	}
	if oldest != info.ModTime().UnixMilli() {
		t.Fatalf("oldest = %d, want the first file's mtime %d", oldest, info.ModTime().UnixMilli())
	}
}

func TestFileNameStaysOnePathComponent(t *testing.T) {
	name := FileName("01JQ9F0R7K3M5N7P9R1T3V5X7Z", "dump-thing_2", 12)
	if filepath.Base(name) != name || filepath.IsAbs(name) {
		t.Fatalf("filename %q is not a single relative component", name)
	}
	if name != "01JQ9F0R7K3M5N7P9R1T3V5X7Z-dump-thing_2-12.json" {
		t.Fatalf("unexpected shape: %s", name)
	}
}

func TestWriteRefusesAFileItCouldNotStamp(t *testing.T) {
	dir := t.TempDir()
	r := aResult()
	r.V = 0
	if err := WriteResult(dir, r); err == nil {
		t.Fatal("a result without a version was written")
	}
}
