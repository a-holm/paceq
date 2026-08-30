package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// The cancel mark (issue #204). A signal tells its receiver nothing about
// why it was sent, and the shim that ends up writing the attempt's result
// only ever sees the signal. The sender knows why, and it knows before it
// signals, so it leaves that knowledge here: an empty file beside the
// attempt's result file, named for the same attempt.
//
// The order is the whole guarantee. The mark is on disk before the signal
// leaves the sender, so a shim that has seen the signal can always read the
// mark, with no polling and nothing to race. The mark is not the durable
// record of anything: it lives only until the shim stamps the reason into
// the result file, which is the copy that outlives both processes. That is
// why it is written without an fsync, and why losing it degrades to the
// verdict a bare signal earns rather than to an error.

// KilledByCancel is the KilledBy value for an attempt whose signal was this
// system answering a cancellation, rather than a hand from outside. KilledBy
// is free text in the format, so a reader that has never heard the word
// still reads the result as the signalled attempt it is, exactly as it did
// before the word existed; a new value here needs no Version bump.
const KilledByCancel = "cancel"

const (
	resultSuffix = ".json"
	cancelSuffix = ".cancel"
)

// CancelMarkPath names one attempt's cancel mark.
func CancelMarkPath(dir, runID, step string, attempt int) string {
	return markBeside(filepath.Join(dir, FileName(runID, step, attempt)))
}

// MarkCancel records that the kill this attempt is about to take answers a
// cancellation. It must return before the signal is sent: everything the
// receiver can conclude rests on that order.
func MarkCancel(dir, runID, step string, attempt int) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("spool: create the spool directory: %w", err)
	}
	path := CancelMarkPath(dir, runID, step, attempt)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		return fmt.Errorf("spool: mark the cancellation of %s: %w", step, err)
	}
	return nil
}

// CancelMarked reports whether the sender of the kill left a mark for this
// attempt. An unreadable mark reads as no mark: the attempt then earns the
// verdict a bare signal earns, which is the answer this package gave before
// the mark existed.
func CancelMarked(dir, runID, step string, attempt int) bool {
	_, err := os.Stat(CancelMarkPath(dir, runID, step, attempt))
	return err == nil
}

// ClearCancelMark drops one attempt's mark. Idempotent, like Remove: a mark
// already gone is a mark that has done its work.
func ClearCancelMark(dir, runID, step string, attempt int) error {
	return removeMark(CancelMarkPath(dir, runID, step, attempt))
}

// markBeside names the cancel mark of the result file at path.
func markBeside(resultPath string) string {
	return strings.TrimSuffix(resultPath, resultSuffix) + cancelSuffix
}

func removeMark(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("spool: remove the cancel mark: %w", err)
	}
	return nil
}
