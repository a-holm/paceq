// Package spool is the durable letterbox between a step's process and the
// database (issue #39). The child of every attempt — the exec shim — writes
// one result file here before it exits, so the knowledge of what happened
// lives on disk even while the daemon that spawned it is dying. Recovery
// consumes the box: a result that arrived is committed instead of guessed,
// which is what closes crash window W8, the microseconds between a child's
// exit and its verdict's commit.
//
// The package is deliberately narrow and knows nothing of the database: it
// defines the file format, writes and reads it atomically, and moves rejected
// files aside. Whoever consumes a result into the database routes it; that
// logic lives with the database's other verdict writers.
package spool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Version is the format version stamped into every written file. A consumer
// that meets a different number is reading a file from another paceq
// generation; it archives the file instead of trusting it, because a shim
// from an older binary may outlive an upgrade and its format is not a public
// contract (#39, design choice 9).
const Version = 1

// Outcome names what the shim saw happen. The strings are the spool format's
// contract, not Go identifiers, because the file may be read back by a
// different binary than the one that wrote it.
const (
	OutcomeSucceeded   = "succeeded"
	OutcomeFailed      = "failed"
	OutcomeTimedOut    = "timed_out"
	OutcomeSignalled   = "signalled"
	OutcomeSpawnFailed = "spawn_failed"
)

// Result is one attempt's outcome as the spool stores it. The identity fields
// are what let a consumer prove the file belongs to the attempt it is about
// to settle, and ClaimEpoch is the fence: a result from an attempt whose
// lease has moved on is stale and is archived, never applied.
//
// StartedAt and EndedAt are unix milliseconds, the house unit.
type Result struct {
	V             int             `json:"v"`
	RunID         string          `json:"run_id"`
	Step          string          `json:"step"`
	Attempt       int             `json:"attempt"`
	ClaimEpoch    int64           `json:"claim_epoch"`
	BootID        string          `json:"boot_id"`
	PID           int             `json:"pid"`
	PIDStartTicks int64           `json:"pid_start_ticks"`
	StartedAt     int64           `json:"started_at"`
	EndedAt       int64           `json:"ended_at"`
	Outcome       string          `json:"outcome"`
	ExitCode      int             `json:"exit_code"`
	Signal        string          `json:"signal"`
	KilledBy      string          `json:"killed_by"`
	ReasonData    json.RawMessage `json:"reason_data,omitempty"`
}

// ErrFormat marks a file that exists but cannot be trusted as written: wrong
// version, wrong identity, or bytes that are not the format at all.
var ErrFormat = errors.New("spool: not a result file this paceq accepts")

// FileName is the spool file's name for one attempt. Step names are validated
// against the spec's name pattern ([a-z0-9_-], issue #39 leans on that rule),
// so the concatenation is a safe single path component.
func FileName(runID, step string, attempt int) string {
	return runID + "-" + step + "-" + strconv.Itoa(attempt) + ".json"
}

// WriteResult stores one result durably, or leaves no result at all. The
// order is the whole guarantee: the bytes are flushed and fsynced in a hidden
// temp file, the name is swapped in by rename (atomic within the directory),
// and the directory itself is fsynced so the new name survives the power cut
// too. A crash between any two steps leaves either the old state or the
// complete new one — never a half-written .json.
func WriteResult(dir string, r Result) error {
	if r.V != Version {
		return fmt.Errorf("spool: refusing to write version %d", r.V)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("spool: create the spool directory: %w", err)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("spool: encode the result: %w", err)
	}
	tmp := filepath.Join(dir, "."+FileName(r.RunID, r.Step, r.Attempt)+".tmp")
	final := filepath.Join(dir, FileName(r.RunID, r.Step, r.Attempt))

	// #nosec G304 - the path is assembled inside the spool directory the
	// daemon owns; the attempt identity comes from the spec, never from
	// job input, and the hidden temp name is built here.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("spool: create the temp file: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("spool: write the result: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("spool: fsync the result: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("spool: close the result: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("spool: swap the result into place: %w", err)
	}
	// #nosec G304 - the directory was created by this function two calls
	// above; the fsync needs the handle, not a path from anywhere else.
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("spool: reopen the spool directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("spool: fsync the spool directory: %w", err)
	}
	return nil
}

// ReadResult reads and validates one result file. A returned error wraps
// ErrFormat when the file is complete nonsense for this generation — the
// caller's cue to archive rather than retry — and plain I/O error when the
// file may simply not be there.
func ReadResult(path string) (Result, error) {
	b, err := os.ReadFile(path) // #nosec G304 - the path is built inside the state directory's spool, never from job input
	if err != nil {
		return Result{}, err
	}
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return Result{}, fmt.Errorf("%w: %s: %v", ErrFormat, filepath.Base(path), err)
	}
	if r.V != Version {
		return Result{}, fmt.Errorf("%w: %s: version %d", ErrFormat, filepath.Base(path), r.V)
	}
	if r.RunID == "" || r.Step == "" || r.Attempt <= 0 {
		return Result{}, fmt.Errorf("%w: %s: incomplete attempt identity", ErrFormat, filepath.Base(path))
	}
	if r.Outcome == "" {
		return Result{}, fmt.Errorf("%w: %s: no outcome", ErrFormat, filepath.Base(path))
	}
	return r, nil
}

// List names the consumable result files in dir, sorted, with the hidden
// temp files of interrupted writes left out: a ".name.json.tmp" is by
// definition a write that never reached its rename, so it is not a known
// outcome and must never be read as one.
func List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("spool: read the spool directory: %w", err)
	}
	var out []string
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") || strings.HasPrefix(ent.Name(), ".") {
			continue
		}
		out = append(out, filepath.Join(dir, ent.Name()))
	}
	return out, nil
}

// UnknownDir is the directory beside dir where rejected results are moved.
// Files land there instead of being deleted so an operator can read what a
// previous generation, a foreign hand, or a stale attempt tried to say.
func UnknownDir(dir string) string {
	return filepath.Join(filepath.Dir(dir), "unknown")
}

// Archive moves one spool file into the unknown directory beside dir,
// keeping its name. It is idempotent: a file already gone reads as done.
func Archive(dir, path string) error {
	dst := UnknownDir(dir)
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return fmt.Errorf("spool: create the unknown directory: %w", err)
	}
	if err := os.Rename(path, filepath.Join(dst, filepath.Base(path))); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("spool: archive %s: %w", filepath.Base(path), err)
	}
	return nil
}

// Remove deletes one consumed result file. Idempotent like Archive: the
// second consumer of the same good news finds nothing and is glad.
func Remove(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("spool: remove the consumed result: %w", err)
	}
	return nil
}

// Backlog reports how many result files are waiting and how old the oldest
// is, in unix milliseconds. A full spool dir is a daemon that died and never
// came back: doctor turns the numbers into words, this only counts.
func Backlog(dir string) (count int, oldestMS int64) {
	paths, err := List(dir)
	if err != nil {
		return 0, 0
	}
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		count++
		mod := info.ModTime().UnixMilli()
		if oldestMS == 0 || mod < oldestMS {
			oldestMS = mod
		}
	}
	return count, oldestMS
}
