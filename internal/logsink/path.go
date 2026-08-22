// Package logsink writes step logs as NDJSON files on disk and reads them
// back.
//
// The decision this package serves is locked in the observability plan: logs
// are files, not rows. tail -f, grep and logrotate work on them without
// paceq, and the one SQLite writer never carries log volume. Only three facts
// about a log reach the database, and they travel with the step's terminal
// transition: where the file is relative to the log root, how many bytes it
// grew to, and whether the quota cut it.
//
// The file format is one JSON object per line:
//
//	{"ts":1789635600123,"stream":"stdout","seq":1,"line":"connecting"}
//	{"ts":1789635601000,"stream":"pulseq","seq":3,"event":"truncated","dropped_bytes":18446744}
//
// seq counts every line the sink was handed, including lines the quota threw
// away. That is what makes loss detectable: a gap in seq means lines are
// missing, and the marker line says how many bytes were dropped. A reader who
// needs proof that a log is complete cannot get it from plain text.
package logsink

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Root is the directory every log file lives under: <state>/logs. Paths stored
// elsewhere are relative to it, so the state directory can be moved or renamed
// without rewriting the database.
type Root struct {
	dir string
}

// LogDirName is the name of the logs directory inside the state directory.
const LogDirName = "logs"

// NewRoot names the log root inside a state directory.
func NewRoot(stateDir string) Root {
	return Root{dir: filepath.Join(stateDir, LogDirName)}
}

// Dir is the log root itself.
func (r Root) Dir() string { return r.dir }

// dateLayout is the day shard under the root. One directory per day is what
// makes retention later a matter of removing whole directories instead of
// walking millions of files.
const dateLayout = "2006-01-02"

// RelFor returns the path of one attempt's log file, relative to the root:
//
//	<yyyy-mm-dd>/<run_id>/<step>.<attempt>.ndjson
//
// The date shard is the UTC date of now, so the same run lands in one shard on
// every machine regardless of local time zone. An empty run id, an empty step,
// a step carrying a path separator, or an attempt below 1 returns "" because
// each of those would build a path outside the layout this package promises.
func (r Root) RelFor(now time.Time, runID, step string, attempt int) string {
	if runID == "" || step == "" || attempt < 1 {
		return ""
	}
	if step == "." || step == ".." ||
		strings.ContainsRune(step, '/') || strings.ContainsRune(step, filepath.Separator) {
		return ""
	}
	if strings.ContainsRune(runID, '/') || strings.ContainsRune(runID, filepath.Separator) {
		return ""
	}
	return filepath.Join(now.UTC().Format(dateLayout), runID,
		fmt.Sprintf("%s.%d.ndjson", step, attempt))
}

// Abs turns a root relative path back into a real path. A relative path that
// escapes the root is refused rather than resolved: log paths end up in the
// database, and anything that has ever been near user input is checked here,
// at the one place a name becomes a file handle.
func (r Root) Abs(rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("log path %q is absolute, want one relative to %s", rel, r.dir)
	}
	clean := filepath.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("log path %q escapes the log root %s", rel, r.dir)
	}
	return filepath.Join(r.dir, clean), nil
}

// AttemptFile is one attempt's log file found by AttemptFiles.
type AttemptFile struct {
	// RelPath is relative to the root, the same form the database stores.
	RelPath string
	Attempt int
}

// AttemptFiles lists every attempt of one step of one run across all date
// shards, in numeric attempt order. Attempts of a long running job can cross
// midnight and land in different shards, so the search spans them instead of
// guessing a date from the clock.
func (r Root) AttemptFiles(runID, step string) ([]AttemptFile, error) {
	if runID == "" || step == "" {
		return nil, nil
	}
	pattern := filepath.Join(r.dir, "*", runID, step+".*.ndjson")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("list attempts of step %s of run %s: %w", step, runID, err)
	}

	prefix := step + "."
	var found []AttemptFile
	for _, match := range matches {
		name := strings.TrimSuffix(filepath.Base(match), ".ndjson")
		number, ok := strings.CutPrefix(name, prefix)
		if !ok {
			continue
		}
		attempt := 0
		if _, err := fmt.Sscanf(number, "%d", &attempt); err != nil || attempt < 1 {
			continue
		}
		rel, err := filepath.Rel(r.dir, match)
		if err != nil {
			return nil, fmt.Errorf("relativise %s: %w", match, err)
		}
		found = append(found, AttemptFile{RelPath: rel, Attempt: attempt})
	}
	SortAttempts(found)
	return found, nil
}

// SortAttempts orders attempt files by their number, so attempt 10 comes after
// attempt 2 and not, as a lexical sort would have it, before.
func SortAttempts(files []AttemptFile) {
	for i := 1; i < len(files); i++ {
		for j := i; j > 0 && files[j].Attempt < files[j-1].Attempt; j-- {
			files[j], files[j-1] = files[j-1], files[j]
		}
	}
}

// dirMode and fileMode are the only modes this package accepts on its own
// output. They are tight enough that no umask can be blamed for anything:
// creating with them is safe under umask 0000 as well as 0077.
const (
	dirMode  fs.FileMode = 0o700
	fileMode fs.FileMode = 0o600
)

// PermissionError refuses a path another user can read. It is a distinct type
// so callers can tell a refusal they can explain from a failure they cannot.
type PermissionError struct {
	Path string
	Got  fs.FileMode
	Want fs.FileMode
}

func (e *PermissionError) Error() string {
	return fmt.Sprintf("%s has mode %#o, paceq requires %#o on its logs and refuses "+
		"to write what another user can read\n  Fix it and start again: chmod %#o %s",
		e.Path, e.Got, e.Want, e.Want, e.Path)
}

// checkMode refuses a path wider than want. It runs after every create as well
// as on paths that already existed: a directory made yesterday at 0755 must be
// refused today with the same message as one created wide just now.
func checkMode(path string, want fs.FileMode) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if got := info.Mode().Perm(); got&^want != 0 {
		return &PermissionError{Path: path, Got: got, Want: want}
	}
	return nil
}

// ensureDir creates dir and its parents at 0700, then re-checks the mode of
// every level down to dir. The walk covers directories that already existed:
// MkdirAll leaves an existing wide directory exactly as wide as it was. floor
// is the highest directory this package may judge: the log root itself, since
// everything above it belongs to store.
func ensureDir(floor, dir string) error {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("create the log directory %s: %w", dir, err)
	}
	for path := dir; ; {
		if err := checkMode(path, dirMode); err != nil {
			return err
		}
		if sameDir(path, floor) {
			return nil
		}
		parent := filepath.Dir(path)
		if sameDir(parent, path) {
			// The filesystem root came first, so floor never matched: the
			// caller built a directory outside the log root.
			return fmt.Errorf("the log directory %s is not inside %s", dir, floor)
		}
		path = parent
	}
}

// sameDir compares two paths after cleaning, which is enough for paths this
// package builds itself: both sides come from Join on one shared prefix.
func sameDir(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}

// Create opens the file for rel with every permission check this package
// applies. Open is the normal entry point; Create exists for callers that
// manage the bytes themselves, and for the tests that prove the checks.
func (r Root) Create(rel string) (*os.File, error) {
	return r.createFile(rel)
}

// createFile opens path at 0600, creating parent directories at 0700 first. An
// existing file keeps its old mode, so the mode is checked after opening and
// the file is closed again when it is too wide: fail closed, never write into
// a log another user can read.
func (r Root) createFile(rel string) (*os.File, error) {
	abs, err := r.Abs(rel)
	if err != nil {
		return nil, err
	}
	if err := ensureDir(r.dir, filepath.Dir(abs)); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return nil, fmt.Errorf("open the log file %s: %w", abs, err)
	}
	if err := checkMode(abs, fileMode); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
