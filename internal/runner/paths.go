package runner

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// escapeHint is the text os.Root uses when a path leaves the root. The
// sentinel is unexported in the os package, so the wording is pinned by
// TestPathsWorkdirEscapeIsRefused; if a Go release renames it, that test
// fails and this file gets updated on purpose rather than by accident.
const escapeHint = "escapes from parent"

// resolveWorkdir validates the workdir and returns what goes into cmd.Dir.
//
// A relative workdir is resolved inside the runner process's working
// directory through os.Root, so a spec that writes "../.." is refused and a
// symlink that leaves the root is refused as well. A missing relative
// directory passes through here on purpose: the chdir in Start is what turns
// "does not exist" into a SpawnFailed with an errno, which is what the
// outcome taxonomy promises.
//
// An absolute workdir is used as given. There is no trusted base to root it
// against at this layer; the engine owns the per run directory layout and
// roots every absolute path it hands over.
func resolveWorkdir(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	// Lexical escape check first: a spec that climbs out with ".." is refused
	// whether or not the intermediate directories happen to exist.
	cleaned := filepath.Clean(path)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("workdir %q escapes the working directory root", path)
	}
	root, err := os.OpenRoot(".")
	if err != nil {
		return "", fmt.Errorf("open working directory root: %w", err)
	}
	defer root.Close()
	if _, err := root.Stat(cleaned); err != nil {
		if strings.Contains(err.Error(), escapeHint) {
			return "", fmt.Errorf("workdir %q escapes the working directory root: %w", path, err)
		}
		// Missing or otherwise broken: let the kernel report it at chdir.
		return cleaned, nil
	}
	return cleaned, nil
}

// loadEnvFile reads the env_file for a spec. Relative paths resolve inside
// the workdir root; absolute paths resolve through the filesystem root, which
// keeps the kernel in charge of every component including symlinks. The mode
// rule is the same on both roads: exactly 0600, checked on the open handle.
func loadEnvFile(workdir, path string) (map[string]string, error) {
	if filepath.IsAbs(path) {
		return parseEnvFile(path)
	}
	root, err := os.OpenRoot(rootBase(workdir))
	if err != nil {
		return nil, fmt.Errorf("open workdir root for env_file: %w", err)
	}
	defer root.Close()
	f, err := root.Open(path)
	if err != nil {
		if strings.Contains(err.Error(), escapeHint) {
			return nil, fmt.Errorf("env_file %q escapes the workdir root: %w", path, err)
		}
		return nil, fmt.Errorf("open env_file %q: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat env_file %q: %w", path, err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read env_file %q: %w", path, err)
	}
	vars, err := parseEnvChecked(info, data, filepath.Base(path))
	if err != nil {
		return nil, err
	}
	return vars, nil
}

// createOutput creates the file PACEQ_OUTPUT points at, before the command
// starts, so the contract "the file exists" holds from the first release even
// though nothing reads it until M4-05. Relative paths resolve inside the
// workdir, which is also the child's working directory, so the child sees the
// same file at the same relative name. Missing parent directories are an
// error: the engine owns the layout, the runner only fills the slot.
//
// The returned closer releases the handle; the file itself stays.
func createOutput(workdir, path string) (func(), error) {
	if path == "" {
		return func() {}, nil
	}

	var f *os.File
	if filepath.IsAbs(path) {
		var err error
		f, err = createAtFilesystemRoot(path)
		if err != nil {
			return nil, fmt.Errorf("create output file: %w", err)
		}
	} else {
		root, err := os.OpenRoot(rootBase(workdir))
		if err != nil {
			return nil, fmt.Errorf("open workdir root for output: %w", err)
		}
		defer root.Close()
		out, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			if strings.Contains(err.Error(), escapeHint) {
				return nil, fmt.Errorf("output path %q escapes the workdir root: %w", path, err)
			}
			return nil, fmt.Errorf("create output file %q: %w", path, err)
		}
		f = out
	}
	return func() { _ = f.Close() }, nil
}

// rootBase is the directory a root opens on. The workdir, when set, anchors
// the spec's relative paths; otherwise the runner's working directory does.
func rootBase(workdir string) string {
	if workdir == "" {
		return "."
	}
	return workdir
}

// openAtFilesystemRoot opens an absolute spec path through os.Root anchored
// at /, so the kernel resolves every component and refuses a symlink that
// would somehow leave it, instead of the code trusting a cleaned string.
func openAtFilesystemRoot(path string) (*os.File, error) {
	root, err := os.OpenRoot("/")
	if err != nil {
		return nil, fmt.Errorf("open filesystem root: %w", err)
	}
	defer root.Close()
	return root.OpenFile(strings.TrimPrefix(filepath.Clean(path), "/"), os.O_RDONLY, 0)
}

// createAtFilesystemRoot creates or truncates an absolute output path with
// mode 0600, through the same kernel resolved road as openAtFilesystemRoot.
func createAtFilesystemRoot(path string) (*os.File, error) {
	root, err := os.OpenRoot("/")
	if err != nil {
		return nil, fmt.Errorf("open filesystem root: %w", err)
	}
	defer root.Close()
	f, err := root.OpenFile(strings.TrimPrefix(filepath.Clean(path), "/"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	return f, nil
}
