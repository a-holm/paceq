package spec

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Discover lists the job spec files under the given roots. Apply loads exactly
// this list, so it is the boundary between the filesystem and the database:
// nothing outside these files can influence a load.
//
// A root is one explicit path from the command line. A root that names a file
// must carry the .yaml or .yml suffix and must not be a symlink; a root that
// names a directory is walked recursively for spec files. The rules inside a
// directory tree are:
//
//   - Only regular files ending in .yaml or .yml are listed. The comparison
//     is case sensitive, matching how the files are written in practice.
//   - Files and directories whose name starts with "." are skipped at every
//     depth, so editor leftovers and VCS internals never become jobs.
//   - Symbolic links are never followed, and symlinked files are never
//     listed. A link inside the jobs directory could otherwise point anywhere
//     on disk (08 T11). os.Root backs the walk, so even a future change that
//     starts resolving links cannot escape the tree.
//   - Special files such as pipes and sockets are skipped.
//
// The result has no duplicates and is sorted by path, so two runs over the
// same catalog produce the same order no matter what the readdir call returns.
// Deterministic order keeps apply output stable across machines.
//
// Discover reports an error when a named root cannot be used at all: a missing
// path, a file with the wrong suffix, or a directory that cannot be read.
// Problems with individual files inside a tree, such as a broken symlink, are
// not errors; they are skipped, because one stray entry must not stop the
// valid jobs around it.
func Discover(roots []string) ([]string, error) {
	seen := map[string]bool{}
	var files []string

	add := func(p string) {
		if !seen[p] {
			seen[p] = true
			files = append(files, p)
		}
	}

	for _, root := range roots {
		abs, err := filepath.Abs(root)
		if err != nil {
			return nil, fmt.Errorf("resolve %s: %w", root, err)
		}
		info, err := os.Lstat(abs)
		if err != nil {
			return nil, fmt.Errorf("discover %s: %w", abs, err)
		}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			return nil, fmt.Errorf("discover %s: job spec paths must not be symlinks, pass the real path", abs)
		case info.IsDir():
			found, err := discoverDir(abs)
			if err != nil {
				return nil, err
			}
			for _, f := range found {
				add(f)
			}
		case info.Mode().IsRegular():
			if !isSpecFile(info.Name()) {
				return nil, fmt.Errorf("discover %s: only .yaml and .yml files can be loaded", abs)
			}
			add(abs)
		default:
			return nil, fmt.Errorf("discover %s: not a file or directory", abs)
		}
	}

	sort.Strings(files)
	return files, nil
}

// discoverDir walks one directory tree and returns the spec files in it,
// sorted by path relative to nothing but their own tree order. Callers sort
// again across trees, so the order here only has to be complete.
func discoverDir(dir string) ([]string, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open jobs dir %s: %w", dir, err)
	}
	defer root.Close()

	var files []string
	err = fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("read %s: %w", filepath.Join(dir, p), err)
		}
		if p == "." {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Type().IsRegular() && isSpecFile(name) {
			files = append(files, filepath.Join(dir, filepath.FromSlash(p)))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// isSpecFile reports whether a file name carries a job spec suffix.
func isSpecFile(name string) bool {
	return strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml")
}
