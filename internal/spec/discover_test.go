package spec_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

// touch creates a file with parent directories and returns its absolute path.
func touch(t *testing.T, parts ...string) string {
	t.Helper()
	p := filepath.Join(parts...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("name: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// symlink creates a symbolic link and returns its path.
func symlink(t *testing.T, target, link string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	return link
}

func TestDiscoverExplicitFile(t *testing.T) {
	dir := t.TempDir()
	want := touch(t, dir, "jobs", "nightly.yaml")

	got, err := spec.Discover([]string{want})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if !reflect.DeepEqual(got, []string{filepath.Clean(want)}) {
		t.Fatalf("got %v, want [%s]", got, filepath.Clean(want))
	}
}

func TestDiscoverExplicitYmlFile(t *testing.T) {
	dir := t.TempDir()
	want := touch(t, dir, "jobs", "nightly.yml")

	got, err := spec.Discover([]string{want})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(got) != 1 || got[0] != filepath.Clean(want) {
		t.Fatalf("got %v, want [%s]", got, filepath.Clean(want))
	}
}

func TestDiscoverExplicitFileWrongExtension(t *testing.T) {
	dir := t.TempDir()
	p := touch(t, dir, "jobs", "notes.txt")

	_, err := spec.Discover([]string{p})
	if err == nil {
		t.Fatal("want error for a file without .yaml or .yml suffix, got nil")
	}
}

func TestDiscoverExplicitFileMissing(t *testing.T) {
	dir := t.TempDir()

	_, err := spec.Discover([]string{filepath.Join(dir, "gone.yaml")})
	if err == nil {
		t.Fatal("want error for a missing file, got nil")
	}
}

func TestDiscoverExplicitSymlinkFileRejected(t *testing.T) {
	dir := t.TempDir()
	outside := touch(t, dir, "outside", "real.yaml")
	link := symlink(t, outside, filepath.Join(dir, "jobs", "link.yaml"))

	if _, err := spec.Discover([]string{link}); err == nil {
		t.Fatalf("want error for symlinked file argument %s, got nil", link)
	}
}

func TestDiscoverDirectoryRecursiveAndSorted(t *testing.T) {
	dir := t.TempDir()
	jobs := filepath.Join(dir, "jobs")
	a := touch(t, jobs, "a.yaml")
	b := touch(t, jobs, "b.yml")
	c := touch(t, jobs, "group", "c.yaml")
	touch(t, jobs, "ignored.txt")
	touch(t, jobs, "group", "ignored.md")

	got, err := spec.Discover([]string{jobs})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	want := []string{filepath.Clean(a), filepath.Clean(b), filepath.Clean(c)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDiscoverSkipsHiddenFilesAndDirs(t *testing.T) {
	dir := t.TempDir()
	jobs := filepath.Join(dir, "jobs")
	keep := touch(t, jobs, "keep.yaml")
	touch(t, jobs, ".hidden.yaml")
	touch(t, jobs, ".git", "hook.yaml")
	touch(t, jobs, "sub", ".cache", "x.yaml")

	got, err := spec.Discover([]string{jobs})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	want := []string{filepath.Clean(keep)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDiscoverIgnoresSymlinksInsideTree(t *testing.T) {
	dir := t.TempDir()
	jobs := filepath.Join(dir, "jobs")
	keep := touch(t, jobs, "keep.yaml")

	// A symlinked file pointing outside the tree must never be listed.
	symlink(t, filepath.Join(dir, "elsewhere.yaml"), filepath.Join(jobs, "evil.yaml"))
	// A symlinked directory must not be walked, even if it holds spec files.
	other := filepath.Join(dir, "other")
	touch(t, other, "inner.yaml")
	symlink(t, other, filepath.Join(jobs, "linked-dir"))
	// A symlink loop must be ignored, not followed forever.
	loop := filepath.Join(jobs, "loop")
	symlink(t, loop, loop)

	got, err := spec.Discover([]string{jobs})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	want := []string{filepath.Clean(keep)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDiscoverDeduplicatesOverlappingRoots(t *testing.T) {
	dir := t.TempDir()
	jobs := filepath.Join(dir, "jobs")
	sub := touch(t, jobs, "sub", "a.yaml")

	got, err := spec.Discover([]string{jobs, filepath.Join(jobs, "sub")})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	want := []string{filepath.Clean(sub)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDiscoverMultipleRoots(t *testing.T) {
	dir := t.TempDir()
	a := touch(t, dir, "one", "a.yaml")
	b := touch(t, dir, "two", "b.yaml")

	got, err := spec.Discover([]string{filepath.Join(dir, "two"), filepath.Join(dir, "one")})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	want := []string{filepath.Clean(a), filepath.Clean(b)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDiscoverMissingDirectory(t *testing.T) {
	dir := t.TempDir()

	if _, err := spec.Discover([]string{filepath.Join(dir, "gone")}); err == nil {
		t.Fatal("want error for a missing directory, got nil")
	}
}

func TestDiscoverNoRoots(t *testing.T) {
	got, err := spec.Discover(nil)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want no files", got)
	}
}
