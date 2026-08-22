package spec

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// mustSymlink creates one link, or skips when links cannot exist where the
// test runs.
func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this filesystem does not carry symlinks: %v", err)
	}
}

// TestCollectSkipsLinksInsideTheTree pins the rule a jobs walk is safe by:
// nothing that can name a file outside the tree is read or listed. A link
// named like a job file would otherwise pull a spec in from anywhere on disk
// (08 T11), and a link to a directory would let the walk leave the tree.
func TestCollectSkipsLinksInsideTheTree(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	outside := filepath.Join(root, "outside")
	if err := os.MkdirAll(filepath.Join(jobs, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}

	write := func(path string) {
		t.Helper()
		if err := os.WriteFile(path, []byte("job: x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(jobs, "real.yaml"))
	write(filepath.Join(jobs, "nested", "deep.yml"))
	write(filepath.Join(outside, "secret.yaml"))
	write(filepath.Join(outside, "more.yaml"))

	mustSymlink(t, filepath.Join(outside, "secret.yaml"), filepath.Join(jobs, "link.yaml"))
	mustSymlink(t, outside, filepath.Join(jobs, "nested", "elsewhere"))

	files, err := Collect([]string{jobs})
	if err != nil {
		t.Fatalf("collect %s: %v", jobs, err)
	}
	want := []string{
		filepath.Join(jobs, "nested", "deep.yml"),
		filepath.Join(jobs, "real.yaml"),
	}
	if !slices.Equal(files, want) {
		t.Errorf("the walk listed %v, want %v", files, want)
	}
}

// TestCollectTakesAnExplicitFileAsItIsIsTheOtherHalf: the safety rule above is
// for trees an operator pointed at as a whole. A single path typed on the
// command line is read whatever it is called, because the operator asked for
// exactly that file.
func TestCollectTakesAnExplicitFileAsItIs(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "jobspec.txt")
	if err := os.WriteFile(path, []byte("job: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	files, err := Collect([]string{path})
	if err != nil {
		t.Fatalf("collect %s: %v", path, err)
	}
	if !slices.Equal(files, []string{path}) {
		t.Errorf("an explicit file came back as %v, want [%s]", files, path)
	}
}

// TestCollectRefusesAMissingPath: a root that is not there is an error rather
// than an empty list, because an empty apply that was meant to load something
// must never look like a clean one.
func TestCollectRefusesAMissingPath(t *testing.T) {
	if _, err := Collect([]string{filepath.Join(t.TempDir(), "not-there")}); err == nil {
		t.Error("a missing root collected without an error")
	}
}
