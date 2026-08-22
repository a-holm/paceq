package runner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPathsWorkdirInsideRootIsAccepted(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	if err := os.MkdirAll("work/deep", 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := resolveWorkdir("work/deep")
	if err != nil {
		t.Fatalf("resolveWorkdir: %v", err)
	}
	if got != "work/deep" {
		t.Errorf("resolveWorkdir = %q, want the cleaned relative path", got)
	}
}

func TestPathsWorkdirEscapeIsRefused(t *testing.T) {
	t.Chdir(t.TempDir())
	cases := []string{
		"../../etc",
		"../outside",
		"inside/../../outside",
		"..",
	}
	for _, path := range cases {
		if _, err := resolveWorkdir(path); err == nil {
			t.Errorf("workdir %q accepted: only the kernel refusing it stops a spec written by mistake or malice", path)
		}
	}
}

func TestPathsWorkdirSymlinkOutIsRefused(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	outside := t.TempDir()
	if err := os.Symlink(outside, "link-out"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveWorkdir("link-out"); err == nil {
		t.Error("a symlink leaving the root was accepted")
	}
}

func TestPathsAbsoluteWorkdirIsDeferredToTheKernel(t *testing.T) {
	// Existence is not checked here on purpose: a missing absolute workdir
	// must surface as SpawnFailed with an errno from the chdir in Start, which
	// is what the outcome taxonomy promises.
	dir := filepath.Join(t.TempDir(), "not-created-yet")
	got, err := resolveWorkdir(dir)
	if err != nil {
		t.Fatalf("resolveWorkdir(%q): %v", dir, err)
	}
	if got != dir {
		t.Errorf("resolveWorkdir = %q, want %q unchanged", got, dir)
	}
}

func TestPathsEmptyWorkdirMeansInherit(t *testing.T) {
	got, err := resolveWorkdir("")
	if err != nil || got != "" {
		t.Errorf("resolveWorkdir(\"\") = %q, %v; want empty, nil", got, err)
	}
}

func TestPathsEnvFileChecks(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file refused", func(t *testing.T) {
		if _, err := loadEnvFile("", filepath.Join(dir, "absent.env")); err == nil {
			t.Fatal("missing env_file accepted")
		}
	})

	t.Run("directory refused", func(t *testing.T) {
		if _, err := loadEnvFile("", dir); err == nil {
			t.Fatal("a directory was opened as env_file")
		}
	})

	t.Run("relative escape refused", func(t *testing.T) {
		root := t.TempDir()
		t.Chdir(root)
		if _, err := loadEnvFile("", "../secrets.env"); err == nil {
			t.Fatal("env_file outside the root accepted")
		}
	})

	t.Run("relative inside root read through the root", func(t *testing.T) {
		root := t.TempDir()
		t.Chdir(root)
		if err := os.WriteFile("job.env", []byte("A=B\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		vars, err := loadEnvFile("", "job.env")
		if err != nil {
			t.Fatalf("loadEnvFile: %v", err)
		}
		if vars["A"] != "B" {
			t.Errorf("parsed %v, want A=B", vars)
		}
	})
}

func TestPathsOutputFileCreatedUnderWorkdirRoot(t *testing.T) {
	work := t.TempDir()

	t.Run("relative output lands inside workdir", func(t *testing.T) {
		out := "artifacts/out.ndjson"
		if err := os.MkdirAll(filepath.Join(work, "artifacts"), 0o700); err != nil {
			t.Fatal(err)
		}
		closer, err := createOutput(work, out)
		if err != nil {
			t.Fatalf("createOutput: %v", err)
		}
		defer closer()
		info, err := os.Stat(filepath.Join(work, out))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("mode = %o, want 600", info.Mode().Perm())
		}
	})

	t.Run("parent missing is an error", func(t *testing.T) {
		closer, err := createOutput(work, "no-such-dir/out.ndjson")
		if err == nil {
			closer()
			t.Fatal("output created inside a directory that does not exist")
		}
	})

	t.Run("absolute output used as given", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "out.ndjson")
		closer, err := createOutput(work, out)
		if err != nil {
			t.Fatalf("createOutput: %v", err)
		}
		defer closer()
		if _, err := os.Stat(out); err != nil {
			t.Errorf("absolute output not created at %q: %v", out, err)
		}
	})
}
