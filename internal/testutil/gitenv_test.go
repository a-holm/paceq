//go:build unix

package testutil_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/testutil"
)

// The bug this guards against (#176): a test helper built its child environment
// with append(os.Environ(), ...) and ran git in a repository it had just made in
// a temp directory. That works at a shell. Under a git hook it does not: git
// exports GIT_DIR to the hook, so every one of those git commands operated on
// the outer repository, and the whole gate-stamp suite failed on `git push`
// while passing for whoever wrote it.
//
// So these tests do not check that GitEnv filters a list of strings. They set
// the variables a hook sets, run git for real, and fail if it answers about the
// wrong repository.

// hookVars is what git exports to a hook in a linked worktree, which is how
// every agent worktree in this project is checked out. GIT_DIR is absolute
// there, so it survives any change of working directory.
func hookVars(gitDir string) []string {
	return []string{
		"GIT_DIR=" + gitDir,
		"GIT_INDEX_FILE=" + filepath.Join(gitDir, "index"),
		"GIT_PREFIX=",
	}
}

// newRepo makes a repository with one commit and returns its path. It uses a
// clean environment so it cannot itself be a victim of the bug under test.
func newRepo(t *testing.T, marker string) string {
	t.Helper()

	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = testutil.GitEnv(t, os.Environ())
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	run("init", "-q", ".")
	run("config", "user.email", "helper@example.invalid")
	run("config", "user.name", "helper")
	if err := os.WriteFile(filepath.Join(dir, marker), []byte("x\n"), 0o600); err != nil {
		t.Fatalf("write %s: %v", marker, err)
	}
	run("add", "-A")
	run("commit", "-qm", "init")
	return dir
}

// TestGitEnvKeepsGitInTheRepositoryTheTestBuilt is the guard. With the hook
// variables set, a helper that inherits them reports the outer repository's
// files instead of its own. Before GitEnv existed this failed exactly the way
// the gate-stamp suite failed under `git push`.
func TestGitEnvKeepsGitInTheRepositoryTheTestBuilt(t *testing.T) {
	outer := newRepo(t, "outer-marker")
	inner := newRepo(t, "inner-marker")

	outerGitDir := filepath.Join(outer, ".git")
	cmd := exec.Command("git", "ls-files")
	cmd.Dir = inner
	cmd.Env = testutil.GitEnv(t, append(os.Environ(), hookVars(outerGitDir)...))

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git ls-files: %v\n%s", err, out)
	}
	listed := strings.Fields(string(out))
	if !slices.Contains(listed, "inner-marker") {
		t.Errorf("git listed %v, want the repository the test built (inner-marker)", listed)
	}
	if slices.Contains(listed, "outer-marker") {
		t.Errorf("git listed %v: the hook's GIT_DIR reached the child and it answered "+
			"about the outer repository", listed)
	}
}

// TestGitEnvCommitsLandInTheRepositoryTheTestBuilt covers the failure the
// gate-stamp suite actually reported: `git commit` said "nothing to commit,
// working tree clean" because it was looking at a different, clean, tree.
func TestGitEnvCommitsLandInTheRepositoryTheTestBuilt(t *testing.T) {
	outer := newRepo(t, "outer-marker")
	inner := newRepo(t, "inner-marker")

	if err := os.WriteFile(filepath.Join(inner, "added"), []byte("y\n"), 0o600); err != nil {
		t.Fatalf("write added: %v", err)
	}

	env := testutil.GitEnv(t, append(os.Environ(), hookVars(filepath.Join(outer, ".git"))...))
	for _, args := range [][]string{{"add", "-A"}, {"commit", "-qm", "second"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = inner
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	cmd := exec.Command("git", "log", "--format=%s")
	cmd.Dir = inner
	cmd.Env = testutil.GitEnv(t, os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git log: %v\n%s", err, out)
	}
	if got := strings.Fields(string(out)); len(got) != 2 || got[0] != "second" {
		t.Errorf("the repository the test built has subjects %v, want the second commit on top", got)
	}
}

// TestGitEnvKeepsWhatGitNeedsToRun. GIT_EXEC_PATH is exported to hooks like
// GIT_DIR is, but it says where git's subcommands live rather than which
// repository to use. Dropping every GIT_* variable would take it with them and
// break git on any installation outside the default prefix.
func TestGitEnvKeepsWhatGitNeedsToRun(t *testing.T) {
	env := testutil.GitEnv(t, []string{
		"GIT_DIR=/somewhere/else/.git",
		"GIT_EXEC_PATH=/opt/git/libexec/git-core",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"PATH=/usr/bin",
	})

	for _, want := range []string{
		"GIT_EXEC_PATH=/opt/git/libexec/git-core",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"PATH=/usr/bin",
	} {
		if !slices.Contains(env, want) {
			t.Errorf("GitEnv dropped %q, which does not choose a repository", want)
		}
	}
	if slices.Contains(env, "GIT_DIR=/somewhere/else/.git") {
		t.Error("GitEnv kept GIT_DIR")
	}
}

// TestGitEnvDropsTheIdentityThatOutranksTheRepositoryConfig. The commit hooks
// export GIT_AUTHOR_NAME and friends, and they beat `git config user.name`, so
// a test that sets its own identity would silently not have it.
func TestGitEnvDropsTheIdentityThatOutranksTheRepositoryConfig(t *testing.T) {
	repo := newRepo(t, "marker")

	if err := os.WriteFile(filepath.Join(repo, "added"), []byte("y\n"), 0o600); err != nil {
		t.Fatalf("write added: %v", err)
	}

	env := testutil.GitEnv(t, append(os.Environ(),
		"GIT_AUTHOR_NAME=hook",
		"GIT_AUTHOR_EMAIL=hook@example.invalid",
		"GIT_COMMITTER_NAME=hook",
		"GIT_COMMITTER_EMAIL=hook@example.invalid"))
	for _, args := range [][]string{{"add", "-A"}, {"commit", "-qm", "second"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	cmd := exec.Command("git", "log", "-1", "--format=%an <%ae>")
	cmd.Dir = repo
	cmd.Env = testutil.GitEnv(t, os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git log: %v\n%s", err, out)
	}
	if got := strings.TrimSpace(string(out)); got != "helper <helper@example.invalid>" {
		t.Errorf("the commit author is %q, want the identity the repository config sets", got)
	}
}
