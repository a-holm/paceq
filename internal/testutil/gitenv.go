package testutil

import (
	"errors"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The identity git exports to the commit hooks. These are not repository-local,
// so `git rev-parse --local-env-vars` does not name them, but they outrank
// user.name and user.email from the repository config. A test that sets its own
// identity with `git config` does not get it while one of these is set.
var gitIdentityVars = []string{
	"GIT_AUTHOR_NAME",
	"GIT_AUTHOR_EMAIL",
	"GIT_AUTHOR_DATE",
	"GIT_COMMITTER_NAME",
	"GIT_COMMITTER_EMAIL",
	"GIT_COMMITTER_DATE",
}

// gitLocalEnvVars asks git which variables bind a process to one repository.
// git is the only authority on that list: it grows between releases, and
// githooks(5) points at this command for exactly this case. The answer is a
// compiled-in list, so it is the same whatever repository, or none, the call
// lands in.
var gitLocalEnvVars = sync.OnceValues(func() ([]string, error) {
	out, err := exec.Command("git", "rev-parse", "--local-env-vars").Output()
	if err != nil {
		return nil, err
	}
	names := strings.Fields(string(out))
	if len(names) == 0 {
		return nil, errors.New("git named no local environment variables")
	}
	return names, nil
})

// GitEnv returns env without the variables that would point a git child process
// at an ambient repository instead of the one the test built.
//
// git exports GIT_DIR and GIT_PREFIX to every hook, adds GIT_INDEX_FILE around
// a commit, and in a linked worktree GIT_DIR is absolute. A test that runs git
// in its own temporary repository and inherits those runs against the outer
// repository instead: it passes on its own and does something else entirely
// under `git push`, which is how it reaches a gate unnoticed.
//
// Dropping every GIT_* variable is not the fix. GIT_EXEC_PATH is exported to
// hooks too and tells git where its own subcommands live, so a git installed
// outside the default prefix stops working without it.
func GitEnv(t *testing.T, env []string) []string {
	t.Helper()

	local, err := gitLocalEnvVars()
	if err != nil {
		// Carrying on would hand the test the ambient repository and call it
		// a pass, which is the failure this helper exists to prevent.
		t.Fatalf("ask git which environment variables are repository-local: %v", err)
	}

	strip := slices.Concat(local, gitIdentityVars)
	kept := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, ok := strings.Cut(entry, "=")
		if ok && slices.Contains(strip, name) {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}
