package cutover

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ErrNotInstalled is what Read and Write report when crontab(1) is not on
// PATH. The caller turns it into the message that names the fix; the
// sentinel exists so a missing binary and a failing binary cannot be
// confused.
var ErrNotInstalled = errors.New("the crontab command is not on PATH")

// ErrNoCrontab is what Read reports when cron has nothing saved for the
// user. An empty crontab is not an error, it is the state before the first
// import, so it travels as its own sentinel instead of a failed read.
var ErrNoCrontab = errors.New("no crontab for user")

// lookCrontab resolves crontab(1) through PATH. The lookup is deliberate:
// it is what lets a test plant its own stub and prove, end to end, that
// paceq never touches a real spool. Every invocation is an argv array -
// exec.Command with fixed flags and at most an operator-typed user name -
// and never a shell string, the same rule the rest of paceq lives by.
func lookCrontab() (string, error) {
	bin, err := exec.LookPath("crontab")
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNotInstalled, err)
	}
	return bin, nil
}

// Read asks crontab(1) to print a crontab: `crontab -l`, or `crontab -u
// user -l` for another user's. No crontab is an empty answer, not a
// failure; every other exit is the wrapped stderr so the operator sees
// what cron said, not what paceq guessed cron meant.
func Read(ctx context.Context, user string) (string, error) {
	bin, err := lookCrontab()
	if err != nil {
		return "", err
	}
	args := []string{"-l"}
	if user != "" {
		args = []string{"-u", user, "-l"}
	}
	// #nosec G204 - crontab resolves through LookPath so a test can plant
	// its own stub on PATH, which is how non-destructiveness gets proven;
	// every argument is a fixed list flag or an operator-typed user name.
	cmd := exec.CommandContext(ctx, bin, args...)
	outBytes, runErr := cmd.Output()
	if runErr == nil {
		return string(outBytes), nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		message := strings.TrimSpace(string(exitErr.Stderr))
		if strings.Contains(message, "no crontab for") {
			return "", fmt.Errorf("%w: %s", ErrNoCrontab, message)
		}
		if message == "" {
			message = fmt.Sprintf("crontab exited with %s", exitErr)
		}
		return "", fmt.Errorf("crontab -l: %s", message)
	}
	return "", fmt.Errorf("could not run crontab: %w", runErr)
}

// Write hands content to crontab(1) on stdin: `crontab -`, or `crontab -u
// user -`. Writing through the command, never straight into the spool, is
// what buys cron's validation, its lock against a concurrent `crontab -e`,
// and the right ownership in the spool directory - none of which paceq
// should reproduce, all of which a direct write would have to get right
// every time.
func Write(ctx context.Context, user, content string) error {
	bin, err := lookCrontab()
	if err != nil {
		return err
	}
	args := []string{"-"}
	if user != "" {
		args = []string{"-u", user, "-"}
	}
	// #nosec G204 - as in Read: fixed flags and an operator-typed name.
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = strings.NewReader(content)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = fmt.Sprintf("crontab exited with %s", err)
		}
		return fmt.Errorf("crontab write failed: %s", message)
	}
	return nil
}

// Backup writes content into dir as one timestamped file and returns its
// path. One file per operation, never overwritten: a second cutover in the
// same second gets a numeric suffix instead of the first backup losing its
// place in history. The mode is 0600 and the write is the durability
// discipline the spool already uses (02 section 5.6): write, sync, rename,
// sync the directory, so a backup that is reported exists even if the
// machine loses power an instant later.
func Backup(dir, content string, now time.Time) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("could not prepare the backup directory: %w", err)
	}
	base := "crontab.backup." + now.Format("2006-01-02T15-04-05")
	path := filepath.Join(dir, base)
	for attempt := 2; ; attempt++ {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return "", fmt.Errorf("could not look at %s: %w", path, err)
		}
		path = filepath.Join(dir, fmt.Sprintf("%s.%d", base, attempt))
	}
	if err := writeFileAtomic(path, []byte(content), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

// ReadBackup reads a backup back. Rollback --from hands the path over; the
// check that it exists with a readable mode belongs here, so the caller's
// error names the file instead of a bare errno.
func ReadBackup(path string) (string, error) {
	data, err := os.ReadFile(path) // #nosec G304 - the path is what the operator asked paceq to restore
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("no backup at %s", path)
		}
		return "", fmt.Errorf("could not read %s: %w", path, err)
	}
	return string(data), nil
}

// writeFileAtomic is the write side of the durability discipline: write a
// temporary file beside the target, sync it, rename it over the target,
// sync the directory. A reader sees either the old file or the new one,
// never a half-written one.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".crontab.backup-*")
	if err != nil {
		return fmt.Errorf("could not create the temporary backup: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("could not write the backup: %w", err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("could not set the backup mode: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("could not flush the backup: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("could not close the backup: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("could not put the backup in place: %w", err)
	}
	tmpName = "" // renamed: the deferred remove must not eat it
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
