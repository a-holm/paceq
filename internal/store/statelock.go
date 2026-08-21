package store

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	// lockFileName is the file the state lock is taken on. It is kept between
	// runs on purpose: the lock lives in the kernel, and deleting the file
	// would give the next process a new inode and therefore no lock at all.
	lockFileName = "paceq.lock"

	// dirMode and lockMode are the only modes paceq accepts. Anything wider
	// means the state was readable by another user, which is a refusal rather
	// than something to correct quietly.
	dirMode  fs.FileMode = 0o700
	lockMode fs.FileMode = 0o600
)

// StateLock is the exclusive claim on one state directory. It is held by an open
// file description for as long as the process lives, so no second paceq can
// write to the same state, and the kernel releases it even when the process is
// killed.
type StateLock struct {
	f    *os.File
	path string
}

// LockedError is returned when another process already owns the state
// directory. It is a distinct type so a caller can tell "somebody else is
// running" from "this directory is broken".
type LockedError struct {
	Path string
	Err  error
}

func (e *LockedError) Error() string {
	return fmt.Sprintf("PQ1001: another paceq process already uses %s", e.Path)
}

func (e *LockedError) Unwrap() error { return e.Err }

// AcquireStateLock takes the exclusive lock on dir, creating the directory when
// it does not exist. It never waits: a daemon that blocks at startup looks
// hung, so a held lock is an immediate error naming the process that holds it.
//
// The lock is taken before the database is opened for writing, which is what
// makes two writers impossible rather than merely unlikely.
func AcquireStateLock(dir string) (*StateLock, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("create the state directory: %w", err)
	}
	if err := checkPerm(dir, dirMode); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, lockMode)
	if err != nil {
		return nil, fmt.Errorf("open the state lock file: %w", err)
	}
	if err := checkPerm(path, lockMode); err != nil {
		_ = f.Close()
		return nil, err
	}

	if err := lockExclusive(f); err != nil {
		_ = f.Close()
		return nil, &LockedError{Path: path, Err: err}
	}
	return &StateLock{f: f, path: path}, nil
}

// Path is the lock file this lock is held on.
func (l *StateLock) Path() string { return l.path }

// Release drops the lock and closes the file. The file itself stays: closing
// the descriptor is what releases the lock, and unlinking it would let a second
// process create a fresh inode and lock that instead.
func (l *StateLock) Release() error {
	if err := unlockFile(l.f); err != nil {
		_ = l.f.Close()
		return fmt.Errorf("release the state lock: %w", err)
	}
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("close the state lock file: %w", err)
	}
	return nil
}

// checkPerm refuses a path that is readable by anyone but its owner. Widening
// permissions back is deliberately not offered: correcting them quietly would
// hide that the state has been exposed, which is the fact an operator needs.
func checkPerm(path string, want fs.FileMode) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	got := info.Mode().Perm()
	if got&^want != 0 {
		return fmt.Errorf("PQ1002: %s has mode %#o, paceq requires %#o and refuses to start "+
			"with state another user can read. Fix it and start again: chmod %#o %s",
			path, got, want, want, path)
	}
	return nil
}
