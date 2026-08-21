package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// dbFileName is the database inside a state directory. The name is fixed so a
// state directory is self describing: one lock, one database, one owner.
const dbFileName = "state.db"

// OpenState claims a state directory and opens the database in it.
//
// The order is the point. The lock is taken before the database is opened for
// writing, so a second paceq is refused before it can touch a single page of a
// file another process owns. The returned store holds the lock until Close.
func OpenState(ctx context.Context, dir string, opt Options) (*Store, error) {
	dbPath := filepath.Join(dir, dbFileName)

	// The filesystem is checked before the lock is taken. flock on NFS or FUSE
	// is the one operation whose semantics are undefined there, so performing it
	// to find out whether the filesystem is supported would be the wrong order.
	if err := guardFilesystem(dir, opt.AllowNetworkFS); err != nil {
		return nil, err
	}

	lock, err := AcquireStateLock(dir)
	if err != nil {
		var locked *LockedError
		if errors.As(err, &locked) {
			locked.Owner = lockOwner(ctx, dbPath)
		}
		return nil, err
	}

	fresh := false
	if _, statErr := os.Stat(dbPath); errors.Is(statErr, os.ErrNotExist) {
		fresh = true
	}

	s, err := Open(ctx, dbPath, opt)
	if err != nil {
		_ = lock.Release()
		return nil, err
	}
	if err := privateDatabase(dbPath, fresh); err != nil {
		_ = s.Close()
		_ = lock.Release()
		return nil, err
	}
	s.lock = lock
	return s, nil
}

// privateDatabase narrows a database paceq just created and refuses one that
// somebody else widened. SQLite creates the file under the process umask, which
// is nothing paceq controls, so a fresh file is set to 0600 here rather than
// left to the environment.
func privateDatabase(dbPath string, fresh bool) error {
	if fresh {
		if err := os.Chmod(dbPath, lockMode); err != nil {
			return fmt.Errorf("restrict %s: %w", dbPath, err)
		}
		return nil
	}
	return checkPerm(dbPath, lockMode)
}

// lockOwner reads the session row of whoever holds the state directory. It
// opens the database read only: the holder is writing to it, and the whole
// point of being refused is to touch nothing.
//
// Every failure here is silent on purpose. This runs on an error path, and a
// refusal that says less is better than one that reports why it could not read
// somebody else's database.
func lockOwner(ctx context.Context, dbPath string) *Session {
	if _, err := os.Stat(dbPath); err != nil {
		return nil
	}
	db, err := sql.Open(driverName, dsn(dbPath, "mode=ro", readerSpecs("NORMAL")))
	if err != nil {
		return nil
	}
	defer func() { _ = db.Close() }()

	// A store with only a reader. withRead needs nothing else, and this is an
	// error path: giving it a clock or a writer would couple the refusal to
	// machinery that has no business running when we are already being told to
	// keep our hands off somebody else's database.
	probe := &Store{r: db}
	session, found, err := probe.OpenSession(ctx)
	if err != nil || !found {
		return nil
	}
	return &session
}

// Error explains the refusal the way every paceq error does: what went wrong,
// where, and what to do next. The owner is named from the session row when
// there is one, because "already running" without a pid is not actionable.
func (e *LockedError) Error() string {
	if e.Owner == nil {
		return fmt.Sprintf("PQ1001: another process already holds %s, and no session row names it: "+
			"it may be a foreign process, or a start that died before its first write\n"+
			"  Run this instance against another state directory\n"+
			"  Or find the holder: fuser %s", e.Path, e.Path)
	}
	return fmt.Sprintf("PQ1001: another paceq process already uses %s\n"+
		"  owner: pid %d, version %s, started %s\n"+
		"  Run this instance against another state directory\n"+
		"  Or stop the running one: systemctl stop paceq, or kill %d",
		e.Path, e.Owner.PID, e.Owner.Version, e.Owner.StartedAt.Format(time.RFC3339), e.Owner.PID)
}
