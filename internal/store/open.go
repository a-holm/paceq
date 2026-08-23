package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"

	"github.com/a-holm/paceq/internal/clock"
)

// driverName is the database/sql name registered by modernc.org/sqlite.
const driverName = "sqlite"

// pragmaSpec is one PRAGMA the pool sets and then reads back. An empty arg
// means verify only: the reader cannot set journal_mode on a read-only
// connection, but it must still see WAL.
type pragmaSpec struct {
	name string
	arg  string
	want string
}

// writerSpecs configure and then verify the single write connection. mmap_size
// is verified as off rather than set: the gain is marginal for this access
// pattern and mmap turns an I/O error into a SIGBUS no error path can catch.
func writerSpecs(sync string) []pragmaSpec {
	return []pragmaSpec{
		{name: "journal_mode", arg: "WAL", want: "wal"},
		{name: "synchronous", arg: sync, want: syncReadback(sync)},
		{name: "busy_timeout", arg: "10000", want: "10000"},
		{name: "foreign_keys", arg: "ON", want: "1"},
		{name: "temp_store", arg: "MEMORY", want: "2"},
		{name: "cache_size", arg: "-16000", want: "-16000"},
		{name: "wal_autocheckpoint", arg: "1000", want: "1000"},
		{name: "journal_size_limit", arg: "67108864", want: "67108864"},
		{name: "query_only", want: "0"},
		{name: "mmap_size", want: "0"},
	}
}

// readerSpecs configure and then verify every read connection. query_only comes
// last so the pragmas before it are not refused, and it makes the engine rather
// than convention enforce that reads cannot write.
func readerSpecs(sync string) []pragmaSpec {
	return []pragmaSpec{
		{name: "synchronous", arg: sync, want: syncReadback(sync)},
		{name: "busy_timeout", arg: "5000", want: "5000"},
		{name: "foreign_keys", arg: "ON", want: "1"},
		{name: "temp_store", arg: "MEMORY", want: "2"},
		{name: "cache_size", arg: "-32000", want: "-32000"},
		{name: "query_only", arg: "ON", want: "1"},
		{name: "journal_mode", want: "wal"},
		{name: "mmap_size", want: "0"},
	}
}

// Options configures Open.
type Options struct {
	// Synchronous is "normal" (default) or "full". Nothing else is accepted.
	Synchronous string

	// AllowNetworkFS skips the refusal to run on a network or FUSE filesystem.
	AllowNetworkFS bool

	// Clock is the clock the retry backoff waits on. A nil Clock means
	// clock.System(). Tests that want the backoff to be instant pass their own.
	Clock clock.Clock
}

// Store owns every connection to the database file. Both handles are private:
// no package outside internal/store may hold a writable database handle.
type Store struct {
	w    *sql.DB
	r    *sql.DB
	path string
	clk  clock.Clock

	// bootID is the machine's boot id reader. It is a field so a test can
	// reproduce a machine restart, which no test can do to /proc.
	bootID func() (string, error)
	// bootWarn keeps the degradation notice to one line per store. Repeating it
	// on every session would train the operator to ignore it.
	bootWarn sync.Once
	// bootChanged is written by StartSession and read by whoever reconciles, so
	// it is atomic rather than a plain bool.
	bootChanged atomic.Bool

	// bootOverride pins the boot id for tests (OverrideBootIDForTest): a
	// machine restart cannot be staged against /proc, but its effect can.
	bootOverride atomic.Pointer[string]

	// onCommit runs after every successful write transaction commit. It is
	// nil in production; only package tests set it, to count real commits
	// where the batch heartbeat proof needs them.
	onCommit func()

	// lock is the state directory claim, held when the store was opened through
	// OpenState. Close releases it.
	lock *StateLock
}

// Open opens the database at path with a single-connection writer pool and a
// read-only reader pool, then verifies that every PRAGMA is actually in effect.
// It creates the file when it does not exist and works against an empty,
// unmigrated database.
func Open(ctx context.Context, path string, opt Options) (*Store, error) {
	sync, err := syncMode(opt.Synchronous)
	if err != nil {
		return nil, err
	}
	if err := guardFilesystem(filepath.Dir(path), opt.AllowNetworkFS); err != nil {
		return nil, err
	}

	w, err := sql.Open(driverName, dsn(path, "_txlock=immediate", writerSpecs(sync)))
	if err != nil {
		return nil, fmt.Errorf("open writer pool: %w", err)
	}
	// database/sql is the write queue: one connection, never recycled.
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	w.SetConnMaxIdleTime(0)

	// The reader pool opens mode=ro, which fails when the file is missing, so
	// the writer has to materialise it first.
	if err := w.PingContext(ctx); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("connect writer pool: %w", err)
	}

	r, err := sql.Open(driverName, dsn(path, "mode=ro", readerSpecs(sync)))
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("open reader pool: %w", err)
	}
	// Idle has to match open, or database/sql keeps the default of two idle
	// connections, closes the rest after every burst, and replays the reader
	// pragmas on each reopen. The cap alone would be inert beyond two.
	r.SetMaxOpenConns(readerPoolSize())
	r.SetMaxIdleConns(readerPoolSize())
	r.SetConnMaxLifetime(0)
	r.SetConnMaxIdleTime(0)

	clk := opt.Clock
	if clk == nil {
		clk = clock.System()
	}

	s := &Store{w: w, r: r, path: path, clk: clk, bootID: readBootID}
	// ensureAutoVacuum can rewrite the file, so it runs first and the
	// verification stays the last thing Open does. Verifying before the rewrite
	// would describe a file the caller never gets.
	if err := s.ensureAutoVacuum(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.verifyPragmas(ctx, sync); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// verifyPragmas reads the settings back from both pools. A mismatch is a
// startup error, never a warning and never a quiet degradation: it means the
// driver ignored the DSN, a driver swap reads _pragma differently, or the file
// is in a journal mode this design does not support.
func (s *Store) verifyPragmas(ctx context.Context, sync string) error {
	if err := verifyPool(ctx, s.w, "writer", writerSpecs(sync)); err != nil {
		return err
	}
	return verifyPool(ctx, s.r, "reader", readerSpecs(sync))
}

// verifyPool checks every spec on a single connection of the pool. Pragmas are
// per connection, so reading them from one connection at a time is the only
// meaningful check.
func verifyPool(ctx context.Context, db *sql.DB, pool string, specs []pragmaSpec) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s pool: take connection: %w", pool, err)
	}
	defer func() { _ = conn.Close() }()

	for _, spec := range specs {
		var got string
		if err := conn.QueryRowContext(ctx, "PRAGMA "+spec.name).Scan(&got); err != nil {
			return fmt.Errorf("%s pool: read PRAGMA %s: %w", pool, spec.name, err)
		}
		if !strings.EqualFold(got, spec.want) {
			return fmt.Errorf("%s pool: PRAGMA %s is %q, want %q: the driver or the "+
				"database file does not honour the requested setting, refusing to start",
				pool, spec.name, got, spec.want)
		}
	}
	return nil
}

// guardFilesystem refuses to run on a filesystem where SQLite locking is
// undefined. AllowNetworkFS is the deliberate way out, not a default.
func guardFilesystem(dir string, allowNetworkFS bool) error {
	if allowNetworkFS {
		return nil
	}
	return checkLocalFS(dir)
}

// Path is the database file this store was opened against.
func (s *Store) Path() string {
	return s.path
}

// Close releases every connection in both pools, and the state lock when this
// store took one. The lock goes last: it may not be handed on while a
// connection to the database it protects is still open.
func (s *Store) Close() error {
	var rErr, wErr error
	if s.r != nil {
		rErr = s.r.Close()
	}
	if s.w != nil {
		wErr = s.w.Close()
	}
	if s.lock != nil {
		lock := s.lock
		// Forgotten before the result is checked: a released lock must not be
		// released twice, and a failed release leaves nothing this store can
		// retry either.
		s.lock = nil
		if err := lock.Release(); err != nil && wErr == nil && rErr == nil {
			return err
		}
	}
	if wErr != nil {
		return wErr
	}
	return rErr
}

// readerPoolSize keeps a floor of 4 so a single-core machine still serves reads
// while the writer holds its connection.
func readerPoolSize() int {
	if n := runtime.NumCPU(); n > 4 {
		return n
	}
	return 4
}

// syncMode maps the config value to the PRAGMA argument. An unknown value is an
// error: silently falling back would be exactly the quiet durability
// degradation the startup verification exists to prevent.
func syncMode(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "normal":
		return "NORMAL", nil
	case "full":
		return "FULL", nil
	default:
		return "", fmt.Errorf("synchronous %q is not supported, use \"normal\" or \"full\"", value)
	}
}

// syncReadback is the numeric value PRAGMA synchronous reports for a mode.
func syncReadback(sync string) string {
	if sync == "FULL" {
		return "2"
	}
	return "1"
}

// dsn renders a connection string. first is the driver parameter that precedes
// the pragmas: _txlock=immediate makes every writer transaction BEGIN
// IMMEDIATE, which is what avoids the lock upgrade that busy_timeout cannot
// retry; mode=ro opens the reader read-only.
func dsn(path, first string, specs []pragmaSpec) string {
	params := make([]string, 0, len(specs)+1)
	params = append(params, first)
	for _, spec := range specs {
		if spec.arg == "" {
			continue
		}
		params = append(params, "_pragma="+spec.name+"("+spec.arg+")")
	}
	return "file:" + path + "?" + strings.Join(params, "&")
}
