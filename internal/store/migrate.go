package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// migrationFS carries the SQL files into the binary. The whole directory is
// embedded rather than the migrations/*.sql pattern: the pattern is a compile
// error while no .sql file exists, and the first one arrives with the initial
// schema. The loader globs *.sql, so the directory's README is ignored.
//
//go:embed migrations
var migrationFS embed.FS

// beforeCommit is the fault point the crash test kills the process at: between
// a migration's DDL and its commit. Nothing sets it outside tests, and it is
// the forerunner of the fault injection package the reliability work brings.
var beforeCommit func()

// migrationDir is the path migrationFS is rooted at.
const migrationDir = "migrations"

const (
	// rebuildDirective marks a migration as a table rebuild, and
	// rebuildDirectiveLines is how far into the file it is read.
	rebuildDirective      = "-- +paceq rebuild"
	rebuildDirectiveLines = 5

	// migrationLockTTL is how long a lock row stays valid. A process killed
	// while migrating blocks the next start for at most this long, and a
	// migration that runs longer than this loses the lock without losing
	// correctness, because applying re-checks the ledger inside its own
	// transaction.
	migrationLockTTL = 5 * time.Minute

	// migrationLockPoll is how often a waiting process retries.
	migrationLockPoll = 50 * time.Millisecond
)

// migrationLockWait is how long a start waits for another process to finish
// migrating before it gives up and says so. Tests shorten it.
var migrationLockWait = 30 * time.Second

// fileRE is the only accepted migration file name: a four digit version, an
// underscore, a lower case name. Anything else fails loading rather than being
// skipped, because a skipped file is a schema that differs between machines.
var fileRE = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// migration is one file from the embedded set.
type migration struct {
	Version  int
	Name     string
	File     string
	SQL      string
	Checksum string
	// Rebuild marks a migration that rebuilds a table and therefore has to run
	// with foreign keys off, which PRAGMA cannot do inside a transaction.
	Rebuild bool
}

// appliedMigration is one row of schema_migrations.
type appliedMigration struct {
	Version   int
	Name      string
	Checksum  string
	AppliedAt int64
}

// Migrate applies every migration the database is missing. It is idempotent and
// safe to call on every startup.
func (s *Store) Migrate(ctx context.Context) error {
	sub, err := fs.Sub(migrationFS, migrationDir)
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}
	return s.migrateFS(ctx, sub)
}

// migrateFS is Migrate against an arbitrary file system. Production passes the
// embedded directory; tests pass a fixture set, which is the only way to
// exercise an edited or missing migration without editing the binary's own.
func (s *Store) migrateFS(ctx context.Context, fsys fs.FS) error {
	all, err := loadMigrations(fsys)
	if err != nil {
		return err
	}
	// The version fence runs before anything is written, so an old binary that
	// meets a newer database leaves that database exactly as it found it.
	if err := s.checkUserVersion(ctx, maxVersion(all)); err != nil {
		return err
	}
	if err := s.ensureMigrationTables(ctx); err != nil {
		return err
	}
	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		return err
	}
	if err := checkApplied(all, applied); err != nil {
		return err
	}
	if len(pending(all, applied)) == 0 {
		// The common start: nothing to do, so nothing is written and no lock is
		// taken. This is what makes Migrate cheap enough to call every time.
		return nil
	}

	if err := s.lockMigrations(ctx); err != nil {
		return err
	}
	defer func() { _ = s.unlockMigrations(ctx) }()

	// The set is read again under the lock: the process that held it before us
	// may have applied some or all of what we saw as pending.
	applied, err = s.appliedMigrations(ctx)
	if err != nil {
		return err
	}
	if err := checkApplied(all, applied); err != nil {
		return err
	}
	for _, m := range pending(all, applied) {
		if err := s.applyOne(ctx, m); err != nil {
			return fmt.Errorf("migration %s: %w", m.File, err)
		}
	}
	return nil
}

// lockMigrations takes the single migrator lock, waiting for whoever holds it.
// Two paceq processes starting together is ordinary, so waiting is the normal
// path and the error is for the process that never finishes.
//
// The lock is a convenience, not the correctness argument. Applying a migration
// re-reads the ledger inside its own IMMEDIATE transaction, which is what makes
// applying the same migration twice impossible even with the lock ignored.
func (s *Store) lockMigrations(ctx context.Context) error {
	deadline := s.clk.Mark()
	for {
		holder, err := s.tryLockMigrations(ctx)
		if err != nil || holder == "" {
			return err
		}
		if s.clk.Since(deadline) >= migrationLockWait {
			return fmt.Errorf("another paceq process (%s) is migrating this database and has not "+
				"finished within %s: wait for it, or stop it and start again", holder, migrationLockWait)
		}
		timer := s.clk.NewTimer(migrationLockPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// tryLockMigrations takes the lock in one statement. It returns the current
// holder when someone else owns a lock that has not expired yet.
func (s *Store) tryLockMigrations(ctx context.Context) (string, error) {
	me := lockHolder()
	var holder string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		now := s.clk.Now().UnixMilli()
		result, err := tx.ExecContext(ctx, `INSERT INTO schema_migration_lock
	(id, holder, acquired_at, expires_at) VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET holder = ?, acquired_at = ?, expires_at = ?
WHERE schema_migration_lock.holder = ? OR schema_migration_lock.expires_at <= ?`,
			me, now, now+migrationLockTTL.Milliseconds(),
			me, now, now+migrationLockTTL.Milliseconds(),
			me, now)
		if err != nil {
			return fmt.Errorf("take the migration lock: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("take the migration lock: %w", err)
		}
		if changed > 0 {
			return nil
		}
		if err := tx.QueryRowContext(ctx,
			"SELECT holder FROM schema_migration_lock WHERE id = 1").Scan(&holder); err != nil {
			return fmt.Errorf("read the migration lock holder: %w", err)
		}
		if !holderAlive(holder) {
			// The holder died with the lock still valid. Without this the next
			// start would wait out the whole TTL after every crash.
			if _, err := tx.ExecContext(ctx, `UPDATE schema_migration_lock
SET holder = ?, acquired_at = ?, expires_at = ?
WHERE id = 1 AND holder = ?`, me, now, now+migrationLockTTL.Milliseconds(), holder); err != nil {
				return fmt.Errorf("take over the migration lock: %w", err)
			}
			holder = ""
		}
		return nil
	})
	return holder, err
}

// unlockMigrations drops our own lock row so the next process starts at once.
// A row left behind by a crash expires instead.
func (s *Store) unlockMigrations(ctx context.Context) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM schema_migration_lock WHERE id = 1 AND holder = ?", lockHolder())
		if err != nil {
			return fmt.Errorf("release the migration lock: %w", err)
		}
		return nil
	})
}

// holderAlive reports whether a lock holder still exists. Only a holder on this
// host can be answered: paceq owns one database file on one machine, so a lock
// row naming another host is left to expire on its own.
func holderAlive(holder string) bool {
	host, pid, found := strings.Cut(holder, "/")
	if !found {
		return true
	}
	me, err := os.Hostname()
	if err != nil || host != me {
		return true
	}
	number, err := strconv.Atoi(pid)
	if err != nil {
		return true
	}
	return processAlive(number)
}

// lockHolder names this process in the lock row. It is what holderAlive parses,
// so the host/pid shape is load bearing.
func lockHolder() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	return host + "/" + strconv.Itoa(os.Getpid())
}

// maxVersion is the highest version this binary carries. An empty set is
// version 0, which is also what an untouched database reports.
func maxVersion(all []migration) int {
	if len(all) == 0 {
		return 0
	}
	return all[len(all)-1].Version
}

// checkUserVersion refuses a database whose schema is newer than this build.
// Writing to it would mean writing rows a later schema defines differently, so
// the process stops instead, naming both versions.
func (s *Store) checkUserVersion(ctx context.Context, maxKnown int) error {
	var current int
	if err := s.w.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	if current > maxKnown {
		return fmt.Errorf("database schema version is %d, this paceq build knows at most %d: "+
			"upgrade paceq, or restore the backup taken before the upgrade", current, maxKnown)
	}
	return nil
}

// checkApplied compares the ledger against the files this binary carries. Both
// findings are refusals: an applied migration that is missing means the binary
// is older than the database, and a changed checksum means a file was edited
// after it ran, so two databases at the same version no longer match.
func checkApplied(all []migration, applied []appliedMigration) error {
	byVersion := make(map[int]migration, len(all))
	for _, m := range all {
		byVersion[m.Version] = m
	}

	highestApplied := 0
	for _, a := range applied {
		if a.Version > highestApplied {
			highestApplied = a.Version
		}
		m, ok := byVersion[a.Version]
		if !ok {
			return fmt.Errorf("migration %04d_%s is applied in the database but is not in this "+
				"paceq build: the database is newer than the binary", a.Version, a.Name)
		}
		if m.Checksum != a.Checksum {
			return fmt.Errorf("migration %s changed after it was applied (file %s, database %s): "+
				"migration files are immutable, write a new migration instead of editing an old one",
				m.File, shortSum(m.Checksum), shortSum(a.Checksum))
		}
	}

	for _, m := range pending(all, applied) {
		if m.Version < highestApplied {
			return fmt.Errorf("migration %s is not applied while %04d is: a migration may not be "+
				"numbered below one the database has already run", m.File, highestApplied)
		}
	}
	return nil
}

// pending is the migrations the ledger does not hold, in version order.
func pending(all []migration, applied []appliedMigration) []migration {
	done := make(map[int]bool, len(applied))
	for _, a := range applied {
		done[a.Version] = true
	}

	var out []migration
	for _, m := range all {
		if !done[m.Version] {
			out = append(out, m)
		}
	}
	return out
}

// shortSum is the checksum prefix used in messages. Twelve hex digits is enough
// to tell two files apart and short enough to read in a terminal.
func shortSum(sum string) string {
	if len(sum) <= 12 {
		return sum
	}
	return sum[:12]
}

// loadMigrations reads and orders the migration set. Ordering is numeric on the
// version prefix, never lexicographic on the file name.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}

	all := make([]migration, 0, len(names))
	for _, name := range names {
		match := fileRE.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("migration file %q does not match NNNN_name.sql", name)
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("migration file %q: %w", name, err)
		}
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", name, err)
		}
		sum := sha256.Sum256(body)
		all = append(all, migration{
			Version:  version,
			Rebuild:  hasRebuildDirective(string(body)),
			Name:     match[2],
			File:     name,
			SQL:      string(body),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Version < all[j].Version })

	// Versions run from 1 with no gaps and no repeats. A gap is a file lost in
	// a merge, and either shape would make the applied order depend on which
	// files a given checkout happens to have.
	for i, m := range all {
		switch {
		case m.Version == i+1:
		case i > 0 && m.Version == all[i-1].Version:
			return nil, fmt.Errorf("migrations %q and %q claim version %04d: one file per version",
				all[i-1].File, m.File, m.Version)
		default:
			return nil, fmt.Errorf("migration %04d is missing: versions run from 0001 upwards with "+
				"no gaps, and a gap means a file was lost", i+1)
		}
	}
	return all, nil
}

// hasRebuildDirective reports whether the file opts into the table rebuild
// path. The marker has to sit in the first few lines so it is visible in a
// review without scrolling.
func hasRebuildDirective(body string) bool {
	lines := strings.SplitN(body, "\n", rebuildDirectiveLines+1)
	for i, line := range lines {
		if i == rebuildDirectiveLines {
			break
		}
		if strings.TrimSpace(line) == rebuildDirective {
			return true
		}
	}
	return false
}

// ensureMigrationTables creates the two tables the engine keeps its own state
// in. They are not themselves migrations: a migration cannot record that it ran
// before the table that records it exists, and the lock that guards migrating
// has to exist before the first migration runs.
func (s *Store) ensureMigrationTables(ctx context.Context) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
	version    INTEGER PRIMARY KEY,
	name       TEXT    NOT NULL,
	checksum   TEXT    NOT NULL,
	applied_at INTEGER NOT NULL
) STRICT`)
		if err != nil {
			return fmt.Errorf("create schema_migrations: %w", err)
		}
		_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migration_lock (
	id          INTEGER PRIMARY KEY CHECK (id = 1),
	holder      TEXT    NOT NULL,
	acquired_at INTEGER NOT NULL,
	expires_at  INTEGER NOT NULL
) STRICT`)
		if err != nil {
			return fmt.Errorf("create schema_migration_lock: %w", err)
		}
		return nil
	})
}

// appliedMigrations reads the ledger, ordered by version.
func (s *Store) appliedMigrations(ctx context.Context) ([]appliedMigration, error) {
	rows, err := s.w.QueryContext(ctx,
		"SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var applied []appliedMigration
	for rows.Next() {
		var a appliedMigration
		if err := rows.Scan(&a.Version, &a.Name, &a.Checksum, &a.AppliedAt); err != nil {
			return nil, fmt.Errorf("scan schema_migrations row: %w", err)
		}
		applied = append(applied, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	return applied, nil
}

// applyOne runs one migration. The DDL, the ledger row and user_version go into
// the same transaction, so a crash leaves the migration either wholly applied
// or not applied at all. SQLite has transactional DDL; this is the whole reason
// a half applied migration cannot exist.
func (s *Store) applyOne(ctx context.Context, m migration) error {
	if m.Rebuild {
		return s.applyRebuild(ctx, m)
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		already, err := migrationApplied(ctx, tx, m.Version)
		if err != nil || already {
			return err
		}
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			return err
		}
		if err := s.recordMigration(ctx, tx, m); err != nil {
			return err
		}
		if beforeCommit != nil {
			beforeCommit()
		}
		return nil
	})
}

// migrationApplied re-reads the ledger under the write lock. Another process
// may have applied this migration between our read of the ledger and this
// transaction, and IMMEDIATE serialises the two, so this is what makes applying
// a migration twice impossible. Both apply paths go through it, including the
// rebuild path: a rebuild is the migration most likely to run longer than the
// lock's expiry.
func migrationApplied(ctx context.Context, tx *sql.Tx, version int) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM schema_migrations WHERE version = ?", version).Scan(&count); err != nil {
		return false, fmt.Errorf("check whether the migration is applied: %w", err)
	}
	return count > 0, nil
}

// setUserVersion writes the schema version fence. PRAGMA takes no bind
// parameter, so the value is rendered; it is an int parsed from the file name's
// four digit prefix and cannot carry anything else.
func setUserVersion(ctx context.Context, tx *sql.Tx, version int) error {
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version = "+strconv.Itoa(version)); err != nil {
		return fmt.Errorf("set user_version: %w", err)
	}
	return nil
}

// recordMigration writes the ledger row and the version fence. Both belong to
// the same transaction as the DDL: that is what makes a partly applied
// migration impossible.
func (s *Store) recordMigration(ctx context.Context, tx *sql.Tx, m migration) error {
	_, err := tx.ExecContext(ctx,
		"INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
		m.Version, m.Name, m.Checksum, s.clk.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	return setUserVersion(ctx, tx, m.Version)
}

// applyRebuild runs a table rebuild. SQLite's ALTER TABLE only adds, renames
// and drops columns; every other change means creating a new table, copying,
// dropping the old one and renaming, and that sequence needs foreign keys off.
// PRAGMA foreign_keys is a no-op inside a transaction, so the pragma has to sit
// outside it, and foreign_key_check inside it is what replaces the enforcement
// that was turned off.
//
// Everything runs on one connection taken from the writer pool rather than
// through withTx. The pool holds a single connection, so a transaction opened
// while this one is checked out would wait for itself.
func (s *Store) applyRebuild(ctx context.Context, m migration) (err error) {
	conn, err := s.w.Conn(ctx)
	if err != nil {
		return fmt.Errorf("take the write connection: %w", err)
	}
	defer func() {
		// Restoring the pragma matters more than the error that got us here:
		// the writer pool never recycles its connection, so foreign keys would
		// stay off for the life of the process.
		if _, offErr := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); offErr != nil && err == nil {
			err = fmt.Errorf("restore foreign_keys: %w", offErr)
		}
		if closeErr := conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("disable foreign_keys: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin write transaction: %w", err)
	}
	if err := s.rebuildInTx(ctx, tx, m); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit write transaction: %w", err)
	}
	return nil
}

func (s *Store) rebuildInTx(ctx context.Context, tx *sql.Tx, m migration) error {
	already, err := migrationApplied(ctx, tx, m.Version)
	if err != nil || already {
		return err
	}
	if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
		return err
	}
	if err := s.recordMigration(ctx, tx, m); err != nil {
		return err
	}
	return checkForeignKeys(ctx, tx)
}

// checkForeignKeys reports the first violation the rebuild left behind. Any row
// at all means the copy lost a parent, and the caller rolls the migration back.
func checkForeignKeys(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		var table, parent sql.NullString
		var rowid, constraint sql.NullInt64
		if err := rows.Scan(&table, &rowid, &parent, &constraint); err != nil {
			return fmt.Errorf("foreign key check: %w", err)
		}
		return fmt.Errorf("the rebuild leaves a foreign key violation in table %q referencing %q, "+
			"rolling back", table.String, parent.String)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	return nil
}
