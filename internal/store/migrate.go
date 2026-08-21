package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

// migrationFS carries the SQL files into the binary. The whole directory is
// embedded rather than the migrations/*.sql pattern: the pattern is a compile
// error while no .sql file exists, and the first one arrives with the initial
// schema. The loader globs *.sql, so the directory's README is ignored.
//
//go:embed migrations
var migrationFS embed.FS

// migrationDir is the path migrationFS is rooted at.
const migrationDir = "migrations"

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
	if err := s.ensureMigrationTable(ctx); err != nil {
		return err
	}
	applied, err := s.appliedMigrations(ctx)
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

// ensureMigrationTable creates the ledger the engine keeps its own state in. It
// is not itself a migration: a migration cannot record that it ran before the
// table that records it exists.
func (s *Store) ensureMigrationTable(ctx context.Context) error {
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
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			"INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
			m.Version, m.Name, m.Checksum, s.clk.Now().UnixMilli())
		if err != nil {
			return fmt.Errorf("record migration: %w", err)
		}
		return setUserVersion(ctx, tx, m.Version)
	})
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
