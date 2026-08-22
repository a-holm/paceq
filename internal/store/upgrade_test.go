package store

import (
	"context"
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"
)

// shippedMigrations is the embedded set as a map, so the fixture helpers can
// slice it the way they slice a fixture set. Slicing the real files is what
// makes the upgrade matrix run against the migrations that ship rather than
// against a snapshot somebody has to remember to refresh.
func shippedMigrations(t *testing.T) fstest.MapFS {
	t.Helper()

	sub, err := fs.Sub(migrationFS, migrationDir)
	if err != nil {
		t.Fatalf("open the embedded migrations: %v", err)
	}
	names, err := fs.Glob(sub, "*.sql")
	if err != nil {
		t.Fatalf("list the embedded migrations: %v", err)
	}
	out := make(fstest.MapFS, len(names))
	for _, name := range names {
		body, err := fs.ReadFile(sub, name)
		if err != nil {
			t.Fatalf("read the embedded migration %s: %v", name, err)
		}
		out[name] = &fstest.MapFile{Data: body}
	}
	if len(out) == 0 {
		t.Fatal("the embedded set is empty: the matrix below would prove nothing")
	}
	return out
}

// TestUpgradeFromEveryShippedVersion is the upgrade matrix against the real
// migrations. A database left at any version this project has ever written has
// to reach the same schema as one migrated from empty, or what an upgrade means
// depends on when the database was created.
func TestUpgradeFromEveryShippedVersion(t *testing.T) {
	ctx := context.Background()
	shipped := shippedMigrations(t)
	newest := len(shipped)

	want := schemaDump(t, migratedStore(t))

	for version := 1; version < newest; version++ {
		t.Run(fmt.Sprintf("from %04d", version), func(t *testing.T) {
			s := testStore(t)
			if err := s.migrateFS(ctx, upTo(shipped, version)); err != nil {
				t.Fatalf("migrate to %04d: %v", version, err)
			}
			if got := userVersion(t, s); got != version {
				t.Fatalf("user_version is %d, want %d", got, version)
			}

			if err := s.Migrate(ctx); err != nil {
				t.Fatalf("upgrade from %04d: %v", version, err)
			}
			if got := userVersion(t, s); got != newest {
				t.Errorf("user_version is %d after the upgrade, want %d", got, newest)
			}
			if got := schemaDump(t, s); got != want {
				t.Errorf("the schema after upgrading from %04d differs from a fresh migrate:\ngot\n%s\nwant\n%s",
					version, got, want)
			}
		})
	}
}

// TestUpgradeFromTheBaseSchemaKeepsItsRows is the other half: the schema
// matching is worth nothing if the upgrade emptied the tables on the way. Every
// table the base schema created is seeded and read back afterwards.
func TestUpgradeFromTheBaseSchemaKeepsItsRows(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	if err := s.migrateFS(ctx, upTo(shippedMigrations(t), 1)); err != nil {
		t.Fatalf("migrate to 0001: %v", err)
	}
	seed := []string{
		`INSERT INTO meta (key, value) VALUES ('created_at', '1000')`,
		`INSERT INTO daemon_sessions (id, version, boot_id, pid, started_at, last_seen_at)
			VALUES ('01J0SESSION', '0.1.0', 'b00t', 4242, 1000, 1500)`,
		`INSERT INTO leases (name, holder, epoch, acquired_at, expires_at)
			VALUES ('scheduler', '01J0NODE', 7, 1000, 16000)`,
		`INSERT INTO outages (from_ts, to_ts, detected_at, kind, prev_session, missed_ticks)
			VALUES (1000, 2000, 2100, 'crash', '01J0SESSION', 3)`,
	}
	for _, stmt := range seed {
		if _, err := s.w.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed the base schema: %v\n%s", err, stmt)
		}
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("upgrade to the newest schema: %v", err)
	}

	rows := map[string]string{
		"meta":            `SELECT count(*) FROM meta WHERE key = 'created_at' AND value = '1000'`,
		"daemon_sessions": `SELECT count(*) FROM daemon_sessions WHERE id = '01J0SESSION' AND boot_id = 'b00t' AND last_seen_at = 1500`,
		"leases":          `SELECT count(*) FROM leases WHERE name = 'scheduler' AND epoch = 7`,
		"outages":         `SELECT count(*) FROM outages WHERE prev_session = '01J0SESSION' AND missed_ticks = 3`,
	}
	for table, query := range rows {
		var count int
		if err := s.w.QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("read %s back: %v", table, err)
		}
		if count != 1 {
			t.Errorf("%s holds %d matching rows after the upgrade, want 1", table, count)
		}
	}
}
