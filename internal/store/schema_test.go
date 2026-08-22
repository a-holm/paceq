package store

import (
	"context"
	"strings"
	"testing"
)

// TestEverySchemaTableIsStrict is the tracer. Every table this project creates
// is STRICT, so a column declared INTEGER cannot quietly hold a string, and the
// check reads the database rather than a list somebody has to remember to
// extend: a new table that forgets STRICT fails here on the day it lands.
//
// SQLite's own tables are the exception it makes for itself. sqlite_sequence
// backs AUTOINCREMENT and is created by the engine, not by a migration.
func TestEverySchemaTableIsStrict(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	rows, err := s.w.QueryContext(ctx,
		`SELECT name, strict FROM pragma_table_list
			WHERE schema = 'main' AND type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("read the table list: %v", err)
	}
	defer func() { _ = rows.Close() }()

	seen := 0
	for rows.Next() {
		var name string
		var strict int
		if err := rows.Scan(&name, &strict); err != nil {
			t.Fatalf("scan the table list: %v", err)
		}
		seen++
		if strict != 1 {
			t.Errorf("table %q is not STRICT", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the table list: %v", err)
	}
	if seen == 0 {
		t.Fatal("the table list is empty: the check would pass on a database with no schema at all")
	}
}

// migratedStore is a store with the shipped migrations applied.
func migratedStore(t *testing.T) *Store {
	t.Helper()

	s := testStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// aSession is a valid daemon_sessions row the constraint cases refer to.
const aSession = `INSERT INTO daemon_sessions (id, version, pid, started_at, last_seen_at)
	VALUES ('01J0SESSION', '0.1.0', 4242, 1000, 1000)`

// TestSchemaAcceptsValidRows keeps the rejection table honest: a schema that
// refused everything would pass it.
func TestSchemaAcceptsValidRows(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	stmts := []string{
		aSession,
		`INSERT INTO meta (key, value) VALUES ('created_at', '1000')`,
		`INSERT INTO leases (name, holder, epoch, acquired_at, expires_at)
			VALUES ('scheduler', '01J0NODE', 1, 1000, 16000)`,
		`INSERT INTO outages (from_ts, to_ts, detected_at, kind, prev_session, missed_ticks)
			VALUES (1000, 2000, 2100, 'crash', '01J0SESSION', 3)`,
		`INSERT INTO daemon_sessions (id, version, boot_id, pid, started_at, last_seen_at, stopped_at, stop_reason)
			VALUES ('01J0SESSION2', '0.1.0', 'b00t', 4243, 2000, 3000, 3000, 'clean')`,
	}
	for _, stmt := range stmts {
		if _, err := s.w.ExecContext(ctx, stmt); err != nil {
			t.Errorf("valid row rejected: %v\n%s", err, stmt)
		}
	}
}

// TestSchemaRejectsInvalidRows is the double enforcement the plan asks for: the
// state machines live in Go, and the database refuses the same values on its
// own. Each case names the constraint it exercises.
func TestSchemaRejectsInvalidRows(t *testing.T) {
	cases := []struct {
		name string
		stmt string
	}{
		{"lease epoch zero", `INSERT INTO leases (name, holder, epoch, acquired_at, expires_at)
			VALUES ('scheduler', '01J0NODE', 0, 1000, 16000)`},
		{"unknown outage kind", `INSERT INTO outages (from_ts, to_ts, detected_at, kind)
			VALUES (1000, 2000, 2100, 'nonsense')`},
		{"outage ends before it starts", `INSERT INTO outages (from_ts, to_ts, detected_at, kind)
			VALUES (2000, 1000, 2100, 'crash')`},
		{"negative missed ticks", `INSERT INTO outages (from_ts, to_ts, detected_at, kind, missed_ticks)
			VALUES (1000, 2000, 2100, 'crash', -1)`},
		{"outage points at a session that never existed", `INSERT INTO outages (from_ts, to_ts, detected_at, kind, prev_session)
			VALUES (1000, 2000, 2100, 'crash', 'no-such-session')`},
		{"unknown stop reason", `INSERT INTO daemon_sessions (id, version, pid, started_at, last_seen_at, stopped_at, stop_reason)
			VALUES ('01J0OTHER', '0.1.0', 1, 1000, 1000, 1000, 'exploded')`},
		{"text in a time column", `INSERT INTO daemon_sessions (id, version, pid, started_at, last_seen_at)
			VALUES ('01J0OTHER', '0.1.0', 1, 'yesterday', 1000)`},
		{"integer in a text column", `INSERT INTO meta (key, value) VALUES (1, x'00')`},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := migratedStore(t)
			if _, err := s.w.ExecContext(ctx, aSession); err != nil {
				t.Fatalf("seed session: %v", err)
			}
			if _, err := s.w.ExecContext(ctx, tc.stmt); err == nil {
				t.Fatalf("the database accepted the row, want a constraint error:\n%s", tc.stmt)
			}
		})
	}
}

// TestOpenSessionLookupUsesAnIndex pins the query the daemon runs on every
// start: it has to be an index lookup, not a scan of every session ever
// recorded.
//
// That the index is partial is pinned by TestGoldenSchema, which holds the
// WHERE clause verbatim. This test passes with either kind of index.
func TestOpenSessionLookupUsesAnIndex(t *testing.T) {
	s := migratedStore(t)

	plan := queryPlan(t, s, `SELECT id FROM daemon_sessions
		WHERE stopped_at IS NULL ORDER BY started_at DESC LIMIT 1`)
	if !strings.Contains(plan, "daemon_sessions_open") {
		t.Fatalf("the open-session lookup does not use daemon_sessions_open:\n%s", plan)
	}
}
