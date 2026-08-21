// Package store owns the SQLite database: connection pools, PRAGMAs, migrations and every SQL query in the project.
//
// # The write model
//
// One file, two pools. The writer pool has exactly one connection, so
// database/sql is the write queue: it serialises writers, applies backpressure
// and honours context deadlines, and no SQLITE_BUSY can come from paceq's own
// writers because there are never two. Every writer transaction begins
// IMMEDIATE, which avoids the lock upgrade that busy_timeout cannot retry. The
// reader pool opens read-only with query_only, so the engine rather than
// convention enforces that reads cannot write.
//
// The payoff is that read, compute in Go, then write is safe inside one
// transaction without optimistic locking. Nobody else can have written in
// between.
//
// # Five rules
//
// Breaking one of these is a review stopper.
//
//  1. No process execution, file I/O or network I/O inside a write
//     transaction, ever. The single write connection is held for the whole
//     callback, and lock hold time is the real scarcity in this system.
//  2. All mutation goes through methods on *Store. The writer handle is
//     private, and an architecture test fails the build if a database handle
//     reaches the exported API.
//  3. Admission control is read, compute, write inside one IMMEDIATE
//     transaction, not a clever atomic statement.
//  4. RETURNING rows are consumed in full before the transaction runs anything
//     else. The writer pool has one connection to lose.
//  5. The database never runs on a network or FUSE filesystem. SQLite file
//     locking is undefined there and the corruption shows up weeks later.
//
// # Schema conventions
//
// These hold for every table in the schema, and a new table that breaks one is
// a review stopper. Retrofitting any of them means rebuilding every table.
//
//  1. Every table is STRICT. A column declared INTEGER holds integers, and a
//     string that looks like a number is an error rather than a silent
//     conversion. This needs SQLite 3.37, which the driver ships.
//  2. Every time column is INTEGER unix milliseconds UTC. Sortable, indexable,
//     no parsing, no timezone ambiguity. A timezone is stored separately as an
//     IANA name, and only where a time has to be interpreted rather than
//     ordered.
//  3. Every status is TEXT with a CHECK listing the values it may take. The
//     premise of this product is that reading the tables with the sqlite3 shell
//     explains what happened, which integer codes do not, and the CHECK mirrors
//     the state machine in internal/model so both ends enforce it.
//  4. Structured values are canonical JSON in a TEXT column: object keys
//     sorted, no insignificant whitespace. Canonical form is what makes a hash
//     of the text stable and two rows comparable.
//
// The schema a migrated database ends up with is checked in as
// schema.golden.sql, and TestGoldenSchema fails on any change to it. Reviewing
// a schema change means reading that diff.
//
// auto_vacuum is INCREMENTAL, set by Open while the database still has no
// schema. It is the one setting here that cannot be changed afterwards without
// a full VACUUM holding an exclusive lock, so it is decided at creation.
// Databases created before paceq set it keep NONE, and the doctor check reports
// them.
//
// # The state directory
//
// A state directory holds one lock file and one database, and one process owns
// both. OpenState takes an exclusive flock on the lock file before the database
// is opened for writing, so a second paceq is refused before it touches a page
// of a file somebody else owns, and it is told which process to stop. The lock
// lives in the kernel: it is released when the process dies, however it dies,
// so there is no stale lock to clean up and the lock file is kept between runs.
//
// The lock covers a state directory, not a database file. Two processes pointed
// at the same database through different state directories both start, which
// the role leases in M2-02 are what make safe.
//
// StartSession records who is running, from when, and on which boot. A session
// row still open at the next start belongs to a run that never got to say
// goodbye, and is marked crashed. The boot id, read from
// /proc/sys/kernel/random/boot_id, is the strongest evidence in the system: a
// changed one means the machine restarted, so no process paceq started can have
// survived. Platforms without it degrade to lease expiry, which is slower and
// still correct.
//
// # Migrations
//
// Migrate applies the SQL files embedded from the migrations directory, in
// version order, on the write connection. Forward only, one migration per
// transaction, sha256 pinned per applied file, and PRAGMA user_version as the
// fence that stops an old binary from writing to a newer database. The rules a
// migration file has to follow are in migrations/README.md.
//
// PRAGMA values are read back from both pools at startup. A mismatch fails
// Open, naming the setting. Misconfigured durability is a startup error, never
// a warning and never a quiet degradation.
//
// Rules 1 and 4 cannot be checked mechanically and are enforced in review.
// Rules 2, 3 and 5 are covered by tests in this package and in internal/arch.
package store
