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
// PRAGMA values are read back from both pools at startup. A mismatch fails
// Open, naming the setting. Misconfigured durability is a startup error, never
// a warning and never a quiet degradation.
//
// Rules 1 and 4 cannot be checked mechanically and are enforced in review.
// Rules 2, 3 and 5 are covered by tests in this package and in internal/arch.
package store
