// Package badhandle is a fixture for the exported handle guard. It lives under
// testdata, so the module build never links it and nothing outside these tests
// can import its values; the guard parses the files directly. Every laundering
// shape appears once, and the package compiles, because a shape the compiler
// rejects is not a laundering shape.
//
// The handles live in this file, next to the database/sql import. The exported
// declarations that hand them out live in launder.go, which imports nothing at
// all, so a check that reads one file at a time sees a handle in neither.
package badhandle

import "database/sql"

// Store stands in for internal/store.Store: it owns the handles and keeps them
// in unexported fields.
type Store struct{ w *sql.DB }

// Direct is the shape the guard caught before package-local names were resolved.
// It stays in the fixture so a rewrite cannot trade one shape for the other.
func (s *Store) Direct() *sql.DB { return s.w }

// deepHandle is the far end of an alias chain written back to front and split
// across both files: it resolves through chainHandle in launder.go, which
// resolves through aliasHandle below. Nothing about it is readable until both
// links are known, so it takes three passes over the package to resolve.
type deepHandle = chainHandle

// aliasHandle is the plain laundering alias: another name for a handle.
type aliasHandle = *sql.DB

// embedHandle promotes every exported method of *sql.DB into itself.
type embedHandle struct{ *sql.DB }

// fieldHandle keeps the handle in a field a caller outside the package can read.
type fieldHandle struct{ DB *sql.Conn }

// callbackHandle hands the handle to a callback instead of returning it.
type callbackHandle = func(*sql.Tx) error

// listHandle carries handles inside a slice.
type listHandle []*sql.Stmt

// ifaceHandle exposes the handle through a method a caller can call.
type ifaceHandle interface {
	Begin() (*sql.Tx, error)
}

// box is an ordinary generic container. It carries no handle by itself.
type box[T any] struct{ V T }

// genericHandle is box instantiated with a handle.
type genericHandle = box[*sql.DB]

// methodHandle hides the handle in an unexported field and hands it back through
// its method set.
type methodHandle struct{ db *sql.DB }

// Unwrap is what makes methodHandle laundering: the field is private, the method
// is not.
func (m methodHandle) Unwrap() *sql.DB { return m.db }

// promotedHandle embeds a package-local type that embeds a handle, so the
// handle's methods are promoted twice.
type promotedHandle struct{ embedHandle }

// Direct2 is the same handle behind a variable whose type is inferred from the
// literal rather than written out.
var Direct2 = func(s *Store) *sql.DB { return s.w }

// Pair puts two inferred values in one declaration. Only the second one carries
// a handle, so only the second one may be named in the failure.
var Harmless, Pair = func() error { return nil }, func(s *Store) *sql.DB { return s.w }
