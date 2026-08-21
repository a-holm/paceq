// Package goodhandle is the other half of the exported handle guard fixture. It
// lives under testdata, so the module build never links it; the guard parses it
// directly. Every declaration here either holds a database handle the
// way internal/store does, out of reach of any caller outside the package, or
// carries a name that only looks like a handle. The guard must stay quiet about
// all of them.
package goodhandle

import "database/sql"

// Store keeps both handles in unexported fields, which is the shape the guard
// exists to protect rather than to flag.
type Store struct {
	w *sql.DB
	r *sql.DB
}

// handle resolves to a string. A guard that read the name instead of the right
// hand side would flag the method below.
type handle = string

// Name returns the harmless alias.
func (s *Store) Name() handle { return "" }

// ids is an unexported alias of an allowed type.
type ids = []string

// IDs returns the harmless alias in a slice.
func (s *Store) IDs() ids { return nil }

// hidden holds a handle in an unexported field and hands it back only through an
// unexported method, so nothing outside the package can reach it.
type hidden struct{ db *sql.DB }

func (h hidden) unwrap() *sql.DB { return h.db }

// Hidden hands out a value whose handle stays private.
func (s *Store) Hidden() hidden { return hidden{db: s.w} }

// keeper holds a laundering alias in an unexported field. Declaring the alias is
// not the offence; handing it out is, and nothing here does.
type keeper struct{ h dbAlias }

type dbAlias = *sql.DB

// Keeper hands out the wrapper, not the handle.
func (s *Store) Keeper() keeper { return keeper{h: s.w} }

// reader mirrors the read side of internal/store: exported methods, no handle
// among them.
type reader interface {
	QueryContext(query string, args ...any) (*sql.Rows, error)
	QueryRowContext(query string, args ...any) *sql.Row
}

// Reader hands out the narrow read interface.
func (s *Store) Reader() reader { return nil }

// Row names database/sql in the exported surface without naming a handle. A
// result set is not a handle: it cannot start a transaction or write anything.
func (s *Store) Row() *sql.Row { return nil }

// box is a generic container instantiated with an allowed type.
type box[T any] struct{ V T }

type names = box[string]

// Names returns the harmless instantiation.
func (s *Store) Names() names { return box[string]{} }

// writer is unexported, so the handle in its signature never leaves the package.
func (s *Store) writer() *sql.DB { return s.w }

// Inferred is a variable whose literal type names the harmless alias.
var Inferred = func() handle { return "" }

// Opener holds a handle inside the literal's body, which is ordinary code inside
// the package and hands nothing out.
var Opener = func() error {
	var db *sql.DB
	_ = db
	return nil
}
