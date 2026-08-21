package badhandle

// This file imports nothing. Every declaration below either hands a caller
// outside the package a database handle under a package-local name declared in
// alias.go, or is the link of the alias chain that connects the two files.

// chainHandle is the middle link of the alias chain. Its right hand side is
// declared in the other file.
type chainHandle = aliasHandle

// Carrier is an exported struct that carries a handle in an exported field,
// spelled with a package-local name.
type Carrier struct{ H aliasHandle }

func (s *Store) Plain() aliasHandle { return s.w }

func (s *Store) Chain() chainHandle { return s.w }

func (s *Store) Deep() deepHandle { return s.w }

func (s *Store) Embed() embedHandle { return embedHandle{s.w} }

func (s *Store) Field() fieldHandle { return fieldHandle{} }

func (s *Store) Callback(fn callbackHandle) error { return fn(nil) }

func (s *Store) List() listHandle { return nil }

func (s *Store) Iface() ifaceHandle { return nil }

func (s *Store) Generic() genericHandle { return genericHandle{V: s.w} }

func (s *Store) Method() methodHandle { return methodHandle{db: s.w} }

func (s *Store) Promoted() promotedHandle { return promotedHandle{embedHandle{s.w}} }

var Laundered = func(s *Store) aliasHandle { return s.w }
