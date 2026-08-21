// Package id generates time-sortable identifiers that order lexicographically by creation time.
//
// An id is a ULID: 26 characters of Crockford base32, a 48 bit millisecond
// timestamp followed by 80 bits of entropy. Three properties earn it its place.
// ORDER BY id is chronological, so listing runs needs no second index and no
// tie-breaking column. Inserts land at the right hand edge of the B-tree, which
// is the cheapest place for SQLite to put them. And a prefix identifies a run,
// so a user can type the first few characters the way they would with a git
// object, and PrefixRange turns that into one range scan.
//
// Entropy is monotonic within a millisecond, which is what holds the order up
// for ids created in the same millisecond, and it is locked, because ids are
// created from several goroutines at once.
package id
