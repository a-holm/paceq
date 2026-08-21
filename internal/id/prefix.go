package id

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Range is the half-open interval of ids that share a prefix: Lower is the
// smallest id with the prefix, Upper the smallest string above every id with it.
// A lookup is one range scan, `id >= Lower AND id < Upper`, which an index on a
// sorted text column answers without touching a row that cannot match.
//
// Upper is not always an id. When the prefix has no successor in the alphabet,
// every id with it ends at the largest id there is, and Upper is one character
// longer so that it still sorts above.
type Range struct {
	Lower string
	Upper string
}

// NormalizePrefix turns what a user typed into something searchable: space
// trimmed, upper case, and every character checked against the alphabet. Rejects
// the empty prefix, because it matches everything and is never what was meant.
func NormalizePrefix(s string) (string, error) {
	typed := strings.TrimSpace(s)
	if typed == "" {
		return "", fmt.Errorf("empty prefix is %w: give at least one character", ErrInvalid)
	}

	// Upper casing byte by byte, rather than with strings.ToUpper, keeps the
	// offsets into the normalised prefix and into what the user typed the same,
	// so the error can quote the character back the way they wrote it. The
	// position reported is a byte offset. It equals the character position only
	// because the alphabet is single byte ASCII, and the first multi-byte
	// character is where the loop stops anyway.
	prefix := []byte(typed)
	for i, c := range prefix {
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if strings.IndexByte(Alphabet, c) < 0 {
			_, size := utf8.DecodeRuneInString(typed[i:])
			return "", fmt.Errorf("%q is %w: character %q at position %d is not in the alphabet %s",
				s, ErrInvalid, typed[i:i+size], i, Alphabet)
		}
		prefix[i] = c
	}
	if len(prefix) > Length {
		return "", fmt.Errorf("%q is %w: an id is %d characters, this is %d",
			s, ErrInvalid, Length, len(prefix))
	}
	return string(prefix), nil
}

// PrefixRange is the range scan for a prefix. The prefix is normalised first, so
// the caller can hand over raw user input.
func PrefixRange(prefix string) (Range, error) {
	normalized, err := NormalizePrefix(prefix)
	if err != nil {
		return Range{}, err
	}

	pad := strings.Repeat(firstLetter, Length-len(normalized))
	rng := Range{Lower: normalized + pad}

	next, ok := successor(normalized)
	if !ok {
		// The prefix is nothing but the last letter of the alphabet, so every id
		// with it ends at the largest id there is. One character more than an id
		// sorts above all of them.
		rng.Upper = strings.Repeat(lastLetter, Length) + firstLetter
		return rng, nil
	}
	rng.Upper = next + pad
	return rng, nil
}

var (
	firstLetter = Alphabet[:1]
	lastLetter  = Alphabet[len(Alphabet)-1:]
)

// successor is the next prefix of the same length in alphabet order, carrying
// from the right. It reports false when the carry runs off the left end, which
// means no prefix of that length sorts above this one.
func successor(prefix string) (string, bool) {
	next := []byte(prefix)
	for i := len(next) - 1; i >= 0; i-- {
		pos := strings.IndexByte(Alphabet, next[i])
		if pos < len(Alphabet)-1 {
			next[i] = Alphabet[pos+1]
			return string(next), true
		}
		next[i] = Alphabet[0]
	}
	return "", false
}
