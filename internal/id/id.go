package id

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// Length is the encoded length of an id. A ULID is always 26 Crockford base32
// characters, which is why a prefix search can work on fixed offsets.
const Length = ulid.EncodedSize

// Alphabet is Crockford base32 as ULID uses it: no I, L, O or U, so the
// characters a human is most likely to confuse cannot appear.
const Alphabet = ulid.Encoding

// ErrInvalid wraps every rejection from Parse, so a caller can tell "this is not
// an id" from "no run has that id" without matching on message text.
var ErrInvalid = errors.New("not a valid id")

// entropy is monotonic within a millisecond, which is what keeps ids created in
// the same millisecond in creation order, and locked, because
// ulid.MonotonicEntropy is documented as unsafe for concurrent use and every
// scheduler in this project creates runs from several goroutines.
var entropy io.Reader = &ulid.LockedMonotonicReader{
	MonotonicReader: ulid.Monotonic(rand.Reader, 0),
}

// New returns the id for something created at t. Ids sort lexicographically in
// the order their timestamps sort, so ORDER BY id is chronological.
func New(t time.Time) (string, error) {
	u, err := ulid.New(ulid.Timestamp(t.UTC()), entropy)
	if err != nil {
		return "", fmt.Errorf("new id for %s: %w", t.UTC().Format(time.RFC3339Nano), err)
	}
	return u.String(), nil
}

// Parse canonicalises a complete id: surrounding space is dropped and lower case
// input is accepted, because a user copying an id out of a log should not have
// to care. Anything that is not a full id is an error wrapping ErrInvalid.
func Parse(s string) (string, error) {
	canonical := strings.ToUpper(strings.TrimSpace(s))
	u, err := ulid.ParseStrict(canonical)
	if err != nil {
		return "", fmt.Errorf("%q is %w: %s", s, ErrInvalid, err)
	}
	return u.String(), nil
}

// Time is the creation timestamp encoded in an id, in UTC and to the
// millisecond, which is the resolution a ULID carries.
func Time(s string) (time.Time, error) {
	canonical, err := Parse(s)
	if err != nil {
		return time.Time{}, err
	}
	u := ulid.MustParse(canonical)
	return ulid.Time(u.Time()).UTC(), nil
}
