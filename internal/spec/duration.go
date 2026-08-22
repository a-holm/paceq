package spec

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// durationUnit is one suffix a job file may write and what it is worth.
type durationUnit struct {
	suffix string
	millis int64
}

// The durations a job file may write. One parser serves every duration field,
// so timeout and retry.initial can never disagree about what "1h30m" means.
//
// Four units, no more. Days and weeks are missing on purpose: a day is not a
// fixed length of time in a zone that observes daylight saving, and a field
// that silently means "24 hours" in March is the kind of thing an operator
// finds out about once a year. The order matters: ms is tried before s, or
// "30ms" would read as 30 minutes followed by a stray s.
var durationUnits = []durationUnit{
	{"ms", 1},
	{"s", 1000},
	{"m", 60 * 1000},
	{"h", 60 * 60 * 1000},
}

// ErrDuration is every way a duration can be unreadable. The message says which
// one; callers only need to know that the text was not a duration.
var ErrDuration = errors.New("not a duration paceq reads")

// maxFractionDigits bounds the fraction of a duration. Nine digits is past
// nanoseconds, so anything longer is not precision.
const maxFractionDigits = 9

// maxDurationMillis is the largest duration that converts to a time.Duration
// without wrapping. A time.Duration counts nanoseconds in an int64, so it runs
// out at about 292 years, which is past every ceiling paceq applies anyway.
const maxDurationMillis = int64(math.MaxInt64) / int64(time.Millisecond)

// ParseDuration reads a duration the way a job file writes one: a run of number
// and unit pairs, such as 30s, 45m, 1h30m or 1500ms. The result is always a
// whole number of milliseconds, which is the unit the IR stores, so a value
// that cannot be one is refused rather than silently rounded.
//
// Negative and zero durations are refused. Every field that takes a duration is
// a timeout or a delay, and neither has a meaning at zero.
func ParseDuration(text string) (time.Duration, error) {
	switch {
	case text == "":
		return 0, fmt.Errorf("an empty value is %w: write a number and a unit, such as 30s", ErrDuration)
	case strings.TrimSpace(text) != text:
		return 0, fmt.Errorf("%q is %w: it has leading or trailing spaces", text, ErrDuration)
	case strings.HasPrefix(text, "-") || strings.HasPrefix(text, "+"):
		return 0, fmt.Errorf("%q is %w: a duration has no sign", text, ErrDuration)
	case strings.ContainsAny(text, " \t"):
		return 0, fmt.Errorf("%q is %w: a duration has no spaces in it, the unit follows the number directly", text, ErrDuration)
	}

	var total int64
	for rest := text; rest != ""; {
		millis, remaining, err := parseDurationPart(text, rest)
		if err != nil {
			return 0, err
		}
		if total, err = addChecked(text, total, millis); err != nil {
			return 0, err
		}
		rest = remaining
	}
	if total == 0 {
		return 0, fmt.Errorf("%q is %w: a timeout or a delay of zero has no meaning", text, ErrDuration)
	}
	return time.Duration(total) * time.Millisecond, nil
}

// parseDurationPart reads one number and unit pair off the front of rest and
// returns what it is worth in milliseconds. whole is the value as written,
// quoted in every message so an error about "1h30x" names what the user typed.
func parseDurationPart(whole, rest string) (millis int64, remaining string, err error) {
	digits, after := takeDigits(rest)
	if digits == "" {
		return 0, "", fmt.Errorf("%q is %w: expected a number at %q", whole, ErrDuration, rest)
	}

	fraction := ""
	if strings.HasPrefix(after, ".") {
		if fraction, after = takeDigits(after[1:]); fraction == "" {
			return 0, "", fmt.Errorf("%q is %w: the decimal point in %q has no digits after it", whole, ErrDuration, rest)
		}
		if len(fraction) > maxFractionDigits {
			return 0, "", fmt.Errorf("%q is %w: %d digits after the decimal point is past nanoseconds", whole, ErrDuration, len(fraction))
		}
	}

	unit, after, err := takeUnit(whole, after)
	if err != nil {
		return 0, "", err
	}

	// The value is computed in scaled integers rather than in floating point:
	// 0.1s in float64 is not a tenth of a second, and a hash taken over the
	// result would depend on how the arithmetic rounded.
	scale := pow10(len(fraction))
	scaled, err := digitsValue(whole, digits+fraction)
	if err != nil {
		return 0, "", err
	}
	product, err := mulChecked(whole, scaled, unit.millis)
	if err != nil {
		return 0, "", err
	}
	if product%scale != 0 {
		return 0, "", fmt.Errorf("%q is %w: it is finer than a millisecond, which is the unit paceq stores", whole, ErrDuration)
	}
	return product / scale, after, nil
}

func takeDigits(s string) (digits, rest string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i], s[i:]
}

func takeUnit(whole, s string) (durationUnit, string, error) {
	for _, candidate := range durationUnits {
		if strings.HasPrefix(s, candidate.suffix) {
			return candidate, s[len(candidate.suffix):], nil
		}
	}
	if s == "" {
		return durationUnit{}, "", fmt.Errorf("%q is %w: the number has no unit, and paceq will not guess between seconds and minutes", whole, ErrDuration)
	}
	name := takeLetters(s)
	if name == "d" || name == "w" {
		return durationUnit{}, "", fmt.Errorf("%q is %w: %q is not a unit paceq has, because a day is not a fixed length of time in a zone with daylight saving", whole, ErrDuration, name)
	}
	return durationUnit{}, "", fmt.Errorf("%q is %w: %q is not one of ms, s, m, h", whole, ErrDuration, name)
}

// takeLetters is what a message quotes back as the unit that was written. It
// falls back to one whole rune, so a value ending in a multi-byte character is
// quoted as that character rather than as half of it.
func takeLetters(s string) string {
	i := 0
	for i < len(s) && ((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z')) {
		i++
	}
	if i > 0 {
		return s[:i]
	}
	_, size := utf8.DecodeRuneInString(s)
	return s[:size]
}

func pow10(n int) int64 {
	value := int64(1)
	for range n {
		value *= 10
	}
	return value
}

// digitsValue reads a run of decimal digits as an integer, refusing one that
// runs past what the arithmetic can hold.
func digitsValue(whole, digits string) (int64, error) {
	value := int64(0)
	for _, c := range []byte(digits) {
		shifted, err := mulChecked(whole, value, 10)
		if err != nil {
			return 0, err
		}
		if value, err = addChecked(whole, shifted, int64(c-'0')); err != nil {
			return 0, err
		}
	}
	return value, nil
}

func mulChecked(whole string, a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	product := a * b
	if product/b != a || product > maxDurationMillis {
		return 0, fmt.Errorf("%q is %w: it is longer than paceq counts", whole, ErrDuration)
	}
	return product, nil
}

func addChecked(whole string, a, b int64) (int64, error) {
	sum := a + b
	if sum < a || sum > maxDurationMillis {
		return 0, fmt.Errorf("%q is %w: it is longer than paceq counts", whole, ErrDuration)
	}
	return sum, nil
}

// FormatDuration writes a duration the way a job file would, so a message that
// quotes a ceiling reads like the field it is about.
func FormatDuration(d time.Duration) string {
	millis := d.Milliseconds()
	if millis <= 0 {
		return "0ms"
	}
	var b strings.Builder
	for _, unit := range []durationUnit{
		{"h", 60 * 60 * 1000},
		{"m", 60 * 1000},
		{"s", 1000},
		{"ms", 1},
	} {
		if count := millis / unit.millis; count > 0 {
			fmt.Fprintf(&b, "%d%s", count, unit.suffix)
			millis -= count * unit.millis
		}
	}
	return b.String()
}
