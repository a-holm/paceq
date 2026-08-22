package spec

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestParseDurationReadsWhatAJobFileWrites is the whole accepted grammar in one
// table. The same function reads every duration field, so a case here holds for
// timeout, retry.initial, retry.max_delay and sensor interval alike, which is
// what stops the two ever disagreeing about what "1h30m" means.
func TestParseDurationReadsWhatAJobFileWrites(t *testing.T) {
	cases := map[string]time.Duration{
		"1ms":       time.Millisecond,
		"500ms":     500 * time.Millisecond,
		"1s":        time.Second,
		"30s":       30 * time.Second,
		"60s":       time.Minute,
		"1m":        time.Minute,
		"45m":       45 * time.Minute,
		"1h":        time.Hour,
		"24h":       24 * time.Hour,
		"1h30m":     90 * time.Minute,
		"1h30m15s":  time.Hour + 30*time.Minute + 15*time.Second,
		"0.5h":      30 * time.Minute,
		"1.5s":      1500 * time.Millisecond,
		"0.001s":    time.Millisecond,
		"2m30s":     150 * time.Second,
		"1000ms":    time.Second,
		"90s":       90 * time.Second,
		"0h1m":      time.Minute,
		"00030s":    30 * time.Second,
		"1h0m0s1ms": time.Hour + time.Millisecond,
	}
	for text, want := range cases {
		t.Run(text, func(t *testing.T) {
			got, err := ParseDuration(text)
			if err != nil {
				t.Fatalf("ParseDuration(%q): %v", text, err)
			}
			if got != want {
				t.Errorf("ParseDuration(%q) = %v, want %v", text, got, want)
			}
		})
	}
}

// TestTheSameDurationSpelledTwoWaysIsOneValue is what the hash rests on. If
// 60s and 1m produced different values, two files that mean the same thing
// would record different job versions.
func TestTheSameDurationSpelledTwoWaysIsOneValue(t *testing.T) {
	groups := [][]string{
		{"60s", "1m", "60000ms", "0.5m30s"},
		{"1h", "60m", "3600s", "0.5h30m"},
		{"90m", "1h30m", "5400s"},
	}
	for _, group := range groups {
		t.Run(group[0], func(t *testing.T) {
			want, err := ParseDuration(group[0])
			if err != nil {
				t.Fatalf("ParseDuration(%q): %v", group[0], err)
			}
			for _, text := range group[1:] {
				got, err := ParseDuration(text)
				if err != nil {
					t.Fatalf("ParseDuration(%q): %v", text, err)
				}
				if got != want {
					t.Errorf("%q is %v and %q is %v", group[0], want, text, got)
				}
			}
		})
	}
}

// TestParseDurationRefusesWhatItCannotStore. Every refusal names the value and
// says what to write instead, because a duration is the field a person gets
// wrong first.
func TestParseDurationRefusesWhatItCannotStore(t *testing.T) {
	cases := map[string]string{
		"":              "empty",
		"45":            "no unit",
		"45x":           "not one of",
		"3 days":        "no spaces",
		"3d":            "not a unit paceq has",
		"2w":            "not a unit paceq has",
		"-5m":           "no sign",
		"+5m":           "no sign",
		" 30s":          "leading or trailing spaces",
		"30s ":          "leading or trailing spaces",
		"0s":            "zero",
		"0ms":           "zero",
		"1us":           "not one of",
		"1ns":           "not one of",
		"1µs":           "not one of",
		"0.5ms":         "finer than a millisecond",
		"1.0005s":       "finer than a millisecond",
		"m":             "expected a number",
		"1.":            "no digits after it",
		"1.5":           "no unit",
		"1h2":           "no unit",
		"99999999h":     "longer than paceq counts",
		"1.1234567890s": "past nanoseconds",
	}
	for text, want := range cases {
		t.Run(text, func(t *testing.T) {
			got, err := ParseDuration(text)
			if err == nil {
				t.Fatalf("ParseDuration(%q) = %v, want a refusal", text, got)
			}
			if !errors.Is(err, ErrDuration) {
				t.Errorf("ParseDuration(%q) returned %v, which does not wrap ErrDuration", text, err)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ParseDuration(%q) said %q, want it to mention %q", text, err, want)
			}
		})
	}
}

// TestEveryAcceptedDurationIsAWholeNumberOfMilliseconds keeps the IR honest:
// the canonical form stores milliseconds, so a value it cannot store is a value
// whose hash would depend on how the rounding went.
func TestEveryAcceptedDurationIsAWholeNumberOfMilliseconds(t *testing.T) {
	for _, text := range []string{"1ms", "1s", "1m", "1h", "1.5s", "0.5h", "1h30m15s", "999ms"} {
		value, err := ParseDuration(text)
		if err != nil {
			t.Fatalf("ParseDuration(%q): %v", text, err)
		}
		if value%time.Millisecond != 0 {
			t.Errorf("ParseDuration(%q) = %v, which is not a whole number of milliseconds", text, value)
		}
	}
}

func TestFormatDurationReadsBackAsItself(t *testing.T) {
	for _, text := range []string{"1ms", "30s", "45m", "1h", "24h", "1h30m", "1h30m15s", "1h30m15s500ms"} {
		value, err := ParseDuration(text)
		if err != nil {
			t.Fatalf("ParseDuration(%q): %v", text, err)
		}
		formatted := FormatDuration(value)
		again, err := ParseDuration(formatted)
		if err != nil {
			t.Fatalf("FormatDuration(%v) produced %q, which does not parse: %v", value, formatted, err)
		}
		if again != value {
			t.Errorf("%q formatted to %q and read back as %v", text, formatted, again)
		}
	}
}
