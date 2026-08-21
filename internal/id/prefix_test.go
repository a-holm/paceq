package id_test

import (
	"errors"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/id"
)

func TestNormalizePrefix(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"lower case", "01jq8", "01JQ8", true},
		{"already canonical", "01JQ8", "01JQ8", true},
		{"surrounding space", "  01jq8\n", "01JQ8", true},
		{"a single character", "0", "0", true},
		{"a whole id", "01JQ8" + strings.Repeat("0", 21), "01JQ8" + strings.Repeat("0", 21), true},
		{"empty", "", "", false},
		{"only space", "   ", "", false},
		{"longer than an id", strings.Repeat("0", 27), "", false},
		{"letter I", "01IQ8", "", false},
		{"letter L", "01LQ8", "", false},
		{"letter O", "01OQ8", "", false},
		{"letter U", "01UQ8", "", false},
		{"a dash", "01-8", "", false},
		{"a wildcard", "01JQ*", "", false},
		{"non ascii", "01JQå", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := id.NormalizePrefix(tc.in)
			if tc.ok {
				if err != nil {
					t.Fatalf("NormalizePrefix(%q) = %v, want nil", tc.in, err)
				}
				if got != tc.want {
					t.Errorf("NormalizePrefix(%q) = %q, want %q", tc.in, got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("NormalizePrefix(%q) = %q, want an error", tc.in, got)
			}
			if !errors.Is(err, id.ErrInvalid) {
				t.Errorf("NormalizePrefix(%q) error = %v, want it to wrap ErrInvalid", tc.in, err)
			}
		})
	}
}

func TestNormalizePrefixNamesTheOffendingCharacter(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"upper case input", "01IQ8", `"I"`},
		{"lower case input", "01iq8", `"i"`},
		{"non ascii input", "01JQå", `"å"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := id.NormalizePrefix(tc.in)
			if err == nil {
				t.Fatalf("NormalizePrefix(%q) = nil error", tc.in)
			}
			// The character is quoted back as the user typed it. An error naming
			// a character that is not on their screen sends them hunting.
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name the rejected character as %s", err, tc.want)
			}
			if !strings.Contains(err.Error(), id.Alphabet) {
				t.Errorf("error %q does not show the alphabet it accepts", err)
			}
		})
	}
}

func TestPrefixRangeBounds(t *testing.T) {
	pad := func(s string, filler byte) string {
		return s + strings.Repeat(string(filler), id.Length-len(s))
	}

	cases := []struct {
		name          string
		prefix        string
		lower, upper  string
		explicitUpper string
	}{
		{name: "ordinary prefix", prefix: "01JQ8", lower: pad("01JQ8", '0'), upper: pad("01JQ9", '0')},
		{name: "lower case input", prefix: "01jq8", lower: pad("01JQ8", '0'), upper: pad("01JQ9", '0')},
		{name: "carry over one place", prefix: "01JQZ", lower: pad("01JQZ", '0'), upper: pad("01JR0", '0')},
		{name: "carry over two places", prefix: "01JZZ", lower: pad("01JZZ", '0'), upper: pad("01K00", '0')},
		{name: "digit to letter", prefix: "019", lower: pad("019", '0'), upper: pad("01A", '0')},
		{name: "single character", prefix: "0", lower: pad("0", '0'), upper: pad("1", '0')},
		{
			name:   "a whole id is a prefix of length 26",
			prefix: pad("01JQ8", '0'),
			lower:  pad("01JQ8", '0'),
			upper:  pad("01JQ8", '0')[:25] + "1",
		},
		{
			name:          "no successor exists",
			prefix:        "ZZ",
			lower:         pad("ZZ", '0'),
			explicitUpper: strings.Repeat("Z", id.Length) + "0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := id.PrefixRange(tc.prefix)
			if err != nil {
				t.Fatalf("PrefixRange(%q): %v", tc.prefix, err)
			}
			if got.Lower != tc.lower {
				t.Errorf("Lower = %q, want %q", got.Lower, tc.lower)
			}
			want := tc.upper
			if tc.explicitUpper != "" {
				want = tc.explicitUpper
			}
			if got.Upper != want {
				t.Errorf("Upper = %q, want %q", got.Upper, want)
			}
			if got.Upper <= got.Lower {
				t.Errorf("Upper %q is not above Lower %q", got.Upper, got.Lower)
			}
		})
	}
}

func TestPrefixRangeRejectsWhatNormalizePrefixRejects(t *testing.T) {
	for _, bad := range []string{"", "01IQ8", strings.Repeat("0", 27), "01-8"} {
		if _, err := id.PrefixRange(bad); !errors.Is(err, id.ErrInvalid) {
			t.Errorf("PrefixRange(%q) error = %v, want it to wrap ErrInvalid", bad, err)
		}
	}
}

// TestPrefixRangeContainsExactlyTheMatchingIDs is the property the range query
// in the CLI depends on: id >= Lower AND id < Upper selects every id with the
// prefix and nothing else.
func TestPrefixRangeContainsExactlyTheMatchingIDs(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))

	randomID := func() string {
		var b strings.Builder
		for range id.Length {
			b.WriteByte(id.Alphabet[r.IntN(len(id.Alphabet))])
		}
		return b.String()
	}

	const samples = 2000

	population := make([]string, 0, samples)
	for range samples {
		population = append(population, randomID())
	}

	for i := range samples {
		subject := population[i]
		n := 1 + r.IntN(id.Length)
		prefix := subject[:n]

		rng, err := id.PrefixRange(prefix)
		if err != nil {
			t.Fatalf("PrefixRange(%q): %v", prefix, err)
		}

		// The two ids at the edges of the prefix. A random population almost
		// never contains them, and they are exactly where an off by one in the
		// bounds shows up.
		edges := []string{
			prefix + strings.Repeat("0", id.Length-n),
			prefix + strings.Repeat("Z", id.Length-n),
		}

		for _, candidate := range append(edges, population[r.IntN(samples)], subject) {
			inRange := candidate >= rng.Lower && candidate < rng.Upper
			hasPrefix := strings.HasPrefix(candidate, prefix)
			if inRange != hasPrefix {
				t.Fatalf("prefix %q, range [%q, %q): id %q has prefix = %v but is in range = %v",
					prefix, rng.Lower, rng.Upper, candidate, hasPrefix, inRange)
			}
		}
	}
}
