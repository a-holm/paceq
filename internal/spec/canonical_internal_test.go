package spec

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestObjectKeysAreSortedWhateverOrderTheyArriveIn is the encoder's half of the
// determinism promise, checked on the encoder rather than through a parse, so a
// failure says which layer moved.
func TestObjectKeysAreSortedWhateverOrderTheyArriveIn(t *testing.T) {
	forwards := canonicalObject{
		{"alpha", canonicalNumber(1)},
		{"bravo", canonicalNumber(2)},
		{"charlie", canonicalNumber(3)},
	}
	backwards := canonicalObject{
		{"charlie", canonicalNumber(3)},
		{"bravo", canonicalNumber(2)},
		{"alpha", canonicalNumber(1)},
	}

	if got, want := encode(forwards), `{"alpha":1,"bravo":2,"charlie":3}`; got != want {
		t.Errorf("encode = %s, want %s", got, want)
	}
	if encode(forwards) != encode(backwards) {
		t.Errorf("the order the members arrived in reached the output: %s and %s", encode(forwards), encode(backwards))
	}
}

// TestEncodingDoesNotEditTheObjectItIsGiven. The sort is on a copy, so a value
// encoded twice encodes the same both times and the caller's slice is untouched.
func TestEncodingDoesNotEditTheObjectItIsGiven(t *testing.T) {
	object := canonicalObject{
		{"zulu", canonicalNumber(1)},
		{"alpha", canonicalNumber(2)},
	}

	first := encode(object)

	if object[0].key != "zulu" {
		t.Errorf("encoding reordered the caller's members: %s came first", object[0].key)
	}
	if again := encode(object); again != first {
		t.Errorf("encoding twice gave %s and %s", first, again)
	}
}

// TestStringsAreEscapedTheWayJSONRequiresAndNoFurther. HTML escaping is what
// encoding/json does by default and what this encoder must not do: the bytes
// are hashed, so an escape is a change to the hash.
func TestStringsAreEscapedTheWayJSONRequiresAndNoFurther(t *testing.T) {
	cases := map[string]string{
		"plain":              `"plain"`,
		`a "quote"`:          `"a \"quote\""`,
		`a\backslash`:        `"a\\backslash"`,
		"a\nnewline":         `"a\nnewline"`,
		"a\ttab":             `"a\ttab"`,
		"a\rreturn":          `"a\rreturn"`,
		"a\bbackspace":       `"a\bbackspace"`,
		"a\fformfeed":        `"a\fformfeed"`,
		"a\x00nul":           `"a\u0000nul"`,
		"a\x1funit":          `"a\u001funit"`,
		"<script>&</td>":     `"<script>&</td>"`,
		"a\u2028separator":   "\"a\u2028separator\"",
		"n\u00f8kkel":        "\"n\u00f8kkel\"",
		"\u65e5\u672c\u8a9e": "\"\u65e5\u672c\u8a9e\"",
		"emoji \U0001f600":   "\"emoji \U0001f600\"",
	}
	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			var b bytes.Buffer
			encodeCanonicalString(&b, input)
			if got := b.String(); got != want {
				t.Errorf("encodeCanonicalString(%q) = %s, want %s", input, got, want)
			}
		})
	}
}

// TestInvalidUTF8EncodesToTheReplacementCharacter keeps the encoder total. A job
// file cannot carry invalid UTF-8, because Parse refuses one, but a hash
// function with an input it panics on is a hash function with a hole.
func TestInvalidUTF8EncodesToTheReplacementCharacter(t *testing.T) {
	var b bytes.Buffer

	encodeCanonicalString(&b, "a\xffb")

	if got, want := b.String(), "\"a�b\""; got != want {
		t.Errorf("encodeCanonicalString = %q, want %q", got, want)
	}
}

// TestTheCanonicalFormIsValidJSON. It is read by the engine, by explain and by
// anything else that stores it, so it has to survive an ordinary parser.
func TestTheCanonicalFormIsValidJSON(t *testing.T) {
	job := &Job{
		Name:          "report",
		Description:   "A \"quoted\" description with <html> in it.",
		Env:           map[string]string{"B": "2", "A": "1"},
		InheritEnv:    []string{"Z", "A"},
		Timeout:       DefaultTimeout,
		MaxConcurrent: 1,
		Steps: []Step{{
			Name:  "only",
			Run:   []string{"/bin/echo", "a\tb"},
			Needs: []string{"z", "a"},
			Retry: &Retry{Max: 3, Backoff: BackoffFixed, Initial: DefaultInitial, MaxDelay: DefaultMaxDelay, Jitter: JitterNone},
		}},
		Schedules: []Schedule{{Name: "nightly", Cron: "0 3 * * *", Timezone: "UTC"}},
		Sensors:   []Sensor{{Name: "watch", Kind: "exec", Run: []string{"/bin/watch"}, Interval: DefaultInitial}},
	}

	canonical := Canonical(job)

	var document map[string]any
	if err := json.Unmarshal(canonical, &document); err != nil {
		t.Fatalf("the canonical form is not JSON: %v\n%s", err, canonical)
	}
	if document["schema"] != SchemaName {
		t.Errorf("schema is %v, want %s", document["schema"], SchemaName)
	}
	if !strings.Contains(string(canonical), `with <html> in it`) {
		t.Errorf("the encoder escaped HTML: %s", canonical)
	}
	if !strings.Contains(string(canonical), `"inherit_env":["A","Z"]`) {
		t.Errorf("inherit_env is not sorted: %s", canonical)
	}
	if !strings.Contains(string(canonical), `"needs":["a","z"]`) {
		t.Errorf("needs is not sorted: %s", canonical)
	}
}

// TestArgvKeepsItsOrder is the other side of that rule: run is a list where the
// order is the meaning, and sorting it would be a bug the hash could not see.
func TestArgvKeepsItsOrder(t *testing.T) {
	job := &Job{
		Name:          "report",
		Timeout:       DefaultTimeout,
		MaxConcurrent: 1,
		Steps:         []Step{{Name: "only", Run: []string{"/bin/echo", "z", "a"}}},
	}

	if got := string(Canonical(job)); !strings.Contains(got, `"run":["/bin/echo","z","a"]`) {
		t.Errorf("argv was reordered: %s", got)
	}
}

// TestHashNamesTheAlgorithmThatProducedIt.
func TestHashNamesTheAlgorithmThatProducedIt(t *testing.T) {
	// The sha256 of the empty string, so the test pins the encoding as well as
	// the prefix.
	const empty = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	if got := Hash(nil); got != empty {
		t.Errorf("Hash(nil) = %s, want %s", got, empty)
	}
	if got := Hash([]byte("a")); got == empty {
		t.Error("Hash does not depend on its input")
	}
}

func encode(value canonicalValue) string {
	var b bytes.Buffer
	value.encode(&b)
	return b.String()
}
