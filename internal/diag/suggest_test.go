package diag_test

import (
	"math/rand/v2"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
)

// jobFields is a candidate list the size of a real one, so the tests measure
// suggestions against the neighbours they will actually have.
var jobFields = []string{
	"name", "description", "env", "env_file", "inherit_env", "workdir",
	"timeout", "max_concurrent", "steps", "schedules", "sensors",
}

func TestSuggestFindsWhatWasMeant(t *testing.T) {
	cases := map[string]string{
		"nam":            "name",
		"names":          "name",
		"naem":           "name",
		"tiemout":        "timeout",
		"timout":         "timeout",
		"step":           "steps",
		"stpes":          "steps",
		"env_files":      "env_file",
		"envfile":        "env_file",
		"inherit-env":    "inherit_env",
		"max_concurent":  "max_concurrent",
		"max_concurrant": "max_concurrent",
		"schedule":       "schedules",
		"sensor":         "sensors",
		"workdirs":       "workdir",
		"descr":          "description",
		"DESCRIPTION":    "description",
	}
	for written, want := range cases {
		t.Run(written, func(t *testing.T) {
			if got := diag.Suggest(written, jobFields); got != want {
				t.Errorf("Suggest(%q) = %q, want %q", written, got, want)
			}
		})
	}
}

// TestSuggestStaysQuietWhenNothingIsClose. A wrong suggestion is worse than
// none: it sends the reader to rename a field to something that was never what
// they meant.
func TestSuggestStaysQuietWhenNothingIsClose(t *testing.T) {
	for _, written := range []string{"zookeeper", "kubernetes", "x", "", "on_failure", "artifacts"} {
		if got := diag.Suggest(written, jobFields); got != "" {
			t.Errorf("Suggest(%q) = %q, want no suggestion", written, got)
		}
	}
}

// TestSuggestSaysNothingAboutANameThatIsAlreadyRight.
func TestSuggestSaysNothingAboutANameThatIsAlreadyRight(t *testing.T) {
	for _, field := range jobFields {
		if got := diag.Suggest(field, jobFields); got != "" {
			t.Errorf("Suggest(%q) = %q for a field that exists", field, got)
		}
	}
}

// TestSuggestIsIndependentOfTheOrderOfTheCandidates. The candidate list comes
// from a struct's field order, and a suggestion that depended on it would
// change the day somebody reordered the struct.
func TestSuggestIsIndependentOfTheOrderOfTheCandidates(t *testing.T) {
	// A fixed seed: the shuffle has to vary between rounds and not between
	// runs, or a failure could not be reproduced.
	random := rand.New(rand.NewPCG(1, 2))

	for _, written := range []string{"nam", "step", "tiemout", "env_files", "zookeeper"} {
		want := diag.Suggest(written, jobFields)
		for range 50 {
			shuffled := append([]string(nil), jobFields...)
			random.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			if got := diag.Suggest(written, shuffled); got != want {
				t.Fatalf("Suggest(%q) = %q in one order and %q in another", written, want, got)
			}
		}
	}
}

// TestSuggestHasNoCandidatesToOffer covers the empty list, which is what a
// block with no fields left to name would hand it.
func TestSuggestHasNoCandidatesToOffer(t *testing.T) {
	if got := diag.Suggest("anything", nil); got != "" {
		t.Errorf("Suggest with no candidates = %q", got)
	}
}

func TestDistanceCountsEdits(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a", "", 1},
		{"", "abc", 3},
		{"retry", "retry", 0},
		{"retry", "retrz", 1},
		{"retry", "rety", 1},
		{"retry", "retrry", 1},
		{"retry", "rerty", 2},
		{"retry", "retries", 3},
		{"nøkkel", "nokkel", 1},
		{"日本", "日本語", 1},
	}
	for _, tc := range cases {
		if got := diag.Distance(tc.a, tc.b); got != tc.want {
			t.Errorf("Distance(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
		if got := diag.Distance(tc.b, tc.a); got != tc.want {
			t.Errorf("Distance(%q, %q) = %d, want %d: the distance is not symmetric", tc.b, tc.a, got, tc.want)
		}
	}
}

// TestDistanceCountsRunesNotBytes. A multi-byte character is one edit, or every
// suggestion over a name with an ø in it would be measured as three.
func TestDistanceCountsRunesNotBytes(t *testing.T) {
	if got := diag.Distance("kø", "ko"); got != 1 {
		t.Errorf(`Distance("kø", "ko") = %d, want 1`, got)
	}
}

// TestNothingIsSuggestedBeyondTheThreshold pins the documented rule rather than
// the arithmetic behind it.
func TestNothingIsSuggestedBeyondTheThreshold(t *testing.T) {
	candidates := []string{"abcdefgh"}

	within := "abcdefgx"
	if got := diag.Suggest(within, candidates); got != "abcdefgh" {
		t.Errorf("Suggest(%q) = %q, want the candidate one edit away", within, got)
	}
	beyond := "abcdxyzw"
	if got := diag.Suggest(beyond, candidates); got != "" {
		t.Errorf("Suggest(%q) = %q, want nothing four edits away", beyond, got)
	}
}
