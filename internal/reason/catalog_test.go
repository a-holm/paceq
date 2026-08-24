package reason

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestEveryEntryCarriesFullMetadata is the error-anatomy rule applied to the
// catalogue itself: a code without a short text cannot appear in a list, one
// without a long explanation cannot answer `paceq error`, and one without a
// remediation hint leaves the operator where it found them (06 section 2.1).
func TestEveryEntryCarriesFullMetadata(t *testing.T) {
	for _, e := range All() {
		if e.Code == "" {
			t.Fatal("an entry carries an empty code")
		}
		if strings.TrimSpace(e.Short) == "" {
			t.Errorf("%s: Short is empty", e.Code)
		}
		if len(e.Short) > 80 {
			t.Errorf("%s: Short is %d characters, over 80: it has to fit a table row", e.Code, len(e.Short))
		}
		if strings.TrimSpace(e.Explanation) == "" {
			t.Errorf("%s: Explanation is empty", e.Code)
		}
		if len(e.Remedy) == 0 {
			t.Errorf("%s: Remedy is empty, and a code without a next step is not explainable", e.Code)
		}
		for i, r := range e.Remedy {
			if strings.TrimSpace(r) == "" {
				t.Errorf("%s: Remedy[%d] is empty", e.Code, i)
			}
		}
		for i, k := range e.DataKeys {
			if !dataKeyRe.MatchString(k) {
				t.Errorf("%s: DataKeys[%d] %q is not a lower snake case key", e.Code, i, k)
			}
		}
	}
}

var dataKeyRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// TestCodesBelongToTheirLevel catches a code filed under the wrong level. A
// step code stored in a tick row would survive the database CHECK constraints,
// because those check presence and not meaning.
func TestCodesBelongToTheirLevel(t *testing.T) {
	prefixes := map[Level]string{
		LevelTick:    "TICK_",
		LevelTrigger: "TRIGGER_",
		LevelRun:     "RUN_",
		LevelStep:    "STEP_",
		LevelLease:   "LEASE_",
	}
	for _, e := range All() {
		want, ok := prefixes[e.Level]
		if !ok {
			t.Errorf("%s: unknown level %q", e.Code, e.Level)
			continue
		}
		if !strings.HasPrefix(string(e.Code), want) {
			t.Errorf("%s: level %s but the name does not start with %s", e.Code, e.Level, want)
		}
	}
}

// TestAllFourLevelsArePopulated is the M1 commitment: the tick, trigger, run
// and step levels all exist now, including the codes M2, M3 and M4 fill in
// later, because codes added after the fact become after-rationalisations.
func TestAllFourLevelsArePopulated(t *testing.T) {
	counts := map[Level]int{}
	for _, e := range All() {
		counts[e.Level]++
	}
	for _, l := range []Level{LevelTick, LevelTrigger, LevelRun, LevelStep} {
		if counts[l] == 0 {
			t.Errorf("level %s has no codes at all", l)
		}
	}
}

// TestTerminalFlagsFollowTheWriteRule ties Entry.Terminal to what the schema
// refuses: every state the CHECK constraints demand a reason for has at least
// one terminal code, and the non-terminal ones are named here so a new code
// cannot quietly join them.
func TestTerminalFlagsFollowTheWriteRule(t *testing.T) {
	notTerminal := map[Code]bool{
		RUNQueuedConcurrency:      true,
		RUNDeferredConcurrencyKey: true,
		// The #13 output warnings are facts beside a verdict, never a
		// verdict: an unread or over-large $PACEQ_OUTPUT line must not
		// end a step whose command exited 0.
		STEPOutputInvalid:   true,
		STEPOutputTruncated: true,
		STEPRetryScheduled:  true,
		// The drain interrupt: work goes back to pending with no attempt
		// spent, so nothing about the run or the step has ended.
		RUNInterruptedShutdown: true,
	}
	for _, e := range All() {
		want := !notTerminal[e.Code]
		if e.Terminal != want {
			t.Errorf("%s: Terminal is %v, want %v", e.Code, e.Terminal, want)
		}
	}

	have := map[Code]bool{}
	for _, e := range All() {
		if e.Terminal {
			have[e.Code] = true
		}
	}
	required := []Code{
		TICKSkippedPaused, TICKErrorSensorFailed, TICKMissedDaemonDown,
		TRIGGERDedupedRunKey, TRIGGERRejectedPayload, TRIGGERAccepted,
		RUNSucceeded, RUNFailedStep, RUNCancelledManual,
		STEPSucceeded, STEPFailedNonzeroExit, STEPSkippedUpstreamFailed, STEPCancelled,
	}
	for _, c := range required {
		if !have[c] {
			t.Errorf("%s should be a terminal code, and none is marked Terminal", c)
		}
	}
}

// TestNoUnknownCodeExists is the rule that keeps the catalogue from rotting:
// there is no UNKNOWN code, so UNKNOWN can never be the easy way out. The
// bare name may not appear in any field either, because a remedy saying
// "store UNKNOWN" would undo the whole catalogue. Specific codes whose names
// end in UNKNOWN, such as TRIGGER_REJECTED_JOB_UNKNOWN, are fine: they say
// something precise, which is the opposite of the escape hatch.
func TestNoUnknownCodeExistsAnywhere(t *testing.T) {
	for _, e := range All() {
		if e.Code == "UNKNOWN" {
			t.Error("the catalogue may never hold a bare UNKNOWN code")
		}
		for _, field := range []string{e.Short, e.Explanation} {
			if containsUnknownToken(field) {
				t.Errorf("%s: metadata mentions UNKNOWN as a value: %q", e.Code, field)
			}
		}
		for _, r := range e.Remedy {
			if containsUnknownToken(r) {
				t.Errorf("%s: remedy mentions UNKNOWN as a value: %q", e.Code, r)
			}
		}
	}
}

// unknownTokenRe matches UNKNOWN standing on its own, not inside a longer
// code name.
var unknownTokenRe = regexp.MustCompile(`(^|[^A-Z0-9_])UNKNOWN([^A-Z0-9_]|$)`)

func containsUnknownToken(s string) bool { return unknownTokenRe.MatchString(s) }

// TestCodesAreLowCardinalityLabels prepares for M5-06: reason codes become
// metric label values, so the shape stays narrow and no id ever rides inside
// one (06 section 6.4).
func TestCodesAreLowCardinalityLabels(t *testing.T) {
	shape := regexp.MustCompile(`^[A-Z][A-Z0-9_]{3,39}$`)
	for _, e := range All() {
		if !shape.MatchString(string(e.Code)) {
			t.Errorf("%s: does not match the code shape ^[A-Z][A-Z0-9_]{3,39}$", e.Code)
		}
	}
}

// TestAllIsSortedAndUnique is what makes the catalogue renderable and diffable:
// the same codes in the same order on every call.
func TestAllIsSortedAndUnique(t *testing.T) {
	all := All()
	seen := map[Code]bool{}
	for i := 1; i < len(all); i++ {
		if all[i-1].Code >= all[i].Code {
			t.Errorf("All() is not strictly sorted at %s and %s", all[i-1].Code, all[i].Code)
		}
		if seen[all[i].Code] {
			t.Errorf("%s appears twice in All()", all[i].Code)
		}
		seen[all[i].Code] = true
	}
}

// TestLookupRoundTrip walks the public list through Lookup and rejects a name
// the catalogue never held.
func TestLookupRoundTrip(t *testing.T) {
	for _, e := range All() {
		got, ok := Lookup(e.Code)
		if !ok {
			t.Errorf("Lookup(%s) returned false for a catalogue code", e.Code)
			continue
		}
		if got.Code != e.Code || got.Level != e.Level || got.Short != e.Short ||
			got.Explanation != e.Explanation || got.Terminal != e.Terminal ||
			strings.Join(got.Remedy, "\n") != strings.Join(e.Remedy, "\n") ||
			strings.Join(got.DataKeys, "\n") != strings.Join(e.DataKeys, "\n") {
			t.Errorf("Lookup(%s) returned a different entry", e.Code)
		}
	}
	if _, ok := Lookup("STEP_NOT_A_CODE"); ok {
		t.Error("Lookup accepted a code outside the catalogue")
	}
	if IsKnown("") {
		t.Error("IsKnown accepted the empty code")
	}
}

// TestDaemonCrashedCodeIsTheHangingTickVerdict pins the verdict startup
// reconciliation (#62) writes over a tick its dying daemon left running. The
// tick moves from running to error, so the code has to be terminal, and it
// lives at the tick level beside the other outcomes the ticks table stores.
func TestDaemonCrashedCodeIsTheHangingTickVerdict(t *testing.T) {
	e, ok := Lookup(TICKErrorDaemonCrashed)
	if !ok {
		t.Fatalf("%s is not in the catalogue", TICKErrorDaemonCrashed)
	}
	if e.Level != LevelTick {
		t.Errorf("%s: level is %s, want %s", e.Code, e.Level, LevelTick)
	}
	if !e.Terminal {
		t.Errorf("%s: Terminal is false, want true: writing it ends the tick", e.Code)
	}
}

// TestCatalogueMatchesTheStableList is the stability contract: deleting or
// renaming a code breaks this test on purpose, so the change has to be argued
// in the commit that makes it. A code means the same thing in 2027 as in 2026,
// or history and alarms both lie (06 section 2.1).
func TestCatalogueMatchesTheStableList(t *testing.T) {
	path := filepath.Join("testdata", "codes.golden.txt")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var want []string
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			want = append(want, line)
		}
	}
	got := Codes()
	if len(got) != len(want) {
		t.Fatalf("catalogue holds %d codes, the stable list holds %d: edit testdata/codes.golden.txt only when removing or renaming a code is the deliberate point of the commit", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("stable list position %d is %s, the catalogue has %s", i, want[i], got[i])
		}
	}
}
