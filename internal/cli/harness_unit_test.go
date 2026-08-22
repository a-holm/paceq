package cli

import (
	"strings"
	"testing"
)

// The masking and comparison helpers behind the golden scripts are pure
// functions, so their contract is unit tested here before any script leans
// on them: same id in, same placeholder out; different ids, different
// placeholders; a stable order no matter how often the mask runs.

func TestMaskVolatileGivesTheSamePlaceholderToTheSameID(t *testing.T) {
	in := []byte(`{"id":"01K5ZQ8V3M7XAAAAAAAAAAAAAA","job":"a"}
{"id":"01K5ZQ8V3M7XAAAAAAAAAAAAAA","job":"b"}`)
	out := string(maskVolatile("/nowhere/work", in))
	if !strings.Contains(out, "<RUN1>") {
		t.Fatalf("the id was not masked:\n%s", out)
	}
	if strings.Count(out, "<RUN1>") != 2 {
		t.Fatalf("the same id must get the same placeholder twice:\n%s", out)
	}
	if strings.Contains(out, "01K5ZQ8V3M7X") {
		t.Fatalf("a raw ULID survived the mask:\n%s", out)
	}
}

func TestMaskVolatileGivesDifferentPlaceholdersToDifferentIDs(t *testing.T) {
	in := []byte("01K5ZQ8V3M7XAAAAAAAAAAAAAA 01K5ZQ8V3M7XBBBBBBBBBBBBBB")
	out := string(maskVolatile("/nowhere/work", in))
	if !strings.Contains(out, "<RUN1>") || !strings.Contains(out, "<RUN2>") {
		t.Fatalf("two ids must become two placeholders:\n%s", out)
	}
}

func TestMaskVolatileAssignsPlaceholdersInFirstSeenOrder(t *testing.T) {
	in := []byte("second 01K5ZQ8V3M7XBBBBBBBBBBBBBB first 01K5ZQ8V3M7XAAAAAAAAAAAAAA")
	out := string(maskVolatile("/nowhere/work", in))
	want := "second <RUN1> first <RUN2>"
	if out != want {
		t.Fatalf("placeholders follow first-seen order:\ngot  %s\nwant %s", out, want)
	}
}

func TestMaskVolatileReplacesTheWorkDirectory(t *testing.T) {
	in := []byte(`{"file":"/nowhere/work/jobs/broken.yaml"}`)
	out := string(maskVolatile("/nowhere/work", in))
	if !strings.Contains(out, "<WORK>/jobs/broken.yaml") {
		t.Fatalf("the work directory was not masked:\n%s", out)
	}
	if strings.Contains(out, "/nowhere") {
		t.Fatalf("a raw absolute path survived the mask:\n%s", out)
	}
}

func TestMaskVolatileLeavesOrdinaryWordsAlone(t *testing.T) {
	in := []byte("state succeeded reason RUN_FAILED_STEP duration_ms 0")
	out := string(maskVolatile("/nowhere/work", in))
	if out != string(in) {
		t.Fatalf("ordinary words must not be touched:\ngot %s", out)
	}
}

func TestMaskVolatileIsStableAcrossRuns(t *testing.T) {
	in := []byte(`{"id":"01K5ZQ8V3M7XAAAAAAAAAAAAAA","file":"/nowhere/work/logs/01K5ZQ8V3M7XBBBBBBBBBBBBBB/x"}`)
	first := string(maskVolatile("/nowhere/work", in))
	for range 10 {
		again := string(maskVolatile("/nowhere/work", in))
		if again != first {
			t.Fatalf("masking is not stable:\nfirst  %s\nagain  %s", first, again)
		}
	}
}

func TestJSONEqualAcceptsTheSameDocumentInDifferentKeyOrder(t *testing.T) {
	a := []byte(`{"a":1,"b":[1,2]}`)
	b := []byte(`{"b":[1,2],"a":1}`)
	equal, err := jsonEqual(a, b)
	if err != nil {
		t.Fatalf("compare failed: %v", err)
	}
	if !equal {
		t.Fatalf("key order must not matter")
	}
}

func TestJSONEqualRejectsADifferentValue(t *testing.T) {
	a := []byte(`{"state":"succeeded"}`)
	b := []byte(`{"state":"failed"}`)
	equal, err := jsonEqual(a, b)
	if err != nil {
		t.Fatalf("compare failed: %v", err)
	}
	if equal {
		t.Fatalf("different values must not compare equal")
	}
}

func TestJSONEqualKeepsArrayOrderMeaningful(t *testing.T) {
	a := []byte(`[{"id":"<RUN1>"},{"id":"<RUN2>"}]`)
	b := []byte(`[{"id":"<RUN2>"},{"id":"<RUN1>"}]`)
	equal, err := jsonEqual(a, b)
	if err != nil {
		t.Fatalf("compare failed: %v", err)
	}
	if equal {
		t.Fatalf("array order carries meaning: newest first is the contract")
	}
}

func TestJSONEqualRejectsBrokenJSON(t *testing.T) {
	if _, err := jsonEqual([]byte(`{`), []byte(`{}`)); err == nil {
		t.Fatalf("broken JSON must be an error, not a silent compare")
	}
}

func TestCanonicalJSONSortsKeysForTheDiff(t *testing.T) {
	out, err := canonicalJSON([]byte(`{"b":1,"a":2}`))
	if err != nil {
		t.Fatalf("canonicalise failed: %v", err)
	}
	want := "{\n  \"a\": 2,\n  \"b\": 1\n}"
	if strings.TrimSpace(string(out)) != want {
		t.Fatalf("canonical form is not sorted:\ngot  %s\nwant %s", out, want)
	}
}
