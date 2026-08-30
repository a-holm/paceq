package spec

import (
	"bytes"
	"strings"
	"testing"
)

const notifyJobSrc = `name: notify-job
steps:
  - name: only
    run: ["/bin/true"]
notify:
  on_failure: [vakt, konsoll]
`

// TestNotifyDecodesHooksAndNames pins the decode half of the job-level hooks:
// the block parses, names follow the global rule, and anything else in the
// mapping is refused by name.
func TestNotifyDecodesHooksAndNames(t *testing.T) {
	job, diags := Parse("x.yaml", []byte(notifyJobSrc))
	if len(diags) != 0 {
		t.Fatalf("a valid notify block raised %v", diags)
	}
	if job.Notify == nil {
		t.Fatal("notify came back nil")
	}
	if strings.Join(job.Notify.OnFailure, ",") != "vakt,konsoll" {
		t.Errorf("on_failure = %v", job.Notify.OnFailure)
	}

	bad := `name: bad
steps:
  - name: only
    run: ["/bin/true"]
notify:
  on_failure: [VAKT!]
`
	_, diags = Parse("x.yaml", []byte(bad))
	found := false
	for _, d := range diags {
		if d.Code == CodeBadName && strings.Contains(d.Message, "VAKT!") {
			found = true
		}
	}
	if !found {
		t.Fatalf("a bad notifier name was not refused with %s: %v", CodeBadName, diags)
	}

	stray := `name: stray
steps:
  - name: only
    run: ["/bin/true"]
notify:
  whenever: [vakt]
`
	_, diags = Parse("x.yaml", []byte(stray))
	if len(diags) == 0 {
		t.Fatalf("an unknown member inside notify was not refused: %v", diags)
	}
	for _, d := range diags {
		if strings.Contains(d.Message, "whenever") || strings.Contains(d.Hint, "whenever") {
			return
		}
	}
	t.Fatalf("the refusal never named the stray member: %v", diags)
}

// TestNotifyHashStability keeps three promises at once (M5 pitfalls): a job
// that says nothing about notifications hashes like one written before the
// field existed; the lists are sets; and a block that names nobody reads as
// the deliberate silence rather than as no block at all.
func TestNotifyHashStability(t *testing.T) {
	base := `name: stable
steps:
  - name: only
    run: ["/bin/true"]
`
	silent := base + `notify: {}
`
	jobPlain, _ := Parse("p.yaml", []byte(base))
	jobSilent, _ := Parse("s.yaml", []byte(silent))

	if jobPlain.Notify != nil {
		t.Fatalf("a job that says nothing about notifications decoded to a block: %+v", jobPlain.Notify)
	}
	if written := string(Canonical(jobPlain)); strings.Contains(written, "notify") {
		t.Fatalf("saying nothing wrote a notify member, and every hash from before the field\n"+
			"existed moves with it:\n%s", written)
	}
	if jobSilent.Notify.Empty() != true {
		t.Fatalf("the decoded empty block should read as Empty")
	}

	aThenB := base + `notify:
  on_failure: [alpha, beta]
`
	bThenA := base + `notify:
  on_failure: [beta, alpha]
`
	dupes := base + `notify:
  on_failure: [alpha, alpha, beta]
`
	hs := map[string]bool{}
	for _, src := range []string{aThenB, bThenA, dupes} {
		job, diags := Parse("h.yaml", []byte(src))
		for _, d := range diags {
			// A repeated name is told about, once, and then collapsed: the
			// file still parses to the same one set.
			if d.Code != CodeNotifyDuplicate {
				t.Fatalf("%q parsed with %v", src, diags)
			}
		}
		hs[string(Hash(Canonical(job)))] = true
	}
	if len(hs) != 1 {
		t.Fatalf("order and duplicates changed one set's hash: %d distinct", len(hs))
	}

	spelled := base + `notify:
  on_failure: [vakt]
`
	jobSpelled, _ := Parse("sp.yaml", []byte(spelled))
	if Hash(Canonical(jobSpelled)) == Hash(Canonical(jobPlain)) {
		t.Fatal("naming a hook hashed like naming none")
	}
}

// TestNotifyRoundTripsThroughIR proves the reader emits exactly what the
// writer froze, hooks included, byte for byte.
func TestNotifyRoundTripsThroughIR(t *testing.T) {
	j := &Job{
		Name:          "hooks",
		Timeout:       DefaultTimeout,
		MaxConcurrent: DefaultMaxConcurrent,
		MaxParallel:   DefaultMaxParallel,
		Steps:         []Step{{Name: "only", Run: []string{"/bin/true"}}},
		Notify: &Notify{
			OnFailure: []string{"vakt"},
			OnSuccess: []string{"konsoll", "archive"},
		},
	}
	canonical := Canonical(j)
	back, err := FromIR(canonical)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(canonical, Canonical(back)) {
		t.Fatal("the hook block did not round-trip byte identical")
	}
	// The canonical document freezes the set sorted, so the read-back is
	// sorted too: order is not information here.
	if !equalSorted(back.Notify.OnSuccess, []string{"archive", "konsoll"}) {
		t.Errorf("on_success lost fields on the way back: %v", back.Notify.OnSuccess)
	}
}

func equalSorted(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestExplicitSilenceSurvivesTheFreeze holds the rule the spec states and the
// planner implements: a block that lists nobody is the deliberate silence, and
// an absent block inherits the daemon defaults. The frozen document is the only
// thing the engine ever reads back, so a distinction the canonical encoding
// drops is a distinction the planner can never act on.
func TestExplicitSilenceSurvivesTheFreeze(t *testing.T) {
	base := `name: silent
steps:
  - name: only
    run: ["/bin/true"]
`
	spellings := []string{
		base + "notify: {}\n",
		base + "notify:\n  on_failure: []\n  on_success: []\n",
	}
	for _, src := range spellings {
		job, diags := Parse("s.yaml", []byte(src))
		if diags.HasErrors() {
			t.Fatalf("%q parsed with errors: %v", src, diags)
		}
		if job.Notify == nil {
			t.Fatalf("%q decoded to no notify block at all", src)
		}
		back, err := FromIR(Canonical(job))
		if err != nil {
			t.Fatalf("read %q back: %v", src, err)
		}
		if back.Notify == nil {
			t.Errorf("%q froze to a document with no notify block: the engine reads nil "+
				"and the planner takes the daemon defaults, where the file said silence", src)
		}
	}

	plain, _ := Parse("p.yaml", []byte(base))
	silent, _ := Parse("s.yaml", []byte(spellings[0]))
	if Hash(Canonical(plain)) == Hash(Canonical(silent)) {
		t.Error("saying nothing and saying nobody hash alike, so the two cannot be told apart again")
	}
}
