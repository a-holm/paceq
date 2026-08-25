package explain

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// buildJobFixture seeds a job whose timeline holds what the prose examples
// show: coalesced skips, a real trigger with its run, and an outage gap with
// synthetic missed ticks.
func buildJobFixture(t *testing.T) (*store.Store, *Resolved) {
	t.Helper()
	st := fixtureStore(t)
	ctx := context.Background()
	seedFixture(t, st)

	sch, err := st.GetSchedule(ctx, "nightly-report", "nightly")
	if err != nil {
		t.Fatalf("read the seeded schedule: %v", err)
	}

	// Three identical stands-down coalesce onto one legible row: x2 extra
	// repeats beside the first, exactly what the timeline must fold.
	base := frozenNow.Add(-24 * time.Hour)
	for i := range 3 {
		if _, err := st.MaterializeTick(ctx, store.TickInput{
			Schedule:       sch,
			ScheduledFor:   base.Add(time.Duration(i) * time.Hour),
			Outcome:        store.OutcomeSkipped,
			ReasonCode:     reason.TICKSkippedPaused,
			UpdateProgress: false,
		}); err != nil {
			t.Fatalf("materialise skip %d: %v", i, err)
		}
	}

	// A real decision: the tick that fired, its trigger, and the run.
	fired := frozenNow.Add(-2 * time.Hour)
	if _, err := st.MaterializeTick(ctx, store.TickInput{
		Schedule:       sch,
		ScheduledFor:   fired,
		Outcome:        store.OutcomeTriggered,
		RunKey:         "2026-08-21T02:00:00Z",
		UpdateProgress: true,
	}); err != nil {
		t.Fatalf("materialise the trigger: %v", err)
	}

	// An outage with two slots nobody evaluated inside it.
	if _, err := st.RecordOutage(ctx, store.OutageInput{
		From: frozenNow.Add(-90 * time.Minute),
		To:   frozenNow.Add(-81 * time.Minute),
		Kind: "crash",
	}); err != nil {
		t.Fatalf("record the outage: %v", err)
	}

	res, err := Resolve(ctx, st, "job/nightly-report")
	if err != nil {
		t.Fatalf("resolve the fixture job: %v", err)
	}
	return st, &res
}

// TestBuildTimelineMergesEverything holds the one-model rule: ticks, triggers
// with their runs, outages and synthetic missed evidence land in ONE reverse
// chronological list, and every terminal decision carries code plus hints.
func TestBuildTimelineMergesEverything(t *testing.T) {
	ctx := context.Background()
	st, res := buildJobFixture(t)

	report, err := Build(ctx, st, *res, Options{
		Since: frozenNow.Add(-48 * time.Hour),
		Clock: clock.NewFake(frozenNow),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var sawCoalesced, sawOutage, sawTriggered bool
	var assertChildren func(entries []Entry)
	assertChildren = func(entries []Entry) {
		for i := range entries {
			e := &entries[i]
			switch {
			case e.Kind == "tick" && e.RepeatCount == 3:
				sawCoalesced = true
				if e.ReasonCode != string(reason.TICKSkippedPaused) || len(e.Hints) == 0 {
					t.Errorf("the coalesced skip lost its code or hints: %+v", e)
				}
			case e.Kind == "outage":
				sawOutage = true
				if e.ReasonCode != string(reason.TICKMissedDaemonDown) {
					t.Errorf("the outage does not say TICK_MISSED_DAEMON_DOWN: %+v", e)
				}
				if e.DurationMS == nil || *e.DurationMS != 9*60*1000 {
					t.Errorf("the outage lost its duration: %+v", e.DurationMS)
				}
				if len(e.Hints) == 0 {
					t.Errorf("the outage carries no remediation hint")
				}
			case e.Kind == "tick" && e.Outcome == "triggered":
				sawTriggered = true
				if len(e.Children) != 1 || e.Children[0].Kind != "trigger" {
					t.Errorf("the triggered tick lost its trigger child: %+v", e.Children)
					continue
				}
				trigger := e.Children[0]
				if trigger.Outcome != "accepted" {
					t.Errorf("the trigger is %+v, want accepted", trigger)
				}
				if len(trigger.Children) != 1 || trigger.Children[0].Kind != "run" {
					t.Errorf("the trigger lost its run child: %+v", trigger.Children)
				}
			}
			if e.Outcome == "skipped" || e.Outcome == "error" || e.Outcome == "missed" {
				if e.ReasonCode == "" || len(e.Hints) == 0 {
					t.Errorf("terminal decision %s (%s) lacks code or hints: %+v", e.Ref, e.Kind, e)
				}
			}
			assertChildren(e.Children)
		}
	}
	assertChildren(report.Entries)

	if !sawCoalesced || !sawOutage || !sawTriggered {
		t.Errorf("timeline incomplete: coalesced=%t outage=%t triggered=%t", sawCoalesced, sawOutage, sawTriggered)
	}

	for i := 1; i < len(report.Entries); i++ {
		if report.Entries[i-1].At < report.Entries[i].At {
			t.Errorf("entries %d and %d are out of order: %d then %d",
				i-1, i, report.Entries[i-1].At, report.Entries[i].At)
		}
	}
	if report.SchemaVersion != 1 {
		t.Errorf("schema_version is %d, want 1", report.SchemaVersion)
	}
	if report.DaemonUp {
		t.Errorf("daemon_up carried through even though the option said down")
	}
}

// TestJSONContractIsSelfContained marshals a built report and checks the
// fields a web UI would need: no dangling references, version pinned.
func TestJSONContractIsSelfContained(t *testing.T) {
	ctx := context.Background()
	st, res := buildJobFixture(t)

	report, err := Build(ctx, st, *res, Options{
		Since: frozenNow.Add(-48 * time.Hour),
		Clock: clock.NewFake(frozenNow),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"schema_version":1`,
		`"generated_at_ms"`, `"since_ms"`, `"daemon_up"`,
		`"summary"`, `"entries"`, `"at_ms"`, `"kind"`, `"outcome"`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("the contract document misses %s:\n%s", want, raw)
		}
	}
}

// TestRenderTextShapesFollowThePlans renders the merged fixture both ways and
// holds the visible facts the plans promise: reasons, remedies, the xN fold.
func TestRenderTextShapesFollowThePlans(t *testing.T) {
	ctx := context.Background()
	st, res := buildJobFixture(t)

	report, err := Build(ctx, st, *res, Options{
		Since: frozenNow.Add(-48 * time.Hour),
		Clock: clock.NewFake(frozenNow),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var ascii bytes.Buffer
	RenderText(&ascii, report, StyleASCII())
	out := ascii.String()

	for _, want := range []string{
		"job nightly-report",
		"TICK_SKIPPED_PAUSED",
		"x2 coalesced",
		"TICK_MISSED_DAEMON_DOWN",
		"-> ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the text form misses %q:\n%s", want, out)
		}
	}
	for _, forbidden := range []string{"✓", "✗", "⚠", "→", "×"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("the ASCII form leaked %q:\n%s", forbidden, out)
		}
	}

	var unicode bytes.Buffer
	RenderText(&unicode, report, StyleUnicode())
	uout := unicode.String()
	if !strings.Contains(uout, "×2 coalesced") || !strings.Contains(uout, "→ ") {
		t.Errorf("the UTF-8 form dropped its symbols:\n%s", uout)
	}
}

// TestEmptyHistoryAnswersWithTheFuture holds the fresh-install rule: nothing
// recorded yet is answered with when the first decision is due, never with a
// silent empty table.
func TestEmptyHistoryAnswersWithTheFuture(t *testing.T) {
	ctx := context.Background()
	st := fixtureStore(t)
	seedFixture(t, st)

	res, err := Resolve(ctx, st, "job/nightly-report")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	report, err := Build(ctx, st, res, Options{
		Since: frozenNow.Add(-48 * time.Hour),
		Clock: clock.NewFake(frozenNow),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(report.Entries) != 0 {
		t.Fatalf("an untouched job produced %d entries", len(report.Entries))
	}

	var out bytes.Buffer
	RenderText(&out, report, StyleASCII())
	if !strings.Contains(out.String(), "no decisions recorded") {
		t.Errorf("the empty history message is missing:\n%s", out.String())
	}
	if report.Summary.NextTickAt == nil {
		t.Errorf("the summary should carry the next due tick for the empty answer")
	}
}

// TestEveryCatalogueCodeShowsReasonAndRemedy walks the whole catalogue and
// proves the presentation layer can show each code with at least one way
// forward. This is the reason-dekning test: it cannot name scenarios (that is
// M5-02) but it makes a silent code impossible in this layer.
func TestEveryCatalogueCodeShowsReasonAndRemedy(t *testing.T) {
	kindForLevel := map[reason.Level]string{
		reason.LevelTick:    "tick",
		reason.LevelTrigger: "trigger",
		reason.LevelRun:     "run",
		reason.LevelStep:    "step",
	}
	shown := 0
	for _, entry := range reason.All() {
		kind, ok := kindForLevel[entry.Level]
		if !ok {
			continue // lease codes are not part of any explain surface yet
		}
		report := &Report{
			SchemaVersion: SchemaVersion,
			Subject:       Subject{Kind: "job", Ref: "job/x", Job: "x"},
			Entries: []Entry{{
				At:         frozenNow.UnixMilli(),
				Kind:       kind,
				Actor:      "scheduler",
				Ref:        "01JTEST",
				Outcome:    "skipped",
				ReasonCode: string(entry.Code),
				Hints:      hintsFor(string(entry.Code)),
			}},
		}

		var out bytes.Buffer
		RenderText(&out, report, StyleASCII())
		text := out.String()
		if !strings.Contains(text, string(entry.Code)) {
			t.Errorf("%s does not appear in the text form:\n%s", entry.Code, text)
		}
		if !strings.Contains(text, "-> ") {
			t.Errorf("%s shows no remediation hint in the text form:\n%s", entry.Code, text)
		}
		if len(report.Entries[0].Hints) == 0 {
			t.Errorf("%s resolves to no hints at all", entry.Code)
		}
		shown++
	}
	if shown < 30 {
		t.Fatalf("only %d codes exercised, the catalogue has grown or shrunk unexpectedly", shown)
	}
}

// TestUnknownCodeStillExplainsItself pins the fallback: a stored code outside
// the catalogue must still render with a pointer to the catalogue instead of
// disappearing.
func TestUnknownCodeStillExplainsItself(t *testing.T) {
	hints := hintsFor("TICK_SKIPPED_NO_SUCH_CODE")
	if len(hints) != 1 || !strings.Contains(hints[0], "paceq error") {
		t.Errorf("an unknown code must fall back to the catalogue pointer, got %v", hints)
	}
}
