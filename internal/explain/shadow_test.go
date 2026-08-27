package explain

import (
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/store"
)

// The shadow report classification (#32), pure cases over composed rows: no
// database, just the verdicts the CLI renders.

func point(source string, t time.Time) store.ShadowTickPoint {
	return store.ShadowTickPoint{SourceName: source, ScheduledFor: t.UTC()}
}

func obs(job string, t time.Time) store.ShadowObservation {
	return store.ShadowObservation{
		ObservedAt: t.UTC(), Source: ObsSourceJournaldLabel,
		Raw: "j", Command: "/bin/x", HasJob: true, JobName: job,
	}
}

func finding(jobs []ShadowJobFinding, name string) ShadowJobFinding {
	for _, f := range jobs {
		if f.JobName == name {
			return f
		}
	}
	return ShadowJobFinding{}
}

func baseInput(now time.Time) ShadowInput {
	return ShadowInput{
		SinceMs:       now.Add(-96 * time.Hour).UnixMilli(),
		Now:           now,
		LocalZoneName: "Europe/Oslo",
	}
}

func agg(source, outcome, code string, n int64) store.ShadowAggregateRow {
	first := time.Date(2027, 1, 4, 3, 0, 0, 0, time.UTC)
	return store.ShadowAggregateRow{
		SourceName: source, Outcome: outcome, ReasonCode: code,
		Ticks: n, Repeats: n,
		FirstScheduled: &first, LastScheduled: &first,
	}
}

const ObsSourceJournaldLabel = "journald"

var reportNow = time.Date(2027, 1, 11, 9, 0, 0, 0, time.UTC)

func TestEveryPairInsideToleranceIsAMatch(t *testing.T) {
	fire := func(min int) time.Time { return time.Date(2027, 1, 5, 6, min, 0, 0, time.UTC) }
	var points []store.ShadowTickPoint
	var obss []store.ShadowObservation
	for _, m := range []int{5, 25, 45} {
		points = append(points, point("nightly/main", fire(m)))
		obss = append(obss, obs("nightly", fire(m).Add(2*time.Second)))
	}
	rep := composeShadowReport(
		[]store.ShadowAggregateRow{agg("nightly/main", "shadow_triggered", "", 3)},
		points, obss, nil, baseInput(reportNow))
	f := finding(rep.Jobs, "nightly")
	if rep.Comparison != "journald" || len(rep.Sources) != 1 {
		t.Fatalf("comparison=%q sources=%v want journald from real rows", rep.Comparison, rep.Sources)
	}
	if f.Matches != 3 || f.Verdict() != ShadowVerdictMatch {
		t.Fatalf("verdict %q matches %d", f.Verdict(), f.Matches)
	}
	if !rep.EnoughData {
		t.Fatal("two days of full agreement counts as enough ground")
	}
}

func TestSteadyOffsetBecomesTheTimezoneFinding(t *testing.T) {
	fire := func(min int) time.Time { return time.Date(2027, 1, 6, 3, min, 0, 0, time.UTC) }
	var points []store.ShadowTickPoint
	var obss []store.ShadowObservation
	for _, m := range []int{15, 35} {
		points = append(points, point("reports/hourly", fire(m)))
		obss = append(obss, obs("reports", fire(m).Add(60*time.Minute)))
	}
	rep := composeShadowReport(nil, points, obss, nil, baseInput(reportNow))
	f := finding(rep.Jobs, "reports")
	if f.Verdict() != ShadowVerdictOffset || f.OffsetMS == nil || *f.OffsetMS != 3600_000 {
		t.Fatalf("offset class missed: %+v", f)
	}
	if !strings.Contains(f.Hint, "timezone: Europe/Oslo") {
		t.Fatalf("the hint must carry the concrete timezone line: %q", f.Hint)
	}
}

func TestCronOnlyCountsOverlapEvidence(t *testing.T) {
	fire := time.Date(2027, 1, 7, 2, 40, 0, 0, time.UTC)
	points := []store.ShadowTickPoint{point("sync/files", fire)}
	obss := []store.ShadowObservation{obs("sync", fire.Add(5*time.Minute))}
	rep := composeShadowReport(
		[]store.ShadowAggregateRow{
			agg("sync/files", "shadow_triggered", "", 4),
			agg("sync/files", "skipped", "TICK_SKIPPED_OVERLAP", 7),
		},
		points, obss, nil, baseInput(reportNow))
	f := finding(rep.Jobs, "sync")
	if f.CronOnly != 1 {
		t.Fatalf("cron-only leftovers = %d, want 1", f.CronOnly)
	}
	if f.SkippedOverlap != 7 {
		t.Fatalf("overlap counter lost: %+v", f)
	}
	if f.Hint == "" || !strings.Contains(f.Hint, "overlap") {
		t.Fatalf("the overlap story must surface in prose: %q", f.Hint)
	}
}

func TestThinDataStaysUnknown(t *testing.T) {
	fire := time.Date(2027, 1, 9, 0, 0, 0, 0, time.UTC)
	points := []store.ShadowTickPoint{point("weekly/jan", fire)}
	obss := []store.ShadowObservation{obs("weekly", fire)}
	rep := composeShadowReport(nil, points, obss, nil, baseInput(reportNow))
	f := finding(rep.Jobs, "weekly")
	if f.Verdict() != ShadowVerdictUnknown {
		t.Fatalf("one run of a weekly job concluded as %q", f.Verdict())
	}
	if rep.EnoughData {
		t.Fatal("a weekly job cannot be conclusive after three days")
	}
}

// The degradation path (AC: falls back without failing): nothing was ever
// observed, so there are no one-sided findings - and the analytic half still
// names a misdeclared timezone by re-reading the expression in the machine's
// own zone.
func TestNoSourceDegradesToAnalyticDiffWithoutFailing(t *testing.T) {
	expr := "45 14 * * *"
	naiveInstants := parseLocalInstants(t, expr, baseInput(reportNow))
	if len(naiveInstants) < 2 {
		t.Fatalf("fixture needs two occurrences, got %d", len(naiveInstants))
	}
	// The recorded tick sits exactly one hour earlier than the machine zone
	// would have it - the classic missing-timezone signature - and every
	// occurrence in the window carries that mark, so nothing stays stranded
	// on one side.
	var misdeclared []store.ShadowTickPoint
	for _, w := range naiveInstants {
		misdeclared = append(misdeclared, point("legacy/cron", w.Add(-time.Hour)))
	}
	rep := composeShadowReport(nil, misdeclared, nil,
		[]store.ScheduleExpressionRow{{
			SourceName: "legacy/cron",
			JobName:    "legacy", Name: "cron", Expr: expr, Timezone: "",
		}},
		baseInput(reportNow))

	if rep.Comparison != "analytic" || len(rep.Sources) != 0 {
		t.Fatalf("the fallback must say what it did: %q / %v", rep.Comparison, rep.Sources)
	}
	f := finding(rep.Jobs, "legacy")
	if f.PulseqOnly != 0 || f.CronOnly != 0 {
		t.Fatalf("one-sided findings are meaningless without observations: %d/%d",
			f.PulseqOnly, f.CronOnly)
	}
	if f.OffsetMS == nil {
		t.Fatalf("the analytic diff missed the steady hour drift: %+v", f)
	}
}

// parseLocalInstants evaluates an expression the way cron would, in this
// machine's zone, across the report window, so fixture expectations hold
// whatever the CI host calls home.
func parseLocalInstants(t *testing.T, expr string, in ShadowInput) []time.Time {
	t.Helper()
	parsed, err := cronx.Parse(expr)
	if err != nil {
		t.Fatalf("fixture expression: %v", err)
	}
	from := time.UnixMilli(in.SinceMs).In(time.Local)
	to := in.Now.In(time.Local)
	occs, _ := parsed.Between(from, to, time.Local, cronx.Policy{})
	var out []time.Time
	for _, o := range occs {
		if !o.Skipped {
			out = append(out, o.At)
		}
	}
	return out
}
