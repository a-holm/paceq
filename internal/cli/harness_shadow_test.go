package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Plant helpers for the shadow goldens (#32). Same discipline as the explain
// fixtures: rows go straight through the store API on the fixture clock,
// never through a daemon or raw SQL, so the scripts isolate presentation.

// cmdPlantShadow plants shadow evidence for one schedule.
//
//	plantshadow JOB NAME TRIGGERED            -> one would-run now-30m
//	plantshadow JOB NAME OVERLAP COUNT        -> skipped/OVERLAP coalesced
func cmdPlantShadow(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: plantshadow JOB NAME TRIGGERED|OVERLAP|CONCURRENCY [count]")
	}
	job, name, kind := args[0], args[1], args[2]
	count := 1
	if len(args) == 4 {
		count = parseCount(ts, args[3])
	}
	s, done := openFixtureWriter(ts)
	defer done()
	ctx := context.Background()

	row, err := s.GetSchedule(ctx, job, name)
	if err != nil {
		ts.Fatalf("no such schedule %s/%s: %v", job, name, err)
	}
	// Marking the row itself is part of the fixture: a real user would have
	// applied shadow:true. Re-upsert keeps id and cursor stable.
	updatedAt := time.Now().UTC()
	row.Shadow = true
	if _, err := s.UpsertSchedule(ctx, store.ScheduleInput{
		JobName:         row.JobName,
		Name:            row.Name,
		Kind:            row.Kind,
		Expr:            row.Expr,
		Timezone:        row.Timezone,
		SpringForward:   row.SpringForward,
		FallBack:        row.FallBack,
		Catchup:         row.Catchup,
		CatchupLimit:    row.CatchupLimit,
		CatchupWindowMS: row.CatchupWindowMS,
		Overlap:         row.Overlap,
		Shadow:          true,
		Paused:          row.Paused,
		NextTickAt:      row.NextTickAt,
	}); err != nil {
		ts.Fatalf("marking %s/%s shadowed: %v", job, name, err)
	}
	_ = updatedAt
	now := plantClock(ts).Now().UTC().Truncate(time.Minute)
	fireBase := now.Add(-90 * time.Minute)
	for i := range count {
		at := fireBase.Add(time.Duration(i*5) * time.Minute)
		in := store.TickInput{
			Schedule:       row,
			ScheduledFor:   at,
			RunKey:         "shadow:" + job + "/" + name + ":" + at.Format(time.RFC3339),
			NextTickAt:     at.Add(5 * time.Minute),
			UpdateProgress: true,
			Actor:          "scheduler",
			Shadow:         true,
		}
		switch strings.ToUpper(kind) {
		case "TRIGGERED":
			in.Outcome = store.OutcomeTriggered
		case "OVERLAP":
			in.Outcome = store.OutcomeSkipped
			in.ReasonCode = reason.TICKSkippedOverlap
			in.ReasonText = "planted by the shadow fixture"
		case "CONCURRENCY":
			in.Outcome = store.OutcomeSkipped
			in.ReasonCode = reason.TICKSkippedConcurrency
			in.ReasonText = "planted by the shadow fixture"
		default:
			ts.Fatalf("plantshadow takes TRIGGERED, OVERLAP or CONCURRENCY")
		}
		if _, err := s.MaterializeTick(ctx, in); err != nil {
			ts.Fatalf("planting %s/%s #%d: %v", job, name, i+1, err)
		}
	}
	ts.Logf("planted %d %s evaluation(s) for %s/%s", count, kind, job, name)
}

// cmdPlantObs records an observed cron start.
//
//	plantobs SOURCE JOB_NAME_OR_DASH OFFSET_FROM_NOW COMMAND...
func cmdPlantObs(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 4 {
		ts.Fatalf("usage: plantobs SOURCE JOB|- OFFSET COMMAND...")
	}
	src := strings.ToLower(args[0])
	switch src {
	case "journald", "file", "manual":
	default:
		ts.Fatalf("%q is not a stored observation source", src)
	}
	job := args[1]
	d, err := time.ParseDuration(strings.TrimPrefix(args[2], "-at="))
	if err != nil || d > 0 {
		ts.Fatalf("%q must be a negative duration from now", args[2])
	}
	cmdText := strings.Join(args[3:], " ")
	o := store.ShadowObservation{
		ObservedAt: plantClock(ts).Now().UTC().Add(d),
		Source:     src,
		Raw:        "(fixture line) (" + src + ") CMD (" + cmdText + ")",
		Command:    cmdText,
		CronUser:   "johan",
	}
	if job != "-" {
		o.JobName, o.HasJob = job, true
	}
	w, done := openFixtureWriter(ts)
	defer done()
	ok, err := w.InsertShadowObservation(context.Background(), o)
	if err != nil || !ok {
		ts.Fatalf("inserting the observation: ok=%v err=%v", ok, err)
	}
	ts.Logf("observed %s start of %s", src, cmdText)
}

var _ = testing.Short
