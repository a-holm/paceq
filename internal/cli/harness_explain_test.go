package cli

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The plant helpers for the explain goldens. Like every command in
// harnesscmds_test.go they are not product code: they write fixture rows
// straight through the store API, never through the engine and never with raw
// SQL, so the golden scripts isolate the presentation layer.

// explainFixtureClock is the frozen clock a plant helper writes rows on. A
// script can move the clock per line by prefixing PULSEQ_FAKE_CLOCK, which is
// how a later tick gets a later stamp.
func explainFixtureClock(ts *testscript.TestScript) clock.Clock {
	return plantClock(ts)
}

// openFixtureWriter opens the state database for writing, on the fixture
// clock. Every helper opens, writes and closes: no handle ever outlives one
// command (the flock rule the CLI tests live under).
func openFixtureWriter(ts *testscript.TestScript) (*store.Store, func()) {
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	s, err := store.Open(ctx, dbPath, store.Options{Clock: explainFixtureClock(ts)})
	if err != nil {
		ts.Fatalf("could not open the state to plant a fixture: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		ts.Fatalf("could not migrate the state to plant a fixture: %v", err)
	}
	return s, func() { _ = s.Close() }
}

// ensureFixtureJob plants the job row a schedule or sensor needs, idempotently.
func ensureFixtureJob(ts *testscript.TestScript, s *store.Store, job string) string {
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       job,
		SpecHash:      "sha256:" + job,
		SpecJSON:      `{"schema":"paceq.job.v1","name":"` + job + `","max_concurrent":4,"steps":[{"name":"build","run":["/bin/true"]}]}`,
		MaxConcurrent: 4,
	})
	if err != nil {
		ts.Fatalf("could not plant job %s: %v", job, err)
	}
	return version.ID
}

// cmdPlantSchedule seeds one schedule row.
//
//	plantschedule nightly-report nightly "0 2 * * *" [paused]
func cmdPlantSchedule(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 || len(args) > 4 {
		ts.Fatalf("usage: plantschedule JOB NAME CRON [paused]")
	}
	s, done := openFixtureWriter(ts)
	defer done()

	job, name, expr := args[0], args[1], args[2]
	ensureFixtureJob(ts, s, job)
	next := explainFixtureClock(ts).Now().UTC().Add(time.Hour).Truncate(time.Minute)
	in := store.ScheduleInput{
		JobName: job, Name: name, Kind: "cron", Expr: expr, Timezone: "UTC",
		NextTickAt: next,
	}
	if len(args) == 4 && args[3] == "paused" {
		in.Paused = true
	}
	if _, err := s.UpsertSchedule(context.Background(), in); err != nil {
		ts.Fatalf("could not plant schedule %s/%s: %v", job, name, err)
	}
	ts.Logf("planted schedule %s/%s", job, name)
}

// cmdPlantScheduleTick records evaluations of one schedule through the real
// write path, so coalescing behaves exactly as production folds it. Repeating
// an identical skip N times leaves ONE row with repeat_count N.
//
//	plantscheduletick JOB SCHEDULE SKIPPED TICK_SKIPPED_PAUSED 12 [offset]
//	plantscheduletick JOB SCHEDULE TRIGGERED - key-1 [offset]
func cmdPlantScheduleTick(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 4 || len(args) > 5 {
		ts.Fatalf("usage: plantscheduletick JOB SCHEDULE SKIPPED CODE COUNT | ... TRIGGERED RUNKEY [COUNT]")
	}
	job, name, outcome, payload := args[0], args[1], strings.ToUpper(args[2]), args[3]
	count, offset := 1, time.Duration(0)
	switch {
	case len(args) == 5 && outcome == "TRIGGERED":
		if !strings.HasPrefix(args[4], "-at=") {
			ts.Fatalf("a triggered tick takes a run key and optionally -at=DURATION")
		}
		d, err := time.ParseDuration(strings.TrimPrefix(args[4], "-at="))
		if err != nil {
			ts.Fatalf("%q is not a duration offset: %v", args[4], err)
		}
		offset = d
	case len(args) == 5:
		count = parseCount(ts, args[4])
	}
	switch outcome {
	case "SKIPPED", "ERROR", "TRIGGERED":
	default:
		ts.Fatalf("%q is not an outcome a tick can take", outcome)
	}

	s, done := openFixtureWriter(ts)
	defer done()

	sch, err := s.GetSchedule(context.Background(), job, name)
	if err != nil {
		ts.Fatalf("could not read schedule %s/%s: %v", job, name, err)
	}

	base := explainFixtureClock(ts).Now().UTC().Add(-24*time.Hour + offset).Truncate(time.Minute)
	for i := range count {
		in := store.TickInput{
			Schedule:     sch,
			ScheduledFor: base.Add(time.Duration(i+1) * time.Minute),
			Outcome:      strings.ToLower(outcome),
		}
		switch outcome {
		case "TRIGGERED":
			in.RunKey = payload + "-" + itoa(i)
		default:
			in.ReasonCode = reason.Code(payload)
			in.ReasonText = "recorded by the explain fixture"
		}
		if _, err := s.MaterializeTick(context.Background(), in); err != nil {
			ts.Fatalf("could not materialise tick %d: %v", i, err)
		}
	}
	ts.Logf("planted %d %s tick(s) on %s/%s", count, outcome, job, name)
}

func parseCount(ts *testscript.TestScript, arg string) int {
	n := 0
	for _, c := range arg {
		if c < '0' || c > '9' {
			ts.Fatalf("%q is not a count", arg)
		}
		n = n*10 + int(c-'0')
	}
	if n <= 0 {
		ts.Fatalf("count must be positive, got %q", arg)
	}
	return n
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}

// cmdPlantOutage records one downtime window with its synthetic missed-tick
// evidence, exactly as startup reconciliation would: the outage row plus one
// TICK_MISSED_DAEMON_DOWN tick per due slot inside the gap.
//
//	plantoutage -9h -8h51m crash nightly-report/nightly 2
func cmdPlantOutage(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 5 {
		ts.Fatalf("usage: plantoutage FROM TO KIND SOURCE_NAME MISSED_COUNT")
	}
	from, err := time.ParseDuration(args[0])
	if err != nil {
		ts.Fatalf("%q is not a negative duration from now: %v", args[0], err)
	}
	to, err := time.ParseDuration(args[1])
	if err != nil {
		ts.Fatalf("%q is not a negative duration from now: %v", args[1], err)
	}
	kind := args[2]
	sourceName := args[3]
	missed := parseCount(ts, args[4])

	now := explainFixtureClock(ts).Now().UTC()
	s, done := openFixtureWriter(ts)
	defer done()

	outageID, err := s.RecordOutage(context.Background(), store.OutageInput{
		From: now.Add(from), To: now.Add(to), Kind: kind,
	})
	if err != nil {
		ts.Fatalf("could not record the outage: %v", err)
	}
	session, err := s.StartSession(context.Background(), "golden")
	if err != nil {
		ts.Fatalf("could not open the fixture session: %v", err)
	}
	ticks := make([]store.MissedTick, missed)
	// Spread the due slots evenly inside the gap.
	step := (to - from) / time.Duration(missed+1)
	for i := range ticks {
		ticks[i] = store.MissedTick{
			SourceName:   sourceName,
			ScheduledFor: now.Add(from).Add(step * time.Duration(i+1)),
		}
	}
	written, err := s.RecordMissedTicks(context.Background(), session.ID, outageID, ticks)
	if err != nil {
		ts.Fatalf("could not record the missed ticks: %v", err)
	}
	if written != missed {
		ts.Fatalf("wrote %d missed ticks, want %d", written, missed)
	}
	if err := s.StopSession(context.Background(), session.ID); err != nil {
		ts.Logf("the fixture session did not stop cleanly: %v", err)
	}
	ts.Logf("planted a %s outage with %d missed ticks", kind, written)
}

// cmdPlantSensorSkip records one finished-but-empty sensor evaluation through
// the real begin/commit path, so `explain sensor` has the same rows a daemon
// would have written. Each call is one tick row; sensors do not fold skips in
// this milestone, and the golden shows them as separate lines.
//
//	plantsensorskip SENSOR "no new files since 04:00" [-at=-3m]
//
// The optional -at offset moves the evaluation away from the shared fixture
// instant, because rows that share a started_at would otherwise order by
// their random id tails, which differ between runs.
func cmdPlantSensorSkip(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 2 || len(args) > 3 {
		ts.Fatalf("usage: plantsensorskip SENSOR REASONTEXT [-at=DURATION]")
	}
	sensorName, reasonText := args[0], args[1]
	offset := time.Duration(0)
	if len(args) == 3 {
		if !strings.HasPrefix(args[2], "-at=") {
			ts.Fatalf("%q is not a -at=DURATION option", args[2])
		}
		d, err := time.ParseDuration(strings.TrimPrefix(args[2], "-at="))
		if err != nil {
			ts.Fatalf("%q is not a duration: %v", args[2], err)
		}
		offset = d
	}
	s, done := openFixtureWriter(ts)
	defer done()

	ctx := context.Background()
	sum, err := s.GetSensor(ctx, sensorName)
	if err != nil {
		ts.Fatalf("could not read sensor %s: %v", sensorName, err)
	}
	at := explainFixtureClock(ts).Now().UTC().Add(offset)
	begin, err := s.BeginSensorTick(ctx, store.BeginSensorTickInput{
		SensorName: sensorName, Now: at,
	})
	if err != nil {
		ts.Fatalf("could not begin a sensor tick: %v", err)
	}
	if _, err := s.CommitSensorTick(ctx, store.SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    sensorName,
		JobName:       sum.JobName,
		CursorVersion: begin.CursorVersion,
		Outcome:       store.OutcomeSkipped,
		ReasonCode:    reason.TICKSkippedSensor,
		ReasonText:    reasonText,
		NextEvalAt:    explainFixtureClock(ts).Now().UnixMilli() + 60_000,
		DurationMs:    3,
		Now:           at,
	}); err != nil {
		ts.Fatalf("could not commit a skipped sensor tick: %v", err)
	}
	ts.Logf("planted a skipped tick on %s", sensorName)
}
