package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
)

// The explain read path (M5-01): keyed pages, never OFFSET, and query plans
// that search the indexes instead of scanning the history tables.

// plantExplainTick inserts one tick row the way the writers do, without going
// through a whole scheduler loop: these tests hold the read contracts, not the
// write ones.
func plantExplainTick(t *testing.T, s *Store, kind, name string, at time.Time, outcome string) string {
	t.Helper()
	tickID := "01JEXPLAIN" + strings.ToUpper(strings.Repeat("0", 10)) + kind[:1] + name[:1] + at.Format("0102150405")
	stmt := `INSERT INTO ticks (id, source_kind, source_name, scheduled_for,
started_at, last_started_at, finished_at, duration_ms, repeat_count, outcome,
reason_code, reason_text, reason_data, trigger_count, deduped_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var code any
	if outcome != "triggered" && outcome != "running" {
		code = string(reason.TICKSkippedPaused)
	}
	if _, err := s.w.Exec(stmt, tickID, kind, name,
		at.UnixMilli(), at.UnixMilli(), at.UnixMilli(), at.UnixMilli()+5, int64(7), 1,
		outcome, code, "because", `{"k":"v"}`, 0, 0); err != nil {
		t.Fatalf("plant a tick: %v", err)
	}
	return tickID
}

// explainTestTickID reads back the id of the one tick planted for a source at
// a fire-time; the write API mints ids internally, so the read tests look them
// up by their unique (source, fire-time) pair.
func explainTestTickID(t *testing.T, s *Store, sourceName string, fire time.Time) string {
	t.Helper()
	var tickID string
	if err := s.r.QueryRow(`SELECT id FROM ticks WHERE source_name = ? AND scheduled_for = ?`,
		sourceName, fire.UnixMilli()).Scan(&tickID); err != nil {
		t.Fatalf("find the tick of %s at %s: %v", sourceName, fire, err)
	}
	return tickID
}

// TestExplainQueryPlansSearchNotScan holds the plan contract on exactly the
// SQL production executes: every core read searches an index and none of
// ticks, runs or run_events is ever scanned in full.
func TestExplainQueryPlansSearchNotScan(t *testing.T) {
	s := migratedStore(t)

	cases := []struct {
		name  string
		query string
		args  []any
		table string
	}{
		{"one source", explainTicksSQL(1), []any{"schedule", "nightly/daily", int64(0), "", "", 50}, "ticks"},
		{"two sources", explainTicksSQL(2), []any{"schedule", "j/a", "sensor", "b", int64(0), "", "", 50}, "ticks"},
		{"run prefix", explainRunsPrefixSQL, []any{"01J000000000000000000000000", "01J100000000000000000000000", 10}, "runs"},
		{"newest run", explainNewestRunSQL(false), []any{"nightly"}, "runs"},
		{"newest success", explainNewestRunSQL(true), []any{"nightly", "succeeded"}, "runs"},
		{"runs by job", explainRunsByJobSQL(), []any{"nightly", "", "", 50}, "runs"},
		{"run events", explainRunEventsSQL, []any{"01J000000000000000000000000"}, "run_events"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := queryPlan(t, s, tc.query, tc.args...)
			if strings.Contains(plan, "SCAN TABLE "+tc.table) {
				t.Errorf("the plan scans %s in full, want a search:\n%s", tc.table, plan)
			}
			if !strings.Contains(plan, "SEARCH") {
				t.Errorf("the plan has no SEARCH line, want an index-driven read:\n%s", plan)
			}
		})
	}
}

// TestExplainTicksKeysetPagination walks a history page by page through the
// same call the report builder makes, and expects every row exactly once in
// descending id order. An OFFSET would also pass the count checks below only
// by re-reading what it skips; the id continuity is what pins the keyset.
func TestExplainTicksKeysetPagination(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	base := time.Date(2026, 8, 21, 2, 0, 0, 0, time.UTC)
	want := make([]string, 0, 7)
	for i := range 7 {
		id := plantExplainTick(t, s, "schedule", "nightly/daily", base.Add(time.Duration(i)*time.Minute), "skipped")
		want = append(want, id)
	}

	var got []string
	before := ""
	for page := 0; page < 10; page++ {
		ticks, err := s.ExplainTicks(ctx, []ExplainSource{{Kind: "schedule", Name: "nightly/daily"}},
			time.Time{}, before, 3)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(ticks) == 0 {
			break
		}
		for _, tick := range ticks {
			got = append(got, tick.ID)
		}
		if len(ticks) > 3 {
			t.Fatalf("page %d returned %d rows, more than the page size", page, len(ticks))
		}
		before = ticks[len(ticks)-1].ID
	}

	if len(got) != len(want) {
		t.Fatalf("walked %d rows, planted %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[len(want)-1-i] {
			t.Fatalf("row %d is %s, want the next older id %s", i, got[i], want[len(want)-1-i])
		}
	}
}

// TestExplainTicksFiltersSourcesAndSince holds the two filters that make one
// query serve all four subcommands: only the named sources, only the window.
func TestExplainTicksFiltersSourcesAndSince(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	base := time.Date(2026, 8, 21, 2, 0, 0, 0, time.UTC)
	inWindow := plantExplainTick(t, s, "schedule", "nightly/daily", base.Add(time.Minute), "skipped")
	plantExplainTick(t, s, "schedule", "other/nightly", base.Add(2*time.Minute), "skipped")
	old := plantExplainTick(t, s, "schedule", "nightly/daily", base.Add(-time.Hour), "skipped")

	sources := []ExplainSource{
		{Kind: "schedule", Name: "nightly/daily"},
		{Kind: "sensor", Name: "dropzone"},
	}
	ticks, err := s.ExplainTicks(ctx, sources, base, "", 50)
	if err != nil {
		t.Fatalf("ExplainTicks: %v", err)
	}
	if len(ticks) != 1 || ticks[0].ID != inWindow {
		t.Fatalf("got %d ticks (%v), want exactly %s inside the window", len(ticks), ticks, inWindow)
	}

	// A since of zero reads the whole history for the sources.
	all, err := s.ExplainTicks(ctx, sources, time.UnixMilli(0), "", 50)
	if err != nil {
		t.Fatalf("ExplainTicks from the beginning: %v", err)
	}
	foundOld := false
	for _, tick := range all {
		if tick.ID == old {
			foundOld = true
		}
		if tick.SourceName == "other/nightly" {
			t.Errorf("a foreign schedule leaked into the result: %+v", tick)
		}
	}
	if !foundOld {
		t.Errorf("the older tick of the requested source is missing from %d rows", len(all))
	}
}

// seedExplainJob creates the job, one schedule and one sensor the chain tests
// share. The generous ceiling keeps admission out of the way.
func seedExplainJob(t *testing.T, s *Store) ScheduleRow {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:       "import-file",
		SpecHash:      "sha256:explain",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"import-file","max_concurrent":3,"steps":[{"name":"parse","run":["true"]}]}`,
		MaxConcurrent: 3,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	row, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName:    "import-file",
		Name:       "hourly",
		Kind:       "interval",
		Expr:       "@every 1h",
		Timezone:   "UTC",
		NextTickAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	return row
}

// TestExplainTriggerChainRoundTrip materialises one triggered tick through the
// real writer and reads the decision back: the trigger hangs off the tick,
// carries the accepted outcome, and names the run it produced.
func TestExplainTriggerChainRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	sch := seedExplainJob(t, s)

	res, err := s.MaterializeTick(ctx, TickInput{
		Schedule:       sch,
		ScheduledFor:   time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC),
		Outcome:        OutcomeTriggered,
		RunKey:         "2026-09-24T11:00:00Z",
		NextTickAt:     time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
		UpdateProgress: true,
	})
	if err != nil || !res.Claimed {
		t.Fatalf("materialise the tick: %v (claimed=%v)", err, res.Claimed)
	}
	tickID := explainTestTickID(t, s, "import-file/hourly",
		time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC))

	grouped, err := s.ExplainTriggersByTicks(ctx, []string{tickID})
	if err != nil {
		t.Fatalf("ExplainTriggersByTicks: %v", err)
	}
	list := grouped[tickID]
	if len(list) != 1 {
		t.Fatalf("got %d triggers for the tick, want 1", len(list))
	}
	trigger := list[0]
	if trigger.Outcome != "accepted" || trigger.JobName != "import-file" ||
		trigger.RunKey != "2026-09-24T11:00:00Z" {
		t.Errorf("the trigger lost its columns: %+v", trigger)
	}
	// An accepted trigger points forward through the run's trigger_id; the
	// reverse pointer is reserved for the dedup answer.
	if res.Run.TriggerID != trigger.ID {
		t.Errorf("the run names trigger %q, want %q", res.Run.TriggerID, trigger.ID)
	}

	byID, ok, err := s.ExplainTriggerByID(ctx, trigger.ID)
	if err != nil || !ok {
		t.Fatalf("ExplainTriggerByID: found=%v err=%v", ok, err)
	}
	if byID.ID != trigger.ID || byID.RunID != trigger.RunID {
		t.Errorf("the whole-id lookup disagrees with the tick-keyed one: %+v vs %+v", byID, trigger)
	}
}

// TestExplainRunEventsOldestFirst reads a run's event chain back in write
// order, which is the order the timeline renders as children.
func TestExplainRunEventsOldestFirst(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	sch := seedExplainJob(t, s)

	res, err := s.MaterializeTick(ctx, TickInput{
		Schedule:       sch,
		ScheduledFor:   time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC),
		Outcome:        OutcomeTriggered,
		RunKey:         "key-1",
		NextTickAt:     time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
		UpdateProgress: true,
	})
	if err != nil || !res.Claimed {
		t.Fatalf("materialise the tick: %v", err)
	}
	runID := res.Run.ID
	events := []RunEvent{
		{RunID: runID, Kind: "run.state", FromState: "queued", ToState: "running", Actor: "worker"},
		{RunID: runID, StepName: "parse", Kind: "step.state", FromState: "pending", ToState: "running", Actor: "worker"},
		{RunID: runID, StepName: "parse", Kind: "step.state", FromState: "running", ToState: "succeeded", ReasonCode: string(reason.STEPSucceeded), Actor: "worker"},
	}
	for _, e := range events {
		if err := s.AppendRunEvent(ctx, e); err != nil {
			t.Fatalf("append an event: %v", err)
		}
	}

	got, err := s.ExplainRunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("ExplainRunEvents: %v", err)
	}
	if len(got) < len(events) {
		t.Fatalf("read %d events, wrote %d (the queued event may precede)", len(got), len(events))
	}
	for i := 1; i < len(got); i++ {
		if got[i].At.Before(got[i-1].At) && got[i].RunID == runID {
			t.Errorf("event %d is older than its predecessor: %+v before %+v", i, got[i], got[i-1])
		}
	}
	last := got[len(got)-1]
	if last.StepName != "parse" || last.ToState != "succeeded" ||
		last.ReasonCode != string(reason.STEPSucceeded) || last.Actor != "worker" {
		t.Errorf("the newest event is not the one written last: %+v", last)
	}
}

// TestExplainRunsByPrefixListsEveryCandidate holds the git-style resolution:
// every match comes back so an ambiguous prefix can name its candidates.
func TestExplainRunsByPrefixListsEveryCandidate(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	sch := seedExplainJob(t, s)

	stamp := time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC)
	clk := clock.NewFake(stamp)
	s2, err := openWithClock(s.path, clk)
	if err != nil {
		t.Fatalf("reopen on the fake clock: %v", err)
	}
	defer func() { _ = s2.Close() }()

	var ids []string
	for i := range 2 {
		res, err := s2.MaterializeTick(ctx, TickInput{
			Schedule:       sch,
			ScheduledFor:   stamp.Add(time.Duration(i) * time.Hour),
			Outcome:        OutcomeTriggered,
			RunKey:         "prefix-" + string(rune('a'+i)),
			NextTickAt:     stamp.Add(time.Duration(i+1) * time.Hour),
			UpdateProgress: true,
		})
		if err != nil || !res.Claimed {
			t.Fatalf("materialise run %d: %v", i, err)
		}
		ids = append(ids, res.Run.ID)
	}

	prefix := ids[0][:8]
	got, err := s2.ExplainRunsByPrefix(ctx, prefix, 10)
	if err != nil {
		t.Fatalf("ExplainRunsByPrefix: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("prefix %q matched %d runs (%v), want both candidates", prefix, len(got), got)
	}
	if got[0].ID >= got[1].ID {
		t.Errorf("candidates are not in id order: %s then %s", got[0].ID, got[1].ID)
	}

	whole, err := s2.ExplainRunsByPrefix(ctx, ids[1], 10)
	if err != nil {
		t.Fatalf("ExplainRunsByPrefix whole id: %v", err)
	}
	if len(whole) != 1 || whole[0].ID != ids[1] || whole[0].State != "queued" {
		t.Errorf("a whole id resolves to exactly itself queued: %v", whole)
	}
}

// openWithClock reopens the same database file on a chosen clock, the way the
// harness does for fixtures that need deterministic ids.
func openWithClock(path string, clk clock.Clock) (*Store, error) {
	return Open(context.Background(), path, Options{Clock: clk})
}

// TestExplainOutagesSinceWindow reads only the downtime overlapping the
// window, newest first, with the missed-tick count intact.
func TestExplainOutagesSinceWindow(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	recent, err := s.RecordOutage(ctx, OutageInput{
		From: time.Date(2026, 8, 19, 13, 58, 0, 0, time.UTC),
		To:   time.Date(2026, 8, 19, 14, 7, 0, 0, time.UTC),
		Kind: "crash",
	})
	if err != nil {
		t.Fatalf("record the recent outage: %v", err)
	}
	ancient := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.RecordOutage(ctx, OutageInput{
		From: ancient, To: ancient.Add(9 * time.Minute), Kind: "boot",
	}); err != nil {
		t.Fatalf("record the ancient outage: %v", err)
	}

	got, err := s.ExplainOutagesSince(ctx, time.Date(2026, 8, 19, 13, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ExplainOutagesSince: %v", err)
	}
	if len(got) != 1 || got[0].ID != recent {
		t.Fatalf("got %+v, want only outage %d", got, recent)
	}
	if d := got[0].To.Sub(got[0].From); d != 9*time.Minute {
		t.Errorf("the outage's span is %s, want 9m", d)
	}
}

// TestScheduleReadsCarryEveryScannedColumn pins the projection contract the
// schedule reads broke once (overlap was added to scanTargets but not to the
// SELECTs, so every read died on its first row): a seeded row must come back
// whole through both the single lookup and the listing.
func TestScheduleReadsCarryEveryScannedColumn(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	seedScheduleJob(t, s)

	seeded := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	in := ScheduleInput{
		JobName: "nightly", Name: "due-late", Kind: "cron", Expr: "*/5 * * * *",
		Timezone: "UTC", Overlap: "queue", NextTickAt: seeded,
	}
	row, err := s.UpsertSchedule(ctx, in)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := s.GetSchedule(ctx, "nightly", "due-late")
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.Overlap != "queue" || got.Name != "due-late" || !got.NextTickAt.Equal(seeded) {
		t.Errorf("the single read lost columns: overlap=%q name=%q next=%s", got.Overlap, got.Name, got.NextTickAt)
	}
	_ = row

	all, err := s.ListAllSchedules(ctx)
	if err != nil {
		t.Fatalf("ListAllSchedules: %v", err)
	}
	if len(all) != 1 || all[0].Overlap != "queue" {
		t.Errorf("the listing lost columns: %+v", all)
	}
}

// TestExplainJobFactsSummaryInputs reads the summary facts: existence, pause,
// ceiling, active runs and the earliest next tick.
func TestExplainJobFactsSummaryInputs(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	facts, err := s.ExplainJobFacts(ctx, "import-file")
	if err != nil || facts.Found {
		t.Fatalf("a missing job must read as not found: %+v %v", facts, err)
	}

	sch := seedExplainJob(t, s)
	facts, err = s.ExplainJobFacts(ctx, "import-file")
	if err != nil {
		t.Fatalf("ExplainJobFacts: %v", err)
	}
	if !facts.Found || facts.Paused || facts.MaxConcurrent != 3 {
		t.Errorf("facts = %+v, want present unpaused ceiling 3", facts)
	}

	active, err := s.ExplainActiveRuns(ctx, "import-file")
	if err != nil {
		t.Fatalf("ExplainActiveRuns: %v", err)
	}
	if active != 0 {
		t.Errorf("%d active runs in a fresh store, want 0", active)
	}

	next, ok, err := s.ExplainNextScheduleTick(ctx, "import-file")
	if err != nil || !ok {
		t.Fatalf("ExplainNextScheduleTick: ok=%v err=%v", ok, err)
	}
	if !next.Equal(time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("next tick %s, want the seeded cursor", next)
	}

	if _, found, err := s.ExplainNewestRun(ctx, "import-file", ""); err != nil || found {
		t.Fatalf("no run exists yet: found=%v err=%v", found, err)
	}
	_ = sch
}
