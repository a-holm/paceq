package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// The coalescing half of #216. The ticks schema states it as a sizing
// property, and SensorTicks repeats it to its callers: repeated identical
// skips fold onto one row, so a sensor that finds nothing all day is one
// legible row rather than one row per evaluation. These tests hold the writer
// to that, and hold the fold predicate strict enough that folding never hides
// a change an operator needs to see.

// sensorTickRow is one ticks row as the coalescing tests read it.
type sensorTickRow struct {
	ID            string
	StartedAt     int64
	LastStartedAt int64
	FinishedAt    int64
	RepeatCount   int
	TriggerCount  int
	Outcome       string
	ReasonCode    string
	ReasonText    string
	CursorBefore  string
}

// sensorTickRows reads every tick of the test sensor, oldest first.
func sensorTickRows(t *testing.T, s *Store) []sensorTickRow {
	t.Helper()
	rows, err := s.r.QueryContext(context.Background(),
		`SELECT id, started_at, last_started_at, COALESCE(finished_at, 0), repeat_count,
trigger_count, outcome, COALESCE(reason_code, ''), COALESCE(reason_text, ''),
COALESCE(cursor_before, '')
FROM ticks WHERE source_kind = 'sensor' AND source_name = ?
ORDER BY started_at`, sensorName)
	if err != nil {
		t.Fatalf("read the ticks of %s: %v", sensorName, err)
	}
	defer func() { _ = rows.Close() }()

	var out []sensorTickRow
	for rows.Next() {
		var r sensorTickRow
		if err := rows.Scan(&r.ID, &r.StartedAt, &r.LastStartedAt, &r.FinishedAt,
			&r.RepeatCount, &r.TriggerCount, &r.Outcome, &r.ReasonCode,
			&r.ReasonText, &r.CursorBefore); err != nil {
			t.Fatalf("scan a tick of %s: %v", sensorName, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the ticks of %s: %v", sensorName, err)
	}
	return out
}

// sensorEvaluation is one whole evaluation, intention row and commit, at a
// pinned instant. It is the pair the daemon and `paceq sensors tick` both
// perform, in the order they perform it.
type sensorEvaluation struct {
	At         time.Time
	Outcome    string
	ReasonCode reason.Code
	ReasonText string
	Cursor     string
	Session    string
	Triggers   []SensorTrigger
}

func runSensorEvaluation(t *testing.T, s *Store, ev sensorEvaluation) SensorTickCommitResult {
	t.Helper()
	ctx := context.Background()
	cursor := ev.Cursor
	if cursor == "" {
		cursor = "a"
	}
	begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName: sensorName, CursorBefore: cursor,
		DaemonSessionID: ev.Session, Now: ev.At,
	})
	if err != nil {
		t.Fatalf("begin the sensor tick at %s: %v", ev.At, err)
	}
	out, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    sensorName,
		JobName:       sensorJob,
		CursorVersion: begin.CursorVersion,
		CursorAfter:   "b",
		Triggers:      ev.Triggers,
		Outcome:       ev.Outcome,
		ReasonCode:    ev.ReasonCode,
		ReasonText:    ev.ReasonText,
		NextEvalAt:    ev.At.Add(30 * time.Second).UnixMilli(),
		DurationMs:    4,
		Now:           ev.At,
	})
	if err != nil {
		t.Fatalf("commit the sensor tick at %s: %v", ev.At, err)
	}
	if out.Fenced {
		t.Fatalf("the commit at %s was fenced; these evaluations are sequential", ev.At)
	}
	return out
}

// skipAt is one evaluation that ran and found nothing.
func skipAt(at time.Time, text string) sensorEvaluation {
	return sensorEvaluation{
		At: at, Outcome: OutcomeSkipped,
		ReasonCode: reason.TICKSkippedSensor, ReasonText: text,
	}
}

func withCursor(ev sensorEvaluation, cursor string) sensorEvaluation {
	ev.Cursor = cursor
	return ev
}

func withSession(ev sensorEvaluation, session string) sensorEvaluation {
	ev.Session = session
	return ev
}

// TestSensorSkipsCoalesceOntoOneRow is the sizing property the ticks schema
// states: a sensor that finds the same nothing every 30 seconds leaves one
// row, not one per evaluation. started_at names the first evaluation and
// last_started_at the most recent, so the row spans the whole quiet stretch.
func TestSensorSkipsCoalesceOntoOneRow(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	const evaluations = 100
	base := time.UnixMilli(1_700_000_000_000).UTC()
	for i := range evaluations {
		out := runSensorEvaluation(t, s, skipAt(base.Add(time.Duration(i)*30*time.Second), "no new files"))
		// The caller is told which of its evaluations left a row, because a
		// forced tick that folds looks like a tick that did nothing.
		if want := i > 0; out.Coalesced != want {
			t.Fatalf("evaluation %d reports Coalesced %v, want %v", i, out.Coalesced, want)
		}
	}

	rows := sensorTickRows(t, s)
	if len(rows) != 1 {
		t.Fatalf("%d identical skips left %d ticks rows, want 1: the schema promises they coalesce",
			evaluations, len(rows))
	}
	got := rows[0]
	if got.RepeatCount != evaluations {
		t.Errorf("repeat_count = %d, want %d", got.RepeatCount, evaluations)
	}
	if want := base.UnixMilli(); got.StartedAt != want {
		t.Errorf("started_at = %d, want %d: the row keeps the first evaluation", got.StartedAt, want)
	}
	last := base.Add((evaluations - 1) * 30 * time.Second).UnixMilli()
	if got.LastStartedAt != last {
		t.Errorf("last_started_at = %d, want %d: the row moves to the newest evaluation",
			got.LastStartedAt, last)
	}
	if got.FinishedAt != last {
		t.Errorf("finished_at = %d, want %d", got.FinishedAt, last)
	}
	if got.Outcome != OutcomeSkipped || got.ReasonCode != string(reason.TICKSkippedSensor) {
		t.Errorf("the coalesced row is %s/%s, want %s/%s",
			got.Outcome, got.ReasonCode, OutcomeSkipped, reason.TICKSkippedSensor)
	}
}

// TestSensorSkipFoldPredicate holds the fold strict. Coalescing is only
// legitimate where the absorbed evaluation says exactly what the row already
// says; anything an operator would read as a change starts its own row.
func TestSensorSkipFoldPredicate(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	at := func(n int) time.Time { return base.Add(time.Duration(n) * 30 * time.Second) }

	cases := []struct {
		name     string
		evals    []sensorEvaluation
		wantRows int
	}{
		{
			name:     "the same skip twice folds",
			evals:    []sensorEvaluation{skipAt(at(0), "no new files"), skipAt(at(1), "no new files")},
			wantRows: 1,
		},
		{
			name: "a different reason code starts a row",
			evals: []sensorEvaluation{
				skipAt(at(0), "no new files"),
				{At: at(1), Outcome: OutcomeSkipped, ReasonCode: reason.TICKSkippedPaused, ReasonText: "no new files"},
			},
			wantRows: 2,
		},
		{
			name: "a different reason text starts a row",
			evals: []sensorEvaluation{
				skipAt(at(0), "no new files"),
				skipAt(at(1), "3 files, all seen"),
			},
			wantRows: 2,
		},
		{
			name: "a triggered evaluation never absorbs a later skip",
			evals: []sensorEvaluation{
				{At: at(0), Outcome: OutcomeTriggered, Triggers: []SensorTrigger{{RunKey: "file:1"}}},
				skipAt(at(1), "no new files"),
			},
			wantRows: 2,
		},
		{
			name: "an errored evaluation between two skips breaks the run",
			evals: []sensorEvaluation{
				skipAt(at(0), "no new files"),
				{At: at(1), Outcome: OutcomeError, ReasonCode: reason.TICKErrorSensorFailed, ReasonText: "exit 1"},
				skipAt(at(2), "no new files"),
			},
			wantRows: 3,
		},
		{
			name: "a moved cursor starts a row",
			evals: []sensorEvaluation{
				withCursor(skipAt(at(0), "no new files"), "a"),
				withCursor(skipAt(at(1), "no new files"), "z"),
			},
			wantRows: 2,
		},
		{
			name: "another daemon session starts a row",
			evals: []sensorEvaluation{
				withSession(skipAt(at(0), "no new files"), "session-1"),
				withSession(skipAt(at(1), "no new files"), "session-2"),
			},
			wantRows: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := migratedStore(t)
			seedSensorJob(t, s)
			seedSensor(t, s, sensorName, "a", 0)
			for _, ev := range tc.evals {
				runSensorEvaluation(t, s, ev)
			}
			if rows := sensorTickRows(t, s); len(rows) != tc.wantRows {
				t.Fatalf("%d evaluations left %d ticks rows, want %d",
					len(tc.evals), len(rows), tc.wantRows)
			}
		})
	}
}

// TestSensorSkipNeverAbsorbsIntoARowWithTriggers holds the clause that keeps a
// fold from swallowing work. The commit path writes no skip that owns a
// trigger today, so the row is seeded directly: the predicate has to refuse it
// on the count rather than on the outcome, because a row that produced a run
// records something no later skip may claim to be a repeat of.
func TestSensorSkipNeverAbsorbsIntoARowWithTriggers(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	base := time.UnixMilli(1_700_000_000_000).UTC()
	at := base.UnixMilli()
	if _, err := s.w.Exec(`INSERT INTO ticks
(id, source_kind, source_name, started_at, last_started_at, finished_at, outcome,
 reason_code, reason_text, trigger_count, cursor_before)
VALUES (?, 'sensor', ?, ?, ?, ?, 'skipped', ?, ?, 1, 'a')`,
		"tick-with-a-trigger", sensorName, at, at, at,
		string(reason.TICKSkippedSensor), "no new files"); err != nil {
		t.Fatalf("seed a skipped tick that owns a trigger: %v", err)
	}

	runSensorEvaluation(t, s, skipAt(base.Add(30*time.Second), "no new files"))

	if rows := sensorTickRows(t, s); len(rows) != 2 {
		t.Fatalf("a skip folded into a row that produced a run: %d ticks rows, want 2", len(rows))
	}
}

// TestSensorSkipFoldLeavesNoRunningRow is the crash story: folding closes the
// evaluation by absorbing it, so the intention row must be gone rather than
// left behind in 'running' for reconciliation to close a second time.
func TestSensorSkipFoldLeavesNoRunningRow(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	base := time.UnixMilli(1_700_000_000_000).UTC()
	runSensorEvaluation(t, s, skipAt(base, "no new files"))
	runSensorEvaluation(t, s, skipAt(base.Add(30*time.Second), "no new files"))

	for _, row := range sensorTickRows(t, s) {
		if row.Outcome == "running" {
			t.Errorf("tick %s is still running after its evaluation committed", row.ID)
		}
	}
}
