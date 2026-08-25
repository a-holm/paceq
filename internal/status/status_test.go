package status

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The classification and rendering half of status (#30). The store tests pin
// what is read; these pin how it reads: which states are deviations, that
// deviations sort to the top, that hints exist only on deviations, and that
// every mark has an ASCII answer.

func TestIsDeviation(t *testing.T) {
	deviations := map[string]bool{
		StateFailed:      true,
		StateStuck:       true,
		StateSLABreached: true,
		StateOK:          false,
		StatePaused:      false,
		StateIdle:        false,
	}
	for state, want := range deviations {
		if got := IsDeviation(state); got != want {
			t.Errorf("IsDeviation(%q) = %t, want %t", state, got, want)
		}
	}
}

// TestHintOnlyOnDeviations pins the R12 rule in both directions: a deviation
// always carries a runnable command, and nothing else ever does - a permanent
// hint footer would be noise nobody reads.
func TestHintOnlyOnDeviations(t *testing.T) {
	for _, state := range []string{StateFailed, StateStuck, StateSLABreached} {
		hint := HintFor(state, "db-backup")
		if hint == "" {
			t.Errorf("%s carries no hint", state)
			continue
		}
		if !strings.HasPrefix(hint, "paceq explain ") {
			t.Errorf("%s hint %q is not a runnable paceq command", state, hint)
		}
	}
	for _, state := range []string{StateOK, StateIdle, StatePaused} {
		if hint := HintFor(state, "db-backup"); hint != "" {
			t.Errorf("%s grew a hint %q; only deviations get one", state, hint)
		}
	}
}

func TestSortJobsPutsDeviationsFirst(t *testing.T) {
	jobs := []Job{
		{Name: "zephyr", State: StateOK},
		{Name: "nightly-report", State: StateFailed},
		{Name: "aaa-idle", State: StateIdle},
		{Name: "stale-cleanup", State: StateStuck},
		{Name: "midway", State: StatePaused},
	}
	sortJobs(jobs)

	want := []string{"nightly-report", "stale-cleanup", "aaa-idle", "midway", "zephyr"}
	got := make([]string, 0, len(jobs))
	for _, j := range jobs {
		got = append(got, j.Name)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v (deviations first, then by name)", got, want)
		}
	}
}

func TestSummarizeCounts(t *testing.T) {
	now := time.Date(2026, 12, 9, 8, 0, 0, 0, time.UTC)
	stamp := func(offset time.Duration) *LastRun {
		return &LastRun{FinishedAt: now.Add(-offset).UTC().Format("2006-01-02T15:04:05Z")}
	}
	jobs := []Job{
		// Two failures: one inside the 24h counter's window, one older.
		{Name: "a", State: StateFailed, LastRun: stamp(time.Hour)},
		{Name: "b", State: StateFailed, LastRun: stamp(30 * time.Hour)},
		{Name: "c", State: StateStuck},
		{Name: "d", State: StateSLABreached},
		{Name: "e", State: StateOK},
		{Name: "f", State: StatePaused},
		{Name: "g", State: StateIdle},
	}
	s := summarize(jobs, len(jobs), now)
	if s.Jobs != 7 || s.Deviations != 4 || s.SLABreached != 1 || s.Failed24h != 1 {
		t.Errorf("summary = %+v, want jobs 7 deviations 4 sla 1 failed24h 1", s)
	}
	// The paused job counts as a job but never as a deviation; the old
	// failure stays a deviation while dropping out of the day counter.
	if s.Deviations != 4 {
		t.Errorf("deviation count = %d, want 4", s.Deviations)
	}
}

// TestRenderTextGolden pins the whole screen for the mixed fleet: aggregate
// line, deviations first, hint lines under deviation rows only. Frozen
// stamps keep it deterministic.
func TestRenderTextGolden(t *testing.T) {
	rep := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   "2026-12-09T08:20:11Z",
		Daemon:        Daemon{Up: true, Since: "2026-12-03T04:02:55Z", Version: "0.1.0"},
		Summary:       Summary{Jobs: 4, Deviations: 2, Running: 1, Queued: 0, Failed24h: 1},
		Jobs: []Job{
			{
				Name: "db-backup", State: StateFailed,
				LastRun:   &LastRun{ID: "01K5A", StartedAt: "2026-12-09T03:00:01Z", FinishedAt: "2026-12-09T03:00:07Z", Outcome: "failed", DurationMS: 6000, ReasonCode: "RUN_FAILED_STEP"},
				NextRunAt: "2026-12-10T03:00:00Z", Hint: "paceq explain job db-backup",
			},
			{
				Name: "stale-cleanup", State: StateStuck,
				Hint: "paceq explain job stale-cleanup",
			},
			{
				Name: "sync-files", State: StateOK,
				LastRun:   &LastRun{ID: "01K5B", StartedAt: "2026-12-09T08:15:00Z", FinishedAt: "2026-12-09T08:16:03Z", Outcome: "succeeded", DurationMS: 63000},
				NextRunAt: "2026-12-09T09:15:00Z",
			},
			{Name: "metrics-export", State: StatePaused},
		},
	}

	var b bytes.Buffer
	RenderText(&b, rep, RenderOptions{Style: StyleASCII()})
	got := b.String()

	wantLines := []string{
		"2 deviations | 4 jobs | 1 running | 0 queued | daemon up 6d 4h",
		"JOB",
	}
	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Errorf("the table lost %q:\n%s", want, got)
		}
	}
	// Deviations first...
	dbPos := strings.Index(got, "db-backup")
	stalePos := strings.Index(got, "stale-cleanup")
	syncPos := strings.Index(got, "sync-files")
	pausePos := strings.Index(got, "metrics-export")
	if !(dbPos >= 0 && dbPos < stalePos && stalePos < syncPos && syncPos < pausePos) {
		t.Errorf("rows out of order (deviations first):\n%s", got)
	}
	// Hints under the deviations, nowhere else:
	if n := strings.Count(got, "run `paceq explain job"); n != 2 {
		t.Errorf("%d hint lines on screen, want exactly one per deviation:\n%s", n, got)
	}
	if !strings.Contains(got, "last run did not succeed - run `paceq explain job db-backup`") {
		t.Errorf("the failed job's hint line is wrong:\n%s", got)
	}
	// Daemon uptime rendered from since -> generated_at:
	if !strings.Contains(got, "daemon up 6d 4h") {
		t.Errorf("daemon uptime missing:\n%s", got)
	}
}

// TestRenderDaemonDownMarksIt pins the daemon-down marker in text form; the
// JSON form's up:false is pinned by the CLI golden.
func TestRenderDaemonDownMarksIt(t *testing.T) {
	rep := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   "2026-12-09T08:20:11Z",
		Daemon:        Daemon{Up: false},
		Jobs:          []Job{{Name: "solo", State: StateOK}},
	}
	var b bytes.Buffer
	RenderText(&b, rep, RenderOptions{Style: StyleUnicode()})
	if !strings.Contains(b.String(), "daemon down") {
		t.Errorf("daemon down is not marked:\n%s", b.String())
	}
}

// TestRenderASCIIFallback draws the same report with both styles and proves
// the unicode marks never leak into the ASCII one.
func TestRenderASCIIFallback(t *testing.T) {
	rep := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   "2026-12-09T08:20:11Z",
		Daemon:        Daemon{Up: true},
		Jobs: []Job{
			{Name: "good", State: StateOK},
			{Name: "bad", State: StateFailed, LastRun: &LastRun{ID: "r1", FinishedAt: "2026-12-09T03:00:07Z", Outcome: "failed"}, Hint: "paceq explain job bad"},
			{Name: "held", State: StatePaused},
			{Name: "hung", State: StateStuck, Hint: "paceq explain job hung"},
			{Name: "fresh-start", State: StateIdle},
		},
	}

	var ascii bytes.Buffer
	RenderText(&ascii, rep, RenderOptions{Style: StyleASCII()})
	out := ascii.String()
	for _, forbidden := range []string{"✓", "✗", "⚠", "⏸", "·", "—", "└"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("ASCII rendering leaks %q:\n%s", forbidden, out)
		}
	}
	for _, want := range []string{"| ", "X", "-"} {
		if !strings.Contains(out, want) {
			t.Errorf("ASCII rendering lost %q:\n%s", want, out)
		}
	}

	var uni bytes.Buffer
	RenderText(&uni, rep, RenderOptions{Style: StyleUnicode()})
	uout := uni.String()
	for _, want := range []string{"✓", "✗", "⏸"} {
		if !strings.Contains(uout, want) {
			t.Errorf("unicode rendering lost %q:\n%s", want, uout)
		}
	}
}

// TestRenderFoldsLongLists pins the >40 fold: forty lines visible, the rest
// named by the fold line, --all lifting the cut.
func TestRenderFoldsLongLists(t *testing.T) {
	jobs := make([]Job, 0, DefaultVisibleJobs+7)
	// One deviation guarantees the sort puts it at the very top of the cut.
	jobs = append(jobs, Job{
		Name: "zz-broken", State: StateFailed,
		LastRun: &LastRun{FinishedAt: "2026-12-09T03:00:07Z", Outcome: "failed"},
		Hint:    "paceq explain job zz-broken",
	})
	for i := 0; i < DefaultVisibleJobs+6; i++ {
		jobs = append(jobs, Job{Name: strings.Repeat("j", 1) + fmt.Sprintf("%03d", i), State: StateOK})
	}
	sortJobs(jobs)
	rep := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   "2026-12-09T08:20:11Z",
		Jobs:          jobs,
	}

	var folded bytes.Buffer
	RenderText(&folded, rep, RenderOptions{Style: StyleASCII()})
	fout := folded.String()
	if !strings.Contains(fout, "and 7 more (paceq status --all)") {
		t.Errorf("the fold line is wrong or absent:\n%s", tailOf(fout))
	}
	if visible := countRows(fout); visible != DefaultVisibleJobs {
		t.Errorf("%d rows visible before the fold, want %d:\n%s", visible, DefaultVisibleJobs, tailOf(fout))
	}
	// The fold keeps the deviation inside the visible head.
	if bb := strings.Index(fout, "zz-broken"); bb < 0 || bb > strings.Index(fout, "and 7 more") {
		t.Errorf("the deviation fell outside the visible head:\n%s", fout)
	}

	var all bytes.Buffer
	RenderText(&all, rep, RenderOptions{Style: StyleASCII(), All: true})
	aout := all.String()
	if strings.Contains(aout, "and 7 more") {
		t.Errorf("--all still folds:\n%s", tailOf(aout))
	}
	if visible := countRows(aout); visible != len(jobs) {
		t.Errorf("--all shows %d of %d jobs", visible, len(jobs))
	}
}

// countRows counts the table's job lines: a row starts in column 0 with the
// name, so header and fold lines never match.
func countRows(out string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if line == "" {
			continue
		}
		if line[0] != ' ' && !strings.HasPrefix(line, "JOB") &&
			!strings.Contains(line, "more (paceq status") &&
			!strings.Contains(line, "deviations") &&
			!strings.Contains(line, "no jobs yet") {
			n++
		}
	}
	return n
}

func TestRelWhen(t *testing.T) {
	now := time.Date(2026, 12, 9, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		then time.Time
		want string
	}{
		{time.Date(2026, 12, 9, 2, 0, 0, 0, time.UTC), "02:00"},
		{time.Date(2026, 12, 9, 23, 5, 0, 0, time.UTC), "23:05"},
		{time.Date(2026, 12, 10, 3, 0, 0, 0, time.UTC), "tomorrow 03:00"},
		{time.Date(2026, 12, 8, 22, 0, 0, 0, time.UTC), "yesterday 22:00"},
		{time.Date(2026, 12, 1, 4, 0, 0, 0, time.UTC), "8d ago"},
		{time.Date(2026, 12, 19, 4, 0, 0, 0, time.UTC), "in 10d"},
	}
	for _, c := range cases {
		if got := relWhen(now, c.then); got != c.want {
			t.Errorf("relWhen(now, %s) = %q, want %q", c.then, got, c.want)
		}
	}
	if got := relWhen(now, time.Time{}); got != "" {
		t.Errorf("relWhen on the zero stamp = %q, want empty", got)
	}
}

func TestCompactDuration(t *testing.T) {
	cases := map[time.Duration]string{
		0:                 "0s",
		12 * time.Second:  "12s",
		63 * time.Second:  "1m 3s",
		252 * time.Second: "4m 12s",
		2 * time.Hour:     "2h",
		26 * time.Hour:    "1d 2h",
		148 * time.Hour:   "6d 4h",
	}
	for d, want := range cases {
		if got := compactDuration(d); got != want {
			t.Errorf("compactDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

func tailOf(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= 12 {
		return s
	}
	return "...(head cut)...\n" + strings.Join(lines[len(lines)-12:], "\n")
}
