package explain

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// The fixtures here seed rows through the store API only - never through raw
// SQL, and never through the engine - so the tests isolate the presentation
// layer from everything that decides.

var frozenNow = time.Date(2026, 8, 21, 4, 32, 10, 0, time.UTC)

// fixtureStore opens a migrated store on a frozen clock.
func fixtureStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"),
		store.Options{Clock: clock.NewFake(frozenNow)})
	if err != nil {
		t.Fatalf("open the fixture store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate the fixture store: %v", err)
	}
	return st
}

// seedFixture creates one job with a schedule and a sensor, the cast every
// resolution case plays against. It returns the job version id for runs.
func seedFixture(t *testing.T, st *store.Store) string {
	t.Helper()
	ctx := context.Background()

	version, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       "nightly-report",
		SpecHash:      "sha256:fixture",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"nightly-report","max_concurrent":2,"steps":[{"name":"build","run":["/bin/true"]}]}`,
		MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if _, err := st.UpsertSchedule(ctx, store.ScheduleInput{
		JobName:    "nightly-report",
		Name:       "nightly",
		Kind:       "cron",
		Expr:       "0 2 * * *",
		Timezone:   "UTC",
		NextTickAt: frozenNow.Add(8 * time.Hour),
	}); err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	if err := st.UpsertSensor(ctx, store.SensorSeedInput{
		Name: "dropzone", JobName: "nightly-report", ExecJSON: `["/bin/echo","{}"]`,
	}); err != nil {
		t.Fatalf("seed the sensor: %v", err)
	}
	return version.ID
}

func TestParseRefForms(t *testing.T) {
	cases := []struct {
		in      string
		wantOK  bool
		wantK   RefKind
		wantPay string
	}{
		{in: "job/nightly-report", wantOK: true, wantK: KindJob, wantPay: "nightly-report"},
		{in: "jobs/nightly-report", wantOK: true, wantK: KindJob, wantPay: "nightly-report"},
		{in: "schedule/nightly-report.nightly", wantOK: true, wantK: KindSchedule, wantPay: "nightly-report.nightly"},
		{in: "schedule/nightly-report/nightly", wantOK: true, wantK: KindSchedule, wantPay: "nightly-report/nightly"},
		{in: "sensor/dropzone", wantOK: true, wantK: KindSensor, wantPay: "dropzone"},
		{in: "run/01JQ9F2M4K", wantOK: true, wantK: KindRun, wantPay: "01JQ9F2M4K"},
		{in: "bare-name", wantOK: true, wantK: "", wantPay: "bare-name"},
		{in: "job/", wantOK: false},
		{in: "schedule/", wantOK: false},
		{in: "sensor/", wantOK: false},
		{in: "run/", wantOK: false},
		{in: "  ", wantOK: false},
	}
	for _, tc := range cases {
		kind, payload, err := ParseRef(tc.in)
		if tc.wantOK {
			if err != nil {
				t.Errorf("ParseRef(%q) refused: %v", tc.in, err)
				continue
			}
			if kind != tc.wantK || payload != tc.wantPay {
				t.Errorf("ParseRef(%q) = (%q, %q), want (%q, %q)", tc.in, kind, payload, tc.wantK, tc.wantPay)
			}
			continue
		}
		var syntax *Syntax
		if !asSyntax(err, &syntax) {
			t.Errorf("ParseRef(%q) = (%q, %q, %v), want a syntax refusal", tc.in, kind, payload, err)
		}
	}
}

func asSyntax(err error, target **Syntax) bool {
	s, ok := err.(*Syntax)
	if ok {
		*target = s
	}
	return ok
}

// TestResolveExplicitForms walks the four explicit namespaces against seeded
// rows and checks the tick producers each subject carries.
func TestResolveExplicitForms(t *testing.T) {
	ctx := context.Background()
	st := fixtureStore(t)
	seedFixture(t, st)

	job, err := Resolve(ctx, st, "job/nightly-report")
	if err != nil {
		t.Fatalf("resolve job: %v", err)
	}
	if job.Kind != KindJob || len(job.Sources) != 2 {
		t.Errorf("job subject resolved to %+v with %d sources, want two (schedule + sensor)", job, len(job.Sources))
	}

	schedule, err := Resolve(ctx, st, "schedule/nightly-report.nightly")
	if err != nil {
		t.Fatalf("resolve schedule by job.name: %v", err)
	}
	if schedule.Kind != KindSchedule || schedule.Job != "nightly-report" || schedule.Schedule != "nightly" {
		t.Errorf("schedule subject is %+v", schedule)
	}
	if len(schedule.Sources) != 1 || schedule.Sources[0].Name != "nightly-report/nightly" {
		t.Errorf("the schedule's tick source is wrong: %+v", schedule.Sources)
	}
	slashed, err := Resolve(ctx, st, "schedule/nightly-report/nightly")
	if err != nil || slashed.Schedule != "nightly" {
		t.Errorf("schedule/job/name spelling refused: %+v %v", slashed, err)
	}

	sensor, err := Resolve(ctx, st, "sensor/dropzone")
	if err != nil {
		t.Fatalf("resolve sensor: %v", err)
	}
	if sensor.Kind != KindSensor || sensor.Job != "nightly-report" {
		t.Errorf("sensor subject is %+v", sensor)
	}
	if len(sensor.Sources) != 1 || sensor.Sources[0].Kind != "sensor" || sensor.Sources[0].Name != "dropzone" {
		t.Errorf("the sensor's tick source is wrong: %+v", sensor.Sources)
	}

	run, err := Resolve(ctx, st, "run/01JZZZZZZZZZZZZZZZZZZZZZZZ")
	if err == nil {
		t.Errorf("an unknown run resolved to %+v, want not-found", run)
		return
	}
	var missing *NotFound
	if !asNotFound(err, &missing) {
		t.Errorf("an unknown run must refuse as NotFound, got %T (%v)", err, err)
	}
}

func asNotFound(err error, target **NotFound) bool {
	nf, ok := err.(*NotFound)
	if ok {
		*target = nf
	}
	return ok
}

func asAmbiguous(err error, target **Ambiguous) bool {
	am, ok := err.(*Ambiguous)
	if ok {
		*target = am
	}
	return ok
}

// TestResolveBareNamesAndAmbiguity holds the heuristic: a bare name resolves
// when exactly one reading exists, and refuses naming every reading when it
// could mean several things.
func TestResolveBareNamesAndAmbiguity(t *testing.T) {
	ctx := context.Background()
	st := fixtureStore(t)
	seedFixture(t, st)

	bare, err := Resolve(ctx, st, "dropzone")
	if err != nil || bare.Kind != KindSensor {
		t.Fatalf("a bare sensor name resolved to %+v (%v), want the sensor", bare, err)
	}
	scheduleHit, err := Resolve(ctx, st, "nightly")
	if err != nil || scheduleHit.Kind != KindSchedule {
		t.Fatalf("a bare unique schedule name resolved to %+v (%v), want the schedule", scheduleHit, err)
	}

	// A second job whose name collides with the sensor makes "dropzone"
	// ambiguous between kinds; nothing may be guessed.
	version, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "dropzone",
		SpecHash: "sha256:collide",
		SpecJSON: `{"schema":"paceq.job.v1","name":"dropzone","steps":[{"name":"build","run":["/bin/true"]}]}`,
	})
	if err != nil {
		t.Fatalf("seed the colliding job: %v", err)
	}
	_ = version
	_, err = Resolve(ctx, st, "dropzone")
	var ambiguous *Ambiguous
	if !asAmbiguous(err, &ambiguous) {
		t.Fatalf("a name that reads two ways must refuse as Ambiguous, got %v", err)
	}
	foundJob, foundSensor := false, false
	for _, cand := range ambiguous.Candidates {
		switch cand {
		case "job/dropzone":
			foundJob = true
		case "sensor/dropzone":
			foundSensor = true
		}
	}
	if !foundJob || !foundSensor {
		t.Errorf("candidates %v miss one of the readings", ambiguous.Candidates)
	}

	_, err = Resolve(ctx, st, "no-such-thing")
	var missing *NotFound
	if !asNotFound(err, &missing) {
		t.Errorf("an unknown bare name must refuse as NotFound, got %v", err)
	}
}

// TestRunRefPrefixCandidates plants two runs sharing an id prefix and holds
// the git-style rule: the refusal names every candidate.
func TestRunRefPrefixCandidates(t *testing.T) {
	ctx := context.Background()
	st := fixtureStore(t)
	versionID := seedFixture(t, st)

	clk := clock.NewFake(time.Date(2026, 9, 24, 11, 0, 0, 0, time.UTC))
	frozen, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"), store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the frozen store: %v", err)
	}
	defer func() { _ = frozen.Close() }()
	if err := frozen.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	v2, _, err := frozen.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       "nightly-report",
		SpecHash:      "sha256:frozen",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"nightly-report","max_concurrent":2,"steps":[{"name":"build","run":["/bin/true"]}]}`,
		MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatalf("seed on the frozen clock: %v", err)
	}
	_ = versionID

	var ids []string
	for i := range 2 {
		run, err := frozen.CreateRunWithSteps(ctx, store.NewRun{
			JobName:      "nightly-report",
			JobVersionID: v2.ID,
			Origin:       "manual",
		})
		if err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
		ids = append(ids, run.ID)
	}

	resolved, err := Resolve(ctx, frozen, "run/"+ids[0])
	if err != nil || resolved.RunID != ids[0] {
		t.Fatalf("a whole id must resolve to itself: %+v (%v)", resolved, err)
	}

	_, err = Resolve(ctx, frozen, "run/"+ids[0][:12])
	var ambiguous *Ambiguous
	if !asAmbiguous(err, &ambiguous) {
		t.Fatalf("a shared prefix must refuse as Ambiguous, got %v", err)
	}
	if len(ambiguous.Candidates) != 2 {
		t.Errorf("candidates %v, want both runs named", ambiguous.Candidates)
		return
	}
	for i, cand := range ambiguous.Candidates {
		want := "run/" + ids[i] + " (nightly-report, queued)"
		if cand != want {
			t.Errorf("candidate %d is %q, want %q", i, cand, want)
		}
	}
}
