package spec_test

import (
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

// The overlap key rides on a schedule: what a firing does when the job's
// max_concurrent is already held. skip (the default) stands down, queue
// defers the run into the future (#68).

const overlapJob = "name: j\nsteps:\n  - name: build\n    run: [/bin/true]\nschedules:\n  - name: nightly\n    cron: \"0 3 * * *\"\n"

func parseOverlapJob(t *testing.T, schedule string) *spec.Job {
	t.Helper()
	job, diags := spec.Parse("jobs/j.yaml", []byte(overlapJob+schedule))
	if diags.HasErrors() {
		t.Fatalf("the job file was refused:\n%s", render(t, diags))
	}
	return job
}

func TestAScheduleCarriesItsOverlapPolicy(t *testing.T) {
	t.Parallel()

	job := parseOverlapJob(t, "    overlap: queue\n")
	if len(job.Schedules) != 1 || job.Schedules[0].Overlap != spec.OverlapQueue {
		t.Fatalf("overlap: queue did not survive the parse: %+v", job.Schedules)
	}

	job = parseOverlapJob(t, "")
	if len(job.Schedules) != 1 || job.Schedules[0].Overlap != spec.DefaultOverlap {
		t.Fatalf("a schedule that says nothing overlaps with %q, want %q",
			job.Schedules[0].Overlap, spec.DefaultOverlap)
	}
}

func TestAnUnknownOverlapValueIsRefused(t *testing.T) {
	t.Parallel()

	_, diags := spec.Parse("jobs/j.yaml", []byte(overlapJob+"    overlap: replace\n"))
	requireCode(t, diags, spec.CodeBadValue)
}

func TestCanonicalCarriesOverlapOnlyOffTheDefault(t *testing.T) {
	t.Parallel()

	queue := spec.Schedule{Name: "nightly", Cron: "0 3 * * *", Timezone: "UTC", Overlap: spec.OverlapQueue}
	skip := spec.Schedule{Name: "nightly", Cron: "0 3 * * *", Timezone: "UTC", Overlap: spec.OverlapSkip}

	if doc := string(spec.Canonical(&spec.Job{Name: "j", Schedules: []spec.Schedule{queue}})); !strings.Contains(doc, `"overlap":"queue"`) {
		t.Errorf("the canonical document dropped a non-default overlap: %s", doc)
	}
	doc := string(spec.Canonical(&spec.Job{Name: "j", Schedules: []spec.Schedule{skip}}))
	if strings.Contains(doc, "overlap") {
		t.Errorf("the canonical document materialised the default overlap: %s", doc)
	}

	round, err := spec.FromIR(spec.Canonical(&spec.Job{Name: "j", Schedules: []spec.Schedule{queue}}))
	if err != nil {
		t.Fatalf("read the canonical document back: %v", err)
	}
	if len(round.Schedules) != 1 || round.Schedules[0].Overlap != spec.OverlapQueue {
		t.Fatalf("the overlap did not survive the roundtrip: %+v", round.Schedules)
	}
}

func TestFromIRRefusesAnUnknownOverlapValue(t *testing.T) {
	t.Parallel()

	bad := []byte(`{"schema":"` + spec.SchemaName + `","name":"j","steps":[],"schedules":[{"name":"n","cron":"* * * * *","timezone":"UTC","overlap":"replace"}]}`)
	if _, err := spec.FromIR(bad); err == nil {
		t.Fatal("FromIR accepted overlap 'replace'")
	}
}
