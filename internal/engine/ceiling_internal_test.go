package engine

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// The ceiling is one answer to one question, so its whole table lives here:
// no store, no process, no clock advance, just the two facts that decide how
// long an attempt may run and who gets the blame when it does not finish.

func TestStepCeilingBoundsAStepByTheJobItRunsUnder(t *testing.T) {
	start := time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		step time.Duration
		job  time.Duration
		// spent is how much of the run's budget earlier steps already used.
		spent time.Duration
		want  time.Duration
		byRun bool
	}{
		{
			name: "a step without one is bounded by the job's",
			job:  6 * time.Hour,
			want: 6 * time.Hour, byRun: true,
		},
		{
			name: "a job shorter than an hour bounds it just as tightly",
			job:  30 * time.Minute,
			want: 30 * time.Minute, byRun: true,
		},
		{
			name: "a step without one is bounded at the job ceiling too",
			job:  24 * time.Hour,
			want: 24 * time.Hour, byRun: true,
		},
		{
			name: "what earlier steps spent comes off it",
			job:  6 * time.Hour, spent: 5*time.Hour + 50*time.Minute,
			want: 10 * time.Minute, byRun: true,
		},
		{
			name: "a step's own ceiling stands inside a longer job",
			step: 2 * time.Hour, job: 6 * time.Hour,
			want: 2 * time.Hour, byRun: false,
		},
		{
			name: "and is lowered when the run has less left than it asks for",
			step: 2 * time.Hour, job: 6 * time.Hour, spent: 5*time.Hour + 50*time.Minute,
			want: 10 * time.Minute, byRun: true,
		},
		{
			name: "a job version with no timeout at all falls back to the spec default",
			want: spec.DefaultTimeout, byRun: false,
		},
		{
			name: "and leaves a step that named one alone",
			step: 90 * time.Minute,
			want: 90 * time.Minute, byRun: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var deadline time.Time
			if tc.job > 0 {
				deadline = start.Add(tc.job)
			}
			got, byRun := stepCeiling(tc.step, deadline, start.Add(tc.spent))
			if got != tc.want || byRun != tc.byRun {
				t.Errorf("stepCeiling(step %s, job %s, spent %s) = %s/byRun=%t, want %s/byRun=%t",
					tc.step, tc.job, tc.spent, got, byRun, tc.want, tc.byRun)
			}
		})
	}
}
