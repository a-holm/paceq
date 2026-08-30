package engine

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/spec"
)

// TestFrozenSilenceReachesThePlanner walks the whole chain a failed run walks:
// the file decodes, the canonical encoding freezes it, the engine reads the
// frozen job back and hands its hooks to the planner. A job that spells out an
// empty notify block has said "nobody", so the daemon defaults must not page
// anyone on its behalf.
func TestFrozenSilenceReachesThePlanner(t *testing.T) {
	base := `name: silent
steps:
  - name: only
    run: ["/bin/true"]
`
	cases := []struct {
		name   string
		source string
		want   []string
	}{
		{"an explicit empty block is silence", base + "notify: {}\n", nil},
		{"no block at all inherits the defaults", base, []string{"vakt"}},
	}

	defaults := model.NotifyDefaults{OnFailure: []string{"vakt"}}
	stamp := time.Date(2026, 3, 29, 2, 30, 0, 0, time.UTC)
	planner := notify.NewPlanner(defaults, func() time.Time { return stamp })

	for _, tc := range cases {
		job, diags := spec.Parse("j.yaml", []byte(tc.source))
		if diags.HasErrors() {
			t.Fatalf("%s: the fixture does not parse: %v", tc.name, diags)
		}
		frozen, err := spec.FromIR(spec.Canonical(job))
		if err != nil {
			t.Fatalf("%s: read the frozen spec back: %v", tc.name, err)
		}

		notes := planner.Plan(notify.RunFacts{
			Topic:      model.TopicRunFailed,
			JobName:    "silent",
			RunID:      "01M17NNQ5Y3EXTKHX9TFCNBZ2J",
			State:      "failed",
			ReasonCode: "RUN_FAILED_STEP",
			StartedAt:  stamp,
			FinishedAt: stamp,
		}, hooksFromSpec(frozen))

		got := make([]string, 0, len(notes))
		for _, n := range notes {
			got = append(got, n.Target)
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s: the planner addressed %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: the planner addressed %v, want %v", tc.name, got, tc.want)
				break
			}
		}
	}
}
