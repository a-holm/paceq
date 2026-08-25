package store_test

import (
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// The injection half of #13: $PACEQ_INPUTS is built ONLY from the upstream
// transitive closure of the frozen step_deps graph. A side-by-side step with
// no needs edge is invisible, however close it sits in the spec order. A
// name collision between two upstream contributors resolves deterministically
// to the highest index, and the merge says who lost.

// emitParamsJSON renders what the engine would fold into a verdict's detail:
// the carried-forward params under their one reserved key.
func emitParamsJSON(t *testing.T, pairs ...string) string {
	t.Helper()
	obj := `"`
	for i, p := range pairs {
		if i > 0 {
			if i%2 == 0 {
				obj += `,"`
			} else {
				obj += `":"`
			}
		}
		obj += p
		if i%2 == 1 {
			obj += `"`
		}
	}
	return `{"emitted_params":{` + obj + `}}`
}

func succeedEmitting(t *testing.T, f *publishFixture, step string, arts []store.Artifact, detail string) {
	t.Helper()
	f.start(step)
	out := store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: "STEP_SUCCEEDED",
		Artifacts:  arts,
		DetailJSON: detail,
	}
	if err := f.s.RecordStepOutcome(f.ctx, f.runID, step, out, f.ref); err != nil {
		t.Fatalf("record %s: %v", step, err)
	}
}

func TestUpstreamInputsCarryTheWholeClosureAndNothingElse(t *testing.T) {
	f := newPublishFixture(t,
		store.NewStep{Name: "a"},
		store.NewStep{Name: "side"},
		store.NewStep{Name: "b", DependsOn: []string{"a"}},
	)

	succeedEmitting(t, f, "a",
		[]store.Artifact{{Name: "raw", URI: "/from-a"}},
		emitParamsJSON(t, "rows", "10"))
	succeedEmitting(t, f, "side",
		[]store.Artifact{{Name: "sneaky", URI: "/from-side"}},
		emitParamsJSON(t, "side", "yes"))

	in, err := f.s.UpstreamInputs(f.ctx, f.runID, "b")
	if err != nil {
		t.Fatalf("build inputs for b: %v", err)
	}
	if got := in.Artifacts["raw"]; got.URI != "/from-a" || got.StepName != "a" {
		t.Errorf("artifacts = %+v, want raw from a", in.Artifacts)
	}
	if _, leaked := in.Artifacts["sneaky"]; leaked {
		t.Error("the side step's artifact leaked into b's inputs without a needs edge")
	}
	if in.Params["rows"] != "10" {
		t.Errorf("params = %v, want rows=10", in.Params)
	}
	if _, leaked := in.Params["side"]; leaked {
		t.Error("the side step's params leaked into b's inputs")
	}
	if len(in.Collisions) != 0 {
		t.Errorf("collisions = %v, want none", in.Collisions)
	}

	top, err := f.s.UpstreamInputs(f.ctx, f.runID, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(top.Artifacts) != 0 || len(top.Params) != 0 {
		t.Errorf("a root step built inputs out of nothing: %+v", top)
	}
}

func TestUpstreamInputsResolveAParamCollisionToTheHighestIndex(t *testing.T) {
	f := newPublishFixture(t,
		store.NewStep{Name: "a"},
		store.NewStep{Name: "b", DependsOn: []string{"a"}},
		store.NewStep{Name: "c", DependsOn: []string{"b"}},
	)
	succeedEmitting(t, f, "a", nil, emitParamsJSON(t, "rows", "1", "mode", "a"))
	succeedEmitting(t, f, "b", nil, emitParamsJSON(t, "rows", "2"))

	in, err := f.s.UpstreamInputs(f.ctx, f.runID, "c")
	if err != nil {
		t.Fatal(err)
	}
	if in.Params["rows"] != "2" || in.Params["mode"] != "a" {
		t.Errorf("params = %v, want the later step's rows with the untouched key kept", in.Params)
	}
	if len(in.Collisions) != 1 || in.Collisions[0].Name != "rows" ||
		in.Collisions[0].Winner != "b" || in.Collisions[0].Loser != "a" {
		t.Errorf("collisions = %+v, want one naming b over a", in.Collisions)
	}
}

func TestUpstreamInputsMarshalInTheFrozenShape(t *testing.T) {
	f := newPublishFixture(t,
		store.NewStep{Name: "a"},
		store.NewStep{Name: "b", DependsOn: []string{"a"}},
	)
	size := int64(4096)
	succeedEmitting(t, f, "a", []store.Artifact{{
		Name:      "raw",
		URI:       "/data/raw.parquet",
		SizeBytes: &size,
		Checksum:  "sha256:abc",
		MediaType: "application/x-parquet",
	}}, `{}`)

	in, err := f.s.UpstreamInputs(f.ctx, f.runID, "b")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := in.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"artifacts":{"raw":{"step":"a","uri":"/data/raw.parquet","media_type":"application/x-parquet","checksum":"sha256:abc","size_bytes":4096}},"params":{}}`
	if doc != want {
		t.Errorf("marshalled\n\t%s\nwant\n\t%s", doc, want)
	}
}
