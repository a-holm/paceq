package spec_test

import (
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

// The job level concurrency key (#17). Three closed forms, no templating:
//
//	concurrency_key: "nightly-report"        # a constant
//	concurrency_key: {param: kunde}          # params["kunde"] at fire time
//	concurrency_key: {from: run_key}         # the trigger's run key
//	on_conflict: defer                       # defer (the default) | skip
//
// The value canonises as <jobname>:<resolved> at materialisation; this file
// pins the grammar, the refusals and the canonical document.

const concJob = "name: j\nsteps:\n  - name: build\n    run: [/bin/true]\n"

func parseConcJob(t *testing.T, jobSrc string) (*spec.Job, diag.List) {
	t.Helper()
	job, diags := spec.Parse("jobs/j.yaml", []byte(jobSrc))
	return job, diags
}

func requireNoErrors(t *testing.T, diags diag.List) {
	t.Helper()
	if diags.HasErrors() {
		t.Fatalf("the job file was refused:\n%s", render(t, diags))
	}
}

func requireWarning(t *testing.T, diags diag.List, code string) diag.Diagnostic {
	t.Helper()
	for _, d := range diags {
		if d.Code == code && !d.IsError() {
			return d
		}
	}
	t.Fatalf("no %s warning among %v", code, codesOf(diags))
	return diag.Diagnostic{}
}

func TestAJobCarriesEachConcurrencyKeyForm(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  string
		want spec.ConcurrencyKey
	}{
		{
			name: "constant",
			src:  concJob + "concurrency_key: \"nightly-report\"\n",
			want: spec.ConcurrencyKey{Constant: "nightly-report"},
		},
		{
			name: "param",
			src:  concJob + "concurrency_key:\n  param: kunde\n",
			want: spec.ConcurrencyKey{Param: "kunde"},
		},
		{
			name: "from run_key",
			src:  concJob + "concurrency_key:\n  from: run_key\n",
			want: spec.ConcurrencyKey{FromRunKey: true},
		},
	}
	for _, tc := range cases {
		job, diags := parseConcJob(t, tc.src)
		requireNoErrors(t, diags)
		if job.ConcurrencyKey == nil || *job.ConcurrencyKey != tc.want {
			t.Fatalf("%s: the key did not survive the parse: %+v", tc.name, job.ConcurrencyKey)
		}
	}
}

func TestAJobWithoutAConcurrencyKeyHasNone(t *testing.T) {
	t.Parallel()

	job, diags := parseConcJob(t, concJob)
	requireNoErrors(t, diags)
	if job.ConcurrencyKey != nil {
		t.Fatalf("an unconfigured job carries %+v", job.ConcurrencyKey)
	}
	if job.OnConflict != spec.DefaultOnConflict {
		t.Fatalf("on_conflict defaults to %q, got %q", spec.DefaultOnConflict, job.OnConflict)
	}
}

func TestTemplatingInTheConstantIsRefusedWithTheDecisionInTheMessage(t *testing.T) {
	t.Parallel()

	_, diags := spec.Parse("jobs/j.yaml", []byte(concJob+"concurrency_key: \"nightly/{{ .params.date }}\"\n"))
	d := requireCode(t, diags, spec.CodeConcurrencyTemplating)
	if !strings.Contains(d.Message, "templating") {
		t.Fatalf("the refusal does not name the templating decision: %s", d.Message)
	}
}

func TestTheConcurrencyKeyGrammarIsClosed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  string
		code string
	}{
		{"unknown mapping key", concJob + "concurrency_key:\n  raw: db-x\n", spec.CodeUnknownField},
		{"two forms at once", concJob + "concurrency_key:\n  param: k\n  from: run_key\n", spec.CodeBadValue},
		{"empty param name", concJob + "concurrency_key:\n  param: \"\"\n", spec.CodeBadValue},
		{"from with a wrong source", concJob + "concurrency_key:\n  from: id\n", spec.CodeBadValue},
		{"mapping that is all empty", concJob + "concurrency_key: {}\n", spec.CodeBadValue},
		{"over the length cap", concJob + "concurrency_key: \"" + strings.Repeat("k", 201) + "\"\n", spec.CodeBadValue},
		{"unknown on_conflict", concJob + "on_conflict: replace\n", spec.CodeBadValue},
	}
	for _, tc := range cases {
		_, diags := spec.Parse("jobs/j.yaml", []byte(tc.src))
		if d := requireCode(t, diags, tc.code); !d.IsError() {
			t.Fatalf("%s came back as a warning, want an error", tc.name)
		}
	}
}

func TestExactlyAtTheLengthCapIsAccepted(t *testing.T) {
	t.Parallel()

	job, diags := parseConcJob(t, concJob+"concurrency_key: \""+strings.Repeat("k", 200)+"\"\n")
	requireNoErrors(t, diags)
	if job.ConcurrencyKey == nil || len(job.ConcurrencyKey.Constant) != 200 {
		t.Fatalf("the long constant did not survive: %+v", job.ConcurrencyKey)
	}
}

func TestOnConflictCarriesSkipAndDefaultsToDefer(t *testing.T) {
	t.Parallel()

	job, diags := parseConcJob(t, concJob+"on_conflict: skip\n")
	requireNoErrors(t, diags)
	if job.OnConflict != spec.OnConflictSkip {
		t.Fatalf("on_conflict: skip read back as %q", job.OnConflict)
	}
}

func TestCanonicalCarriesTheKeyAndOmitsTheDefaultPolicy(t *testing.T) {
	t.Parallel()

	withKey := &spec.Job{Name: "j", ConcurrencyKey: &spec.ConcurrencyKey{Param: "kunde"}}
	doc := string(spec.Canonical(withKey))
	if !strings.Contains(doc, `"concurrency_key":{"param":"kunde"}`) {
		t.Fatalf("the canonical document dropped the key: %s", doc)
	}
	if strings.Contains(doc, "on_conflict") {
		t.Fatalf("the canonical document materialised the default policy: %s", doc)
	}

	skip := &spec.Job{Name: "j", ConcurrencyKey: &spec.ConcurrencyKey{Constant: "x"}, OnConflict: spec.OnConflictSkip}
	doc = string(spec.Canonical(skip))
	if !strings.Contains(doc, `"on_conflict":"skip"`) {
		t.Fatalf("the canonical document dropped on_conflict: skip: %s", doc)
	}
	if !strings.Contains(doc, `"concurrency_key":{"constant":"x"}`) {
		t.Fatalf("the canonical document mangled the constant form: %s", doc)
	}
}

func TestAnExplicitDeferAndNoPolicyHashTheSame(t *testing.T) {
	t.Parallel()

	explicit := spec.Hash(spec.Canonical(&spec.Job{Name: "j", ConcurrencyKey: &spec.ConcurrencyKey{Param: "k"}, OnConflict: spec.DefaultOnConflict}))
	omitted := spec.Hash(spec.Canonical(&spec.Job{Name: "j", ConcurrencyKey: &spec.ConcurrencyKey{Param: "k"}}))
	if explicit != omitted {
		t.Fatalf("an explicit default changed the hash:\n%s\n%s", explicit, omitted)
	}
}

func TestTheKeySurvivesTheRoundtrip(t *testing.T) {
	t.Parallel()

	in := &spec.Job{
		Name:           "j",
		ConcurrencyKey: &spec.ConcurrencyKey{FromRunKey: true},
		OnConflict:     spec.OnConflictSkip,
	}
	out, err := spec.FromIR(spec.Canonical(in))
	if err != nil {
		t.Fatalf("read the canonical document back: %v", err)
	}
	if out.ConcurrencyKey == nil || !out.ConcurrencyKey.FromRunKey {
		t.Fatalf("from: run_key did not survive: %+v", out.ConcurrencyKey)
	}
	if out.OnConflict != spec.OnConflictSkip {
		t.Fatalf("on_conflict did not survive: %q", out.OnConflict)
	}
}

func TestFromIRRefusesABadKeyDocument(t *testing.T) {
	t.Parallel()

	base := `{"schema":"` + spec.SchemaName + `","name":"j","steps":[],"concurrency_key":`
	cases := []string{
		base + `"nightly/{{ x }}"}`,
		base + `{"raw":"db-x"}}`,
		base + `{"param":""}}`,
		base + `{"from":"id"}}`,
		base + `{"param":"a","from":"run_key"}}`,
		base + `12}`,
		`{"schema":"` + spec.SchemaName + `","name":"j","steps":[],"on_conflict":"replace"}`,
	}
	for _, doc := range cases {
		if _, err := spec.FromIR([]byte(doc)); err == nil {
			t.Errorf("FromIR accepted %s", doc)
		}
	}
}

func TestAParamKeyWarnsAtParseBecauseATriggerMayLackIt(t *testing.T) {
	t.Parallel()

	_, diags := spec.Parse("jobs/j.yaml", []byte(concJob+"concurrency_key:\n  param: kunde\n"))
	requireNoErrors(t, diags)
	d := requireWarning(t, diags, spec.CodeConcurrencyParamUnresolved)
	if !strings.Contains(d.Message, "kunde") {
		t.Fatalf("the warning does not name the parameter: %s", d.Message)
	}
}

func TestConstantAndRunKeyFormsDoNotWarn(t *testing.T) {
	t.Parallel()

	for _, src := range []string{
		concJob + "concurrency_key: \"nightly\"\n",
		concJob + "concurrency_key:\n  from: run_key\n",
	} {
		_, diags := spec.Parse("jobs/j.yaml", []byte(src))
		requireNoErrors(t, diags)
		for _, d := range diags {
			if d.Code == spec.CodeConcurrencyParamUnresolved {
				t.Fatalf("a static form raised the parameter warning: %s", d.Message)
			}
		}
	}
}
