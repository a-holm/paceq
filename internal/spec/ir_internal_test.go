package spec

import (
	"strings"
	"testing"
	"time"
)

// FromIR reads a canonical document back into a Job. The property that makes
// it the right reader for runs is the roundtrip: whatever went in through
// Compile comes out byte identical, so a run materialised from stored bytes
// runs exactly what was applied, however long ago that was.

func TestFromIRRoundTripsThroughCanonical(t *testing.T) {
	jobs := []*Job{
		{
			Name:          "minimal",
			Timeout:       DefaultTimeout,
			MaxConcurrent: DefaultMaxConcurrent,
			Steps: []Step{
				{Name: "only", Run: []string{"/bin/true"}},
			},
		},
		{
			Name:          "full",
			Description:   "every field set",
			Env:           map[string]string{"A": "1", "B": "two"},
			EnvFile:       "/etc/paceq/env",
			InheritEnv:    []string{"PATH", "HOME"},
			Workdir:       "var/lib/paceq",
			Timeout:       90 * time.Minute,
			MaxConcurrent: 3,
			Steps: []Step{
				{
					Name:    "fetch",
					Run:     []string{"/usr/bin/curl", "-s", "https://example.invalid"},
					Workdir: "srv/data",
					Timeout: 5 * time.Minute,
				},
				{
					Name:  "build",
					Run:   []string{"/bin/sh", "-c", "make all"},
					Shell: true,
					Retry: &Retry{
						Max:      2,
						Backoff:  BackoffFixed,
						Initial:  time.Second,
						MaxDelay: time.Minute,
						Jitter:   JitterNone,
					},
					Needs: []string{"fetch"},
				},
			},
			Schedules: []Schedule{{Name: "nightly", Cron: "0 2 * * *", Timezone: "Europe/Oslo"}},
			Sensors:   []Sensor{{Name: "dropbox", Type: "exec", Interval: 30 * time.Second}},
		},
	}

	for _, j := range jobs {
		hashed := Compile(j)
		back, err := FromIR(hashed.Canonical)
		if err != nil {
			t.Fatalf("job %s: FromIR: %v", j.Name, err)
		}
		again := Compile(back)
		if string(again.Canonical) != string(hashed.Canonical) {
			t.Errorf("job %s: roundtrip changed the canonical bytes:\n want %s\n got  %s",
				j.Name, hashed.Canonical, again.Canonical)
		}
		if again.Hash != hashed.Hash {
			t.Errorf("job %s: roundtrip hash = %s, want %s", j.Name, again.Hash, hashed.Hash)
		}
	}
}

func TestFromIRDecodesEveryField(t *testing.T) {
	in := []string{"/usr/bin/tool", "--flag", "value with spaces"}
	src := Compile(&Job{
		Name:          "decode",
		Description:   "d",
		Env:           map[string]string{"K": "V"},
		InheritEnv:    []string{"TERM"},
		Workdir:       "w",
		Timeout:       250 * time.Millisecond,
		MaxConcurrent: 2,
		Steps: []Step{
			{
				Name:    "one",
				Run:     in,
				Shell:   true,
				Workdir: "sw",
				Timeout: 1500 * time.Millisecond,
				Retry:   &Retry{Max: 4, Backoff: BackoffExponential, Initial: 500 * time.Millisecond, MaxDelay: 5 * time.Minute, Jitter: JitterFull},
				Needs:   []string{"zero"},
			},
		},
	})

	j, err := FromIR(src.Canonical)
	if err != nil {
		t.Fatalf("FromIR: %v", err)
	}
	if j.Name != "decode" || j.Description != "d" || j.EnvFile != "" {
		t.Errorf("scalars decoded wrong: %+v", j)
	}
	if j.Env["K"] != "V" || len(j.Env) != 1 {
		t.Errorf("env = %v, want one key K", j.Env)
	}
	if len(j.InheritEnv) != 1 || j.InheritEnv[0] != "TERM" {
		t.Errorf("inherit_env = %v", j.InheritEnv)
	}
	if j.Workdir != "w" || j.Timeout != 250*time.Millisecond || j.MaxConcurrent != 2 {
		t.Errorf("job scalars decoded wrong: %+v", j)
	}
	if len(j.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(j.Steps))
	}
	step := j.Steps[0]
	if step.Name != "one" || step.Shell != true || step.Workdir != "sw" {
		t.Errorf("step scalars decoded wrong: %+v", step)
	}
	if strings.Join(step.Run, "|") != strings.Join(in, "|") {
		t.Errorf("run = %v, want %v (argv order is the meaning)", step.Run, in)
	}
	if step.Timeout != 1500*time.Millisecond {
		t.Errorf("step timeout = %s, want 1.5s", step.Timeout)
	}
	if step.Retry == nil {
		t.Fatal("retry was dropped")
	}
	want := Retry{Max: 4, Backoff: BackoffExponential, Initial: 500 * time.Millisecond, MaxDelay: 5 * time.Minute, Jitter: JitterFull}
	if *step.Retry != want {
		t.Errorf("retry = %+v, want %+v", *step.Retry, want)
	}
	if len(step.Needs) != 1 || step.Needs[0] != "zero" {
		t.Errorf("needs = %v", step.Needs)
	}
}

// The refusals below are about corrupted or future documents. A version row is
// immutable, so the only way to meet one of these is a bug upstream of us, and
// the error has to say that instead of running something half read.
func TestFromIRRefusesBrokenDocuments(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"not json", `{`, "parse"},
		{"not an object", `[1,2]`, "object"},
		{"wrong schema", `{"schema":"paceq.job.v2","name":"x"}`, "schema"},
		{"missing schema", `{"name":"x"}`, "schema"},
		{"missing name", `{"schema":"paceq.job.v1"}`, "name"},
		{"unknown key", `{"schema":"paceq.job.v1","name":"x","surprise":1}`, "unexpected key"},
		{"bad number", `{"schema":"paceq.job.v1","name":"x","timeout_ms":"soon"}`, "timeout_ms"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FromIR([]byte(tc.doc))
			if err == nil {
				t.Fatalf("FromIR(%s) accepted a broken document", tc.doc)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}
