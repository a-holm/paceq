package spec_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

const validSensor = `name: trigger-import
steps:
  - name: only
    run: ["/bin/true"]
sensors:
  - name: dropzone
    kind: exec
    run: ["/srv/etl/nye-objekter.sh"]
    interval: 15s
`

func TestASensorParsesWithItsDefaultsMaterialised(t *testing.T) {
	job, diags := spec.Parse("sensor.yaml", []byte(validSensor))
	if diags.HasErrors() {
		t.Fatalf("a valid sensor was refused:\n%s", render(t, diags))
	}
	if len(job.Sensors) != 1 {
		t.Fatalf("got %d sensors, want 1", len(job.Sensors))
	}
	s := job.Sensors[0]
	if s.Name != "dropzone" || s.Kind != spec.DefaultSensorKind {
		t.Errorf("name/kind = %q/%q, want dropzone/exec", s.Name, s.Kind)
	}
	if s.Interval != 15*time.Second {
		t.Errorf("interval is %v, want 15s", s.Interval)
	}
	if s.MinInterval != time.Second {
		t.Errorf("min_interval is %v, want the 1s default", s.MinInterval)
	}
	if s.Timeout != spec.DefaultSensorTimeout {
		t.Errorf("timeout is %v, want the 30s default", s.Timeout)
	}
	if s.MaxTriggersPerTick != spec.DefaultSensorMaxTriggers {
		t.Errorf("max_triggers_per_tick is %d, want %d", s.MaxTriggersPerTick, spec.DefaultSensorMaxTriggers)
	}
	if s.Paused {
		t.Error("a fresh sensor is paused")
	}
}

func TestSensorDefaultsLandInTheIR(t *testing.T) {
	job, diags := spec.Parse("sensor.yaml", []byte(validSensor))
	if diags.HasErrors() {
		t.Fatalf("parse failed:\n%s", render(t, diags))
	}
	ir := string(spec.Canonical(job))
	for _, want := range []string{
		`"min_interval_ms":1000`,
		`"timeout_ms":30000`,
		`"max_triggers_per_tick":100`,
		`"paused":false`,
		`"kind":"exec"`,
	} {
		if !strings.Contains(ir, want) {
			t.Errorf("the IR does not carry %s:\n%s", want, ir)
		}
	}
}

func TestSensorIntervalNormalisesToMilliseconds(t *testing.T) {
	sixty := strings.Replace(validSensor, "interval: 15s", "interval: 60s", 1)
	one := strings.Replace(validSensor, "interval: 15s", "interval: 1m", 1)
	if hashString(t, sixty) != hashString(t, one) {
		t.Error("60s and 1m hashed differently")
	}
	twoMin := strings.Replace(validSensor, "interval: 15s", "interval: 2m", 1)
	if hashString(t, sixty) == hashString(t, twoMin) {
		t.Error("60s and 2m hashed the same")
	}
}

func TestIntervalUnderOneSecondIsRefused(t *testing.T) {
	_, diags := spec.Parse("sensor.yaml", []byte(strings.Replace(validSensor, "interval: 15s", "interval: 999ms", 1)))
	d := requireCode(t, diags, spec.CodeSensorIntervalMin)
	if d.Line == 0 || d.Col == 0 {
		t.Errorf("the refusal has no position: %s", d.Message)
	}
}

func TestIntervalOfExactlyOneSecondIsAccepted(t *testing.T) {
	job, diags := spec.Parse("sensor.yaml", []byte(strings.Replace(validSensor, "interval: 15s", "interval: 1s", 1)))
	if diags.HasErrors() {
		t.Fatalf("an interval of 1s was refused:\n%s", render(t, diags))
	}
	if job.Sensors[0].Interval != time.Second {
		t.Errorf("interval is %v, want 1s", job.Sensors[0].Interval)
	}
}

func TestMinIntervalOverTheIntervalIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, "interval: 15s\n", "interval: 15s\n    min_interval: 30s\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorMinInterval)
	if d.Line == 0 {
		t.Errorf("no position: %s", d.Message)
	}
}

func TestTimeoutOverTheCeilingIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, "interval: 15s\n", "interval: 15s\n    timeout: 10m\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorTimeout)
	if !strings.Contains(d.Hint, "max_triggers_per_tick") {
		t.Errorf("the next step does not point at chunking: %s", d.Hint)
	}
}

func TestTimeoutBelowTheFloorIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, "interval: 15s\n", "interval: 15s\n    timeout: 10ms\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	requireCode(t, diags, spec.CodeSensorTimeout)
}

func TestMaxTriggersOutsideTheRangeIsRefused(t *testing.T) {
	for _, bad := range []string{"0", "10001"} {
		src := strings.Replace(validSensor, "interval: 15s\n", "interval: 15s\n    max_triggers_per_tick: "+bad+"\n", 1)
		_, diags := spec.Parse("sensor.yaml", []byte(src))
		d := requireCode(t, diags, spec.CodeSensorTriggers)
		if !strings.Contains(d.Message, bad) {
			t.Errorf("the message does not name %s: %s", bad, d.Message)
		}
	}
}

func TestKindOtherThanExecIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, "kind: exec", "kind: file", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorKind)
	if !strings.Contains(d.Hint, "v0.3") {
		t.Errorf("the next step does not say when built in types arrive: %s", d.Hint)
	}
	if d.Line == 0 {
		t.Error("the refusal has no position")
	}
}

func TestRunWrittenAsAStringIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, `run: ["/srv/etl/nye-objekter.sh"]`, `run: "/usr/bin/sleep 60"`, 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	requireCode(t, diags, spec.CodeSensorRun)
}

func TestRunWithAnEmptyArgumentIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, `run: ["/srv/etl/nye-objekter.sh"]`, `run: ["/bin/echo", ""]`, 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorRun)
	if !strings.Contains(d.Message, "empty") {
		t.Errorf("the message does not name the empty argument: %s", d.Message)
	}
}

func TestSensorMissingRunIsRefused(t *testing.T) {
	_, diags := spec.Parse("sensor.yaml", []byte(strings.Replace(validSensor, `    run: ["/srv/etl/nye-objekter.sh"]`+"\n", "", 1)))
	requireCode(t, diags, spec.CodeSensorRun)
}

func TestSensorMissingIntervalIsRefused(t *testing.T) {
	_, diags := spec.Parse("sensor.yaml", []byte(strings.Replace(validSensor, "    interval: 15s\n", "", 1)))
	requireCode(t, diags, spec.CodeSensorIntervalMin)
}

func TestSensorMissingNameIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, "  - name: dropzone\n", "  -\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	requireCode(t, diags, spec.CodeSensorBadName)
}

func TestSensorNameMustMatchTheNameRule(t *testing.T) {
	// The name rule is the one every job, step, schedule and sensor shares
	// (one rule, not two), so a malformed sensor name is the shared refusal.
	src := strings.Replace(validSensor, "  - name: dropzone", "  - name: Bad Name", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	requireCode(t, diags, spec.CodeBadName)
}

func TestTwoSensorsInOneJobWithOneNameAreRefused(t *testing.T) {
	src := validSensor +
		"  - name: dropzone\n" +
		"    kind: exec\n" +
		`    run: ["/srv/etl/second.sh"]` + "\n" +
		"    interval: 30s\n"
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorNameTaken)
	if d.Line == 0 {
		t.Errorf("the second sensor is not pointed at: %s", d.Message)
	}
}

// TestTwoJobsSharingOneSensorNameAreRefused is the primary key across jobs
// made visible: a sensor name lives in one row for the whole catalog, so two
// files that both define it cannot both materialise. The check names the file
// that loses the name and the one that already owns it.
func TestTwoJobsSharingOneSensorNameAreRefused(t *testing.T) {
	parse := func(t *testing.T, path, sensorName string) *spec.Job {
		t.Helper()
		src := strings.Replace(validSensor, "  - name: dropzone", "  - name: "+sensorName, 1)
		job, diags := spec.Parse(path, []byte(src))
		if diags.HasErrors() {
			t.Fatalf("%s does not parse:\n%s", path, render(t, diags))
		}
		return job
	}
	jobs := []spec.NamedJob{
		{Path: "jobs/first.yaml", Job: parse(t, "jobs/first.yaml", "dropzone")},
		{Path: "jobs/second.yaml", Job: parse(t, "jobs/second.yaml", "dropzone")},
	}

	conflicts := spec.CheckGlobalSensorNames(jobs)
	if len(conflicts) != 1 {
		t.Fatalf("got %d conflicts, want 1", len(conflicts))
	}
	c := conflicts[0]
	if c.Code != spec.CodeSensorNameTaken {
		t.Errorf("code is %s, want %s", c.Code, spec.CodeSensorNameTaken)
	}
	if !strings.Contains(c.Message, "jobs/first.yaml") || c.File != "jobs/second.yaml" {
		t.Errorf("the conflict does not point at both files: %s in %s", c.Message, c.File)
	}
}

func TestDistinctSensorsAcrossJobsAreAllowed(t *testing.T) {
	parse := func(path, name string) spec.NamedJob {
		src := strings.Replace(validSensor, "  - name: dropzone", "  - name: "+name, 1)
		job, _ := spec.Parse(path, []byte(src))
		return spec.NamedJob{Path: path, Job: job}
	}
	conflicts := spec.CheckGlobalSensorNames([]spec.NamedJob{
		parse("jobs/a.yaml", "dropzone"),
		parse("jobs/b.yaml", "archive"),
	})
	if len(conflicts) != 0 {
		t.Errorf("distinct sensor names conflicted: %+v", conflicts)
	}
}

func TestEnvKeysWithTheReservedPrefixAreRefused(t *testing.T) {
	src := strings.Replace(validSensor, "    interval: 15s\n", "    interval: 15s\n    env:\n      PULSEQ_CURSOR: x\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorEnvKey)
	if !strings.Contains(d.Message, "PULSEQ_") {
		t.Errorf("the message does not name the reserved prefix: %s", d.Message)
	}
}

func TestAnUnknownSensorFieldIsRefused(t *testing.T) {
	src := strings.Replace(validSensor, "    interval: 15s\n", "    interval: 15s\n    fuzz: 1\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	requireCode(t, diags, spec.CodeUnknownField)
}

func TestAWorkdirThatIsNotAbsoluteIsAWarning(t *testing.T) {
	src := strings.Replace(validSensor, "    interval: 15s\n", "    interval: 15s\n    workdir: srv/etl\n", 1)
	_, diags := spec.Parse("sensor.yaml", []byte(src))
	d := requireCode(t, diags, spec.CodeSensorWorkdir)
	if !strings.Contains(d.Message, "absolute") {
		t.Errorf("the workdir rule does not say to make the path absolute: %s", d.Message)
	}
}

func TestSensorIRRoundTrips(t *testing.T) {
	src := strings.Replace(validSensor, "    interval: 15s\n",
		"    interval: 15s\n    min_interval: 5s\n    timeout: 45s\n    max_triggers_per_tick: 20\n"+
			"    paused: true\n    workdir: /srv/etl\n    description: waves at the dropzone\n    env:\n      BUCKET: acme\n", 1)
	job, diags := spec.Parse("sensor.yaml", []byte(src))
	if diags.HasErrors() {
		t.Fatalf("parse failed:\n%s", render(t, diags))
	}
	hashed := spec.Compile(job)
	back, err := spec.FromIR(hashed.Canonical)
	if err != nil {
		t.Fatalf("from IR failed: %v", err)
	}
	if !bytes.Equal(spec.Canonical(back), hashed.Canonical) {
		t.Error("the round trip moved the canonical bytes")
	}
	want, got := job.Sensors[0], back.Sensors[0]
	if want.Name != got.Name || want.Kind != got.Kind || want.Interval != got.Interval ||
		want.MinInterval != got.MinInterval || want.Timeout != got.Timeout ||
		want.MaxTriggersPerTick != got.MaxTriggersPerTick || want.Paused != got.Paused ||
		want.Workdir != got.Workdir || want.Description != got.Description || !sameEnv(want.Env, got.Env) {
		t.Errorf("the round trip changed a sensor\nwant %+v\ngot  %+v", want, got)
	}
}

func hashString(t *testing.T, src string) string {
	t.Helper()
	job, diags := spec.Parse("sensor.yaml", []byte(src))
	if diags.HasErrors() {
		t.Fatalf("the source does not parse:\n%s", render(t, diags))
	}
	return spec.Compile(job).Hash
}

func sameEnv(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
