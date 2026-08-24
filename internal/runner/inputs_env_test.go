package runner

import (
	"strings"
	"testing"
)

// The $PACEQ_INPUTS contract (#13): the merged upstream JSON travels inline
// while it is small, and as a file beside the run's other artifacts once it
// crosses 128 KiB, with $PACEQ_INPUTS set to null so a jq pipeline still
// reads it. The file variable simply does not exist in the inline world.

func renderEnv(t *testing.T, s Spec) map[string]string {
	t.Helper()
	lines, err := buildEnv(s, "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	out := map[string]string{}
	for _, l := range lines {
		k, v, _ := strings.Cut(l, "=")
		out[k] = v
	}
	return out
}

func anInputSpec() Spec {
	return Spec{
		Argv:    []string{"/bin/true"},
		Timeout: 1,
		Ctx:     RunContext{RunID: "r", Job: "j", Step: "s", Attempt: 1},
	}
}

func TestBuildEnvRendersTheInputsContract(t *testing.T) {
	env := renderEnv(t, anInputSpec())
	if env["PACEQ_INPUTS"] != "{}" {
		t.Errorf("default PACEQ_INPUTS = %q, want {}", env["PACEQ_INPUTS"])
	}
	if _, set := env["PACEQ_INPUTS_FILE"]; set {
		t.Error("PACEQ_INPUTS_FILE exists without a spill")
	}

	s := anInputSpec()
	s.InputsJSON = `{"artifacts":{"raw":{"step":"a","uri":"/x"}},"params":{"rows":"1"}}`
	env = renderEnv(t, s)
	if env["PACEQ_INPUTS"] != s.InputsJSON {
		t.Errorf("PACEQ_INPUTS = %q, want the merged JSON verbatim", env["PACEQ_INPUTS"])
	}
	if _, set := env["PACEQ_INPUTS_FILE"]; set {
		t.Error("PACEQ_INPUTS_FILE exists without a spill")
	}
}

func TestBuildEnvRendersTheSpilledVariant(t *testing.T) {
	s := anInputSpec()
	s.InputsJSON = "null" // the engine's marker for "read the file instead"
	s.InputsFile = "/state/runs/r/s.1.inputs.json"
	env := renderEnv(t, s)
	if env["PACEQ_INPUTS"] != "null" {
		t.Errorf("PACEQ_INPUTS = %q, want the literal null", env["PACEQ_INPUTS"])
	}
	if env["PACEQ_INPUTS_FILE"] != s.InputsFile {
		t.Errorf("PACEQ_INPUTS_FILE = %q, want %q", env["PACEQ_INPUTS_FILE"], s.InputsFile)
	}
}

func TestBuildEnvRefusesAJobThatSetsTheInputsKeys(t *testing.T) {
	for _, key := range []string{"PACEQ_INPUTS", "PACEQ_INPUTS_FILE"} {
		s := anInputSpec()
		s.Env = map[string]string{key: "mine"}
		if _, err := buildEnv(s, ""); err == nil {
			t.Errorf("%s accepted from job env", key)
		}
		s = anInputSpec()
		s.InheritEnv = []string{key}
		if _, err := buildEnv(s, ""); err == nil {
			t.Errorf("%s accepted through inherit_env", key)
		}
	}
}
