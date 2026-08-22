package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func envFileSpec(t *testing.T) Spec {
	t.Helper()

	return Spec{
		Ctx: RunContext{
			RunID:   "01JENV0000000000000000000",
			Job:     "nightly",
			Step:    "load",
			Attempt: 2,
		},
	}
}

// envMap turns the KEY=VALUE list into a map so assertions read like the
// contract they check.
func envMap(env []string) map[string]string {
	m := map[string]string{}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		m[k] = v
	}
	return m
}

func TestEnvBaselineIsFixedAndNeverInherited(t *testing.T) {
	t.Setenv("PATH", "/daemon/paths")
	t.Setenv("HOME", "/daemon/home")
	t.Setenv("DAEMON_ONLY", "leak")

	env, err := buildEnv(envFileSpec(t), "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	got := envMap(env)
	if got["PATH"] != DefaultPath {
		t.Errorf("PATH = %q, want the fixed default %q, never the daemon's", got["PATH"], DefaultPath)
	}
	if got["HOME"] != "/daemon/home" {
		t.Errorf("HOME = %q, want the daemon's value passed through under its known name", got["HOME"])
	}
	if _, ok := got["DAEMON_ONLY"]; ok {
		t.Error("DAEMON_ONLY leaked: deny by default means nothing passes without an inherit_env entry")
	}
	for _, name := range []string{"PACEQ_RUN_ID", "PACEQ_JOB", "PACEQ_STEP", "PACEQ_ATTEMPT"} {
		if got[name] == "" {
			t.Errorf("%s missing from the context contract", name)
		}
	}
}

func TestEnvBaselineOmitsUnsetNames(t *testing.T) {
	t.Setenv("TZ", "")
	os.Unsetenv("TZ") // Setenv with "" sets an empty variable; unset removes it entirely
	env, err := buildEnv(envFileSpec(t), "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	for _, kv := range env {
		if strings.HasPrefix(kv, "TZ=") {
			t.Errorf("TZ present but unset in the runner: %q", kv)
		}
	}
}

func TestEnvPrecedenceJobOverFileOverInherit(t *testing.T) {
	t.Setenv("SHARED_LAYER", "from-daemon")
	dir := t.TempDir()
	file := filepath.Join(dir, "job.env")
	if err := os.WriteFile(file, []byte("SHARED_LAYER=from-file\nFILE_ONLY=f\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := envFileSpec(t)
	s.EnvFile = file
	s.Env = map[string]string{"SHARED_LAYER": "from-job"}
	s.InheritEnv = []string{"SHARED_LAYER"}

	env, err := buildEnv(s, "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	got := envMap(env)
	if got["SHARED_LAYER"] != "from-job" {
		t.Errorf("SHARED_LAYER = %q, want from-job: reviewed job config wins", got["SHARED_LAYER"])
	}
	if got["FILE_ONLY"] != "f" {
		t.Errorf("FILE_ONLY = %q, want f", got["FILE_ONLY"])
	}
}

func TestEnvInheritTakesOnlyListedNames(t *testing.T) {
	t.Setenv("WANTED", "yes")
	t.Setenv("UNWANTED", "no")
	s := envFileSpec(t)
	s.InheritEnv = []string{"WANTED"}

	env, err := buildEnv(s, "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	got := envMap(env)
	if got["WANTED"] != "yes" {
		t.Errorf("WANTED = %q, want yes", got["WANTED"])
	}
	if _, ok := got["UNWANTED"]; ok {
		t.Error("UNWANTED passed through though it was not listed in inherit_env")
	}
}

func TestEnvMissingInheritedNameIsSilentlySkipped(t *testing.T) {
	s := envFileSpec(t)
	s.InheritEnv = []string{"NOT_SET_ANYWHERE"}
	env, err := buildEnv(s, "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	if _, ok := envMap(env)["NOT_SET_ANYWHERE"]; ok {
		t.Error("a name that does not exist must not appear empty")
	}
}

func TestEnvReservedPrefixIsRefusedEverywhere(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Spec)
	}{
		{"job env", func(s *Spec) { s.Env = map[string]string{"PACEQ_STEP": "forged"} }},
		{"inherit_env", func(s *Spec) { s.InheritEnv = []string{"PACEQ_RUN_ID"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := envFileSpec(t)
			tc.mut(&s)
			if _, err := buildEnv(s, ""); err == nil {
				t.Fatal("a forged PACEQ_ key was accepted; the context contract is not optional")
			}
		})
	}
}

func TestEnvFileMustBe0600(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		mode os.FileMode
		ok   bool
	}{
		{0o600, true},
		{0o644, false},
		{0o666, false},
		{0o400, false},
		{0o700, false},
	}
	for _, tc := range cases {
		file := filepath.Join(dir, tc.mode.String()+".env")
		if err := os.WriteFile(file, []byte("K=V\n"), tc.mode); err != nil {
			t.Fatal(err)
		}
		s := envFileSpec(t)
		s.EnvFile = file
		_, err := buildEnv(s, "")
		if tc.ok && err != nil {
			t.Errorf("mode %o rejected: %v", tc.mode, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("mode %o accepted: looser than 0600 must fail closed", tc.mode)
		}
	}
}

func TestEnvFileParsingRules(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    map[string]string
		wantErr bool
	}{
		{
			name:    "plain pairs",
			content: "A=1\nB=hello world\n",
			want:    map[string]string{"A": "1", "B": "hello world"},
		},
		{
			name:    "comments and blank lines",
			content: "# lead\n\n  # indented comment\nC=3\n",
			want:    map[string]string{"C": "3"},
		},
		{
			name:    "crlf tolerated",
			content: "D=4\r\nE=5\r\n",
			want:    map[string]string{"D": "4", "E": "5"},
		},
		{
			name:    "empty value allowed",
			content: "F=\n",
			want:    map[string]string{"F": ""},
		},
		{
			name:    "value may contain equals",
			content: "G=a=b=c\n",
			want:    map[string]string{"G": "a=b=c"},
		},
		{
			name:    "duplicate keys last wins",
			content: "H=1\nH=2\n",
			want:    map[string]string{"H": "2"},
		},
		{
			name:    "line without equals refused",
			content: "I J\n",
			wantErr: true,
		},
		{
			name:    "reserved key refused",
			content: "PACEQ_JOB=nope\n",
			wantErr: true,
		},
		{
			name:    "export prefix is shell syntax and refused",
			content: "export K=1\n",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "job.env")
			if err := os.WriteFile(file, []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := parseEnvFile(file)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("accepted %q", tc.content)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("parsed[%q] = %q, want %q", k, got[k], v)
				}
				delete(got, k)
			}
			if len(got) > 0 {
				t.Errorf("unexpected parsed keys %v", got)
			}
		})
	}
}

func TestEnvIdempotencyKeyFormula(t *testing.T) {
	got, want := idempotencyKey("run-1", "step-2"), "a1ff9ef4987333e6ccf9d30aa029ec98"
	if got != want {
		t.Errorf("idempotencyKey = %q, want %q (sha256 of run:step, first 32 hex)", got, want)
	}
}

func TestEnvContextValuesAreRenderedPerContract(t *testing.T) {
	s := envFileSpec(t)
	s.Ctx.Params = map[string]any{"n": float64(3)}
	s.Ctx.RunKey = "k"
	s.Ctx.ScheduledFor = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	env, err := buildEnv(s, "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	got := envMap(env)
	want := map[string]string{
		"PACEQ_RUN_ID":          "01JENV0000000000000000000",
		"PACEQ_JOB":             "nightly",
		"PACEQ_STEP":            "load",
		"PACEQ_ATTEMPT":         "2",
		"PACEQ_RUN_KEY":         "k",
		"PACEQ_PARAMS":          `{"n":3}`,
		"PACEQ_SCHEDULED_FOR":   "2026-01-02T03:04:05Z",
		"PACEQ_INPUTS":          "{}",
		"PACEQ_OUTPUT":          "",
		"PACEQ_IDEMPOTENCY_KEY": idempotencyKey(s.Ctx.RunID, s.Ctx.Step),
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestEnvManualRunHasEmptyScheduledFor(t *testing.T) {
	s := envFileSpec(t)
	env, err := buildEnv(s, "")
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	if got := envMap(env)["PACEQ_SCHEDULED_FOR"]; got != "" {
		t.Errorf("PACEQ_SCHEDULED_FOR = %q, want empty for a manual run", got)
	}
}
