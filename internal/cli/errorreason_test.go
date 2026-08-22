package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
)

// The reason half of `paceq error`: the runtime codes from internal/reason are
// served by the same command as the diagnostic ones, an unknown code still
// exits 3 with a did you mean, and --list prints the whole catalogue for
// machines.

func TestErrorExplainsAReasonCode(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "STEP_FAILED_SPAWN", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq error STEP_FAILED_SPAWN = %d, want %d\n%s%s",
			got.code, ExitOK, got.stdout, got.stderr)
	}
	for _, want := range []string{
		"STEP_FAILED_SPAWN",
		"the command never started",
		"[step, ends the object]",
		"What to do next:",
		"ls -l",
		"argv0, errno, workdir",
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the explanation does not mention %q:\n%s", want, got.stdout)
		}
	}
}

func TestErrorExplainsANonTerminalReasonCode(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "STEP_RETRY_SCHEDULED", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("exit = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	if strings.Contains(got.stdout, "ends the object") {
		t.Errorf("a non-terminal code is tagged as terminal:\n%s", got.stdout)
	}
	if !strings.Contains(got.stdout, "[step]") {
		t.Errorf("the tag does not name the level:\n%s", got.stdout)
	}
}

// TestErrorReasonCodeCaseAndSpaceInsensitive keeps the lookup forgiving the
// same way the diagnostic one is: codes arrive pasted from terminals and logs.
func TestErrorReasonCodeCaseAndSpaceInsensitive(t *testing.T) {
	for _, arg := range []string{"step_failed_spawn", "  STEP_FAILED_SPAWN  "} {
		got := runCLI(t, t.TempDir(), nil, "error", arg)
		if got.code != ExitOK {
			t.Errorf("paceq error %q = %d, want %d\n%s", arg, got.code, ExitOK, got.stderr)
		}
	}
}

func TestUnknownReasonCodeExitsThreeWithASuggestion(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "STEP_FAILD_SPAWN")

	if got.code != ExitNotFound {
		t.Fatalf("paceq error STEP_FAILD_SPAWN = %d, want %d\n%s%s",
			got.code, ExitNotFound, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stderr, "STEP_FAILD_SPAWN") {
		t.Errorf("the message does not name the code asked for:\n%s", got.stderr)
	}
	if !strings.Contains(got.stderr, "STEP_FAILED_SPAWN") {
		t.Errorf("no did you mean suggestion for a near miss:\n%s", got.stderr)
	}
}

func TestUnknownReasonShapeExitsThreeWithoutASuggestion(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "RUN_NOT_A_THING")

	if got.code != ExitNotFound {
		t.Fatalf("exit = %d, want %d\n%s%s", got.code, ExitNotFound, got.stdout, got.stderr)
	}
	if strings.Contains(got.stderr, "did you mean") {
		t.Errorf("suggested something for a code that is nobody's near miss:\n%s", got.stderr)
	}
}

// TestErrorListJSONIsTheWholeCatalogue is the machine contract for UI and M7:
// one stable array, sorted by code, holding both series with their anatomy.
func TestErrorListJSONIsTheWholeCatalogue(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "--list", "-o", "json")

	if got.code != ExitOK {
		t.Fatalf("paceq error --list = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	var entries []struct {
		Code        string   `json:"code"`
		Series      string   `json:"series"`
		Level       string   `json:"level"`
		Terminal    *bool    `json:"terminal"`
		Title       string   `json:"title"`
		Explanation string   `json:"explanation"`
		Next        []string `json:"next"`
		DataKeys    []string `json:"data_keys"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &entries); err != nil {
		t.Fatalf("stdout is not one JSON array: %v\n%.200s", err, got.stdout)
	}

	wantCount := len(reason.Codes()) + len(catalogue)
	if len(entries) != wantCount {
		t.Fatalf("--list holds %d entries, the two catalogues hold %d", len(entries), wantCount)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Code >= entries[i].Code {
			t.Fatalf("--list is not strictly sorted at %s and %s", entries[i-1].Code, entries[i].Code)
		}
	}

	byCode := map[string]int{}
	for i, e := range entries {
		byCode[e.Code] = i
	}
	spawn := entries[byCode["STEP_FAILED_SPAWN"]]
	if spawn.Series != "reason" || spawn.Level != "step" || spawn.Terminal == nil || !*spawn.Terminal {
		t.Errorf("STEP_FAILED_SPAWN carries the wrong anatomy: %+v", spawn)
	}
	if len(spawn.Next) == 0 || spawn.Title == "" || spawn.Explanation == "" {
		t.Errorf("STEP_FAILED_SPAWN arrives without its full anatomy: %+v", spawn)
	}
	pq := entries[byCode["PQ1040"]]
	if pq.Series != "diagnostic" || pq.Level != "" || pq.Terminal != nil {
		t.Errorf("PQ1040 should carry no reason anatomy: %+v", pq)
	}
	for _, e := range entries {
		if e.Title == "" || e.Explanation == "" || len(e.Next) == 0 {
			t.Errorf("%s arrives without full anatomy: %+v", e.Code, e)
		}
	}
}

func TestErrorListTextNamesBothSeries(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "error", "--list", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("exit = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	for _, want := range []string{"PQ1000", "TICK_SKIPPED_PAUSED", "TRIGGER_ACCEPTED", "RUN_SUCCEEDED", "STEP_CANCELLED"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("--list does not mention %s:\n%s", want, got.stdout)
		}
	}
}

// TestEveryReasonEntryCarriesItsRemedyThroughTheContract re-checks the
// anatomy rule at the boundary a UI actually reads: no reason code may reach
// --list JSON without at least one next step.
func TestEveryReasonEntryCarriesItsRemedyThroughTheContract(t *testing.T) {
	for _, e := range reason.All() {
		if len(e.Remedy) == 0 {
			t.Errorf("%s reaches the contract without a remedy", e.Code)
		}
	}
}
