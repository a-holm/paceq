package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunPrintsHelp(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"--help"}} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), args, &stdout, &stderr); code != 0 {
			t.Errorf("run(%v) = %d, want 0", args, code)
		}
		if !strings.Contains(stdout.String(), "Usage:") {
			t.Errorf("run(%v) printed no usage text: %q", args, stdout.String())
		}
		if stderr.Len() != 0 {
			t.Errorf("run(%v) wrote to stderr: %q", args, stderr.String())
		}
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"nope"}, &stdout, &stderr); code != 2 {
		t.Errorf("run([nope]) = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Errorf("stderr = %q, want an unknown command message", stderr.String())
	}
}
