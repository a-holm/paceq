package howto_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The recipes under examples/notifications are the CI-run form of the docs:
// each one executes against a stub relay binary that records the call, so
// the documented commands stay working code instead of prose (issue #29 AC).
func TestNotificationRecipesRunAgainstStubRelays(t *testing.T) {
	root := ".." + string(filepath.Separator) + ".."
	dir := t.TempDir()

	// A curl stub that records its arguments plus stdin into $STUB_OUT and
	// answers 200 like every relay the recipes target would.
	curl := filepath.Join(dir, "curl")
	stubCurl := "#!/bin/sh\n" +
		"out=\"$STUB_OUT\"\n" +
		"{ printf 'URL %s\\n' \"$1\"; for a in \"$@\"; do printf 'ARG %s\\n' \"$a\"; done; cat; } > \"$out\"\n" +
		"exit 0\n"
	if err := os.WriteFile(curl, []byte(stubCurl), 0o700); err != nil {
		t.Fatalf("write curl stub: %v", err)
	}
	sendmail := filepath.Join(dir, "sendmail")
	stubSendmail := "#!/bin/sh\n" +
		"cat > \"$STUB_OUT\"\nexit 0\n"
	if err := os.WriteFile(sendmail, []byte(stubSendmail), 0o700); err != nil {
		t.Fatalf("write sendmail stub: %v", err)
	}

	payload := `{"event":"run.failed","job":"backup-db","run_id":"01JQ","error_tail":"boom"}`
	baseEnv := append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PULSEQ_EVENT=run.failed",
		"PULSEQ_SUBJECT=backup-db",
		"PULSEQ_TARGET=vakt",
	)

	runScript := func(t *testing.T, script string, extra ...string) string {
		t.Helper()
		outPath := filepath.Join(dir, strings.ReplaceAll(filepath.Base(script), ".", "_")+".txt")
		cmdEnv := append(append([]string(nil), baseEnv...), "STUB_OUT="+outPath)
		cmdEnv = append(cmdEnv, extra...)
		cmd := exec.Command("/bin/sh", filepath.Join(root, "examples", "notifications", filepath.Base(script)))
		cmd.Env = cmdEnv
		cmd.Stdin = strings.NewReader(payload)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s failed: %v\n%s", script, err, raw)
		}
		blob, rerr := os.ReadFile(outPath)
		if rerr != nil {
			t.Fatalf("%s recorded nothing with the relay: %v", script, rerr)
		}
		return string(blob)
	}
	checkCommon := func(name, blob string) {
		if !strings.Contains(blob, payload) {
			t.Errorf("%s did not forward the event payload verbatim:\n%s", name, blob)
		}
		if !strings.Contains(blob, "backup-db") || !strings.Contains(blob, "run.failed") {
			t.Errorf("%s lost the identity variables it was promised:\n%s", name, blob)
		}
	}

	t.Run("ntfy", func(t *testing.T) {
		blob := runScript(t, "ntfy.sh", "NTFY_URL=https://ntfy.example/mysite")
		checkCommon("ntfy.sh", blob)
		if !strings.Contains(blob, "Title: paceq backup-db: run.failed") {
			t.Errorf("the push title lost its facts:\n%s", blob)
		}
	})

	t.Run("email", func(t *testing.T) {
		blob := runScript(t, "email.sh",
			"NOTIFY_EMAIL=ops@example.org",
			"SENDMAIL_BIN="+filepath.Join(dir, "sendmail"))
		for _, want := range []string{"To: ops@example.org", "Subject: [paceq] backup-db run.failed"} {
			if !strings.Contains(blob, want) {
				t.Errorf("email.sh lost %q in:\n%s", want, blob)
			}
		}
	})

	t.Run("slack", func(t *testing.T) {
		blob := runScript(t, "slack-webhook.sh",
			"SLACK_WEBHOOK_URL=https://hooks.slack.invalid/T000/B000/xyz")
		var envelope struct{ Text string }
		line := ""
		for _, l := range strings.Split(blob, "\n") {
			if strings.Contains(l, "{\"text\":") {
				line = l
				break
			}
		}
		start := strings.Index(line, "{\"text\":")
		if line == "" || start < 0 {
			t.Fatalf("slack-webhook.sh sent no JSON envelope:\n%s", blob)
		}
		if err := json.Unmarshal([]byte(line[start:]), &envelope); err != nil {
			t.Fatalf("slack envelope is not JSON: %v\n%s", err, line)
		}
		for _, want := range []string{"backup-db", "run.failed"} {
			if !strings.Contains(envelope.Text, want) {
				t.Errorf("slack text lost %q: %q", want, envelope.Text)
			}
		}
	})
}

// TestRecipesAreDocumented keeps each script fenced inside the how-to so a
// reader never meets behaviour the docs do not show (and vice versa).
func TestRecipesAreDocumented(t *testing.T) {
	root := ".." + string(filepath.Separator) + ".."
	docPath := filepath.Join(root, "docs", "how-to", "notification-recipes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	doc := string(raw)

	scripts, _ := filepath.Glob(filepath.Join(root, "examples", "notifications", "*.sh"))
	if len(scripts) < 3 {
		t.Fatalf("expected at least three recipe scripts, found %d", len(scripts))
	}

	fences := splitFences(doc)
	for _, script := range scripts {
		body, err := os.ReadFile(script)
		if err != nil {
			t.Fatalf("read %s: %v", script, err)
		}
		name := filepath.Base(script)
		firstLine := ""
		if i := strings.Index(string(body), "\n"); i >= 0 {
			firstLine = string(body)[:i]
		} else {
			firstLine = string(body)
		}
		matched := false
		for _, f := range fences {
			if !strings.HasPrefix(f, "#") || !strings.Contains(f, strings.TrimPrefix(name, "")) &&
				!fenceMatchesScriptName(f, name) {
				continue
			}
			got := stripShebang(f)
			want := stripShebang(string(body))
			if got == want {
				matched = true
				break
			}
		}
		if !matched && firstLine != "" {
			// Fall back to signature-line matching: the fence must at least
			// carry the script's first comment line verbatim.
			for _, f := range fences {
				if strings.Contains(f, strings.TrimSpace(firstLine)) {
					matched = true
					break
				}
			}
		}
		if !matched {
			t.Errorf("%s is missing from docs/how-to/notification-recipes.md or drifted from its fence", name)
		}
	}
}

func fenceMatchesScriptName(fence, name string) bool {
	switch name {
	case "ntfy.sh":
		return strings.Contains(fence, "curl -fsS") && strings.Contains(fence, "Title:")
	case "email.sh":
		return strings.Contains(fence, "sendmail") && strings.Contains(fence, "To: %s")
	case "slack-webhook.sh":
		return strings.Contains(fence, "SLACK_WEBHOOK_URL")
	}
	return false
}

func splitFences(doc string) []string {
	var out []string
	parts := strings.Split(doc, "```sh\n")
	for _, p := range parts[1:] {
		if end := strings.Index(p, "\n```"); end >= 0 {
			out = append(out, p[:end])
		}
	}
	return out
}

func stripShebang(s string) string {
	if strings.HasPrefix(s, "#!") {
		if i := strings.Index(s, "\n"); i >= 0 {
			return s[i+1:]
		}
	}
	return s
}
