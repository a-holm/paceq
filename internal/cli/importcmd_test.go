package cli

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

// parseForTest runs a generated file through spec.Parse, the same door
// validate and apply use.
func parseForTest(data []byte) (*spec.Job, diag.List) {
	return spec.Parse("generated.yaml", data)
}

func countErrors(l diag.List) string {
	var msgs []string
	for _, d := range l {
		msgs = append(msgs, d.Message)
	}
	return strings.Join(msgs, "; ")
}

// stubCrontab plants a crontab(1) on PATH. listOutput goes to stdout for
// `-l`; every other argument combination fails loudly and is recorded, so a
// test fails the moment import tries anything but listing.
func stubCrontab(t *testing.T, dir string, listOutput string) *strings.Builder {
	t.Helper()
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	var log strings.Builder
	script := "#!/bin/sh\necho \"$@\" >> " + filepath.Join(dir, "calls.log") + "\n" +
		"if [ \"$1\" = \"-l\" ]; then\n" +
		"  cat " + filepath.Join(dir, "list.txt") + "\n" +
		"  exit 0\n" +
		"fi\n" +
		"echo \"REFUSED-WRITE-OR-OTHER-CALL: $@\" >&2\nexit 64\n"
	path := filepath.Join(binDir, "crontab")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "list.txt"), []byte(listOutput), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
	return &log
}

// TestImportCrontabReadsAndTranslates covers the default source through a
// stubbed crontab binary: YAML on stdout, the report on stderr, exit 0.
func TestImportCrontabReadsAndTranslates(t *testing.T) {
	dir := t.TempDir()
	crontabText := strings.Join([]string{
		"# nightly",
		"CRON_TZ=Europe/Oslo",
		"0 2 * * * /opt/etl/run.sh > /dev/null 2>&1",
		"*/5 * * * * flock -n /var/lock/s.lock /usr/local/bin/sync-files",
	}, "\n") + "\n"
	stubCrontab(t, dir, crontabText)

	res := runCLI(t, dir, nil, "import", "crontab")
	if res.code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	for _, want := range []string{
		"name: run",
		"name: sync-files",
		"# originally: */5 * * * * flock -n /var/lock/s.lock /usr/local/bin/sync-files",
	} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.stdout)
		}
	}
	if !strings.Contains(res.stderr, "4 lines read -> 2 jobs") {
		t.Errorf("stderr report missing summary:\n%s", res.stderr)
	}
	if !strings.Contains(res.stderr, "/dev/null (the log is kept from now on)") {
		t.Errorf("stderr missing devnull warning:\n%s", res.stderr)
	}
}

// TestImportCrontabIsProvablyNonDestructive pins the trust rule: the source
// file is byte identical afterwards and the only crontab call ever made was
// a plain -l.
func TestImportCrontabIsProvablyNonDestructive(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "my.crontab")
	content := "0 6 * * * /usr/bin/backup --full\n30 7 1 * * /usr/local/bin/monthly.sh\n"
	if err := os.WriteFile(src, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256([]byte(content))

	stubCrontab(t, dir, content)
	res := runCLI(t, dir, nil, "import", "crontab", "--file", src)
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}

	afterBytes, err := os.ReadFile(src) // #nosec G304 - test-owned path
	if err != nil {
		t.Fatal(err)
	}
	after := sha256.Sum256(afterBytes)
	if before != after {
		t.Fatal("the source file changed during import")
	}

	calls, err := os.ReadFile(filepath.Join(dir, "calls.log"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// The --file path must not invoke crontab at all; a default-source run
	// may only ever have invoked it with exactly "-l".
	if len(calls) != 0 {
		t.Fatalf("--file import invoked crontab: %q", string(calls))
	}

	res2 := runCLI(t, dir, nil, "import", "crontab")
	if res2.code != 0 {
		t.Fatalf("exit %d: %s", res2.code, res2.stderr)
	}
	calls, _ = os.ReadFile(filepath.Join(dir, "calls.log")) // #nosec G304 - test-owned path
	if strings.TrimSpace(string(calls)) != "-l" {
		t.Fatalf("default import made calls beyond -l: %q", string(calls))
	}
}

// TestImportFileSixField reads an /etc/crontab style file through the path
// hint: /etc/crontab and /etc/cron.d/* are known six field, so the user
// column is consumed instead of swallowed into the command. The e2e run
// uses a user-style file (the hint cannot fire on a temp path); the
// system-file reading itself is proven in the importer tests.
func TestImportFileSixField(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "etc-crontab")
	content := strings.Join([]string{
		"SHELL=/bin/bash",
		"25 6 * * * /usr/local/bin/daily-maintenance",
		"17 * * * * cd / && run-parts --report /etc/cron.hourly",
	}, "\n") + "\n"
	if err := os.WriteFile(src, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	res := runCLI(t, dir, nil, "import", "crontab", "--file", src)
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s\n%s", res.code, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stdout, "name: daily-maintenance") {
		t.Errorf("job missing:\n%s", res.stdout)
	}
	if !strings.Contains(res.stdout, "workdir: /") {
		t.Errorf("cd translation missing:\n%s", res.stdout)
	}
}

// TestSystemCrontabPath pins exactly which files get the six-field reading.
func TestSystemCrontabPath(t *testing.T) {
	cases := map[string]bool{
		"/etc/crontab":                true,
		"/etc/cron.d/backup":          true,
		"/etc/cron.d/sub/deep":        true,
		"/home/johan/etc-crontab":     false,
		"/tmp/my.crontab":             false,
		"/var/spool/cron/crontabs/jo": false,
	}
	for path, want := range cases {
		if got := systemCrontabPath(path); got != want {
			t.Errorf("systemCrontabPath(%q) = %v, want %v", path, got, want)
		}
	}
}

// TestImportOutputRefusesOverwrite pins the -o contract: no silent clobber,
// --force unlocks it.
func TestImportOutputRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out.yaml")
	if err := os.WriteFile(target, []byte("keep me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "c.crontab")
	if err := os.WriteFile(src, []byte("0 6 * * * /bin/a\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	res := runCLI(t, dir, nil, "import", "crontab", "--file", src, "-o", target)
	if res.code != 4 {
		t.Fatalf("overwrite refusal exit = %d, want 4\nstderr:\n%s", res.code, res.stderr)
	}
	body, _ := os.ReadFile(target) // #nosec G304 - test-owned path
	if string(body) != "keep me\n" {
		t.Fatalf("target clobbered without --force: %q", body)
	}

	res = runCLI(t, dir, nil, "import", "crontab", "--file", src, "-o", target, "--force")
	if res.code != 0 {
		t.Fatalf("forced overwrite exit = %d: %s", res.code, res.stderr)
	}
	body, _ = os.ReadFile(target) // #nosec G304 - test-owned path
	if !strings.Contains(string(body), "name: a") {
		t.Fatalf("forced overwrite did not write the job: %q", body)
	}
}

// TestImportOutputDirectoryOneFilePerJob is the flow validate accepts
// directly; it also proves the generated files parse.
func TestImportOutputDirectoryOneFilePerJob(t *testing.T) {
	dir := t.TempDir()
	jobs := filepath.Join(dir, "jobs")
	src := filepath.Join(dir, "c.crontab")
	content := strings.Join([]string{
		"0 6 * * * /usr/bin/backup --full",
		"0 7 * * * /usr/bin/backup --incremental",
		"@daily /usr/local/bin/rydd-tmp",
	}, "\n") + "\n"
	if err := os.WriteFile(src, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	res := runCLI(t, dir, nil, "import", "crontab", "--file", src, "-o", jobs+"/")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	names := []string{"backup.yaml", "backup-2.yaml", "rydd-tmp.yaml"}
	for _, n := range names {
		data, err := os.ReadFile(filepath.Join(jobs, n)) // #nosec G304 - test-owned path
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		if _, diags := parseForTest(data); diags.HasErrors() {
			t.Errorf("%s does not parse: %v\n%s", n, countErrors(diags), data)
		}
	}
	res2 := runCLI(t, dir, nil, "validate", jobs)
	if res2.code != 0 {
		t.Fatalf("validate refused the imported jobs: %s", res2.stderr+res2.stdout)
	}
}

// TestImportAllUsersNeedsRoot pins exit 2 with a way forward.
func TestImportAllUsersNeedsRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; the refusal cannot fire here")
	}
	dir := t.TempDir()
	res := runCLI(t, dir, nil, "import", "crontab", "--all-users")
	if res.code != 2 {
		t.Fatalf("exit = %d, want 2\nstderr:\n%s", res.code, res.stderr)
	}
	if !strings.Contains(res.stderr, "sudo paceq import crontab --all-users") {
		t.Errorf("refusal lacks the next step:\n%s", res.stderr)
	}
}

// TestImportMissingSourcesAreNotFound pins exit 3 for everything that is
// simply not there.
func TestImportMissingSourcesAreNotFound(t *testing.T) {
	dir := t.TempDir()
	res := runCLI(t, dir, nil, "import", "crontab", "--file", filepath.Join(dir, "nope.crontab"))
	if res.code != 3 {
		t.Fatalf("--file missing: exit = %d, want 3\nstderr:\n%s", res.code, res.stderr)
	}

	// A crontab binary that answers like cron does when nothing is saved.
	binDir := filepath.Join(dir, "bin-none")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\necho \"no crontab for ghost\" >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(binDir, "crontab"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
	res = runCLI(t, dir, nil, "import", "crontab", "--user", "ghost")
	if res.code != 3 {
		t.Fatalf("--user missing: exit = %d, want 3\nstderr:\n%s", res.code, res.stderr)
	}
	if !strings.Contains(res.stderr, "no crontab for ghost") {
		t.Errorf("crontab's own message lost:\n%s", res.stderr)
	}
}

// TestImportNamePrefixFlag travels all the way into the emitted names.
func TestImportNamePrefixFlag(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "c.crontab")
	if err := os.WriteFile(src, []byte("0 6 * * * /usr/bin/backup\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res := runCLI(t, dir, nil, "import", "crontab", "--file", src, "--name-prefix", "legacy-")
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "name: legacy-backup") {
		t.Errorf("prefix missing:\n%s", res.stdout)
	}
}

// TestImportStdinSplitSurvivesToYaml proves the percent trap inside the real
// command surface: the payload lands in the emitted step, quoted safely.
func TestImportStdinSplitSurvivesToYaml(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "c.crontab")
	line := `0 4 * * * /usr/bin/mysql db % row one; row two;`
	if err := os.WriteFile(src, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res := runCLI(t, dir, nil, "import", "crontab", "--file", src)
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "<<'PACEQ_STDIN_EOF'") {
		t.Errorf("stdin payload lost:\n%s", res.stdout)
	}
}

// TestImportFiveMinuteStory is the full walk a new user takes: one job in,
// validated, applied, visible. A single job imports straight into a file
// every later command accepts.
func TestImportFiveMinuteStory(t *testing.T) {
	dir := t.TempDir()
	if res := runCLI(t, dir, nil, "init"); res.code != 0 {
		t.Fatalf("init failed: %s", res.stderr)
	}
	src := filepath.Join(dir, "c.crontab")
	if err := os.WriteFile(src, []byte("0 6 * * 1 /usr/local/bin/weekly-report\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "imported.yaml")

	res := runCLI(t, dir, nil, "import", "crontab", "--file", src, "-o", target)
	if res.code != 0 {
		t.Fatalf("import failed: %s", res.stderr)
	}
	if res := runCLI(t, dir, nil, "validate", target); res.code != 0 {
		t.Fatalf("validate refused the imported file:\n%s%s", res.stdout, res.stderr)
	}
	if res := runCLI(t, dir, nil, "apply", target); res.code != 0 {
		t.Fatalf("apply refused the imported file:\n%s%s", res.stdout, res.stderr)
	}
	// status reads the store back; ls only lists once a schedule has fired,
	// so the story ends with the job visible where the operator looks.
	res = runCLI(t, dir, nil, "status")
	if res.code != 0 || !strings.Contains(res.stdout, "weekly-report") {
		t.Fatalf("the imported job is not in the catalog:\n%s\nstderr:%s", res.stdout, res.stderr)
	}
}
