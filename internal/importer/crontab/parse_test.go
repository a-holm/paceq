package crontab

import (
	"strings"
	"testing"
)

func TestClassifyLineKinds(t *testing.T) {
	cases := []struct {
		text string
		want lineKind
	}{
		{"", kindBlank},
		{"   ", kindBlank},
		{"# a comment", kindComment},
		{"PATH=/usr/bin:/bin", kindEnv},
		{"MAILTO=", kindEnv},
		{"CRON_TZ=Europe/Oslo", kindEnv},
		{"0 6 * * * /usr/bin/backup", kindSchedule},
		{"*/5 * * * * flock -n /var/lock/x /bin/sync", kindSchedule},
		{"@daily /usr/bin/rydd", kindSpecial},
		{"@reboot /usr/local/bin/boot-thing", kindSpecial},
		{"root 0 6 * * * /usr/bin/backup", kindUnknown},
		{"this is not a cron line", kindUnknown},
	}
	for _, c := range cases {
		got := classifyLine(1, c.text)
		if got.Kind != c.want {
			t.Errorf("classifyLine(%q) = kind %d, want %d", c.text, got.Kind, c.want)
		}
	}
}

// Detection, both ways. Auto mode never guesses a user column into a line;
// the forced reading takes the column off exactly where cron writes it.
func TestSystemCrontabDetection(t *testing.T) {
	// Auto mode: `root 0 6 ...` has no legal five-field reading (a minute
	// field carries no letters), so the line stays verbatim for review.
	auto := classifyLine(3, "root 0 6 * * * /usr/bin/x")
	if auto.Kind != kindUnknown {
		t.Fatalf("auto lettered-first line misread as kind %d", auto.Kind)
	}
	// Auto mode keeps a user line intact even when its command starts with
	// something that looks like a user name.
	user := classifyLine(4, "25 6 * * * backup /usr/local/bin/run")
	if user.Kind != kindSchedule || user.User != "" || user.Command != "backup /usr/local/bin/run" {
		t.Fatalf("user line misread as %+v", user)
	}

	// Forced mode (/etc/crontab, /etc/cron.d): the user column sits between
	// the schedule and the command and comes off.
	sys := classifyLineAs(5, "25 6 * * * root /usr/bin/x", true)
	if sys.Kind != kindSystem {
		t.Fatalf("forced system line misread as kind %d", sys.Kind)
	}
	if sys.User != "root" || sys.Schedule != "25 6 * * *" || sys.Command != "/usr/bin/x" {
		t.Fatalf("system fields wrong: %+v", sys)
	}
	special := classifyLineAs(6, "@daily root /usr/local/bin/x", true)
	if special.Kind != kindSpecial || special.User != "root" || special.Command != "/usr/local/bin/x" {
		t.Fatalf("forced special misread as %+v", special)
	}
}

// Forced six-field reading is what /etc/crontab gets: the user column comes
// off even when a five-field reading would also have parsed.
func TestSixFieldForcedReading(t *testing.T) {
	src := []byte(strings.Join([]string{
		"SHELL=/bin/bash",
		"17 *  * * * root cd / && run-parts --report /etc/cron.hourly",
		"25 6 * * * backup /usr/local/bin/daily-maintenance",
	}, "\n") + "\n")
	res := Import(src, Options{SixField: true})
	if len(res.Docs) != 2 {
		t.Fatalf("docs = %d", len(res.Docs))
	}
	first := res.Docs[0]
	if first.Origin == "" || strings.Contains(first.Job.Steps[0].Run[2], "root cd") {
		t.Fatalf("user column leaked into the command: %q",
			first.Job.Steps[0].Run)
	}
	second := res.Docs[1]
	if second.Name != "daily-maintenance" {
		t.Fatalf("name %q", second.Name)
	}
}

func TestCommandTextKeepsItsSpacing(t *testing.T) {
	ln := classifyLine(9, `17 3 * * 1 /usr/bin/psql  -c   'VACUUM ANALYZE'`)
	if ln.Kind != kindSchedule {
		t.Fatalf("kind %d", ln.Kind)
	}
	if ln.Command != `/usr/bin/psql  -c   'VACUUM ANALYZE'` {
		t.Fatalf("command spacing lost: %q", ln.Command)
	}
	if ln.Schedule != "17 3 * * 1" {
		t.Fatalf("schedule %q", ln.Schedule)
	}
}

func TestParseLinesCountsAndNormalises(t *testing.T) {
	src := []byte("0 6 * * * /bin/a\r\n\r\n# note\n")
	lines := parseLines(src)
	if len(lines) != 3 {
		t.Fatalf("got %d lines", len(lines))
	}
	if lines[0].Text != "0 6 * * * /bin/a" {
		t.Fatalf("carriage return survived: %q", lines[0].Text)
	}
	if lines[2].Number != 3 {
		t.Fatalf("line number %d", lines[2].Number)
	}
}
