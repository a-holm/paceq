package crontab

import (
	"reflect"
	"strings"
	"testing"
)

func TestSplitPercent(t *testing.T) {
	cases := []struct {
		in          string
		command     string
		stdin       string
		description string
	}{
		{`date +\%Y`, "date +%Y", "", "backslash escaped percent becomes literal in the command"},
		{"date +%Y", "date +", "Y", "an unescaped percent always starts stdin, even in a date format"},
		{"mysql db % row one", "mysql db ", " row one", "first percent starts stdin"},
		{"a%%b", "a", "\nb", "every later percent is a newline"},
		{"x%", "x", "", "trailing percent, empty stdin"},
		{"no percent at all", "no percent at all", "", "plain command untouched"},
	}
	for _, c := range cases {
		cmd, stdin := splitPercent(c.in)
		if cmd != c.command || stdin != c.stdin {
			t.Errorf("%s: splitPercent(%q) = (%q, %q), want (%q, %q)",
				c.description, c.in, cmd, stdin, c.command, c.stdin)
		}
	}
}

func TestToArgv(t *testing.T) {
	cases := []struct {
		in         string
		want       []string
		needsShell bool
	}{
		{"/usr/local/bin/sync-files --fast", []string{"/usr/local/bin/sync-files", "--fast"}, false},
		{`/usr/bin/psql -c 'VACUUM ANALYZE'`, []string{"/usr/bin/psql", "-c", "VACUUM ANALYZE"}, false},
		{`/bin/echo "two words"`, []string{"/bin/echo", "two words"}, false},
		{"/opt/rapport/kjor.sh --dato=$(date +%Y-%m-%d)", nil, true},
		{"cd /srv && ./run.sh", nil, true},
		{"/bin/kill -9 123 ; /bin/true", nil, true},
		{"/bin/grep 'a|b' f", nil, true}, // the pipe character is meta even quoted
		{`/bin/echo trailing\`, nil, true},
	}
	for _, c := range cases {
		got, needsShell := toArgv(c.in)
		if needsShell != c.needsShell {
			t.Errorf("toArgv(%q) needsShell = %v, want %v", c.in, needsShell, c.needsShell)
			continue
		}
		if !c.needsShell && !reflect.DeepEqual(got, c.want) {
			t.Errorf("toArgv(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestShlexSplitQuoteForms(t *testing.T) {
	cases := []struct {
		in   string
		want []string
		ok   bool
	}{
		{`/bin/a b c`, []string{"/bin/a", "b", "c"}, true},
		{`/bin/a 'b  c'`, []string{"/bin/a", "b  c"}, true},
		{`/bin/a "b\"c"`, []string{"/bin/a", `b"c`}, true},
		{`/bin/a b\ c`, []string{"/bin/a", "b c"}, true},
		{`/bin/a 'open`, nil, false},
		{`/bin/a "open`, nil, false},
		{`/bin/a lone\`, nil, false},
		{`/bin/a ""`, []string{"/bin/a", ""}, true},
	}
	for _, c := range cases {
		got, ok := shlexSplit(c.in)
		if ok != c.ok {
			t.Errorf("shlexSplit(%q) ok = %v, want %v", c.in, ok, c.ok)
			continue
		}
		if ok && !reflect.DeepEqual(got, c.want) {
			t.Errorf("shlexSplit(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestHeredocWrapsStdinFaithfully(t *testing.T) {
	payload, ok := heredoc("row one;\nrow two;")
	if !ok {
		t.Fatal("plain payload refused")
	}
	if !strings.HasPrefix(payload, "<<'PACEQ_STDIN_EOF'\n") || !strings.HasSuffix(payload, "\nPACEQ_STDIN_EOF") {
		t.Fatalf("heredoc shape wrong: %q", payload)
	}
	if _, ok := heredoc("PACEQ_STDIN_EOF"); ok {
		t.Fatal("payload colliding with the mark must be refused")
	}
}
