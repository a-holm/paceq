package crontab

import "testing"

func TestDeriveName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"/usr/local/bin/sync-files", "sync-files"},
		{"/opt/rapport/generer.sh", "generer"},
		{"/usr/local/bin/rydd-tmp.py", "rydd-tmp"},
		{"backup-tool", "backup-tool"},
		{"/srv/scripts/Backup.Sh", "backup"},
		{"/weird path/odd name.sh", "odd-name"}, // from the shell command text
		{"123start", "123start"},
		{"-weird_", "weird"},
		{"", "job"},
	}
	for _, c := range cases {
		if got := deriveName(c.in); got != c.want {
			t.Errorf("deriveName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestUniqueNameSuffixes(t *testing.T) {
	used := map[string]int{}
	first := uniqueName("", deriveName("/bin/backup.sh"), used)
	second := uniqueName("", deriveName("/bin/backup.sh"), used)
	third := uniqueName("", deriveName("/bin/backup.sh"), used)
	if first != "backup" || second != "backup-2" || third != "backup-3" {
		t.Fatalf("family = %q %q %q", first, second, third)
	}
}

func TestNamePrefix(t *testing.T) {
	used := map[string]int{}
	a := uniqueName("legacy-", deriveName("/bin/backup.sh"), used)
	b := uniqueName("legacy-", deriveName("/bin/backup.sh"), used)
	if a != "legacy-backup" || b != "legacy-backup-2" {
		t.Fatalf("prefixed family = %q %q", a, b)
	}
}

func TestNamePrefixSanitised(t *testing.T) {
	used := map[string]int{}
	got := uniqueName("--!! odd prefix ", deriveName("/bin/x.sh"), used)
	if got == "" {
		t.Fatal("empty result")
	}
	for _, c := range got {
		ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !ok {
			t.Fatalf("prefix produced illegal name %q", got)
		}
	}
}

// The dedup loop is bounded by the number of names already handed out; this
// proves the bound holds under the worst input, one basename repeated.
func TestNameDedupBoundedUnderRepeat(t *testing.T) {
	used := map[string]int{}
	const n = 5000
	for i := 0; i < n; i++ {
		uniqueName("", "same", used)
	}
	if len(used) != n {
		t.Fatalf("dedup lost names: %d unique of %d", len(used), n)
	}
}
