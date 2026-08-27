package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// Parser fixtures from the four log shapes cron actually writes (#32, test 5).
// The `%` cases matter because vixie-cron splits commands at % into stdin and
// the log records the raw line: the matcher must survive it.
func TestParseJournalLine(t *testing.T) {
	ref := time.Date(2027, 1, 4, 6, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		line    string
		wantOK  bool
		user    string
		command string
	}{
		{
			name:    "debian journal with zone offset",
			line:    "2027-01-04T06:00:01+0100 host CRON[12345]: (johan) CMD (/home/johan/bin/backup.sh --full)",
			wantOK:  true,
			user:    "johan",
			command: "/home/johan/bin/backup.sh --full",
		},
		{
			name:    "ubuntu journal fractional seconds",
			line:    "2027-01-04T06:00:01.123456+0100 host CRON[99]: (root) CMD (/usr/local/bin/sync-files)",
			wantOK:  true,
			user:    "root",
			command: "/usr/local/bin/sync-files",
		},
		{
			name:    "rhel crond spelling",
			line:    "2027-01-04T05:00:02Z host CROND[555]: (root) CMD (/usr/sbin/logrotate /etc/logrotate.conf)",
			wantOK:  true,
			user:    "root",
			command: "/usr/sbin/logrotate /etc/logrotate.conf",
		},
		{
			name:    "percent trap in the command body",
			line:    "2027-01-04T06:00:00Z host CRON[12]: (johan) CMD (/bin/feed -i % STDIN body)",
			wantOK:  true,
			user:    "johan",
			command: "/bin/feed -i % STDIN body",
		},
		{
			name:   "CMDOUT is not a start",
			line:   "2027-01-04T06:03:33Z host CRON[12]: (johan) CMDOUT ((failed))",
			wantOK: false,
		},
		{
			name:   "unrelated daemon line",
			line:   "2027-01-04T06:03:33Z host sshd[77]: Accepted publickey for johan",
			wantOK: false,
		},
		{
			name:   "garbage after the stamp",
			line:   "2027-01-04T06:03:33Z",
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseJournalLine(tc.line)
			if ok != tc.wantOK {
				t.Fatalf("ParseJournalLine ok=%v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if got.User != tc.user || got.Command != tc.command {
				t.Fatalf("parsed (%q, %q), want (%q, %q)", got.User, got.Command, tc.user, tc.command)
			}
			if !got.At.Before(ref.Add(24*time.Hour)) || got.At.After(ref.Add(48*time.Hour)) {
				t.Fatalf("stamp %v is nowhere near the fixture window", got.At)
			}
		})
	}
}

func TestParseSyslogLine(t *testing.T) {
	now := time.Date(2027, 1, 5, 10, 0, 0, 0, time.FixedZone("CET", 3600))
	cases := []struct {
		name   string
		line   string
		wantOK bool
		day    int
		hour   int
		minute int
	}{
		{
			name:   "debian classic double space day",
			line:   "Jan  4 06:00:01 host CRON[4123]: (johan) CMD (/home/johan/bin/backup.sh)",
			wantOK: true, day: 4, hour: 6, minute: 0,
		},
		{
			name:   "rhel classic crond tag",
			line:   "Jan  4 06:00:01 host crond[811]: (root) CMD (/usr/bin/cleanup --deep)",
			wantOK: true, day: 4, hour: 6, minute: 0,
		},
		{
			name:   "ubuntu lowercase user prefix inside parens only",
			line:   "Jan  4 23:59:58 host cron[512]: (www-data) CMD ([ -x /srv/ping ] && /srv/ping)",
			wantOK: true, day: 4, hour: 23, minute: 59,
		},
		{
			name:   "year wrap back into january is inferred",
			line:   "Jan  1 00:00:30 host cron[9]: (johan) CMD (/bin/happy-new-year)",
			wantOK: true, day: 1, hour: 0, minute: 0,
		},
		{
			name:   "a random service is refused",
			line:   "Jan  4 06:00:01 host systemd[1]: Started Daily apt download.",
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseSyslogLine(tc.line, now)
			if ok != tc.wantOK {
				t.Fatalf("ParseSyslogLine ok=%v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if got.At.Day() != tc.day || int(got.At.Month()) != 1 ||
				got.At.Hour() != tc.hour || got.At.Minute() != tc.minute {
				t.Fatalf("inferred %v, want Jan %d %02d:%02d in the machine zone",
					got.At, tc.day, tc.hour, tc.minute)
			}
		})
	}
}

func TestMatchedJobPrefersExactThenFirstToken(t *testing.T) {
	refs := []store.JobCommandRef{
		{JobName: "sync", Command: "/usr/local/bin/sync-files -q"},
		{JobName: "feed", Command: "/bin/feed /etc/feed.conf"},
	}
	if got := MatchedJob("/bin/feed /etc/feed.conf", refs); got != "feed" {
		t.Fatalf("exact match returned %q", got)
	}
	if got := MatchedJob("/usr/local/bin/sync-files -v", refs); got != "sync" {
		t.Fatalf("first-token match returned %q", got)
	}
	if got := MatchedJob("/completely/other", refs); got != "" {
		t.Fatalf("a stranger matched %q", got)
	}
}

// Fuzz: parsing must never panic whatever a foreign syslog/journal writes,
// and every accepted line must be deterministic on a re-parse.
func FuzzParseJournalLine(f *testing.F) {
	f.Add("2027-01-04T06:00:01+0100 host CRON[12345]: (johan) CMD (/bin/true)")
	f.Add("2027-01-04T06:00:01.999999Z host CRON[1]: (root) CMD ()")
	f.Add("x y z w")
	f.Add("")
	f.Fuzz(func(t *testing.T, line string) {
		a, _ := ParseJournalLine(line)
		b, _ := ParseJournalLine(line)
		if a != b {
			t.Fatalf("parser is not deterministic on %q", line)
		}
	})
}

func FuzzParseSyslogLine(f *testing.F) {
	f.Add("Jan  4 06:00:01 host cron[9]: (johan) CMD (/bin/true)")
	f.Add("Dec 31 23:59:59 h crond[2]: (x) CMD (echo \\%here)")
	f.Add("\xff\xfe garbage")
	f.Add("")
	f.Fuzz(func(t *testing.T, line string) {
		now := time.Date(2026, 6, 15, 9, 0, 0, 0, time.UTC)
		a, _ := ParseSyslogLine(line, now)
		b, _ := ParseSyslogLine(line, now)
		if a != b {
			t.Fatalf("syslog parser is not deterministic on %q", line)
		}
	})
}

// The sweep folds observations through the UNIQUE gate: the same source seen
// twice lands once, and the matcher names the imported job.
func TestSweepStoresAndMatchesObservations(t *testing.T) {
	ctx := context.Background()
	st := newObsStore(t)

	src := stubSource{starts: []ObservedStart{
		{
			At: time.Date(2026, 8, 20, 6, 0, 1, 0, time.UTC), User: "johan",
			Command: "/usr/bin/backup.sh --full",
			Raw:     "Aug 20 06:00:01 host cron[99]: (johan) CMD (/usr/bin/backup.sh --full)",
		},
		{
			At: time.Date(2026, 8, 20, 6, 0, 1, 0, time.UTC), User: "nobody",
			Command: "/unknown/binary",
			Raw:     "Aug 20 06:00:01 host cron[98]: (nobody) CMD (/unknown/binary)",
		},
	}}
	n, err := Sweep(ctx, st, src, time.Time{}, time.Date(2026, 8, 20, 7, 0, 0, 0, time.UTC))
	if err != nil || n != 2 {
		t.Fatalf("first sweep inserted %d (err=%v)", n, err)
	}
	n, err = Sweep(ctx, st, src, time.Date(2026, 8, 20, 7, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 20, 7, 1, 0, 0, time.UTC))
	if err != nil || n != 0 {
		t.Fatalf("reread of the same window re-inserted %d (err=%v)", n, err)
	}
	rows, err := st.ListShadowObservations(ctx, 0, "")
	if err != nil || len(rows) != 2 {
		t.Fatalf("%d rows stored (err=%v)", len(rows), err)
	}
	if rows[0].JobName != "backup" || !rows[0].HasJob {
		t.Fatalf("the first-token match failed to name job backup: %+v", rows[0])
	}
	if rows[1].HasJob {
		t.Fatalf("an unknown binary must stay unmatched: %+v", rows[1])
	}
}

// newObsStore builds a real store whose job backup carries one exec step that
// the observation matcher resolves against.
func newObsStore(t *testing.T) *store.Store {
	t.Helper()
	s := testutil.TempStore(t)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "backup",
		SpecHash: "sha256:obs",
		SpecJSON: `{"schema":"paceq.job.v1","name":"backup","steps":[{"name":"dump","run":["/usr/bin/backup.sh","--full"]}]}`,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return s
}

type stubSource struct{ starts []ObservedStart }

func (s stubSource) Name() string { return store.ObsSourceFile }
func (s stubSource) Fetch(_ context.Context, since time.Time) ([]ObservedStart, error) {
	var out []ObservedStart
	for _, s := range s.starts {
		if !s.At.Before(since) {
			out = append(out, s)
		}
	}
	return out, nil
}
