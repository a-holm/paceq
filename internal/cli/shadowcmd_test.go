package cli

import (
	"testing"
	"time"
)

// The --since grammar cron users type: day and week units, hour/minute
// compounds, and refusals that say why rather than silently meaning zero.
func TestParseSince(t *testing.T) {
	h, m := time.Hour, time.Minute
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{in: "7d", want: 7 * 24 * h},
		{in: "12h", want: 12 * h},
		{in: "90m", want: 90 * m},
		{in: "2w", want: 14 * 24 * h},
		{in: "1d12h", want: 36 * h},
		{in: "48h", want: 48 * h},
		{in: "5x", err: true},
		{in: "d", err: true},
		{in: "", err: true},
	}
	for _, tc := range cases {
		got, err := parseSince(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("parseSince(%q) accepted %q, want refusal", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSince(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseSince(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}
