package doctor

import "testing"

// TestByteText covers the boundaries between units, which is where a size
// reads as a thousand times what it is.
func TestByteText(t *testing.T) {
	cases := []struct {
		bytes uint64
		want  string
	}{
		{bytes: 0, want: "0 B"},
		{bytes: 999, want: "999 B"},
		{bytes: 1000, want: "1.0 kB"},
		{bytes: 32768, want: "32.8 kB"},
		{bytes: 999999, want: "1000.0 kB"},
		{bytes: 1000000, want: "1.0 MB"},
		{bytes: 64 << 20, want: "67.1 MB"},
		{bytes: 40 << 30, want: "42.9 GB"},
		{bytes: 3 << 40, want: "3.3 TB"},
	}
	for _, c := range cases {
		if got := byteText(c.bytes); got != c.want {
			t.Errorf("byteText(%d) = %q, want %q", c.bytes, got, c.want)
		}
	}
}
