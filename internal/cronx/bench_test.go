package cronx

import (
	"testing"
	"time"
)

// BenchmarkBetweenOneYearOfEveryFiveMinutes is the preview and backfill
// budget from the issue: a year of */5 (about 105k occurrences) must come
// back well under 200ms.
func BenchmarkBetweenOneYearOfEveryFiveMinutes(b *testing.B) {
	s, err := Parse("*/5 * * * *")
	if err != nil {
		b.Fatal(err)
	}
	u, err := LoadZone("UTC")
	if err != nil {
		b.Fatal(err)
	}
	from, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")
	to, _ := time.Parse(time.RFC3339, "2027-01-01T00:00:00Z")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := s.Between(from, to, u, Policy{})
		if err != nil {
			b.Fatal(err)
		}
		if len(got) < 100000 {
			b.Fatalf("only %d occurrences, the benchmark proves nothing", len(got))
		}
	}
}
