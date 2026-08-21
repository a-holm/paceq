package id_test

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

var t0 = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

func TestNewIsAValidTwentySixCharacterULID(t *testing.T) {
	got, err := id.New(t0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(got) != id.Length {
		t.Errorf("New() = %q, length %d, want %d", got, len(got), id.Length)
	}
	if got != strings.ToUpper(got) {
		t.Errorf("New() = %q, want upper case Crockford base32", got)
	}
	if _, err := id.Parse(got); err != nil {
		t.Errorf("Parse(New()) = %v, want nil", err)
	}
}

func TestTimeRoundTripsToTheMillisecond(t *testing.T) {
	stamp := t0.Add(123 * time.Millisecond)
	s, err := id.New(stamp)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := id.Time(s)
	if err != nil {
		t.Fatalf("Time(%q): %v", s, err)
	}
	if !got.Equal(stamp) {
		t.Errorf("Time(%q) = %v, want %v", s, got, stamp)
	}
	if got.Location() != time.UTC {
		t.Errorf("Time(%q) location = %v, want UTC", s, got.Location())
	}
}

func TestParseRejectsBadInputAndCanonicalisesGoodInput(t *testing.T) {
	valid, err := id.New(t0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"generated id", valid, valid, true},
		{"lower case input", strings.ToLower(valid), valid, true},
		{"surrounding space", " " + valid + " ", valid, true},
		{"empty", "", "", false},
		{"too short", valid[:25], "", false},
		{"too long", valid + "0", "", false},
		{"letter I is not in the alphabet", "0" + strings.Repeat("I", 25), "", false},
		{"letter U is not in the alphabet", "0" + strings.Repeat("U", 25), "", false},
		{"punctuation", strings.Repeat("-", 26), "", false},
		{"overflows 128 bits", "8" + strings.Repeat("0", 25), "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := id.Parse(tc.in)
			if tc.ok {
				if err != nil {
					t.Fatalf("Parse(%q) = %v, want nil", tc.in, err)
				}
				if got != tc.want {
					t.Errorf("Parse(%q) = %q, want %q", tc.in, got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("Parse(%q) = %q, want an error", tc.in, got)
			}
			if !errors.Is(err, id.ErrInvalid) {
				t.Errorf("Parse(%q) error = %v, want it to wrap ErrInvalid", tc.in, err)
			}
		})
	}
}

// TestOrderIsChronological is the whole reason the project uses ULIDs: ORDER BY
// id is ORDER BY creation time, for free, with B-tree locality.
func TestOrderIsChronological(t *testing.T) {
	const n = 10_000

	ids := make([]string, 0, n)
	for i := range n {
		// Several ids share a millisecond, which is where monotonic entropy has
		// to hold the order up.
		s, err := id.New(t0.Add(time.Duration(i/4) * time.Millisecond))
		if err != nil {
			t.Fatalf("New #%d: %v", i, err)
		}
		ids = append(ids, s)
	}

	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)

	for i := range ids {
		if ids[i] != sorted[i] {
			t.Fatalf("id %d is %q after sorting but %q in creation order: lexicographic order is not chronological order",
				i, sorted[i], ids[i])
		}
	}
}

func TestNewIsSafeForConcurrentUse(t *testing.T) {
	const (
		goroutines = 32
		perGoro    = 10_000
	)

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		all = make(map[string]bool, goroutines*perGoro)
	)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]string, 0, perGoro)
			for range perGoro {
				s, err := id.New(t0)
				if err != nil {
					t.Errorf("New: %v", err)
					return
				}
				local = append(local, s)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, s := range local {
				if all[s] {
					t.Errorf("duplicate id %q", s)
					return
				}
				all[s] = true
			}
		}()
	}
	wg.Wait()

	if len(all) != goroutines*perGoro {
		t.Errorf("generated %d distinct ids, want %d", len(all), goroutines*perGoro)
	}
}

func TestNewSurvivesAWallClockJumpBackwards(t *testing.T) {
	later, err := id.New(t0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	earlier, err := id.New(t0.Add(-time.Hour))
	if err != nil {
		t.Fatalf("New with a timestamp an hour earlier: %v", err)
	}
	if earlier >= later {
		t.Errorf("id for the earlier timestamp %q sorts at or after %q", earlier, later)
	}
}
