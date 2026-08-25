package chaos

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestRegressionSeedsFileParses keeps regressions.txt honest from the
// ordinary suite: a malformed row must fail a pull request, not silently
// skip a replay on the nightly, where TestChaosRegressions (behind the
// chaos tag) is the one that reads the file for real.
func TestRegressionSeedsFileParses(t *testing.T) {
	if seeds := readRegressionSeeds(t); len(seeds) == 0 {
		t.Log("regressions.txt holds no seeds yet; every night still runs the standing seed")
	}
}

func TestSeedFromEnvUsesFallbackAndOverride(t *testing.T) {
	if got := seedFromEnv(t, 17); got != 17 {
		t.Fatalf("unset seed = %d, want fallback 17", got)
	}
	t.Setenv(envSeed, "23")
	if got := seedFromEnv(t, 17); got != 23 {
		t.Fatalf("set seed = %d, want 23", got)
	}
}

func seedFromEnv(t *testing.T, fallback int64) int64 {
	t.Helper()

	v := os.Getenv(envSeed)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q is not an integer", envSeed, v)
	}
	return n
}

// readRegressionSeeds parses regressions.txt: one integer seed per line,
// blank lines and # comments ignored. A malformed row is a test failure
// here, not a silent skip, because a seed nobody can parse is a regression
// nobody replays.
func readRegressionSeeds(t *testing.T) []int64 {
	t.Helper()

	path := moduleRoot(t) + "/test/chaos/regressions.txt"
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	var seeds []int64
	scan := bufio.NewScanner(f)
	for line := 1; scan.Scan(); line++ {
		text := strings.TrimSpace(scan.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			t.Fatalf("regressions.txt line %d: %q is not an integer seed", line, fields[0])
		}
		seeds = append(seeds, n)
	}
	if err := scan.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return seeds
}
