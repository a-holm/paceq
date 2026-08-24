//go:build unix

package sensor

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// fakeSource returns a fixed due list, the stand-in for the store-backed
// reader until M3-01.
type fakeSource struct{ specs []Spec }

func (f *fakeSource) Due(_ context.Context, limit int) ([]Spec, error) {
	if limit > 0 && len(f.specs) > limit {
		return f.specs[:limit], nil
	}
	return f.specs, nil
}

// recSink records every committed Result, the stand-in for M3-03's commit
// transaction in the runtime tests.
type recSink struct {
	mu      sync.Mutex
	commits []struct {
		name   string
		result Result
	}
}

func (r *recSink) Commit(_ context.Context, name string, res Result) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, struct {
		name   string
		result Result
	}{name, res})
	return nil
}

func (r *recSink) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.commits {
		if c.name == name {
			n++
		}
	}
	return n
}

func (r *recSink) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.commits)
}

func (r *recSink) find(name string) (Result, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.commits {
		if c.name == name {
			return c.result, true
		}
	}
	return Result{}, false
}

// waitCommits polls until the sink has recorded n evaluations.
func waitCommits(t *testing.T, s *recSink, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for s.total() < n {
		if time.Now().After(deadline) {
			t.Fatalf("sink recorded %d evaluations after %s, want %d", s.total(), within, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// trackMax reconstructs, from a sensor-track file, how many evaluations of
// one name (and how many total) were in flight at the busiest moment.
func trackMax(t *testing.T, path string) (global int, perName map[string]int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open track: %v", err)
	}
	defer f.Close()
	active := map[string]int{}
	perMax := map[string]int{}
	g := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			t.Fatalf("track line %q: want 3 fields", sc.Text())
		}
		kind, name := fields[0], fields[1]
		switch kind {
		case "start":
			active[name]++
			if active[name] > perMax[name] {
				perMax[name] = active[name]
			}
			g++
			if g > global {
				global = g
			}
		case "end":
			active[name]--
			g--
		}
	}
	return global, perMax
}

// newRuntimeWith builds a runtime over the shared fixture whose due batch
// holds count sensors that each sleep for the given duration when tracked.
func newRuntimeWith(t *testing.T, maxParallel int, fc, track string, count int, sleep string, names string) (*Runtime, *recSink) {
	t.Helper()
	var specs []Spec
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("n%d", i)
		if names != "" {
			name = names
		}
		specs = append(specs, Spec{
			Name:        name,
			Job:         "job",
			Argv:        []string{fc, "sensor-track", track, sleep},
			Timeout:     10 * time.Second,
			MaxTriggers: 100,
		})
	}
	sink := &recSink{}
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source:       &fakeSource{specs: specs},
		Sink:         sink,
		MaxParallel:  maxParallel,
		DrainTimeout: 6 * time.Second,
	})
	return rt, sink
}

// TestRuntimeNeverRunsASensorTwice proves the per-sensor serialisation: a due
// batch that names the same sensor five times starts it exactly once.
func TestRuntimeNeverRunsASensorTwice(t *testing.T) {
	fc := fakecmd(t)
	var specs []Spec
	for range 5 {
		specs = append(specs, Spec{
			Name: "a", Job: "job",
			Argv: []string{fc, "sleep", "300ms"}, Timeout: 5 * time.Second, MaxTriggers: 100,
		})
	}
	sink := &recSink{}
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source: &fakeSource{specs: specs}, Sink: sink, MaxParallel: 4,
	})
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCommits(t, sink, 1, 5*time.Second)
	time.Sleep(400 * time.Millisecond) // let a wrongly-parallel twin declare itself
	if got := sink.count("a"); got != 1 {
		t.Fatalf("sensor %q ran %d times, want exactly once", "a", got)
	}
}

// TestGlobalSemaphore proves the cap from observation: eight due sensors at
// once, a global cap of four, and exactly four are ever in flight.
func TestGlobalSemaphore(t *testing.T) {
	track := filepath.Join(t.TempDir(), "t")
	fc := fakecmd(t)
	rt, sink := newRuntimeWith(t, 4, fc, track, 8, "250ms", "")
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Exactly four missions start on the first wake; the four permits are the
	// only slots. The other four wait for a later wake.
	waitCommits(t, sink, 4, 10*time.Second)
	global, _ := trackMax(t, track)
	if global > 4 {
		t.Fatalf("max concurrent evaluations = %d, want at most 4", global)
	}
	if global != 4 {
		t.Fatalf("max concurrent = %d, want exactly 4 with four permits", global)
	}
}

// TestHangingSensorDoesNotBlockOthers runs one long hung sensor beside four
// fast ones: the fast ones finish while the hung one is still alive, and the
// hung one is killed by its own timeout.
func TestHangingSensorDoesNotBlockOthers(t *testing.T) {
	fc := fakecmd(t)
	probe := filepath.Join(t.TempDir(), "t")
	var specs []Spec
	specs = append(specs, Spec{Name: "g", Argv: []string{fc, "ignore-term", "1h"}, Timeout: 300 * time.Millisecond, MaxTriggers: 100})
	for i := 0; i < 4; i++ {
		specs = append(specs, Spec{
			Name: fmt.Sprintf("n%d", i), Job: "job",
			Argv: []string{fc, "sensor-track", probe, "200ms"}, Timeout: 10 * time.Second, MaxTriggers: 100,
		})
	}
	sink := &recSink{}
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source: &fakeSource{specs: specs}, Sink: sink, MaxParallel: 10, DrainTimeout: 6 * time.Second,
	})
	start := time.Now()
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Step returned at once, not after the hung sensor: a slow sensor never
	// blocks the loop.
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Step blocked for %s behind a hanging sensor; the loop must return at once", d)
	}
	// Four fast sensors finish while the hung one is still in flight.
	waitCommits(t, sink, 4, 10*time.Second)
	// Then the hung one is killed by its own timeout.
	waitCommits(t, sink, 5, 8*time.Second)
	r, ok := sink.find("g")
	if !ok {
		t.Fatal("hung sensor produced no result")
	}
	if r.Outcome != Errored || r.ReasonCode != reason.TICKErrorSensorTimeout {
		t.Fatalf("hung sensor outcome = %v, want an errored timeout", r)
	}
}

// TestRuntimeDrainsOnCancel proves the shutdown story: with an evaluation in
// flight, cancelling the context drains it and Step returns only after no
// subprocess is left.
func TestRuntimeDrainsOnCancel(t *testing.T) {
	fc := fakecmd(t)
	spec := Spec{Name: "a", Argv: []string{fc, "ignore-term", "2s"}, Timeout: 30 * time.Second, MaxTriggers: 100}
	sink := &recSink{}
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source: &fakeSource{specs: []Spec{spec}}, Sink: sink, MaxParallel: 1, DrainTimeout: 6 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.Step(ctx); err != nil {
		t.Fatal(err)
	}
	// The evaluation is in flight; cancel, then Step should drain (kill the
	// group) and return without leaving a subprocess behind.
	cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Step(ctx) }()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Step after cancel returned %v, want context.Canceled", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Step did not drain within the drain timeout")
	}
}
