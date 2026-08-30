package obs

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

// counterSamples parses one exposition document and returns the value of every
// sample belonging to a family the document itself declares a counter. The
// declaration is the contract Prometheus reads, so it is what the guard holds.
func counterSamples(doc string) map[string]float64 {
	counters := map[string]bool{}
	for _, line := range strings.Split(doc, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 4 && fields[0] == "#" && fields[1] == "TYPE" && fields[3] == "counter" {
			counters[fields[2]] = true
		}
	}
	out := map[string]float64{}
	for _, line := range strings.Split(doc, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndex(line, " ")
		if cut < 0 {
			continue
		}
		series, raw := line[:cut], line[cut+1:]
		name := series
		if brace := strings.IndexByte(series, '{'); brace >= 0 {
			name = series[:brace]
		}
		if !counters[name] {
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			continue
		}
		out[series] = v
	}
	return out
}

// TestEveryCounterSeriesOnlyRises walks the operator's own sequence - a
// permanent failure, an operator retry, a delivery, a retention prune - and
// holds every counter-typed series to the one promise the type makes. A
// counter that falls makes rate() invent traffic that never happened.
func TestEveryCounterSeriesOnlyRises(t *testing.T) {
	src, clk := fixture()
	c := NewCollector(src, src.counters, clk,
		Identity{Version: "0.1.0-test", Commit: "a1b2c3d", GoVersion: "go1.27.0"}, "")
	ctx := context.Background()

	// Three notifications exhausted max_attempts.
	for i := 0; i < 3; i++ {
		src.counters.ObserveNotificationGaveUp()
	}
	src.notifGivenUp = 3
	previous := counterSamples(string(c.Scrape(ctx)))
	gaveUpAtStart := previous["pulseq_notifications_failed_total"]

	steps := []struct {
		what string
		do   func()
	}{
		{"paceq notifications retry clears failed_at on one row", func() {
			src.notifGivenUp = 2
			src.notifPending = 1
		}},
		{"the retried row delivers", func() { src.notifPending = 0 }},
		{"retention prunes the delivered row", func() {}},
		{"another notification exhausts max_attempts", func() {
			src.counters.ObserveNotificationGaveUp()
			src.notifGivenUp = 3
		}},
	}
	for _, step := range steps {
		step.do()
		now := counterSamples(string(c.Scrape(ctx)))
		for series, was := range previous {
			is, still := now[series]
			if !still {
				t.Errorf("after %s the counter series %s disappeared", step.what, series)
				continue
			}
			if is < was {
				t.Errorf("after %s the counter series %s fell from %v to %v",
					step.what, series, was, is)
			}
		}
		previous = now
	}
	if got := counterSamples(string(c.Scrape(ctx)))["pulseq_notifications_failed_total"]; got != gaveUpAtStart+1 {
		t.Errorf("the permanent-failure counter reads %v after one more give-up, want %v",
			got, gaveUpAtStart+1)
	}
	if !strings.Contains(string(c.Scrape(ctx)), "pulseq_notifications_given_up 3") {
		t.Error("the row-state gauge never reports the three rows sitting given up on")
	}
}
