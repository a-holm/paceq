package scheduler

import (
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/reason"
)

// Catch-up is a policy, never an accident of uptime. This file decides, for
// one schedule's overdue fire-times, which ones attempt materialisation and
// which ones are recorded as skips with a reason from the catalogue. It is a
// pure function: no database, no clock, no I/O, so the whole acceptance
// matrix can run against fake occurrences.

// freshNowWindow is how close to now an occurrence must sit for the skip
// policy to still run it: a tick that came due within the last second is
// "now", not backlog. Older than that, and only catchup=last/all will run it.
const freshNowWindow = time.Second

// skipDecision is one overdue fire-time that will not attempt a run, together
// with the catalogue code that says why.
type skipDecision struct {
	Occurrence cronx.Occurrence
	Code       reason.Code
}

// applyCatchup sorts one schedule's overdue occurrences into attempts and
// explained skips. occs must hold the real (non-DST-skipped) occurrences in
// Between's order, oldest timetable position first.
//
// The three policies, per the issue:
//
//   - skip (default): only a fresh-now occurrence runs; everything older is
//     thrown away with TICK_SKIPPED_CATCHUP_DISABLED. Cron users coming from
//     plain cron get the least surprising behaviour: nothing runs late.
//   - last: exactly the newest occurrence runs, the rest carry
//     TICK_SKIPPED_CATCHUP_LAST_ONLY.
//   - all: every in-window occurrence runs, drip-fed at most catchupLimit
//     per call; the caller recomputes the remainder next wake, because each
//     committed tick advances the cursor.
//
// Whatever the policy, occurrences older than now minus catchup_window_ms go
// out first with TICK_SKIPPED_CATCHUP_WINDOW: the window bounds how far back
// a restart digs.
func applyCatchup(occs []cronx.Occurrence, catchup string, catchupLimit int,
	catchupWindowMS int64, now time.Time,
) ([]cronx.Occurrence, []skipDecision) {
	cutoff := now.Add(-time.Duration(catchupWindowMS) * time.Millisecond)
	var attempts []cronx.Occurrence
	var skips []skipDecision

	// Window filter first: however eager the policy, nothing older than the
	// window is ever replayed.
	var inWindow []cronx.Occurrence
	for _, o := range occs {
		if o.At.Before(cutoff) {
			skips = append(skips, skipDecision{Occurrence: o, Code: reason.TICKSkippedCatchupWindow})
			continue
		}
		inWindow = append(inWindow, o)
	}
	if len(inWindow) == 0 {
		return attempts, skips
	}

	switch catchup {
	case "last":
		last := inWindow[len(inWindow)-1]
		attempts = append(attempts, last)
		for _, o := range inWindow[:len(inWindow)-1] {
			skips = append(skips, skipDecision{Occurrence: o, Code: reason.TICKSkippedCatchupLastOnly})
		}

	case "all":
		dose := inWindow
		if len(dose) > catchupLimit {
			// Drip, not storm: this pass takes the oldest slice, and the
			// remainder is recomputed next wake from the advanced cursor.
			dose = dose[:catchupLimit]
		}
		attempts = append(attempts, dose...)

	default:
		// "skip" and anything unrecognised: run only what is effectively
		// happening right now, explain everything older.
		fresh := now.Add(-freshNowWindow)
		last := inWindow[len(inWindow)-1]
		if last.At.After(fresh) {
			attempts = append(attempts, last)
			for _, o := range inWindow[:len(inWindow)-1] {
				skips = append(skips, skipDecision{Occurrence: o, Code: reason.TICKSkippedCatchupDisabled})
			}
			return attempts, skips
		}
		for _, o := range inWindow {
			skips = append(skips, skipDecision{Occurrence: o, Code: reason.TICKSkippedCatchupDisabled})
		}
	}
	return attempts, skips
}
