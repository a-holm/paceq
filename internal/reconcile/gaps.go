package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/store"
)

// Gap detection: the answer to "was the system up at all". The downtime gap
// becomes one outages row plus the synthetic missed ticks that name, slot by
// slot, what nobody evaluated while it lasted. The ticks are explanation, not
// catch-up: whether anything should actually run late is the scheduler's
// policy decision, made after this sweep, from its own cursor.
//
// Every part of the write is idempotent. A repeated pass over the same slots
// inserts nothing (the tick slot uniqueness decides), and an outage row whose
// evidence was cut off by a crash is completed on a later pass: the row with
// zero ticks IS the marker that work remains.

// recordGap writes the outages row for the captured gap and fills it with
// synthetic evidence. Gaps shorter than the threshold write nothing at all.
func recordGap(ctx context.Context, st *store.Store, opts Options, t0 time.Time, bootChanged bool) error {
	from := opts.GapFrom.UTC()
	if t0.Sub(from) < GapThreshold {
		return nil
	}

	kind := "crash"
	if bootChanged {
		// The schema's word. The issue sketch says 'reboot'; the CHECK
		// constraint in migrations/0001 says 'boot', and it wins.
		kind = "boot"
	}

	outageID, err := st.RecordOutage(ctx, store.OutageInput{
		From:        from,
		To:          t0,
		Kind:        kind,
		PrevSession: opts.PrevSessionID,
	})
	if err != nil {
		return fmt.Errorf("write the %s outage row for [%s, %s]: %w", kind, from, t0, err)
	}

	n, err := fillOutage(ctx, st, opts, store.Outage{ID: outageID, From: from, To: t0})
	if err != nil {
		return fmt.Errorf("write the tick evidence of outage %d: %w", outageID, err)
	}
	faults.Point("M2:reconcile:after_outage_for_ticks")

	// R9 without a new table: run_events cannot hold a daemon-scoped event
	// (its run_id is NOT NULL), so the outages row itself carries the facts,
	// and the log line says them out loud for anyone watching a restart.
	opts.logger().Info("daemon recovered",
		"kind", kind,
		"gap_ms", t0.Sub(from).Milliseconds(),
		"missed_ticks", n,
		"outage", outageID,
		"prev_session", opts.PrevSessionID)
	return nil
}

// backfillOutages completes outage rows an interrupted pass left behind.
// A row with zero ticks either genuinely covered no slots or lost its
// evidence to a crash mid write; walking it again settles which, cheaply and
// idempotently, every time.
func backfillOutages(ctx context.Context, st *store.Store, opts Options) error {
	pending, err := st.OutagesWithoutTicks(ctx)
	if err != nil {
		return fmt.Errorf("find the outage rows still missing their ticks: %w", err)
	}
	for _, o := range pending {
		n, err := fillOutage(ctx, st, opts, o)
		if err != nil {
			return fmt.Errorf("complete outage %d: %w", o.ID, err)
		}
		if n > 0 {
			opts.logger().Info("finished the tick evidence of an outage left half written",
				"outage", o.ID, "added_ticks", n)
		}
	}
	return nil
}

// fillOutage walks every enabled schedule across [o.From, o.To] and writes
// one missed tick per fire time nobody evaluated. It returns how many rows it
// actually inserted; slots that already had a tick were really evaluated
// before the crash and are never rewritten.
func fillOutage(ctx context.Context, st *store.Store, opts Options, o store.Outage) (int, error) {
	schedules, err := st.GapSchedules(ctx)
	if err != nil {
		return 0, fmt.Errorf("list the schedules to walk: %w", err)
	}

	var batch []store.MissedTick
	for _, g := range schedules {
		sched, err := cronx.Parse(g.Expr)
		if err != nil {
			// A definition that does not parse today explains nothing about
			// yesterday's slots. Leave it out, say so, keep walking.
			opts.logger().Warn("a schedule could not be parsed during the gap walk",
				"job", g.JobName, "schedule", g.Name, "error", err)
			continue
		}
		loc, err := cronx.LoadZone(g.Timezone)
		if err != nil {
			opts.logger().Warn("a schedule's zone could not be loaded during the gap walk",
				"job", g.JobName, "schedule", g.Name, "zone", g.Timezone, "error", err)
			continue
		}
		occs, err := sched.Between(o.From, o.To, loc, cronx.Policy{})
		if err != nil && len(occs) == 0 {
			opts.logger().Warn("a schedule produced no walkable slots in the gap",
				"job", g.JobName, "schedule", g.Name, "error", err)
			continue
		}
		for _, oc := range occs {
			batch = append(batch, store.MissedTick{
				SourceName:   sourceNameOf(g),
				ScheduledFor: oc.At,
			})
		}
	}
	return st.RecordMissedTicks(ctx, opts.SessionID, o.ID, batch)
}

// sourceNameOf builds the ticks.source_name value exactly as the scheduler
// does (internal/store MaterializeTick): "job/schedule". The two writers must
// agree character for character, or the slot uniqueness would let one gap
// produce two histories for one schedule.
func sourceNameOf(g store.GapSchedule) string {
	return g.JobName + "/" + g.Name
}
