package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The scheduler loop turns due schedules into runs. It wakes whenever the
// daemon asks (notify bus plus a safety ticker, both owned by daemon.loop),
// and each wake is one Tick: one discovery query, then one decision per due
// schedule, then one transaction per fire-time.
//
// Leadership: every wake first takes or renews the fenced "scheduler" role
// lease, and the pass re-checks it between transactions. Losing the lease
// therefore stops the pass exactly at a transaction boundary, leaving no
// partially written state behind. A rival leader running the same discovery
// against the same database recomputes the same fire-times from the same
// cursors, and the UNIQUE gate on (source_kind, source_name, scheduled_for)
// arbitrates, so even a lost race costs nothing but silence.

// LeaseName is the role this loop leads under.
const LeaseName = "scheduler"

// DefaultTTL and DefaultRenew mirror the role-lease defaults: fifteen seconds
// of tolerated silence, renewed every five.
const (
	DefaultTTL   = 15 * time.Second
	DefaultRenew = 5 * time.Second
)

// dueBatchLimit caps one wake's discovery. A schedule that turns up again
// while still owed is fine: the next wake looks again, and the tick gate
// decides who owns what.
const dueBatchLimit = 100

// configSweepDelay is where a broken schedule's cursor rests after a
// TICK_ERROR_CONFIG row: the definition cannot be interpreted far enough to
// know when it should come due, so the loop re-reads it once an hour instead
// of spinning on it every second. The schedule stays due in between; nothing
// is silently skipped past.
const configSweepDelay = time.Hour

// Store is the narrow slice of the store the loop needs. The loop never sees
// a database handle; tests script this interface.
type Store interface {
	DueSchedules(ctx context.Context, nowMilli int64, max int) ([]store.ScheduleRow, error)
	MaterializeTick(ctx context.Context, in store.TickInput) (store.TickResult, error)
	AcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (store.LeaseGrant, bool, error)
}

// Config builds a Source.
type Config struct {
	// Store is the state database.
	Store Store

	// Clock drives every wall reading. Nil means clock.System().
	Clock clock.Clock

	// Holder identifies this instance in the lease table. Required: without
	// an identity there is no fencing and two loops would race freely.
	Holder string

	// Log receives one structured line per decision worth remembering.
	// Nil means slog.Default().
	Log *slog.Logger
}

// Source implements daemon.ScheduleSource. Construct it with New.
type Source struct {
	st     Store
	clk    clock.Clock
	log    *slog.Logger
	holder string
}

// New wires a scheduler source onto a store.
func New(cfg Config) (*Source, error) {
	if cfg.Store == nil {
		return nil, errors.New("scheduler: no store was named")
	}
	if cfg.Holder == "" {
		return nil, errors.New("scheduler: no holder identity was named")
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System()
	}
	return &Source{st: cfg.Store, clk: clk, log: log, holder: cfg.Holder}, nil
}

// leading renews the role lease and reports whether this loop may keep
// deciding. It runs before every materialisation, which is what bounds a
// leadership handover to at most one transaction of overlap.
func (src *Source) leading(ctx context.Context) bool {
	_, ok, err := src.st.AcquireOrRenew(ctx, LeaseName, src.holder, DefaultTTL)
	if err != nil {
		src.log.Warn("lease renewal errored mid pass", "err", err.Error())
		return false
	}
	return ok
}

// Tick runs one full pass: admit under the lease, discover, decide,
// materialise. It never returns an error except for cancellation; a failed
// schedule or a busy database is logged and left for the next wake, because
// one bad minute must never kill the daemon.
func (src *Source) Tick(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	g, ok, err := src.st.AcquireOrRenew(ctx, LeaseName, src.holder, DefaultTTL)
	if err != nil {
		src.log.Warn("lease admission errored", "err", err.Error())
		return nil
	}
	if !ok {
		return nil // another instance leads; its wakes carry the work
	}
	_ = g

	now := src.clk.Now().UTC()
	due, err := src.st.DueSchedules(ctx, now.UnixMilli(), dueBatchLimit)
	if err != nil {
		src.log.Error("the due query failed", "err", err.Error())
		return nil
	}

	for _, sched := range due {
		if err := ctx.Err(); err != nil {
			return err
		}
		src.processSchedule(ctx, sched, now)
	}
	return nil
}

// processSchedule evaluates one due schedule end to end. Every failure inside
// is either recorded as a TICK_ERROR_CONFIG row or logged; neither escapes,
// so one broken definition cannot stall the others sharing the wake.
func (src *Source) processSchedule(ctx context.Context, sched store.ScheduleRow, now time.Time) {
	sourceName := sched.JobName + "/" + sched.Name

	parsed, err := cronx.Parse(sched.Expr)
	if err != nil {
		src.configError(ctx, sched, now, fmt.Sprintf("bad expression %q: %v", sched.Expr, err))
		return
	}
	tz, err := cronx.LoadZone(sched.Timezone)
	if err != nil {
		src.configError(ctx, sched, now, fmt.Sprintf("unknown timezone %q: %v", sched.Timezone, err))
		return
	}

	pol := policyOf(sched)
	// Discovery always recomputes from the persisted cursor, never from when
	// this process last ran: two instances with different uptimes must see
	// the same fire-times (G9). A fresh schedule starts just before its first
	// promised instant, because Between is half open at from.
	from := now.Add(-time.Millisecond)
	if sched.LastTickAt != nil {
		from = *sched.LastTickAt
	} else if !sched.NextTickAt.IsZero() {
		from = sched.NextTickAt.Add(-time.Millisecond)
	}

	occs, err := parsed.Between(from, now, tz, pol)
	if err != nil && len(occs) == 0 {
		// The expression can never land again (an impossible day-month pair,
		// say): that is configuration rot, not an empty minute.
		src.configError(ctx, sched, now, fmt.Sprintf("the expression has no occurrences left: %v", err))
		return
	}
	if len(occs) == 0 {
		// Nothing owed after all: another leader moved faster. Writing
		// nothing here is the whole point of the tick definition; there is
		// no such thing as TICK_SKIPPED_NOT_DUE.
		return
	}

	var evaluations []store.TickInput
	for _, o := range occs {
		switch {
		case o.Skipped && o.SkipReason == cronx.SkipReasonNonexistent:
			evaluations = append(evaluations, src.skipDecision(sched, o, reason.TICKSkippedDSTNonexistent, o.LocalWall))
		case o.Skipped && o.SkipReason == cronx.SkipReasonDuplicate:
			evaluations = append(evaluations, src.skipDecision(sched, o, reason.TICKSkippedDSTDuplicate, o.LocalWall))
		case o.Skipped:
			// cronx knows a skip shape this build does not map; record it as
			// config rather than inventing a meaning for it.
			src.configError(ctx, sched, now, fmt.Sprintf("unmapped skip reason %q at %v", o.SkipReason, o.At))
		}
	}
	attempts, skips := applyCatchup(realOccurrences(occs), sched.Catchup,
		sched.CatchupLimit, sched.CatchupWindowMS, now)
	for _, s := range skips {
		evaluations = append(evaluations, src.skipDecision(sched, s.Occurrence, s.Code, ""))
	}
	for _, o := range attempts {
		evaluations = append(evaluations, src.attemptDecision(sched, parsed, tz, pol, o))
	}

	for _, in := range evaluations {
		if err := ctx.Err(); err != nil {
			return
		}
		if !src.leading(ctx) {
			// Leadership changed hands mid pass. Stop here: the winner
			// recomputes the same remaining window from the same cursor.
			return
		}
		res, err := src.st.MaterializeTick(ctx, in)
		if err != nil {
			src.log.Error("a tick transaction failed",
				"schedule", sourceName, "scheduled_for", in.ScheduledFor.Format(time.RFC3339), "err", err.Error())
			continue
		}
		_ = res // Claimed=false means someone owned the fire-time already: silence is correct.
	}
}

// attemptDecision builds one triggered evaluation with its deterministic run
// key: <job>/<schedule>:<RFC3339>. A crash anywhere followed by a recomputed
// window produces the same key, and run_keys deduplicates it.
func (src *Source) attemptDecision(sched store.ScheduleRow, parsed cronx.Schedule,
	tz *time.Location, pol cronx.Policy, o cronx.Occurrence,
) store.TickInput {
	next := nextAfter(parsed, tz, pol, o.At)
	return store.TickInput{
		Schedule:       sched,
		ScheduledFor:   o.At,
		Outcome:        store.OutcomeTriggered,
		RunKey:         fmt.Sprintf("%s/%s:%s", sched.JobName, sched.Name, o.At.UTC().Format(time.RFC3339)),
		NextTickAt:     next,
		UpdateProgress: true,
		Actor:          "scheduler",
	}
}

func (src *Source) skipDecision(sched store.ScheduleRow, o cronx.Occurrence,
	code reason.Code, detail string,
) store.TickInput {
	parsed, tz, pol, ok := src.parsedOf(sched)
	next := time.Time{}
	if ok {
		next = nextAfter(parsed, tz, pol, o.At)
	}
	in := store.TickInput{
		Schedule:       sched,
		ScheduledFor:   o.At,
		Outcome:        store.OutcomeSkipped,
		ReasonCode:     code,
		NextTickAt:     next,
		UpdateProgress: true,
		Actor:          "scheduler",
	}
	if detail != "" {
		in.ReasonData = fmt.Sprintf(`{"local_time":%q}`, detail)
	}
	return in
}

// configError records that a schedule's definition could not be interpreted.
// The row is keyed at the instant that made the schedule due, and the cursor
// stays put: the schedule remains visibly due, repeats hit the idempotency
// gate instead of growing history, and fixing the definition resumes exactly
// where the backlog began.
func (src *Source) configError(ctx context.Context, sched store.ScheduleRow, now time.Time, text string) {
	due := sched.NextTickAt
	if due.IsZero() {
		due = now
	}
	sweep := now.Add(configSweepDelay)
	in := store.TickInput{
		Schedule:     sched,
		ScheduledFor: due,
		Outcome:      store.OutcomeError,
		ReasonCode:   reason.TICKErrorConfig,
		ReasonText:   text,
		NextTickAt:   sweep,
		Actor:        "scheduler",
	}
	if _, err := src.st.MaterializeTick(ctx, in); err != nil {
		src.log.Error("recording the config error failed",
			"schedule", sched.JobName+"/"+sched.Name, "err", err.Error())
		return
	}
	src.log.Error("a schedule could not be evaluated",
		"schedule", sched.JobName+"/"+sched.Name, "reason", text)
}

// parsedOf re-parses for the skip path; the pass already proved these parse,
// so an error here falls back to a zero next stamp rather than a second
// config row.
func (src *Source) parsedOf(sched store.ScheduleRow) (cronx.Schedule, *time.Location, cronx.Policy, bool) {
	parsed, err := cronx.Parse(sched.Expr)
	if err != nil {
		return cronx.Schedule{}, nil, cronx.Policy{}, false
	}
	tz, err := cronx.LoadZone(sched.Timezone)
	if err != nil {
		return cronx.Schedule{}, nil, cronx.Policy{}, false
	}
	return parsed, tz, policyOf(sched), true
}

// nextAfter names the moment after t the schedule lands on next, which is
// what progress writes into next_tick_at. An expression whose matches have
// run out sweeps hourly instead of claiming a future it cannot know.
func nextAfter(parsed cronx.Schedule, tz *time.Location, pol cronx.Policy, t time.Time) time.Time {
	o, err := parsed.Next(t, tz, pol)
	if err != nil || o.At.IsZero() {
		return t.Add(configSweepDelay)
	}
	return o.At
}

// realOccurrences keeps the occurrences that represent real firing moments;
// DST-skipped slots were already turned into their own decisions.
func realOccurrences(occs []cronx.Occurrence) []cronx.Occurrence {
	out := make([]cronx.Occurrence, 0, len(occs))
	for _, o := range occs {
		if !o.Skipped {
			out = append(out, o)
		}
	}
	return out
}

// policyOf maps the schema's two policy columns onto cronx's.
func policyOf(sched store.ScheduleRow) cronx.Policy {
	var p cronx.Policy
	if sched.SpringForward == "shift" {
		p.SpringForward = cronx.Shift
	}
	if sched.FallBack == "both" {
		p.FallBack = cronx.Both
	}
	return p
}
