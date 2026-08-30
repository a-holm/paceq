package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The scheduler loop turns due schedules into runs. It wakes whenever the
// daemon asks (notify bus plus a safety ticker, both owned by daemon.loop),
// and each wake is one Tick: one discovery query, then a pure plan per due
// schedule, then one transaction per fire-time written strictly oldest
// fire-time first across the whole batch.
//
// Leadership: every wake first takes or renews the fenced "scheduler" role
// lease, and the pass re-checks it between transactions. Losing the lease
// therefore stops the pass exactly at a transaction boundary, leaving no
// partially written state behind. Because the batch is written in fire-time
// order, that boundary is also a chronological cut: every fire-time older
// than the newest materialised one is already materialised or explicitly
// recorded, and anything younger still owes itself to a cursor this pass
// never moved. A rival leader running the same discovery against the same
// database recomputes the same fire-times from the same cursors, and the
// UNIQUE gate on (source_kind, source_name, scheduled_for) arbitrates, so
// even a lost race costs nothing but silence.

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

	// Renew overrides the leadership re-check cadence inside one pass.
	// Zero means DefaultRenew. Negative forces a lease proof before every
	// single decision, which is how tests script a mid-pass takeover;
	// production keeps the role-lease default.
	Renew time.Duration

	// Shadow turns the whole instance into a recorder (#32): every due
	// evaluation still plans, claims its fire-time and advances its cursor,
	// but nothing ever creates a run or starts a process. Per-schedule
	// shadow flags (schedules.shadow) shadow individual schedules instead;
	// the global switch dominates the row flag.
	Shadow bool
}

// Source implements daemon.ScheduleSource. Construct it with New.
type Source struct {
	st     Store
	clk    clock.Clock
	log    *slog.Logger
	holder string

	// shadow records the instance-wide switch: fills every would-trigger's
	// input here, is otherwise never consulted again below this point -
	// all real behaviour lives behind store.MaterializeTick (#32).
	shadow bool

	// confirmed is the monotonic mark of the last lease answer this source
	// has seen; leading() renews once the budget since it passes Renew.
	confirmed clock.Mono
	renew     time.Duration
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
	renew := cfg.Renew
	if renew == 0 {
		renew = DefaultRenew // negatives pass through: prove before every decision
	}
	return &Source{
		st: cfg.Store, clk: clk, log: log, holder: cfg.Holder,
		renew: renew, shadow: cfg.Shadow,
	}, nil
}

// leading reports whether this loop may keep deciding, and proves it to the
// database at most once per renewal interval. Between renewals the answer is
// carried, which is exactly the role-lease contract from #42: a renewal
// every Renew with a DefaultTTL budget of tolerated silence, and a
// leadership change noticed no later than one transaction after its renewal
// comes due. A stale pass racing a handover cannot corrupt anything either
// way: the tick gate refuses foreign fire-times and progress moves forward
// only.
func (src *Source) leading(ctx context.Context) bool {
	if src.renew > 0 && src.clk.Since(src.confirmed) < src.renew {
		return true
	}
	_, ok, err := src.st.AcquireOrRenew(ctx, LeaseName, src.holder, DefaultTTL)
	if err != nil {
		src.log.Warn("lease renewal errored mid pass", "err", err.Error())
		return false
	}
	if !ok {
		return false
	}
	src.confirmed = src.clk.Mark()
	return true
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
	src.confirmed = src.clk.Mark()
	_ = g

	now := src.clk.Now().UTC()
	due, err := src.st.DueSchedules(ctx, now.UnixMilli(), dueBatchLimit)
	if err != nil {
		src.log.Error("the due query failed", "err", err.Error())
		return nil
	}

	// Plan everything before writing anything. Every evaluation of the
	// whole batch lands in one list sorted by fire-time, so whatever point
	// a stop reaches, every fire-time older than the newest materialised
	// one has been materialised or explicitly recorded per policy. What is
	// left still sits behind its own schedule's untouched cursor, and the
	// next pass replans it under the same policy.
	var plans []store.TickInput
	for _, sched := range due {
		if err := ctx.Err(); err != nil {
			return err
		}
		plans = append(plans, src.planSchedule(sched, now)...)
	}
	sort.SliceStable(plans, func(i, j int) bool {
		return plans[i].ScheduledFor.Compare(plans[j].ScheduledFor) < 0
	})

	// One immediate transaction per fire-time, oldest first. A schedule
	// whose transaction errored stops for the rest of the pass: writing a
	// later fire-time of the same schedule would push its cursor past work
	// that never landed. Other schedules keep going; their cursors stand
	// independent of the failure.
	blocked := make(map[string]bool)
	// Whatever this pass meets at the tick gate is reported once at the end,
	// including when the pass is cut short: the fire-times it did reach were
	// still decided.
	gaps := newGapLedger()
	defer gaps.report(src.log)

	for _, in := range plans {
		if err := ctx.Err(); err != nil {
			return err
		}
		if blocked[in.Schedule.ID] {
			continue
		}
		if !src.leading(ctx) {
			// Leadership changed hands mid pass. Stop here: the winner
			// recomputes the same remaining window from the same cursor.
			return nil
		}
		res, err := src.st.MaterializeTick(ctx, in)
		if err != nil {
			blocked[in.Schedule.ID] = true
			src.log.Error("a tick transaction failed",
				"schedule", in.Schedule.JobName+"/"+in.Schedule.Name,
				"scheduled_for", in.ScheduledFor.Format(time.RFC3339), "err", err.Error())
			continue
		}
		gaps.record(in, res)
	}
	return nil
}

// gapLedger accumulates what one wake met at the tick gate where startup
// reconciliation had already been: fire-times replayed out of its evidence,
// and fire-times left to it because the policy wanted no run there.
//
// It reports once per schedule rather than once per fire-time, because an
// outage produces one row per slot and an operator wants the shape of the
// outage, not a line per minute of it. A loss to a rival holder is not
// counted at all: that is the ordinary follower answer during a handover and
// says nothing about any decision.
type gapLedger struct {
	order []string
	seen  map[string]*gapEntry
}

type gapEntry struct {
	replayed int
	left     int
	oldest   time.Time
	newest   time.Time
}

func newGapLedger() *gapLedger {
	return &gapLedger{seen: make(map[string]*gapEntry)}
}

func (l *gapLedger) record(in store.TickInput, res store.TickResult) {
	if !res.Replayed && res.LostTo != store.LossMissed {
		return
	}
	name := in.Schedule.JobName + "/" + in.Schedule.Name
	e := l.seen[name]
	if e == nil {
		e = &gapEntry{oldest: in.ScheduledFor, newest: in.ScheduledFor}
		l.seen[name] = e
		l.order = append(l.order, name)
	}
	if in.ScheduledFor.Before(e.oldest) {
		e.oldest = in.ScheduledFor
	}
	if in.ScheduledFor.After(e.newest) {
		e.newest = in.ScheduledFor
	}
	if res.Replayed {
		e.replayed++
		return
	}
	e.left++
}

// report is the line that used to be missing. Before it, a catch-up overruled
// by gap detection left no run, no row and nothing in the log, so an operator
// following the troubleshooting page ran out of places to look.
func (l *gapLedger) report(log *slog.Logger) {
	for _, name := range l.order {
		e := l.seen[name]
		log.Info("catch-up met gap detection's evidence at the tick gate",
			"schedule", name,
			"replayed", e.replayed,
			"left_as_missed", e.left,
			"oldest", e.oldest.Format(time.RFC3339),
			"newest", e.newest.Format(time.RFC3339))
	}
}

// planSchedule evaluates one due schedule into the list of transactions it
// owes this wake. Planning is pure computation: nothing is written here. The
// caller merges every schedule's plan and sorts them into one strict
// fire-time order before the first write, so a stop anywhere in the batch
// cannot leave an older fire-time unsaid under a moved cursor. Every failure
// inside becomes a recorded TICK_ERROR_CONFIG input or a log line; none of
// them escapes, so one broken definition cannot stall the others sharing the
// wake.
func (src *Source) planSchedule(sched store.ScheduleRow, now time.Time) []store.TickInput {
	parsed, err := cronx.Parse(sched.Expr)
	if err != nil {
		return []store.TickInput{src.configInput(sched, now, fmt.Sprintf("bad expression %q: %v", sched.Expr, err))}
	}
	tz, err := cronx.LoadZone(sched.Timezone)
	if err != nil {
		return []store.TickInput{src.configInput(sched, now, fmt.Sprintf("unknown timezone %q: %v", sched.Timezone, err))}
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
		return []store.TickInput{src.configInput(sched, now, fmt.Sprintf("the expression has no occurrences left: %v", err))}
	}
	if len(occs) == 0 {
		// Nothing owed after all: another leader moved faster. Writing
		// nothing here is the whole point of the tick definition; there is
		// no such thing as TICK_SKIPPED_NOT_DUE.
		return nil
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
			evaluations = append(evaluations, src.configInput(sched, now, fmt.Sprintf("unmapped skip reason %q at %v", o.SkipReason, o.At)))
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

	sort.SliceStable(evaluations, func(i, j int) bool {
		return evaluations[i].ScheduledFor.Compare(evaluations[j].ScheduledFor) < 0
	})
	return evaluations
}

// attemptDecision builds one triggered evaluation with its deterministic run
// key: <job>/<schedule>:<RFC3339>. A crash anywhere followed by a recomputed
// window produces the same key, and run_keys deduplicates it. In shadow mode
// (#32) nothing else changes here: the same key is computed, only to be
// discarded by the store, which records a marker instead of materialising.
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
		Shadow:         src.shadow || sched.Shadow,
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

// configInput records that a schedule's definition could not be interpreted.
// The row is keyed at the instant that made the schedule due, and the cursor
// stays put: the schedule remains visibly due, repeats hit the idempotency
// gate instead of growing history, and fixing the definition resumes exactly
// where the backlog began. The input joins the batch's fire-time order like
// every other decision; the log line fires when the decision is made.
func (src *Source) configInput(sched store.ScheduleRow, now time.Time, text string) store.TickInput {
	src.log.Error("a schedule could not be evaluated",
		"schedule", sched.JobName+"/"+sched.Name, "reason", text)
	due := sched.NextTickAt
	if due.IsZero() {
		due = now
	}
	sweep := now.Add(configSweepDelay)
	return store.TickInput{
		Schedule:     sched,
		ScheduledFor: due,
		Outcome:      store.OutcomeError,
		ReasonCode:   reason.TICKErrorConfig,
		ReasonText:   text,
		NextTickAt:   sweep,
		Actor:        "scheduler",
	}
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
