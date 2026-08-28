package explain

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Options are the knobs a caller sets on one report: the window and what the
// daemon looked like when asked. The clock exists so tests freeze time.
type Options struct {
	Since    time.Time
	Clock    clock.Clock
	DaemonUp bool
}

// Build assembles the report for a resolved subject: summary first, then the
// reverse chronological decision list. Every store call runs alone under its
// own short deadline; nothing here opens a transaction or holds a snapshot,
// because a held read snapshot is what stops WAL checkpointing dead.
func Build(ctx context.Context, st *store.Store, res Resolved, opts Options) (*Report, error) {
	clk := opts.Clock
	if clk == nil {
		clk = clock.System()
	}
	report := &Report{
		SchemaVersion: SchemaVersion,
		Subject: Subject{
			Kind:     string(res.Kind),
			Ref:      res.Raw,
			Job:      res.Job,
			Schedule: res.Schedule,
			Sensor:   res.Sensor,
			RunID:    res.RunID,
		},
		GeneratedAt: clk.Now().UTC().UnixMilli(),
		Since:       opts.Since.UTC().UnixMilli(),
		DaemonUp:    opts.DaemonUp,
	}

	switch res.Kind {
	case KindJob, KindSchedule, KindSensor:
		entries, err := buildTimeline(ctx, st, res, opts)
		if err != nil {
			return nil, err
		}
		report.Entries = entries
	case KindRun:
		entry, err := buildRunEntry(ctx, st, res)
		if err != nil {
			return nil, err
		}
		if entry != nil {
			report.Entries = []Entry{*entry}
		}
	default:
		return nil, fmt.Errorf("unknown subject kind %q", res.Kind)
	}

	summary, err := buildSummary(ctx, st, res)
	if err != nil {
		return nil, err
	}
	report.Summary = *summary
	return report, nil
}

// buildSummary reads the headline facts. Each read is one indexed query; the
// five queries together still touch only rows this subject owns.
func buildSummary(ctx context.Context, st *store.Store, res Resolved) (*Summary, error) {
	s, err := buildShadowAwareSummary(ctx, st, res)
	if err != nil {
		return nil, err
	}
	// The instance-wide marker last: while a shadow serve runs, every
	// subject in this state directory records instead of executing (#32).
	if info, e := st.ShadowRuntime(ctx); e == nil && info.Running {
		s.InstanceShadow = true
	}
	return s, nil
}

// buildShadowAwareSummary is the plain path; buildSummary wraps it with the
// instance shadow marker so callers never forget it.
func buildShadowAwareSummary(ctx context.Context, st *store.Store, res Resolved) (*Summary, error) {
	if res.Job == "" && res.Kind == KindRun {
		return &Summary{FreshnessState: freshnessUnknown}, nil
	}
	facts, err := withinDeadline(ctx, func(ctx context.Context) (store.ExplainJobFacts, error) {
		return st.ExplainJobFacts(ctx, res.Job)
	})
	if err != nil {
		return nil, err
	}
	summary := &Summary{
		FreshnessState: freshnessUnknown,
		Paused:         facts.Paused,
		MaxConcurrent:  facts.MaxConcurrent,
	}

	active, err := withinDeadline(ctx, func(ctx context.Context) (int, error) {
		return st.ExplainActiveRuns(ctx, res.Job)
	})
	if err != nil {
		return nil, err
	}
	summary.ActiveRuns = active

	if newest, found, err := within2(ctx, func(ctx context.Context) (store.RunSummary, bool, error) {
		return st.ExplainNewestRun(ctx, res.Job, "")
	}); err != nil {
		return nil, err
	} else if found {
		summary.LastOutcome = newest.State
		if !newest.StartedAt.IsZero() && !newest.FinishedAt.IsZero() {
			ms := newest.FinishedAt.Sub(newest.StartedAt).Milliseconds()
			summary.LastDurationMs = &ms
			at := newest.FinishedAt.UnixMilli()
			summary.LastRunAt = &at
		}
	}

	if last, found, err := within2(ctx, func(ctx context.Context) (store.RunSummary, bool, error) {
		return st.ExplainNewestRun(ctx, res.Job, "succeeded")
	}); err != nil {
		return nil, err
	} else if found && !last.FinishedAt.IsZero() {
		ms := last.FinishedAt.UnixMilli()
		summary.LastSuccessAt = &ms
		if summary.LastOutcome == "" {
			summary.LastOutcome = last.State
		}
	}

	switch res.Kind {
	case KindSchedule:
		row, err := st.GetSchedule(ctx, res.Job, res.Schedule)
		if err != nil {
			return nil, err
		}
		summary.Paused = row.Paused
		summary.Shadow = row.Shadow
		next := row.NextTickAt.UTC().UnixMilli()
		summary.NextTickAt = &next
	case KindSensor:
		sum, err := st.GetSensor(ctx, res.Sensor)
		if err != nil {
			return nil, err
		}
		summary.Paused = sum.Paused
		next := sum.NextEvalAt
		summary.NextTickAt = &next
		if sum.LastOutcome != "" {
			summary.LastOutcome = sum.LastOutcome
		}
	default:
		if next, ok, err := within2(ctx, func(ctx context.Context) (time.Time, bool, error) {
			return st.ExplainNextScheduleTick(ctx, res.Job)
		}); err != nil {
			return nil, err
		} else if ok {
			ms := next.UTC().UnixMilli()
			summary.NextTickAt = &ms
		}
	}
	return summary, nil
}

const freshnessUnknown = "unknown"

// buildTimeline merges the window's ticks and outages into one reverse
// chronological list, hanging each trigger (and its run) under the tick that
// produced it.
func buildTimeline(ctx context.Context, st *store.Store, res Resolved, opts Options) ([]Entry, error) {
	ticks, err := collectTicks(ctx, st, res.Sources, opts.Since)
	if err != nil {
		return nil, err
	}

	tickIDs := make([]string, 0, len(ticks))
	for _, t := range ticks {
		tickIDs = append(tickIDs, t.ID)
	}
	triggers, err := withinDeadline(ctx, func(ctx context.Context) (map[string][]store.ExplainTrigger, error) {
		return st.ExplainTriggersByTicks(ctx, tickIDs)
	})
	if err != nil {
		return nil, err
	}

	outages, err := withinDeadline(ctx, func(ctx context.Context) ([]store.Outage, error) {
		return st.ExplainOutagesSince(ctx, opts.Since)
	})
	if err != nil {
		return nil, err
	}

	// The runs of the window, joined to their triggers in memory: the schema
	// points forward (run -> trigger), so the reverse hop happens here and not
	// through a query the database would have to guess at.
	byTrigger, err := collectRunsByTrigger(ctx, st, res.Job)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, 0, len(ticks)+len(outages))
	for _, t := range ticks {
		entry := tickEntry(t)
		for _, tg := range triggers[t.ID] {
			child := triggerEntry(tg, actorOf(t.SourceKind))
			switch {
			case tg.RunID != "":
				// A deduped trigger names the original run directly.
				if runChild, err := runChildEntry(ctx, st, tg.RunID); err != nil {
					return nil, err
				} else if runChild != nil {
					child.Children = append(child.Children, *runChild)
				}
			case byTrigger[tg.ID] != nil:
				child.Children = append(child.Children, runTimelineEntry(byTrigger[tg.ID]))
			}
			entry.Children = append(entry.Children, child)
		}
		entries = append(entries, entry)
	}
	for _, o := range outages {
		entries = append(entries, outageEntry(o))
	}

	sort.SliceStable(entries, func(i, j int) bool { return entries[i].At > entries[j].At })
	if len(entries) > pageLimit {
		entries = entries[:pageLimit]
	}
	return entries, nil
}

// collectTicks walks the history keyset page by keyset page: every query is
// `id < cursor LIMIT n`, so an old window costs one walk of exactly its own
// rows and never an OFFSET re-read.
func collectTicks(ctx context.Context, st *store.Store, sources []store.ExplainSource, since time.Time) ([]store.ExplainTick, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	var out []store.ExplainTick
	before := ""
	for {
		page, err := withinDeadline(ctx, func(ctx context.Context) ([]store.ExplainTick, error) {
			return st.ExplainTicks(ctx, sources, since, before, store.ExplainPageSize)
		})
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < store.ExplainPageSize || len(out) >= pageLimit {
			break
		}
		before = page[len(page)-1].ID
	}
	if len(out) > pageLimit {
		out = out[:pageLimit]
	}
	return out, nil
}

// buildRunEntry turns one run into the root entry of a run report: the run
// itself, its producer chain (the tick and trigger that caused it), its steps
// and its notable events as children.
func buildRunEntry(ctx context.Context, st *store.Store, res Resolved) (*Entry, error) {
	detail, err := st.GetRun(ctx, res.RunID)
	if err != nil {
		return nil, err
	}

	run := detail.Run
	at := run.CreatedAt.UTC().UnixMilli()
	if !run.StartedAt.IsZero() {
		at = run.StartedAt.UTC().UnixMilli()
	}
	entry := &Entry{
		At:      at,
		Kind:    "run",
		Actor:   actorOf(run.Origin),
		Ref:     run.ID,
		Outcome: run.State,
		Hints:   hintsFor(run.ReasonCode),
	}
	entry.ReasonCode, entry.ReasonText = describe(run.ReasonCode, run.ReasonText)
	entry.ReasonData = map[string]any{}
	if run.RunKey != "" {
		entry.ReasonData["run_key"] = run.RunKey
	}
	if !run.ScheduledFor.IsZero() {
		entry.ReasonData["scheduled_for"] = run.ScheduledFor.UTC().Format(time.RFC3339)
	}
	if !run.StartedAt.IsZero() && !run.FinishedAt.IsZero() {
		ms := run.FinishedAt.Sub(run.StartedAt).Milliseconds()
		entry.DurationMS = &ms
		entry.ReasonData["duration_ms"] = ms
	}
	if run.Attempt > 0 {
		entry.ReasonData["attempt"] = run.Attempt
		entry.ReasonData["max_attempts"] = run.MaxAttempts
	}
	if run.Error != "" {
		entry.ReasonData["error"] = run.Error
	}
	appendReasonData(entry.ReasonData, run.ReasonData)

	// The producer chain: which tick fired, which trigger was accepted, so
	// the report answers "why did this run exist at all".
	if run.TriggerID != "" {
		if tg, ok, err := within2(ctx, func(ctx context.Context) (store.ExplainTrigger, bool, error) {
			return st.ExplainTriggerByID(ctx, run.TriggerID)
		}); err != nil {
			return nil, err
		} else if ok {
			trigger := triggerEntry(tg, actorOf(run.Origin))
			if tick, found, err := within2(ctx, func(ctx context.Context) (store.ExplainTick, bool, error) {
				return st.ExplainTickByID(ctx, tg.TickID)
			}); err != nil {
				return nil, err
			} else if found {
				trigger.Children = append(trigger.Children, tickEntry(tick))
			}
			entry.Children = append(entry.Children, trigger)
		}
	}

	// Notable run-level decisions between queued and done: deferrals, requeues,
	// cancellations. The plain queue/start/end transitions are the entry above.
	events, err := st.ExplainRunEvents(ctx, run.ID)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if !notableEvent(e.Kind) {
			continue
		}
		child := Entry{
			At:      e.At.UTC().UnixMilli(),
			Kind:    "event",
			Actor:   e.Actor,
			Ref:     fmt.Sprintf("%s/%s", e.Kind, e.StepName),
			Outcome: e.ToState,
		}
		child.ReasonCode, child.ReasonText = describe(e.ReasonCode, "")
		child.Hints = hintsFor(e.ReasonCode)
		if data := parseDetail(e.DetailJSON); data != nil {
			child.ReasonData = data
		}
		entry.Children = append(entry.Children, child)
	}

	// One child per step, carrying everything the prose needs: attempts, exit
	// codes, and the error tail that lives in the database, not on disk.
	for _, s := range detail.Steps {
		entry.Children = append(entry.Children, stepEntry(s))
	}

	sort.SliceStable(entry.Children, func(i, j int) bool {
		return entry.Children[i].At > entry.Children[j].At
	})
	return entry, nil
}

// collectRunsByTrigger walks a job's runs newest first (keyed pages) and maps
// them by the trigger that caused them. Only the window's producers matter,
// so the walk stops once it has passed the oldest tick in hand.
func collectRunsByTrigger(ctx context.Context, st *store.Store, job string) (map[string]*store.ExplainRun, error) {
	if job == "" {
		return nil, nil
	}
	out := make(map[string]*store.ExplainRun)
	before := ""
	for {
		page, err := withinDeadline(ctx, func(ctx context.Context) ([]store.ExplainRun, error) {
			return st.ExplainRunsByJob(ctx, job, before, store.ExplainPageSize)
		})
		if err != nil {
			return nil, err
		}
		for i := range page {
			run := &page[i]
			if run.TriggerID != "" {
				if _, taken := out[run.TriggerID]; !taken {
					out[run.TriggerID] = run
				}
			}
		}
		if len(page) < store.ExplainPageSize || len(out) >= pageLimit {
			break
		}
		before = page[len(page)-1].ID
	}
	return out, nil
}

// runTimelineEntry is the light child a trigger carries for its own run.
func runTimelineEntry(r *store.ExplainRun) Entry {
	at := r.CreatedAt.UTC().UnixMilli()
	if !r.StartedAt.IsZero() {
		at = r.StartedAt.UTC().UnixMilli()
	}
	e := Entry{
		At:      at,
		Kind:    "run",
		Actor:   actorOf(r.Origin),
		Ref:     r.ID,
		Outcome: r.State,
	}
	e.ReasonCode, e.ReasonText = describe(r.ReasonCode, r.ReasonText)
	e.Hints = hintsFor(r.ReasonCode)
	data := map[string]any{}
	if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() {
		ms := r.FinishedAt.Sub(r.StartedAt).Milliseconds()
		e.DurationMS = &ms
		data["duration_ms"] = ms
	}
	if r.DeferReason != "" {
		data["defer_reason"] = r.DeferReason
	}
	if len(data) > 0 {
		e.ReasonData = data
	}
	return e
}

// runChildEntry reads the light facts of the run a trigger produced: state,
// timing and outcome, without its steps. A run that no longer exists (swept,
// or a dedup pointer at a removed row) simply contributes no child.
func runChildEntry(ctx context.Context, st *store.Store, runID string) (*Entry, error) {
	matches, err := withinDeadline(ctx, func(ctx context.Context) ([]store.RunSummary, error) {
		return st.ExplainRunsByPrefix(ctx, runID, 1)
	})
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 || matches[0].ID != runID {
		return nil, nil
	}
	r := matches[0]
	at := r.CreatedAt.UTC().UnixMilli()
	if !r.StartedAt.IsZero() {
		at = r.StartedAt.UTC().UnixMilli()
	}
	e := &Entry{
		At:      at,
		Kind:    "run",
		Actor:   actorOf(r.Origin),
		Ref:     r.ID,
		Outcome: r.State,
	}
	e.ReasonCode, e.ReasonText = describe(r.ReasonCode, "")
	e.Hints = hintsFor(r.ReasonCode)
	data := map[string]any{}
	if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() {
		ms := r.FinishedAt.Sub(r.StartedAt).Milliseconds()
		e.DurationMS = &ms
		data["duration_ms"] = ms
	}
	if r.DeferReason != "" {
		data["defer_reason"] = r.DeferReason
	}
	appendReasonData(data, r.ReasonData)
	if len(data) > 0 {
		e.ReasonData = data
	}
	return e, nil
}

// notableEvent names the run-level transitions worth their own timeline line.
func notableEvent(kind string) bool {
	switch kind {
	case "run.deferred", "run.requeued", "run.cancelled", "run.poisoned",
		"run.orphan_killed", "run.result_discarded", "run.drained", "step.interrupted":
		return true
	}
	return false
}

func stepEntry(s store.Step) Entry {
	at := int64(0)
	if !s.StartedAt.IsZero() {
		at = s.StartedAt.UTC().UnixMilli()
	}
	e := Entry{
		At:      at,
		Kind:    "step",
		Actor:   "executor",
		Ref:     s.Name,
		Outcome: s.State,
	}
	e.ReasonCode, e.ReasonText = describe(s.ReasonCode, s.ReasonText)
	if e.ReasonText == "" {
		e.ReasonText = s.Error
	}
	e.Hints = hintsFor(s.ReasonCode)
	data := map[string]any{
		"attempt":      s.Attempt,
		"max_attempts": s.MaxAttempts,
	}
	if s.HasExitCode {
		data["exit_code"] = s.ExitCode
	}
	if s.Signal != "" {
		data["signal"] = s.Signal
	}
	if s.DurationMS > 0 {
		data["duration_ms"] = s.DurationMS
		e.DurationMS = &s.DurationMS
	}
	if !s.NextAttemptAt.IsZero() {
		data["next_attempt_at"] = s.NextAttemptAt.UTC().Format(time.RFC3339)
	}
	if s.LogBytes > 0 {
		data["log_bytes"] = s.LogBytes
	}
	if s.LogTruncated {
		data["log_truncated"] = true
	}
	// The log files are the truth and the database only points at them
	// (#44); when the pointer is gone but the evidence columns remain, the
	// shard the file lived in was pruned. The error tail below survives
	// that, and the report says so instead of letting the absence read as
	// "nothing was ever logged".
	if s.LogPath == "" && (s.LogBytes > 0 || s.LogTruncated) {
		data["log_pruned"] = true
	}
	if tail := strings.TrimSpace(s.ErrorTail); tail != "" {
		data["error_tail"] = tail
	}
	if len(data) > 0 {
		e.ReasonData = data
	}
	return e
}

func tickEntry(t store.ExplainTick) Entry {
	at := t.StartedAt
	if !t.ScheduledFor.IsZero() {
		at = t.ScheduledFor
	}
	e := Entry{
		At:          at.UTC().UnixMilli(),
		Kind:        "tick",
		Actor:       actorOf(t.SourceKind),
		Ref:         t.ID,
		Outcome:     t.Outcome,
		RepeatCount: t.RepeatCount,
	}
	e.ReasonCode, e.ReasonText = describe(t.ReasonCode, t.ReasonText)
	e.Hints = hintsFor(t.ReasonCode)
	data := map[string]any{}
	if !t.ScheduledFor.IsZero() {
		data["scheduled_for"] = t.ScheduledFor.UTC().Format(time.RFC3339)
	}
	if t.TriggerCount > 0 {
		data["trigger_count"] = t.TriggerCount
	}
	if t.DedupedCount > 0 {
		data["deduped_count"] = t.DedupedCount
	}
	if t.CursorBefore != "" || t.CursorAfter != "" {
		data["cursor_before"] = t.CursorBefore
		data["cursor_after"] = t.CursorAfter
	}
	appendReasonData(data, t.ReasonData)
	if len(data) > 0 {
		e.ReasonData = data
	}
	return e
}

func triggerEntry(tg store.ExplainTrigger, actor string) Entry {
	e := Entry{
		At:      tg.CreatedAt.UTC().UnixMilli(),
		Kind:    "trigger",
		Actor:   actor,
		Ref:     tg.ID,
		Outcome: tg.Outcome,
	}
	e.ReasonCode, e.ReasonText = describe(tg.ReasonCode, tg.ReasonText)
	e.Hints = hintsFor(tg.ReasonCode)
	data := map[string]any{}
	if tg.RunKey != "" {
		data["run_key"] = tg.RunKey
	}
	if tg.RunID != "" {
		data["run_id"] = tg.RunID
	}
	if len(data) > 0 {
		e.ReasonData = data
	}
	return e
}

func outageEntry(o store.Outage) Entry {
	durationMS := o.To.Sub(o.From).Milliseconds()
	e := Entry{
		At:         o.From.UTC().UnixMilli(),
		Kind:       "outage",
		Actor:      "daemon",
		Ref:        strconv.FormatInt(o.ID, 10),
		Outcome:    "missed",
		ReasonCode: string(reason.TICKMissedDaemonDown),
		DurationMS: &durationMS,
		ReasonData: map[string]any{
			"to_ms":        o.To.UTC().UnixMilli(),
			"detected_at":  o.DetectedAt.UTC().Format(time.RFC3339),
			"outage_kind":  o.Kind,
			"missed_ticks": o.MissedTicks,
		},
	}
	if o.PrevSession != "" {
		e.ReasonData["prev_session"] = o.PrevSession
	}
	e.ReasonText = "the daemon was down for " + humanDuration(durationMS)
	e.Hints = hintsFor(e.ReasonCode)
	return e
}

// describe resolves a stored reason code against the catalogue: the short
// text fills an empty stored reason_text, and an unknown code stays visible
// instead of being invented around.
func describe(code, text string) (string, string) {
	if code == "" {
		return "", text
	}
	if entry, ok := reason.Lookup(reason.Code(code)); ok && text == "" {
		return code, entry.Short
	}
	return code, text
}

// hintsFor maps a reason code onto its remediation hints. A code outside the
// catalogue gets one hint that says where the catalogue lives, because every
// terminal decision must show at least one way forward.
func hintsFor(code string) []string {
	if code == "" {
		return nil
	}
	if entry, ok := reason.Lookup(reason.Code(code)); ok {
		if len(entry.Remedy) > 0 {
			return entry.Remedy
		}
	}
	return []string{"paceq error " + code + " explains this code"}
}

// actorOf names who acted, from the object's own kind.
func actorOf(sourceKind string) string {
	switch sourceKind {
	case "schedule":
		return "scheduler"
	case "sensor":
		return "sensor"
	case "manual":
		return "cli"
	default:
		return sourceKind
	}
}

// parseDetail decodes a stored reason_data / detail_json object. Anything
// that is not an object comes back nil: explain renders objects, never
// guesses at scalars.
func parseDetail(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// appendReasonData merges a stored reason_data object into an entry's data,
// without letting stored keys clobber the ones built here.
func appendReasonData(into map[string]any, raw string) {
	stored := parseDetail(raw)
	for k, v := range stored {
		if _, taken := into[k]; !taken {
			into[k] = v
		}
	}
}

// within2 wraps a store call that answers "value, found" in its own deadline.
func within2[A, B any](ctx context.Context, call func(context.Context) (A, B, error)) (A, B, error) {
	callCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	return call(callCtx)
}

// withinDeadline wraps one store call in its own 5 second deadline.
func withinDeadline[O any](ctx context.Context, call func(context.Context) (O, error)) (O, error) {
	callCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	return call(callCtx)
}
