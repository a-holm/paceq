package notify

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/model"
)

// RunFacts is what the engine knows about a finished run, in the shape the
// payload builder wants. It is a value: nothing here points at live state.
type RunFacts struct {
	Topic      string // model.TopicRunFailed or model.TopicRunSucceeded
	JobName    string
	RunID      string
	Attempt    int
	State      string
	ReasonCode string
	ReasonText string

	// Failing step facts. Empty step means the run-level verdict carried
	// them (timeout), and they are omitted from the payload.
	Step        string
	ExitCode    int
	HasExitCode bool
	ErrorTail   string

	StartedAt  time.Time
	FinishedAt time.Time
	DurationMS int64

	// Host names the machine that ran the job. It comes from wiring, never
	// from os.Hostname() inside this package, so tests stay hermetic.
	Host string
}

// DefaultDeliveryTimeout is how long one send attempt may take before it is
// treated as failed. The issue's sketch pins thirty seconds; long enough for
// a webhook relay, short enough that a hung notifier cannot stall the loop.
const DefaultDeliveryTimeout = 30 * time.Second

// retryCmdTemplate is fixed text with exactly one substitution made through
// fmt, never text/template (#29 AC): the payload must contain an executable
// way forward and must not smuggle spec values through a template engine.
const (
	retryCmdTemplate   = "paceq runs retry %s"
	explainCmdTemplate = "paceq explain run %s"
)

// BuildRunPayload renders the event envelope (#29). The contract is stable at
// the `--json` level of detail: every field below is promised to recipe
// authors, so removing one is a breaking change.
func BuildRunPayload(f RunFacts) string {
	env := map[string]any{
		"event":       f.Topic,
		"at":          f.FinishedAt.UnixMilli(),
		"job":         f.JobName,
		"run_id":      f.RunID,
		"attempt":     f.Attempt,
		"state":       f.State,
		"reason_code": f.ReasonCode,
		"reason_text": f.ReasonText,
		"explain_cmd": fmt.Sprintf(explainCmdTemplate, f.RunID),
		"retry_cmd":   fmt.Sprintf(retryCmdTemplate, f.RunID),
	}
	if !f.StartedAt.IsZero() {
		env["started_at"] = f.StartedAt.UnixMilli()
	}
	if !f.FinishedAt.IsZero() {
		env["finished_at"] = f.FinishedAt.UnixMilli()
	}
	if f.DurationMS > 0 {
		env["duration_ms"] = f.DurationMS
	} else if !f.StartedAt.IsZero() && !f.FinishedAt.IsZero() {
		env["duration_ms"] = f.FinishedAt.Sub(f.StartedAt).Milliseconds()
	}
	if f.Step != "" {
		env["step"] = f.Step
	}
	if f.HasExitCode {
		env["exit_code"] = f.ExitCode
	}
	if f.ErrorTail != "" {
		env["error_tail"] = f.ErrorTail
	}
	if f.Host != "" {
		env["host"] = f.Host
	}
	b, err := json.Marshal(env)
	if err != nil {
		// encoding/json cannot fail on maps of strings and ints; treat any
		// future failure as a bug, surfaced as an empty-but-valid payload.
		return "{}"
	}
	return string(b)
}

// Closed field names group_by accepts. Anything else in notify_defaults is a
// configuration error: the vocabulary stays closed so GroupKey built by the
// planner and rebuilt nowhere else can never disagree.
var groupFields = map[string]bool{"job": true, "reason_code": true}

// ValidGroupField reports whether name carries meaning to the grouper.
func ValidGroupField(name string) bool { return groupFields[name] }

// Planner turns finished-run facts into the notification rows the store will
// write inside the finish transaction. It applies job hooks over daemon
// defaults, dedup keys, and throttle groups.
//
// Hook resolution rule: a job whose Notify block lists targets uses them as
// written - including deliberately empty lists, which silence that side -
// while a job without a block inherits the daemon defaults. Nothing else
// merges; surprises come from clever combinations, not from either simple
// reading of "the job wins".
type Planner struct {
	Defaults model.NotifyDefaults
	// Now stamps CreatedAt and AvailableAt. Constructed through
	// NewPlanner, it is always the wiring's clock; a zero value plans
	// nothing rather than guessing, so silence is never accidental.
	Now func() time.Time
}

// NewPlanner builds a planner whose stamps come from exactly one place.
func NewPlanner(def model.NotifyDefaults, now func() time.Time) *Planner {
	return &Planner{Defaults: def, Now: now}
}

// now fails closed: a planner without a clock has no business stamping
// anything, and empty output writes no rows.
func (p *Planner) now() time.Time { return p.Now() }

// JobHooks carries the frozen spec's hooks into planning; nil means the job
// says nothing about notifications.
type JobHooks struct {
	OnFailure []string
	OnSuccess []string
}

// Plan builds one Notification per (topic, target). An empty result simply
// writes nothing.
func (p *Planner) Plan(f RunFacts, hooks *JobHooks) []model.Notification {
	var targets []string
	switch {
	case hooks == nil:
		// The job said nothing: the defaults answer per side.
		if f.Topic == model.TopicRunFailed {
			targets = p.Defaults.OnFailure
		} else {
			targets = p.Defaults.OnSuccess
		}
	case f.Topic == model.TopicRunFailed:
		targets = hooks.OnFailure
	default:
		targets = hooks.OnSuccess
	}
	if len(targets) == 0 {
		return nil
	}

	now := p.now().UTC()
	payload := BuildRunPayload(f)
	group := buildGroupKey(p.Defaults.GroupBy, f)

	out := make([]model.Notification, 0, len(targets))
	for _, t := range dedupe(targets) {
		out = append(out, model.Notification{
			Topic:       f.Topic,
			Subject:     f.JobName,
			Target:      t,
			Payload:     payload,
			DedupKey:    strings.Join([]string{f.Topic, f.JobName, t, f.RunID + ":" + itoa(int64(f.Attempt))}, "|"),
			GroupKey:    group,
			Throttle:    p.Defaults.Throttle,
			CreatedAt:   now,
			AvailableAt: now,
		})
	}
	return out
}

// SLAPlan builds the breach notifications for one episode transition. The
// dedup key rides the breached-at millisecond, which makes each episode its
// own event even if an old row survives retention.
func (p *Planner) SLAPlan(job string, breachedAt time.Time, lastSuccessAt time.Time, within time.Duration, host string) []model.Notification {
	targets := p.Defaults.OnFailure
	if len(targets) == 0 {
		return nil
	}
	now := p.now().UTC().Truncate(time.Millisecond)
	breachedAt = breachedAt.Truncate(time.Millisecond)
	payload := buildSLAPayload(job, breachedAt, lastSuccessAt, within, host)
	group := buildGroupKey(p.Defaults.GroupBy, RunFacts{Topic: model.TopicSLABreached, JobName: job})
	out := make([]model.Notification, 0, len(targets))
	for _, t := range dedupe(targets) {
		out = append(out, model.Notification{
			Topic:       model.TopicSLABreached,
			Subject:     job,
			Target:      t,
			Payload:     payload,
			DedupKey:    strings.Join([]string{model.TopicSLABreached, job, t, itoa(breachedAt.UnixMilli())}, "|"),
			GroupKey:    group,
			Throttle:    p.Defaults.Throttle,
			CreatedAt:   now,
			AvailableAt: now,
		})
	}
	return out
}

func buildSLAPayload(job string, breachedAt, lastSuccess time.Time, within time.Duration, host string) string {
	env := map[string]any{
		"event":              model.TopicSLABreached,
		"at":                 breachedAt.UnixMilli(),
		"job":                job,
		"state":              "sla_breached",
		"reason_code":        "JOB_SLA_BREACHED",
		"reason_text":        "no successful run since the expected_within deadline passed",
		"expected_within_ms": within.Milliseconds(),
		"breached_at":        breachedAt.UnixMilli(),
		"explain_cmd":        fmt.Sprintf("paceq explain job %s", job),
		"list_cmd":           fmt.Sprintf("paceq runs --job %s", job),
	}
	if !lastSuccess.IsZero() {
		env["last_success_at"] = lastSuccess.UnixMilli()
	}
	if host != "" {
		env["host"] = host
	}
	b, err := json.Marshal(env)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// buildGroupKey joins the configured fields' values. Unknown names were
// refused at configuration load; the second guard keeps garbage out anyway.
func buildGroupKey(fields []string, f RunFacts) string {
	parts := make([]string, 0, len(fields))
	for _, name := range fields {
		if !ValidGroupField(name) {
			continue
		}
		switch name {
		case "job":
			parts = append(parts, "job="+f.JobName)
		case "reason_code":
			parts = append(parts, "reason_code="+f.ReasonCode)
		}
	}
	return strings.Join(parts, ",")
}

func dedupe(names []string) []string {
	out := names[:0]
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
	}
	return out
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	if neg {
		return "-" + digits
	}
	return digits
}
