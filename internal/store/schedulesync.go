package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/a-holm/paceq/internal/spec"
)

// Schedule sync is the apply path's half of the schedules table: it makes one
// job's schedule rows identical to the job's schedules[] block, in the same
// transaction that writes the job version. schedules.go owns everything the
// scheduler loop touches, the due query, the tick transaction and the upsert
// its own tests seed with; this file owns the write that materialises a
// declaration and nothing else.
//
// The definition columns (kind, expr, timezone, overlap, shadow) come from the
// file and a re-apply may replace them. The drift columns (last_tick_at,
// next_tick_at, paused) belong to the scheduler and to the operator, never to a
// file. They are named in the INSERT branch only, so a re-apply of an unchanged
// spec cannot move a cursor or resume a paused schedule. The row's id and
// created_at stay behind for the same reason: history keeps pointing at the row
// it always pointed at.

// scheduleDef is the part of a schedule row a job file owns, with the spec
// defaults resolved, so a sparse declaration and an explicit one compare equal.
type scheduleDef struct {
	Kind     string
	Expr     string
	Timezone string
	Overlap  string
	Shadow   bool
}

// scheduleKindCron is the only kind the grammar can express. The interval kind
// the schema allows has no spelling in a job file.
const scheduleKindCron = "cron"

// scheduleDefOf reads one declaration into the columns it decides.
func scheduleDefOf(s spec.Schedule) scheduleDef {
	def := scheduleDef{
		Kind:     scheduleKindCron,
		Expr:     s.Cron,
		Timezone: s.Timezone,
		Overlap:  s.Overlap,
		Shadow:   s.Shadow,
	}
	// FromIR leaves both keys out at their default, so a document written
	// before the key existed reads back as the default rather than as empty.
	if def.Timezone == "" {
		def.Timezone = spec.DefaultTimezone
	}
	if def.Overlap == "" {
		def.Overlap = spec.DefaultOverlap
	}
	return def
}

// digest is the frozen definition form of one schedule, and the idempotency
// guard sensors.go gets from its spec_json column. The schedules table carries
// no such column, so the comparison runs over the definition columns and both
// sides of it are built here. encoding/json sorts map keys, so the bytes are
// stable whatever order the fields were written in.
func (d scheduleDef) digest() string {
	bytes, _ := json.Marshal(map[string]any{
		"kind":     d.Kind,
		"expr":     d.Expr,
		"timezone": d.Timezone,
		"overlap":  d.Overlap,
		"shadow":   d.Shadow,
	})
	return string(bytes)
}

// schedulePlanItem is one line of work an apply settled on before it takes the
// write lock.
type schedulePlanItem struct {
	name string
	def  *scheduleDef
	kind string
}

// scheduleReader is the handle currentScheduleDefs runs on. Classification is a
// read, so it goes through the read pool and never through the single writer.
type scheduleReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// currentScheduleDefs reads the definitions of one job's schedule rows, outside
// any write transaction, keyed by schedule name.
func currentScheduleDefs(ctx context.Context, r scheduleReader, job string) (map[string]string, error) {
	rows, err := r.QueryContext(ctx, `SELECT name, kind, expr, timezone, overlap, shadow
FROM schedules WHERE job_name = ?`, job)
	if err != nil {
		return nil, fmt.Errorf("read the current schedules of job %s: %w", job, err)
	}
	defer func() { _ = rows.Close() }()

	existing := map[string]string{}
	for rows.Next() {
		var name string
		var def scheduleDef
		var shadow int
		if err := rows.Scan(&name, &def.Kind, &def.Expr, &def.Timezone, &def.Overlap, &shadow); err != nil {
			return nil, fmt.Errorf("scan a schedule row of job %s: %w", job, err)
		}
		def.Shadow = shadow == 1
		existing[name] = def.digest()
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the current schedules of job %s: %w", job, err)
	}
	return existing, nil
}

// buildSchedulePlan compares what the job wants against what the table holds
// and settles on the additions, replacements, no-ops and removals.
func buildSchedulePlan(schedules []spec.Schedule, existing map[string]string) []schedulePlanItem {
	var plan []schedulePlanItem
	want := make(map[string]bool, len(schedules))
	for _, s := range schedules {
		want[s.Name] = true
		def := scheduleDefOf(s)
		last, wasThere := existing[s.Name]
		switch {
		case !wasThere:
			plan = append(plan, schedulePlanItem{name: s.Name, def: &def, kind: lifecycleCreated})
		case last != def.digest():
			plan = append(plan, schedulePlanItem{name: s.Name, def: &def, kind: lifecycleSpecChanged})
		default:
			plan = append(plan, schedulePlanItem{name: s.Name, kind: ""})
		}
	}
	for name := range existing {
		if !want[name] {
			plan = append(plan, schedulePlanItem{name: name, kind: lifecycleRemoved})
		}
	}
	return plan
}

// scheduleResultOf folds a schedule plan into the SyncResult that reports it.
func scheduleResultOf(plan []schedulePlanItem) SyncResult {
	var res SyncResult
	var unchanged []string
	for _, item := range plan {
		switch item.kind {
		case lifecycleCreated:
			res.Created = append(res.Created, item.name)
		case lifecycleSpecChanged:
			res.Updated = append(res.Updated, item.name)
		case lifecycleRemoved:
			res.Removed = append(res.Removed, item.name)
		case "":
			unchanged = append(unchanged, item.name)
		}
	}
	res.Unchanged = unchanged
	return res
}

// applyScheduleSQL writes one schedule's definition. The cursor columns are
// named in the INSERT branch only, so the calendar belongs to the scheduler.
//
// A new row lands due at once because internal/store may not import cronx: now
// is the only stamp apply can compute. The loop plans from that stamp and
// writes nothing until the expression first lands, so until then the row stays
// due and every surface that prints next_tick_at shows the apply instant.
//
// The conflict branch keeps the cursor as well, so a changed expression first
// fires at the stamp the old one left behind.
//
// paused is left to the schema default for the same reason a reload never
// resumes a paused job: pausing is an operator decision about a schedule, not
// a property of the file.
const applyScheduleSQL = `INSERT INTO schedules
(id, job_name, name, kind, expr, timezone, overlap, shadow,
 next_tick_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_name, name) DO UPDATE SET
    kind       = excluded.kind,
    expr       = excluded.expr,
    timezone   = excluded.timezone,
    overlap    = excluded.overlap,
    shadow     = excluded.shadow,
    updated_at = excluded.updated_at`

// deleteScheduleSQL drops a schedule that left the spec. The history stays:
// ticks names a schedule by the text job/name in source_name and carries no
// foreign key to schedules, so every past decision remains readable, and a
// schedule of that name added back cannot replay any of it because the UNIQUE
// (source_kind, source_name, scheduled_for) gate still holds the fire-times it
// already claimed.
const deleteScheduleSQL = `DELETE FROM schedules WHERE job_name = ? AND name = ?`

// syncSchedulesTx performs the plan inside a transaction the caller owns, so a
// reader never sees the schedule rows half way to the spec. rowIDs is one id
// per plan item, minted before the write lock was taken; the removals and the
// no-ops leave theirs unused.
func syncSchedulesTx(tx *sql.Tx, at int64, job string, plan []schedulePlanItem, rowIDs []string) error {
	for i, item := range plan {
		switch item.kind {
		case lifecycleCreated, lifecycleSpecChanged:
			def := item.def
			if def == nil {
				return fmt.Errorf("sync %s: a %s plan item carries no schedule", job, item.kind)
			}
			shadow := 0
			if def.Shadow {
				shadow = 1
			}
			if _, err := tx.Exec(applyScheduleSQL,
				rowIDs[i], job, item.name, def.Kind, def.Expr, def.Timezone,
				def.Overlap, shadow, at, at, at); err != nil {
				return fmt.Errorf("sync schedule %s/%s: %w", job, item.name, err)
			}
		case lifecycleRemoved:
			if _, err := tx.Exec(deleteScheduleSQL, job, item.name); err != nil {
				return fmt.Errorf("delete schedule %s/%s: %w", job, item.name, err)
			}
		}
	}
	return nil
}
