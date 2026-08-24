package store

// The concurrency key index model (#17). This comment is the design decision
// the tests pin; read it before changing anything in this file.
//
// CHOSEN MODEL: keyless deferral, keyed start.
//
//   - Creation is insert-first. A run that wants a key is inserted with the
//     key against the partial unique index ux_runs_conc_key (concurrency_key
//     WHERE concurrency_key IS NOT NULL AND state IN ('queued','running')),
//     using a conflict target that names that exact predicate. The insert
//     result is the only conflict signal: no COUNT, no pre-read, nothing that
//     can be out of date.
//   - The loser is stored KEYLESS. state stays queued, concurrency_key is
//     NULL, available_at is a backoff into the future, defer_reason is
//     "concurrency_key" and reason_data carries BOTH the wanted key and the
//     blocking run id. A deferred run holds nothing and blocks nothing: two
//     deferred runs can never deadlock against each other, because the index
//     does not see NULL keys at all.
//   - The ONLY way out of that shape is a claim. The claim candidate query
//     refuses every row whose defer_reason is "concurrency_key", so a
//     keyless-but-keyed-wanting row can never flip to running through the
//     ordinary queue path. The claim pass instead tries a single guarded
//     UPDATE per due deferral: set the key and start the run in one
//     statement, with NOT EXISTS refusing any other active holder of the key.
//     The index backs the same rule for every other writer. If the key is
//     still held the row simply stays deferred and is retried next pass;
//     correctness never depended on the wake that released it.
//   - Convergence needs no bus. The ticker alone re-attempts every due
//     deferral on each claim cycle; notify.Bus wakes are latency reductions,
//     not dependencies. That is the same standing rule the rest of the write
//     model keeps.
//
// WHY NOT THE ALTERNATIVES. Storing the loser WITH its key cannot work under
// this index predicate: the loser is queued, so it would collide with the very
// blocker that deferred it, and worse, once the blocker went terminal the
// loser would silently become a second holder while still waiting. A separate
// semaphore table with counters was declined in the issue itself: counters can
// drift out of sync with the rows they describe, and the binary key needs none.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
)

// canonicalConcurrencyKey namespaces a resolved value under its job's name.
// The index is global on purpose (two jobs may exclude each other), but two
// jobs that happen to pick the same word must not collide by accident, so the
// convention is baked in here rather than left to authors.
func canonicalConcurrencyKey(jobName, value string) string {
	return jobName + ":" + value
}

// resolvedConcurrencyKey works out the full key one fire will carry, or ""
// when the fire has no key and is unlimited. paramsJSON is the trigger's
// parameter object; an unreadable object counts as no parameters rather than
// as a fault, because the key must degrade to unlimited exactly like a missing
// parameter does.
func resolvedConcurrencyKey(job *spec.Job, paramsJSON, runKey string) string {
	if job == nil || job.ConcurrencyKey == nil {
		return ""
	}
	var params map[string]string
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		params = nil
	}
	value, ok := job.ConcurrencyKey.Value(params, runKey)
	if !ok {
		return ""
	}
	return canonicalConcurrencyKey(job.Name, value)
}

// concDeferDataJSON builds the reason_data object a key deferral records: the
// key the run wanted and the run that held it. Same discipline as
// deferDataJSON above: one id beside the fact is the difference between an
// explanation and a shrug.
func concDeferDataJSON(key, blocking string) string {
	data := fmt.Sprintf(`{"blocking_run_id":%q`, blocking)
	if key != "" {
		data += fmt.Sprintf(`,"concurrency_key":%q`, key)
	}
	return data + "}"
}

// blockingRunForKeyTx names the active run holding a key. It runs AFTER the
// insert said "held": this is attribution for the record, never a decision,
// which is why reading it here breaks no insert-first rule. Empty when the
// holder vanished inside the same transaction (it cannot) or carries no id.
func blockingRunForKeyTx(tx *sql.Tx, key string) string {
	var id string
	err := tx.QueryRow(`SELECT id FROM runs
WHERE concurrency_key = ? AND state IN ('queued', 'running')
ORDER BY id LIMIT 1`, key).Scan(&id)
	if err != nil {
		return ""
	}
	return id
}

// rejectTriggerConcurrencyKeyTx turns the trigger a refused fire already wrote
// into the rejection the policy promises, and withdraws the dedup registration
// the fire made for itself. The delete is scoped to THIS fire's own run id, so
// it can never free someone else's registration.
func rejectTriggerConcurrencyKeyTx(tx *sql.Tx, triggerID, sourceID string, epoch int64, runKey, ownRunID, blocking, key string) error {
	text := fmt.Sprintf("the run %s holds concurrency key %s", blocking, key)
	// The rejection points at the run that holds the key, exactly like a
	// deduped trigger points at the run it folded into.
	if _, err := tx.Exec(`UPDATE triggers SET outcome = 'rejected',
reason_code = ?, reason_text = ?, run_id = ?
WHERE id = ?`, string(reason.TRIGGERRejectedConcurrencyKey), text, nullIfEmpty(blocking), triggerID); err != nil {
		return fmt.Errorf("reject the trigger %s: %w", triggerID, err)
	}
	if _, err := tx.Exec(`DELETE FROM run_keys
WHERE source_id = ? AND epoch = ? AND run_key = ? AND run_id = ?`,
		sourceID, epoch, runKey, ownRunID); err != nil {
		return fmt.Errorf("withdraw the run key of trigger %s: %w", triggerID, err)
	}
	return nil
}

// activeKeysSQL is the I12 sweep for keys (#17): the active rows grouped by
// the key they hold. Keyless deferred rows do not appear: they hold nothing.
const activeKeysSQL = `SELECT concurrency_key, COUNT(*)
FROM runs
WHERE concurrency_key IS NOT NULL AND state IN ('queued', 'running')
GROUP BY concurrency_key HAVING COUNT(*) > 1`

// ActiveConcurrencyKeyViolations returns every key held by more than one
// active run: the I12 variant of 02 section 4.3. The partial unique index
// makes this state unreachable through the code, which is exactly why fsck
// looks for it anyway: a violation here means a dropped index, a hand edit or
// a broken migration, not an ordinary bug.
func (s *Store) ActiveConcurrencyKeyViolations(ctx context.Context) ([]Violation, error) {
	rows, err := s.r.QueryContext(ctx, activeKeysSQL)
	if err != nil {
		return nil, fmt.Errorf("fsck I12 keys: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Violation
	for rows.Next() {
		var key string
		var n int64
		if err := rows.Scan(&key, &n); err != nil {
			return nil, fmt.Errorf("fsck I12 keys: %w", err)
		}
		out = append(out, Violation{
			Check:   "I12",
			Subject: "key " + key,
			Detail:  fmt.Sprintf("%d active runs hold the concurrency key", n),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fsck I12 keys: %w", err)
	}
	return out, nil
}

// keyDeferredCandidatesSQL reads the due key deferrals in claim order. The
// same ordering and gates as the ordinary queue, one different defer_reason.
const keyDeferredCandidatesSQL = `SELECT id, reason_data FROM runs
WHERE state = 'queued'
	AND available_at <= ?
	AND cancel_requested_at IS NULL
	AND defer_reason = 'concurrency_key'
%s
ORDER BY scheduled_for, created_at, id
LIMIT ?`

// keyedStartSQL is the keyed start (#17), the only exit a keyless deferral
// has. One statement gives the row its key back AND starts it, and the NOT
// EXISTS guard refuses any other active holder of that key: the same atomic
// decision shape as the step claim predicate, with the partial unique index
// standing behind it for every writer this statement does not cover. A row
// this statement does not touch stays exactly as it was, deferred and whole.
//
// nolint:fencing: queued rows hold no lease, so there is no token to fence
// with; the claim itself mints the epoch, exactly as the batch claim does.
const keyedStartSQL = `UPDATE runs SET
	state = 'running',
	lease_owner = ?1,
	lease_epoch = lease_epoch + 1,
	lease_expires_at = ?3,
	heartbeat_at = ?2,
	started_at = COALESCE(started_at, ?2),
	updated_at = ?2,
	concurrency_key = ?5,
	defer_reason = NULL,
	reason_code = NULL,
	reason_data = NULL
WHERE id = ?4
	AND state = 'queued'
	AND available_at <= ?2
	AND cancel_requested_at IS NULL
	AND NOT EXISTS (
		SELECT 1 FROM runs other
		WHERE other.concurrency_key = ?5
			AND other.state IN ('queued', 'running'))
RETURNING id, job_name, job_version_id, run_key, attempt, lease_epoch, params_json`

// keyDeferredOnlyClause mirrors the Only filter of ClaimSpec for deferrals:
// an executor claiming one exact run gets that run or nothing else.
func keyDeferredOnlyClause(only []string) string {
	if len(only) == 0 {
		return ""
	}
	return "AND id IN (" + strings.TrimSuffix(strings.Repeat("?, ", len(only)), ", ") + ")"
}

// claimKeyDeferredTx starts due key deferrals while the caller's budget lasts.
// Each is one guarded keyed start; a start that loses (the key is still held)
// leaves its row deferred for the next pass, which is the whole release model:
// correctness never depended on the wake, only on the next look.
func claimKeyDeferredTx(tx *sql.Tx, spec ClaimSpec, now, expires time.Time,
	budget int, out *[]ClaimedRun,
) error {
	if budget <= 0 {
		return nil
	}
	query := fmt.Sprintf(keyDeferredCandidatesSQL, keyDeferredOnlyClause(spec.Only)) // #nosec G201 - the %s inserts only "?" placeholder marks, never user data; every value is a bound arg below
	args := []any{now.UnixMilli()}
	for _, id := range spec.Only {
		args = append(args, id)
	}
	args = append(args, budget)

	rows, err := tx.Query(query, args...)
	if err != nil {
		return fmt.Errorf("read the due key deferrals for %s: %w", spec.Owner, err)
	}
	type pendingDeferral struct {
		id   string
		data string
	}
	var cands []pendingDeferral
	for rows.Next() {
		var p pendingDeferral
		var data sql.NullString
		if err := rows.Scan(&p.id, &data); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan a due key deferral for %s: %w", spec.Owner, err)
		}
		p.data = data.String
		cands = append(cands, p)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read the due key deferrals for %s: %w", spec.Owner, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close the due key deferrals of %s: %w", spec.Owner, err)
	}

	started := 0
	for _, p := range cands {
		if started >= budget {
			break
		}
		key, _ := concDataOf(p.data)
		if key == "" {
			// A deferral without a wanted key cannot be started by this
			// path at all. It stays put; fsck's sweeps are what surface
			// such a row, because no writer produces one.
			continue
		}
		startRows, err := tx.Query(keyedStartSQL,
			spec.Owner, now.UnixMilli(), expires.UnixMilli(), p.id, key)
		if err != nil {
			return fmt.Errorf("start the keyed deferral %s for %s: %w", p.id, spec.Owner, err)
		}
		var claimed *ClaimedRun
		for startRows.Next() {
			var cl ClaimedRun
			var runKey sql.NullString
			if err := startRows.Scan(&cl.ID, &cl.JobName, &cl.JobVersionID, &runKey,
				&cl.Attempt, &cl.LeaseEpoch, &cl.ParamsJSON); err != nil {
				_ = startRows.Close()
				return fmt.Errorf("scan the keyed start of %s: %w", p.id, err)
			}
			cl.RunKey = runKey.String
			claimed = &cl
		}
		if err := startRows.Err(); err != nil {
			_ = startRows.Close()
			return fmt.Errorf("read the keyed start of %s: %w", p.id, err)
		}
		if err := startRows.Close(); err != nil {
			return fmt.Errorf("close the keyed start of %s: %w", p.id, err)
		}
		if claimed == nil {
			continue
		}
		*out = append(*out, *claimed)
		started++
	}
	return nil
}

// concDataOf reads the wanted key back out of a deferral's reason_data. It
// exists so the claim pass never has to trust the row's own columns: the key
// lives in the data object the deferral wrote, beside the blocking run.
func concDataOf(data string) (key, blocking string) {
	if data == "" {
		return "", ""
	}
	var parsed struct {
		Key      string `json:"concurrency_key"`
		Blocking string `json:"blocking_run_id"`
	}
	if err := json.Unmarshal([]byte(data), &parsed); err != nil {
		return "", ""
	}
	return parsed.Key, parsed.Blocking
}
