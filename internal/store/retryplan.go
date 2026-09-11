package store

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/retry"
	"github.com/a-holm/paceq/internal/spec"
)

// The schedule of a retry nobody planned (#213).
//
// A step going back to pending is runnable again at next_attempt_at, and what
// fills that column in is arithmetic over three facts: the policy the run
// froze, the attempt that just ended, and when it ended. The live executor
// holds all three and attaches a RetryPlan. No other writer of the same
// verdict holds them. The spool committer has two entry points, and one of
// them, the reconciler's sweep, may not import the spec package at all.
//
// So the store computes it where the facts already are. The policy is in
// job_versions.spec_json, frozen at materialisation and immutable after it;
// the attempt is on the step row; the finish stamp is in the verdict. A
// caller that watched an attempt end therefore gets the policy applied
// whether or not it remembered to work the backoff out itself.

// The values of the outcome_source column, which names how the writer of a
// verdict came by it (#39).
const (
	outcomeSourceDirect     = "direct"
	outcomeSourceSpool      = "spool"
	outcomeSourceReconciled = "reconciled"
)

// verdictObserved says whether this outcome reports an ending somebody read,
// as against one assumed over a dead executor. Only an observed ending is
// evidence about the work: a crashed holder proves the machine stumbled and
// says nothing about the command, so its next attempt waits on the run's own
// requeue backoff rather than on a policy meant to space failing work out.
//
// An empty source is not a claim to have observed anything.
func verdictObserved(source string) bool {
	return source == outcomeSourceDirect || source == outcomeSourceSpool
}

// RetryPolicyOf translates a frozen spec's retry block into the pure
// calculator's vocabulary, filling in the defaults the parser promises. A
// block missing fields in a hand written document therefore behaves exactly
// like one the decoder normalised: exponential growth from thirty seconds,
// capped at ten minutes, full jitter.
func RetryPolicyOf(r *spec.Retry) retry.Policy {
	p := retry.Policy{
		Backoff:  retry.Exponential,
		Initial:  spec.DefaultInitial,
		MaxDelay: spec.DefaultMaxDelay,
		Jitter:   retry.JitterFull,
	}
	if r == nil {
		return p
	}
	switch r.Backoff {
	case spec.BackoffFixed:
		p.Backoff = retry.Fixed
	case spec.BackoffExponential:
		p.Backoff = retry.Exponential
	}
	if r.Initial > 0 {
		p.Initial = r.Initial
	}
	if r.MaxDelay > 0 {
		p.MaxDelay = r.MaxDelay
	}
	switch r.Jitter {
	case spec.JitterNone:
		p.Jitter = retry.JitterNone
	case spec.JitterFull:
		p.Jitter = retry.JitterFull
	}
	return p
}

// plannedRetryTx builds the schedule for a step the machine has just sent
// back to pending, from the policy the run froze. It reports nil when the run
// names no step of this name in its spec, which is what a row created
// straight through CreateRunWithSteps looks like: there is no policy to
// apply, so the step stays runnable at once, exactly as before.
func plannedRetryTx(tx *sql.Tx, runID, name string, attempt int, finishedAt time.Time, detail string) (*RetryPlan, error) {
	var specJSON string
	err := tx.QueryRow(`SELECT v.spec_json FROM runs r
JOIN job_versions v ON v.id = r.job_version_id
WHERE r.id = ?`, runID).Scan(&specJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the frozen spec of run %s: %w", runID, err)
	}
	job, err := spec.FromIR([]byte(specJSON))
	if err != nil {
		return nil, fmt.Errorf("read the frozen spec of run %s: %w", runID, err)
	}
	var policy retry.Policy
	found := false
	for _, st := range job.Steps {
		if st.Name == name {
			policy = RetryPolicyOf(st.Retry)
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}

	delay := retry.Delay(policy, attempt, jitterFor(runID, name, attempt))
	next := finishedAt.Add(delay)
	facts, err := mergeRetryFacts(detail, map[string]any{
		"attempt":         attempt,
		"backoff_ms":      delay.Milliseconds(),
		"next_attempt_at": next.UnixMilli(),
	})
	if err != nil {
		return nil, fmt.Errorf("build the retry detail of %s in run %s: %w", name, runID, err)
	}
	return &RetryPlan{
		NextAttemptAt: next,
		ReasonCode:    reason.STEPRetryScheduled,
		DetailJSON:    facts,
	}, nil
}

// jitterFor is the draw source for one attempt's backoff. It is seeded from
// the attempt's own identity rather than from entropy, so settling the same
// attempt twice can only ever produce the same instant, while two steps
// stumbling together still draw independently and the herd stays broken.
func jitterFor(runID, name string, attempt int) *rand.Rand {
	h := fnv.New128a()
	_, _ = fmt.Fprintf(h, "%s/%s#%d", runID, name, attempt)
	var sum [16]byte
	// The source only spaces retry attempts out; nothing security
	// relevant reads its output.
	return rand.New(rand.NewPCG( // #nosec G404 - backoff spacing, not a secret
		binary.LittleEndian.Uint64(h.Sum(sum[:0])[:8]),
		binary.LittleEndian.Uint64(h.Sum(sum[:0])[8:]),
	))
}

// mergeRetryFacts writes the schedule's facts over whatever the verdict
// already carried, so a payload that named who recovered the attempt keeps
// saying so. Marshalling a map sorts the keys, which is the canonical shape
// every other detail object in the database has.
func mergeRetryFacts(detail string, facts map[string]any) (string, error) {
	merged := map[string]any{}
	if detail != "" {
		if err := json.Unmarshal([]byte(detail), &merged); err != nil {
			// A payload that is not an object has nothing to merge
			// into; the schedule's own facts are what matter here.
			merged = map[string]any{}
		}
	}
	for k, v := range facts {
		merged[k] = v
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
