package sensor

import (
	"math"
	"time"
)

// FailureKind classifies how a sensor evaluation ended, which decides how the
// breaker and backoff treat it. The three classes split the contract's verdicts:
// transient failures are retried without penalty, config errors pause at once,
// and everything else counts toward the circuit breaker.
type FailureKind int

const (
	// FailurePermanent is a real failure: the sensor crashed, timed out,
	// produced unreadable output, or exited non-zero. It counts toward the
	// circuit breaker and grows the backoff exponentially.
	FailurePermanent FailureKind = iota
	// FailureTransient is exit 75 (EX_TEMPFAIL): a network glitch or a rate
	// limit. It gets a short backoff and never counts toward the breaker, so
	// a flaky endpoint does not pause a healthy sensor over night.
	FailureTransient
	// FailureConfig is exit 64 (EX_USAGE): the sensor says its configuration
	// is wrong. Repeating is wasted and noisy, so the caller pauses at once.
	FailureConfig
)

// ClassifyFailure maps an exit code to the failure class. Exit 75 is the only
// transient code and 64 the only config code in the contract; everything else,
// including the non-exit verdicts the evaluator names (timeout, signalled,
// output overflow), is permanent.
func ClassifyFailure(exitCode int) FailureKind {
	switch exitCode {
	case 75:
		return FailureTransient
	case 64:
		return FailureConfig
	default:
		return FailurePermanent
	}
}

// BackoffCap is the ceiling on the permanent-failure backoff. The formula is
// interval times 2^min(n,6) capped at one hour (plan 05 section 6.2); a sensor
// that keeps failing past the sixth consecutive failure waits no longer than
// one hour between attempts.
const BackoffCap = time.Hour

// MaxConsecutiveFailures is the breaker threshold: ten consecutive permanent
// failures pause the sensor, after which only an operator resumes it (plan 05
// section 6.2 / plan 02 section 5.5).
const MaxConsecutiveFailures = 10

// nextEvalAt computes when a failed sensor becomes due again. base is the
// sensor's own interval, n is the consecutive-failure count feeding the
// exponent, minInterval is the absolute hot-loop floor, and frac is a random
// value in the closed interval [0,1] that the caller derives from its jitter
// source, so the tests can pin exact times with a fixed frac.
//
// Permanent failures grow exponentially toward the cap; transient failures
// are short (a fraction of the base, never grown); a config failure schedules
// nothing, because the caller pauses the sensor instead. The result is never
// before now+minInterval, the absolute guard against a hot loop.
func nextEvalAt(now time.Time, base, minInterval time.Duration, n int, kind FailureKind, frac float64) time.Time {
	// minInterval <= 0 means the caller has no explicit floor; it applies
	// where one is configured. In production the runtime always passes the
	// spec'd min_interval (default 1s), so the hot-loop guard is the caller's
	// concern; the pure function just honours what it is given.
	var delay time.Duration
	switch kind {
	case FailureTransient:
		delay = time.Duration(float64(base) * frac)
	case FailureConfig:
		delay = 0
	default:
		exp := n
		if exp > 6 {
			exp = 6
		}
		if exp < 0 {
			exp = 0
		}
		d := time.Duration(float64(base) * math.Pow(2, float64(exp)))
		if d > BackoffCap {
			d = BackoffCap
		}
		delay = time.Duration(float64(d) * frac) // full jitter on [0, d]
	}

	sched := now.Add(delay)
	if minInterval > 0 && sched.Before(now.Add(minInterval)) {
		sched = now.Add(minInterval)
	}
	return sched.UTC()
}
