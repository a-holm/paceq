package retry

import (
	"math/rand/v2"
	"time"
)

// Kind names the backoff shape a policy grows its delays by.
type Kind string

const (
	// Exponential doubles the initial delay every attempt.
	Exponential Kind = "exponential"
	// Fixed keeps the initial delay flat across attempts.
	Fixed Kind = "fixed"
)

// The jitter vocabulary, spelled the way the spec spells it. An empty field
// means full jitter, which is the default the project chose: retries without
// spread synchronise into a thundering herd against whatever just stumbled.
const (
	JitterFull = "full"
	JitterNone = "none"
)

// Policy is one step's retry timing, already parsed and validated by
// internal/spec. It is pure data: everything a delay needs and nothing about
// who computes it or when.
type Policy struct {
	Backoff  Kind
	Initial  time.Duration
	MaxDelay time.Duration
	Jitter   string
}

// Delay returns how long to wait before the attempt after the one that just
// failed. Attempt is 1-based and names the attempt that failed; values below
// one count as the first. Rnd is the injected source of randomness; it must
// be non-nil unless the policy's jitter is none, and the same seed always
// replays the same draws.
//
// The function is pure: no clock, no globals, no state. Monotonic in the
// attempt until the cap, never above MaxDelay, never negative, and free of
// overflow for any attempt number, because growth stops at the cap instead
// of doubling past it. Durations this side of 292 years are safe.
func Delay(p Policy, attempt int, rnd *rand.Rand) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := p.Initial
	if p.Backoff != Fixed {
		// Exponential, and the empty kind beside it. Doubling stops at
		// the cap, so a large attempt number can never wrap the int64
		// behind time.Duration: the value lands on the cap and stays
		// there. This is the cap applied after the growth, which is
		// what keeps min(base*2^n, cap) honest for every n.
		for i := 1; i < attempt; i++ {
			next := base * 2
			if next < base || next >= p.MaxDelay {
				base = p.MaxDelay
				break
			}
			base = next
		}
	}
	if p.MaxDelay > 0 && base > p.MaxDelay {
		base = p.MaxDelay
	}
	if base < 0 {
		base = 0
	}
	if p.Jitter == JitterNone {
		return base
	}
	// Full jitter: a uniform draw in [0, base], endpoints included. The
	// expectation stays half the capped base, so it still grows with the
	// attempt while breaking the synchronisation between retries.
	return time.Duration(rnd.Int64N(int64(base) + 1))
}
