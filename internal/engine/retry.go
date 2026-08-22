package engine

import (
	"github.com/a-holm/paceq/internal/retry"
	"github.com/a-holm/paceq/internal/spec"
)

// retryPolicyOf translates the frozen spec's retry block into the pure
// calculator's vocabulary, filling in the defaults the parser promises. A
// block missing fields in a hand written document therefore behaves exactly
// like one the decoder normalised: exponential growth from thirty seconds,
// capped at ten minutes, full jitter.
func retryPolicyOf(r *spec.Retry) retry.Policy {
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
