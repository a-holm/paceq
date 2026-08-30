package model

import "time"

// The sensor circuit breaker, in the one place every arrow reads it.
//
// A breaker exists to be visible: an operator has to know that a sensor stopped
// being asked. So its whole state is two columns on the sensor's row. The count
// of consecutive permanent failures is written by the same transaction that
// records the evaluation causing it, and the open state is not written at all;
// it is SensorBreakerOpen over that count. breaker_opened_at is not a second
// opinion about openness, it is when the current open period began, which is
// what the cooldown measures from and what an operator reads.
//
// No daemon holds breaker state, which is what makes the two answers one: a
// restart cannot rearm a tripped sensor, a fenced commit cannot move a count no
// transaction wrote, and the number the runtime refuses work on is the number
// every health surface prints.

// SensorBreakerThreshold is how many consecutive permanent failures open a
// sensor's breaker. Past it the sensor is not evaluated until the cooldown
// admits a probe, or an operator resumes it. This is the only threshold: the
// runtime, the status report and the shipped alert rule all read this constant.
const SensorBreakerThreshold = 10

// SensorFailuresAfter folds one finished evaluation into a sensor's
// consecutive-failure count.
//
// errored is whether the evaluation ended in an error rather than a triggered
// or a skipped verdict. A skip is an answer, so it clears the count exactly as
// a trigger does: the sensor ran and reported.
//
// exempt names the errors that must not burn breaker budget. Exit 75
// (EX_TEMPFAIL) is a glitch and exit 64 (EX_USAGE) is a configuration fault the
// sensor is paused for; neither is evidence that the sensor is down. An exempt
// error holds the count where it is rather than clearing it, so a sensor that
// alternates permanent and transient failures still reaches the threshold.
func SensorFailuresAfter(before int, errored, exempt bool) int {
	switch {
	case !errored:
		return 0
	case exempt:
		return before
	default:
		return before + 1
	}
}

// SensorBreakerOpen reports whether that many consecutive permanent failures
// have tripped the breaker.
func SensorBreakerOpen(failures int) bool {
	return failures >= SensorBreakerThreshold
}

// SensorBreakerOpenedAt folds one finished evaluation into the sensor's open
// stamp. before and after are the consecutive-failure counts either side of the
// evaluation, openedAt is the stamp on the row, and now is the commit instant.
//
// A closed breaker has no open period, so the stamp clears with the count. A
// failure that counts while the breaker is open is a failed probe: it restarts
// the cooldown, which is what holds a down sensor to one attempt per cooldown
// however often it comes due. An exempt failure moves neither, so a flaky
// endpoint does not extend the silence.
func SensorBreakerOpenedAt(before, after int, openedAt, now time.Time) time.Time {
	switch {
	case !SensorBreakerOpen(after):
		return time.Time{}
	case after > before:
		return now
	default:
		return openedAt
	}
}

// SensorBreakerAdmits reports whether a sensor carrying this breaker state may
// be evaluated at now. A closed breaker admits always; an open one refuses
// until its cooldown has elapsed and then admits the probe that either recovers
// the sensor or restarts the cooldown.
//
// A count at the threshold with no stamp admits. Only a row written before this
// rule existed can be in that state, and letting it probe is what gets it back
// to a count the rest of the fold can move.
func SensorBreakerAdmits(failures int, openedAt, now time.Time, cooldown time.Duration) bool {
	if !SensorBreakerOpen(failures) {
		return true
	}
	if openedAt.IsZero() {
		return true
	}
	return !now.Before(openedAt.Add(cooldown))
}
