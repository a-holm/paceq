package sensor

// ApplyLimit bounds one evaluation's trigger list to the sensor's
// max_triggers_per_tick and reports whether anything was dropped. It is the
// chunking primitive: a tick takes the first N triggers, the next tick is left
// due, and the remainder is handled over the following ticks without loss or
// duplication.
//
// It is exported because the ceiling is not the daemon's rule, it is the
// sensor's. Both evaluations reach it through one commit (#215): a forced
// `paceq sensors tick` with no daemon running used to commit the whole batch
// and move the cursor past it, so the number `paceq sensors show` printed was
// one the system did not honour.
//
// The cursor rule is deliberately conservative (plan 04 section 5.2 / plan 02
// section 5.5): the cursor is left unchanged unless the evaluation has a
// trustworthy partial-cursor answer for element N. When this milestone has no
// such answer, truncation keeps CursorAfter nil, so a re-evaluation replays
// the same cursor and the dedup gate (M3-04) folds the replayed keys into the
// original runs instead of creating twins. That is the whole of the no-loss
// guarantee: dropping triggers is always safe because none of them can
// silently become a second run later.
func ApplyLimit(res *Result, max int) (truncated bool) {
	if max <= 0 || len(res.Triggers) <= max {
		if res.ReasonData != nil {
			res.ReasonData["truncated"] = false
		}
		return false
	}

	dropped := len(res.Triggers) - max
	res.Triggers = res.Triggers[:max]
	// Conservative default: no trusted partial cursor, so the cursor does not
	// move. Dedup makes the replay lossless.
	res.CursorAfter = nil
	if res.ReasonData == nil {
		res.ReasonData = map[string]any{}
	}
	res.ReasonData["truncated"] = true
	res.ReasonData["dropped"] = dropped
	return true
}
