package sensor

// applyLimit bounds one evaluation's trigger list to the sensor's
// max_triggers_per_tick and reports whether anything was dropped. It is the
// chunking primitive: a tick takes the first N triggers, the runtime schedules
// an immediate re-tick (bounded by min_interval), and the remainder is handled
// over the next ticks without loss or duplication.
//
// The cursor rule is deliberately conservative (plan 04 section 5.2 / plan 02
// section 5.5): the cursor is left unchanged unless the evaluation has a
// trustworthy partial-cursor answer for element N. When this milestone has no
// such answer, truncation keeps CursorAfter nil, so a re-evaluation replays
// the same cursor and the dedup gate (M3-04) folds the replayed keys into the
// original runs instead of creating twins. That is the whole of the no-loss
// guarantee: dropping triggers is always safe because none of them can
// silently become a second run later.
func applyLimit(res *Result, max int) (truncated bool) {
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
