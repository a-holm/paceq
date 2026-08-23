package sensor

// cappedStdout is a bounded stdout sink for one sensor evaluation. It counts
// every byte received and keeps only up to the limit, so the evaluator can
// parse the JSON object it got while still detecting an oversized stream. An
// oversized stream is an error, not a truncated parse that guesses.
type cappedStdout struct {
	limit    int64
	received int64
	overflow bool
	buf      []byte
}

func newCappedStdout(limit int64) *cappedStdout {
	return &cappedStdout{limit: limit}
}

func (c *cappedStdout) Write(p []byte) (int, error) {
	c.received += int64(len(p))
	if c.received > c.limit && !c.overflow {
		c.overflow = true
	}
	if n := c.limit - int64(len(c.buf)); n > 0 {
		if n > int64(len(p)) {
			n = int64(len(p))
		}
		c.buf = append(c.buf, p[:int(n)]...)
	}
	return len(p), nil
}

// tailStderr keeps the last tailLimit bytes of the sensor's stderr. The whole
// stream is read (so the child never blocks on a full pipe), but only the tail
// is held for the result; a sensor that dies rarely explains itself at its own
// beginning.
type tailStderr struct {
	limit int64
	buf   []byte
}

func newTailStderr(limit int64) *tailStderr {
	return &tailStderr{limit: limit}
}

func (t *tailStderr) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if cut := int64(len(t.buf)) - t.limit; cut > 0 {
		t.buf = t.buf[int(cut):]
	}
	return len(p), nil
}

func (t *tailStderr) String() string { return string(t.buf) }
