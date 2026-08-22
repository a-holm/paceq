package logsink

import "strings"

// ring keeps the newest lines of the tail phase in memory. It holds whole
// encoded lines up to a byte budget; pushing a line that does not fit evicts
// the oldest lines first. Evicted lines are gone for good, which is exactly
// why the sink counts their seq numbers and their bytes: the marker line at
// Finish turns what was silently thrown away into a stated fact.
type ring struct {
	entries []ringEntry
	bytes   int64
	limit   int64
}

type ringEntry struct {
	encoded []byte
	// rawLen is the size of the job's own bytes before JSON wrapping. The
	// marker reports dropped bytes in this measure, because that is the
	// measure a reader cares about.
	rawLen int
}

func newRing(limit int64) *ring {
	return &ring{limit: limit}
}

// push stores one encoded line and returns how many raw bytes were evicted to
// make room. A line bigger than the whole ring is dropped at once: keeping a
// prefix of a line would print half a sentence as if it were the whole story.
func (r *ring) push(encoded []byte, rawLen int) int64 {
	var evicted int64
	for r.bytes+int64(len(encoded)) > r.limit && len(r.entries) > 0 {
		oldest := r.entries[0]
		r.entries = r.entries[1:]
		r.bytes -= int64(len(oldest.encoded))
		evicted += int64(oldest.rawLen)
	}
	if int64(len(encoded)) > r.limit {
		return evicted + int64(rawLen)
	}
	// Copy: the caller reuses its buffers.
	stored := make([]byte, len(encoded))
	copy(stored, encoded)
	r.entries = append(r.entries, ringEntry{encoded: stored, rawLen: rawLen})
	r.bytes += int64(len(encoded))
	return evicted
}

// flush hands the retained lines to fn in the order they were pushed, oldest
// first, which is the order they were produced in.
func (r *ring) flush(fn func(encoded []byte) error) error {
	for _, entry := range r.entries {
		if err := fn(entry.encoded); err != nil {
			return err
		}
	}
	return nil
}

// textTail keeps the last cap bytes of one stream as plain text. This is the
// material error_tail is made of: the newest bytes of what the job actually
// wrote, not a summary of it.
type textTail struct {
	buf []byte
	cap int
}

func newTextTail(capacity int) *textTail {
	return &textTail{cap: capacity}
}

// add appends bytes and trims the front beyond the capacity. The trim moves
// whole bytes; a multi-byte rune cut in half is repaired at reading time by
// the UTF-8 sanitiser.
func (t *textTail) add(p []byte) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.cap {
		t.buf = t.buf[len(t.buf)-t.cap:]
	}
}

// text returns what it kept. Bytes that never formed valid UTF-8 become the
// replacement character here rather than in the database layer, so what the
// caller receives is always safe to store and print.
func (t *textTail) text() string {
	return strings.ToValidUTF8(string(t.buf), "\ufffd")
}

// empty reports whether this stream never said anything.
func (t *textTail) empty() bool {
	return len(t.buf) == 0
}
