package logsink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/a-holm/paceq/internal/clock"
)

// Stream names in the log format. stdout and stderr belong to the job; pulseq
// is the system speaking about the attempt in the same file, so a reader can
// tell the two apart without guessing.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
	StreamPulseq = "pulseq"
)

// DefaultQuota is the hard size limit per attempt: 16 MiB. Enough for real
// debugging, small enough that one runaway step cannot fill a disk.
const DefaultQuota int64 = 16 << 20

// maxFragment is the hard split size. A line longer than this is cut into
// pieces of this size, each carrying the split flag, so a job that pours out
// one endless line cannot eat the quota in memory before any check runs.
const maxFragment = 64 << 10

// errorTailCap is how much of the newest output the error tail keeps.
const errorTailCap = 4 << 10

// Line is one decoded log record. It is the read side of the format that the
// sink writes and the logs command renders.
type Line struct {
	TS     int64  `json:"ts"`
	Stream string `json:"stream"`
	Seq    int64  `json:"seq"`
	Line   string `json:"line,omitempty"`
	Event  string `json:"event,omitempty"`
	Split  bool   `json:"split,omitempty"`
	// DroppedBytes is set on the truncated marker only: how many raw job
	// bytes the quota threw away between the head and the surviving tail.
	DroppedBytes int64 `json:"dropped_bytes,omitempty"`
}

// Options configures one sink.
type Options struct {
	// Quota caps the attempt's log at this many bytes, split into a head
	// kept on disk and a tail kept in memory until Finish. Zero means
	// DefaultQuota. The field exists so tests can exercise the truncation
	// rules in kilobytes instead of mebibytes.
	Quota int64

	// Clock timestamps every line. Nil means clock.System.
	Clock clock.Clock
}

// Sink writes one attempt's log. The engine hands its writers to the runner as
// the command's stdout and stderr, which means the job process sees pipes and
// never a handle to its own log file.
//
// The quota works in two phases. While the file is under a quarter of the
// quota, lines go straight to disk: the head, which holds how the job
// started. After that, lines go into an in memory ring of at most three
// quarters of the quota: the tail, which holds how the job ended. Finish
// writes the truncated marker and then the tail, so the file always reads
// head, marker, tail.
type Sink struct {
	mu        sync.Mutex
	f         *os.File
	rel       string
	seq       int64
	written   int64
	headLimit int64
	phase     phase
	ring      *ring
	dropped   int64
	truncated bool

	stderrTail   *textTail
	combinedTail *textTail

	// assemblers are the per stream line builders handed out by Writer.
	// Finish flushes their unterminated remainders before anything else,
	// so a job killed mid line still leaves that fragment on disk.
	assemblers []*assembler

	clk clock.Clock
}

// phase is where in the quota the sink is.
type phase int

const (
	phaseHead phase = iota
	phaseTail
)

// Open starts the log for one attempt at the root. The date shard comes from
// the clock, so tests freeze time and get a stable path.
func Open(root Root, runID, step string, attempt int, opt Options) (*Sink, error) {
	clk := opt.Clock
	if clk == nil {
		clk = clock.System()
	}
	quota := opt.Quota
	if quota <= 0 {
		quota = DefaultQuota
	}
	rel := root.RelFor(clk.Now(), runID, step, attempt)
	if rel == "" {
		return nil, fmt.Errorf("open a log for run %s step %s attempt %d: the names do not form a log path", runID, step, attempt)
	}
	f, err := root.createFile(rel)
	if err != nil {
		return nil, err
	}
	head := quota / 4
	return &Sink{
		f:            f,
		rel:          rel,
		ring:         newRing(quota - head),
		stderrTail:   newTextTail(errorTailCap),
		combinedTail: newTextTail(errorTailCap),
		clk:          clk,
		phase:        phaseHead,
		headLimit:    head,
	}, nil
}

// RelPath is the log's path relative to the log root, in the form the
// database stores.
func (s *Sink) RelPath() string { return s.rel }

// Writer returns the io.Writer for one job stream. The runner gets one of
// these per stream, which is what makes exec build a pipe for the job: a
// Writer is not an *os.File, so the child never sees the log file itself.
// stream must be stdout or stderr; the pulseq stream belongs to Emit.
func (s *Sink) Writer(stream string) io.Writer {
	a := &assembler{sink: s, stream: stream, partial: make([]byte, 0, maxFragment+1)}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assemblers = append(s.assemblers, a)
	return a
}

// assembler collects a stream's chunks into lines. Chunks arrive in arbitrary
// sizes, so a partial line waits here until its newline shows up. The wait is
// bounded: once the accumulated partial line reaches maxFragment it is cut
// and emitted, so the buffer never holds more than that however the stream
// is sliced.
type assembler struct {
	sink    *Sink
	stream  string
	partial []byte
}

// Write splits p into lines and hands each to the sink. It always consumes
// all of p: the only error is one the sink itself raised, such as a full
// disk, and the runner's copier must see it.
func (a *assembler) Write(p []byte) (int, error) {
	rest := p
	for {
		if i := bytes.IndexByte(rest, '\n'); i >= 0 {
			if err := a.sink.writeLine(a.stream, appendTo(a.partial, rest[:i]), false); err != nil {
				return len(p) - len(rest), err
			}
			a.partial = a.partial[:0]
			rest = rest[i+1:]
			continue
		}
		// No newline left in the payload. Fill the partial line up to the
		// fragment window and cut the moment the window is full: the split
		// is driven by the accumulated line length, never by how much one
		// call happens to carry. An endless line is cut the same way
		// whether exec hands it over as one huge write or as a river of
		// pipe-sized chunks, so the quota machinery sees the bytes either
		// way and the partial buffer stays bounded.
		for {
			space := maxFragment - len(a.partial)
			if space > len(rest) {
				space = len(rest)
			}
			a.partial = appendTo(a.partial, rest[:space])
			rest = rest[space:]
			if len(a.partial) < maxFragment {
				// Genuinely short, still waiting for its newline.
				break
			}
			// The window filled without a newline: hard split. The slice
			// pins the cap so writeLineLocked's newline append for the
			// text tails cannot write into the buffer behind the window.
			if err := a.sink.writeLine(a.stream, a.partial[:maxFragment:maxFragment], true); err != nil {
				return len(p) - len(rest), err
			}
			a.partial = a.partial[:0]
		}
		return len(p), nil
	}
}

// appendTo appends src to dst and returns the result, keeping the assembler's
// buffer as the backing store so a stream of small writes does not allocate a
// new slice per line.
func appendTo(dst, src []byte) []byte {
	return append(dst, src...)
}

// flushPartial emits whatever an unterminated line left behind. It runs at
// Finish under the sink lock, so it takes the locked path directly.
func (a *assembler) flushPartial() error {
	if len(a.partial) == 0 {
		return nil
	}
	err := a.sink.writeLineLocked(a.stream, a.partial, true)
	a.partial = a.partial[:0]
	return err
}

// Emit writes a pulseq event line: the system's own marks in the same file as
// the job output. Fields are optional and sorted by name, so the same event
// always encodes to the same bytes.
func (s *Sink) Emit(event string, fields map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	line := []byte(fmt.Sprintf(`{"ts":%d,"stream":%q,"seq":%d,"event":%q`,
		s.clk.Now().UnixMilli(), StreamPulseq, s.seq, event))
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, err := json.Marshal(fields[name])
		if err != nil {
			return fmt.Errorf("encode the field %s of the %s event: %w", name, event, err)
		}
		line = append(line, fmt.Sprintf(`,%q:`, name)...)
		line = append(line, value...)
	}
	line = append(line, '}', '\n')
	return s.write(lineRecord{raw: nil, encoded: line})
}

// lineRecord pairs one line's encoded form with its raw size. The raw size is
// what dropped_bytes counts: JSON quoting can change the byte count, and what
// was lost from the job's point of view is the raw text.
type lineRecord struct {
	raw     []byte
	encoded []byte
}

// writeLine is the single path every job line takes: number it, encode it,
// remember it for the error tail, then either write it to the file or move it
// into the tail ring under the quota.
func (s *Sink) writeLine(stream string, raw []byte, split bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLineLocked(stream, raw, split)
}

// writeLineLocked is writeLine without the lock. Finish calls it directly for
// the assemblers' unterminated remainders while already holding the lock.
func (s *Sink) writeLineLocked(stream string, raw []byte, split bool) error {
	s.seq++
	rec := Line{
		TS:     s.clk.Now().UnixMilli(),
		Stream: stream,
		Seq:    s.seq,
		Line:   string(raw),
		Split:  split,
	}
	encoded, err := json.Marshal(rec)
	if err != nil {
		// Marshal of a string cannot fail on content: invalid UTF-8 is
		// replaced, not refused. Reaching this is a bug, and it must be
		// loud rather than silently dropping a line.
		return fmt.Errorf("encode log line %d: %w", s.seq, err)
	}
	encoded = append(encoded, '\n')

	// The error tail watches the raw bytes: stderr first, everything second.
	// The newline belongs to the text, so lines survive as lines.
	withNewline := append(raw, '\n')
	s.combinedTail.add(withNewline)
	if stream == StreamStderr {
		s.stderrTail.add(withNewline)
	}

	return s.write(lineRecord{raw: raw, encoded: encoded})
}

// write puts one encoded line where the quota says it belongs.
func (s *Sink) write(rec lineRecord) error {
	if s.phase == phaseHead {
		if _, err := s.f.Write(rec.encoded); err != nil {
			return fmt.Errorf("write the log: %w", err)
		}
		s.written += int64(len(rec.encoded))
		if s.written >= s.headLimit {
			s.phase = phaseTail
		}
		return nil
	}
	// Reaching this branch is what truncation means: a line did not fit in
	// the head and now lives in the tail, or was thrown away outright. A log
	// that stopped exactly at the limit never gets here, so it is never
	// marked truncated and never carries an empty marker.
	s.truncated = true
	dropped := s.ring.push(rec.encoded, len(rec.raw))
	s.dropped += dropped
	return nil
}

// Finish ends the log: it writes the marker and the surviving tail, flushes
// everything to disk and closes the file. All of that happens before the
// caller opens its database transaction, so what is left inside the
// transaction is pure SQL. The returned tail prefers stderr and falls back to
// the combined streams; bytes is the file's final size; truncated reports
// whether the quota cut anything.
func (s *Sink) Finish() (tail string, bytes int64, truncated bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unterminated remainders go out first, so they land where the job
	// produced them: still in the head, or in the ring before the marker.
	for _, a := range s.assemblers {
		if err := a.flushPartial(); err != nil {
			return "", 0, false, err
		}
	}
	if s.phase == phaseTail {
		if err := s.writeMarkerLocked(); err != nil {
			return "", 0, false, err
		}
		if err := s.ring.flush(func(encoded []byte) error {
			_, err := s.f.Write(encoded)
			return err
		}); err != nil {
			return "", 0, false, fmt.Errorf("write the log tail: %w", err)
		}
	}

	info, err := s.f.Stat()
	if err != nil {
		return "", 0, false, fmt.Errorf("measure the log: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return "", 0, false, fmt.Errorf("flush the log to disk: %w", err)
	}
	if err := s.f.Close(); err != nil {
		return "", 0, false, fmt.Errorf("close the log: %w", err)
	}
	s.f = nil

	if s.stderrTail.empty() {
		tail = s.combinedTail.text()
	} else {
		tail = s.stderrTail.text()
	}
	return tail, info.Size(), s.truncated, nil
}

// writeMarkerLocked writes the truncated marker line: what the reader needs to
// explain the seq gap it is about to see. It runs after the head and before
// the tail.
func (s *Sink) writeMarkerLocked() error {
	s.seq++
	marker, err := json.Marshal(Line{
		TS:           s.clk.Now().UnixMilli(),
		Stream:       StreamPulseq,
		Seq:          s.seq,
		Event:        "truncated",
		DroppedBytes: s.dropped,
	})
	if err != nil {
		return fmt.Errorf("encode the truncated marker: %w", err)
	}
	marker = append(marker, '\n')
	if _, err := s.f.Write(marker); err != nil {
		return fmt.Errorf("write the truncated marker: %w", err)
	}
	return nil
}

// Close abandons the log without a verdict. An attempt that never reached a
// terminal state gets no truncated marker: closing the file is all it takes.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
