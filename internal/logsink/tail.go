package logsink

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// SeqCheck walks the seq numbers of one file in order and reports where lines
// went missing. This is the reader half of why every line is numbered: a
// complete log has no gaps, and anything that ever lost lines says so.
//
// A gap is counted when the next seq is higher than last plus one: those
// numbers were consumed and their lines never reached this file in this place,
// which is either the quota dropping them or the surviving tail sitting behind
// the truncated marker. A seq at or below the last one seen restarts the
// neighbourhood, which is what the marker line causes; it resets the baseline
// instead of reporting a backwards jump.
type SeqCheck struct {
	last int64
	seen bool
}

// Next takes the next seq in file order. ok is true when lines went missing
// before it, and gap then holds how many.
func (c *SeqCheck) Next(seq int64) (gap int64, ok bool) {
	if !c.seen {
		c.last = seq
		c.seen = true
		return 0, false
	}
	if seq <= c.last {
		c.last = seq
		return 0, false
	}
	gap = seq - c.last - 1
	c.last = seq
	return gap, gap > 0
}

// ReadFrom reads complete NDJSON lines from path starting at byte off, handing
// each to fn. It returns the offset to resume from: the end of the last
// complete line it read. A trailing fragment without its newline waits for the
// next call unless allowPartial is set, which emits it as a split marked line
// for a final drain of a log whose writer is gone.
//
// A path that does not exist yet is not an error: following a running step
// starts before its first write. The offset comes back unchanged.
func ReadFrom(path string, off int64, allowPartial bool, fn func(Line) error) (int64, error) {
	f, err := os.Open(path) // #nosec G304 - the path comes from Root.Abs inside the state directory
	if err != nil {
		if os.IsNotExist(err) {
			return off, nil
		}
		return off, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return off, fmt.Errorf("seek %s: %w", path, err)
	}
	reader := bufio.NewReader(f)
	resume := off
	for {
		chunk, readErr := reader.ReadBytes('\n')
		switch {
		case len(chunk) > 0 && chunk[len(chunk)-1] == '\n':
			line, err := decodeLine(chunk)
			if err != nil {
				return resume, err
			}
			if err := fn(line); err != nil {
				return resume, err
			}
			resume += int64(len(chunk))
		case allowPartial && len(bytes.TrimSpace(chunk)) > 0:
			line, err := decodeSplit(chunk)
			if err != nil {
				return resume, err
			}
			if err := fn(line); err != nil {
				return resume, err
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return resume, nil
			}
			return resume, fmt.Errorf("read %s: %w", path, readErr)
		}
	}
}

// decodeLine decodes one complete NDJSON record including its newline.
func decodeLine(raw []byte) (Line, error) {
	var line Line
	if err := json.Unmarshal(raw, &line); err != nil {
		return Line{}, fmt.Errorf("a log line is not valid JSON: %w", err)
	}
	return line, nil
}

// decodeSplit wraps an unterminated trailing fragment. A record that parses is
// handed through with the split flag set; bytes that are not JSON at all come
// back as an undecodable pulseq note, so nothing read from a log disappears
// without a trace.
func decodeSplit(raw []byte) (Line, error) {
	var line Line
	if err := json.Unmarshal(bytes.TrimSuffix(raw, []byte("\n")), &line); err != nil {
		return Line{
			Stream: StreamPulseq,
			Event:  "undecodable",
			Split:  true,
			Line:   strings.ToValidUTF8(string(bytes.TrimSpace(raw)), "\ufffd"),
		}, nil
	}
	line.Split = true
	return line, nil
}
