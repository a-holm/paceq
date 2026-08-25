package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// ExpectedWithinFromIR reads just the freshness SLA out of a canonical
// document, the one field the /metrics collector needs from every job version
// at scrape time (#40). It exists beside FromIR because a scrape must not pay
// for decoding steps and sensors it never looks at: the whole document is
// parsed into the generic shape, one top-level key is read, and nothing about
// step or sensor grammar is validated here - those bytes were already frozen
// through FromIR when the version was applied.
//
// ok is false when the document declares no expectation, which is the "no
// series" case, never an error: absence is a meaningful answer.
func ExpectedWithinFromIR(data []byte) (time.Duration, bool, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return 0, false, fmt.Errorf("parse the canonical document: %w", err)
	}
	raw, ok := root["expected_within_ms"]
	if !ok {
		return 0, false, nil
	}
	ms, err := milliseconds(raw, "expected_within_ms")
	if err != nil {
		return 0, false, err
	}
	if ms <= 0 {
		return 0, false, fmt.Errorf("expected_within_ms is %d, want a positive number of milliseconds", ms)
	}
	return time.Duration(ms) * time.Millisecond, true, nil
}
