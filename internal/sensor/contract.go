// Package sensor holds the evaluator runtime: the part of the daemon that
// finds due sensors, runs each one as an isolated subprocess against the
// frozen public contract, and hands a pure Result on to the commit layer.
// Everything here is read only: no sensor evaluation writes to the database,
// and the commit turns the Result into tick and trigger rows lives in M3-03,
// not in this package.
package sensor

import (
	"encoding/json"

	"github.com/a-holm/paceq/internal/reason"
)

// Outcome is the tick verdict an evaluation produces. Three values cover
// every way a run of the sensor program can end; the reason catalogue fills
// in why, and a tick is never written without a reason code (the M1-05 rule).
type Outcome int

const (
	// Triggered means the sensor elected one or more triggers.
	Triggered Outcome = 0
	// Skipped means the sensor answered within its deadline and reported that
	// nothing was worth a run, either through skip_reason or through silence.
	Skipped Outcome = 1
	// Errored means the sensor failed in any way the contract does not call a
	// normal skip: a crash, a timeout, unreadable output, or a config error.
	Errored Outcome = 2
)

// Input is the inbound contract object, serialised to JSON on the sensor's
// standard input and surfaced as PACEQ_* environment. The field set is frozen
// at v0.1: M3-07 and M5-08 build docs on exactly these names.
type Input struct {
	Sensor      string  `json:"sensor"`
	Job         string  `json:"job"`
	Cursor      *string `json:"cursor"`
	LastTickAt  *int64  `json:"last_tick_at"`
	Now         int64   `json:"now"`
	MaxTriggers int     `json:"max_triggers"`
	DeadlineMS  int64   `json:"deadline_ms"`
	DryRun      bool    `json:"dry_run"`
}

// Trigger is one run the sensor asks for. It carries a stable run key, which
// run_keys later deduplicates, and optional params the job may bind.
type Trigger struct {
	RunKey string          `json:"run_key"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Output is the single JSON object the sensor prints on stdout. Exactly one of
// these is the only accepted stdout form in the MVP (SYNTESE section 4.4).
type Output struct {
	Cursor     *string   `json:"cursor"`
	Triggers   []Trigger `json:"triggers,omitempty"`
	SkipReason *string   `json:"skip_reason"`
}

// Result is the pure verdict the evaluator returns. Nothing in it is a write:
// M3-03 reads it to build the tick, trigger and cursor rows, and a failed
// evaluation carries no cursor value that could advance state by accident.
type Result struct {
	Outcome    Outcome
	ReasonCode reason.Code
	ReasonText string
	ReasonData map[string]any

	// CursorBefore is the cursor the evaluation started from; CursorAfter is
	// the value the sensor reported, or nil when the sensor did not move it.
	// An error leaves CursorAfter nil. What the commit does with a reported
	// cursor is the store's decision, not this package's: only a triggered
	// evaluation commits one, so a failed or skipped tick never advances the
	// cursor (G4).
	CursorBefore *string
	CursorAfter  *string

	Triggers []Trigger

	ExitCode       int
	Signal         string
	DurationMS     int64
	StderrExcerpt  string // at most the configured tail, 4 KiB by default
	StdoutBytes    int64
	OutputOverflow bool
	TimedOut       bool
}

// skip builds a Skipped Result and triggered builds a Triggered Result; both
// are the shape the classification uses so the reason code always comes from
// the catalogue.
func skipped(code reason.Code, text string, cursorAfter *string, before *string) Result {
	return Result{Outcome: Skipped, ReasonCode: code, ReasonText: text, CursorBefore: before, CursorAfter: cursorAfter}
}

func errored(code reason.Code, data map[string]any, exit int, sig string, before *string, excerpt string) Result {
	return Result{Outcome: Errored, ReasonCode: code, ReasonData: data, ExitCode: exit, Signal: sig, CursorBefore: before, StderrExcerpt: excerpt}
}
