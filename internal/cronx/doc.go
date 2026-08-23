// Package cronx turns a schedule definition into concrete fire times.
//
// It owns three things and nothing else: parsing a cron expression or an
// interval into a Schedule, iterating that schedule over a named IANA time
// zone (Next, Prev, Between), and applying an explicit DST policy at every
// daylight saving transition. All returned times are UTC; local wall clock
// text appears only inside Occurrence.LocalWall for display.
//
// # The DST policy
//
// At a spring forward transition some local wall times do not exist (Oslo
// 2026-03-29 has no 02:00). At a fall back transition some local wall times
// happen twice (Oslo 2026-10-25 has two 02:00). Libraries usually pick a
// behaviour silently; this package makes the choice a per schedule Policy:
//
//	spring_forward = skip (default)  the missing slot produces an occurrence
//	                                 marked Skipped with reason dst_nonexistent
//	spring_forward = shift           the slot fires at the instant the missing
//	                                 wall clock maps to after the clocks jump
//	fall_back = first (default)      only the first (earliest UTC) instance of
//	                                 a doubled slot is real; the second is an
//	                                 occurrence marked Skipped with reason
//	                                 dst_duplicate
//	fall_back = both                 both instances are real; their different
//	                                 UTC instants make them distinct ticks
//
// Skipped occurrences are results, not holes: Between returns them in
// chronological position so a consumer can record why nothing ran. Absence of
// a run is not data; a row that says so is.
//
// # Chaining Next across a DST seam
//
// Next is a point query: the first occurrence strictly after the cursor in
// the schedule's local timetable order. That order puts wall clock position
// first and UTC instant second, so around a transition the instants of
// consecutive results do not always rise, and a loop that chains Next by At
// cannot reproduce Between there:
//
//   - Spring forward: a skipped slot normalises to the same UTC instant as a
//     later real slot. Between returns both; a chain keyed on the bare
//     instant can only reach the first of each tied pair. Over Oslo's
//     2026-03-29 day, Between("*/15 * * * *") returns 100 occurrences while
//     the chain yields 96.
//   - Fall back: the second instance of a doubled slot sits at a wall
//     position between first pass slots whose instants are earlier, so its
//     successor carries an instant earlier than the cursor. Over Oslo's
//     2026-10-25 day a naive chain stops at the first doubled slot.
//
// This is inherent to a time.Time cursor at a seam: the cursor cannot carry
// which side of the transition it came from. Consumers that enumerate MUST
// use Between, the catch-up primitive; Next exists for single lookups such as
// "next fire after now". The shapes above are pinned by
// TestNextChainContractAtBothSeams.
//
// Interval schedules ("@every 90m") are pure UTC arithmetic anchored to the
// Unix epoch, so DST never affects them and two daemons always agree.
//
// The package takes no clock dependency at all: every time value comes in as a
// parameter, which makes every function here pure and deterministic.
package cronx
