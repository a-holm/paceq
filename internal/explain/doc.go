// Package explain is the only read path into run history and the reasoning
// behind each decision. It is a presentation layer over rows that already
// exist: it never re-derives what should have happened, never writes to the
// database, and never imports a package that decides anything (no scheduler,
// no sensor evaluator, no engine). External surfaces - the command line
// today, the web UI later - read through here and nowhere else.
//
// The contract is Report: one reverse chronological decision list per
// subject, versioned by SchemaVersion, self-contained so a consumer never
// needs a follow-up query. Text rendering turns the same structure into
// prose; nothing computes a second truth.
package explain
