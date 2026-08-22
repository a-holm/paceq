// Package cli is the command line: the command tree, the global flags, the
// output modes and the exit codes every command answers with.
//
// Three rules run through it. Output follows the stream, so a terminal gets
// text and a pipe gets JSON, PACEQ_OUTPUT pins either side without a flag,
// and -o overrides everything. Data goes to stdout and
// nothing else does, so a pipe stays parseable while progress notes are on.
// Every failure carries what went wrong, where, and what to do next; a message
// without the third part is a bug, and a test refuses one.
package cli
