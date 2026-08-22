// Package diag is one diagnostic and the way it reaches a person: a code, a
// position in a file, an excerpt with a caret under the offending column, a
// cause and at least one next step.
//
// The rule the package exists to enforce is 03 section 8.1. A message without a
// next step is not a message, it is a dead end, so the next step lives in its
// own field rather than inside the prose, and a test can see whether it is
// there. The code is in its own field for the same reason: `paceq error PQ1040`
// is the long form, and it can only find the entry if the short form carried
// the code.
//
// Nothing here knows about jobs, YAML or the command line. The producer fills
// in a Diagnostic, the consumer renders it or serialises it, and the two never
// meet.
package diag
