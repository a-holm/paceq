package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/a-holm/paceq/internal/store"
)

// Error is a failure on its way to the user. It carries the three parts every
// paceq message has (03 section 8.1, 09 section 6.4): what went wrong, where,
// and what to do now. A message without the third part is a bug, so the type
// keeps the next steps beside the text rather than inside it.
//
// The exit code travels with the message for the same reason: the code and the
// explanation are one decision, and splitting them is how a refusal ends up
// reported as an internal failure.
type Error struct {
	code  int
	what  string
	where string
	next  []string
	err   error
	// silent marks a pure exit-code passthrough: nothing to say, a number
	// to return. The exec shim (#39) uses it for a child's own exit code,
	// which is a fact the daemon reads, not an error to print.
	silent bool
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString(e.what)
	if e.where != "" {
		fmt.Fprintf(&b, "\n  at %s", e.where)
	}
	for _, step := range e.next {
		fmt.Fprintf(&b, "\n  %s", step)
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.err }

// usageError is a command line that cannot be carried out as written.
func usageError(what string, next ...string) *Error {
	return &Error{code: ExitUsage, what: what, next: next}
}

// notFoundError is a resource the user named that does not exist. where says
// what was searched, because "not found" without a scope is not actionable.
func notFoundError(what, where string, next ...string) *Error {
	return &Error{code: ExitNotFound, what: what, where: where, next: next}
}

// isNotFound reports whether an error is this package's "nothing there"
// verdict. A caller that has to tell an absent state directory from an
// unreadable one asks here rather than matching on the message.
func isNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.code == ExitNotFound
}

// validationError is a state paceq refuses to work with, such as a database
// other users can read. Nothing is broken; something has to be corrected.
func validationError(what string, err error, next ...string) *Error {
	return &Error{code: ExitValidation, what: what, err: err, next: next}
}

// busyError is somebody else holding what this command needs. The store's
// refusal already names the holder and what to do, so it is used as written.
func busyError(err error) *Error {
	return &Error{
		code: ExitBusy,
		what: err.Error(),
		err:  err,
		next: []string{"paceq doctor  reports who holds the state directory"},
	}
}

func timeoutError(what string, err error) *Error {
	return &Error{
		code: ExitTimeout,
		what: what,
		err:  err,
		next: []string{
			"run the command again: a timeout is not a failed write, and paceq leaves nothing half done",
			"paceq doctor  reports whether another process is holding the state",
		},
	}
}

// interruptedError is the exit a script must not confuse with a failure: the
// operator stopped the command.
func interruptedError(err error) *Error {
	return &Error{
		code: ExitInterrupted,
		what: "interrupted before the command finished",
		err:  err,
		next: []string{
			"nothing was left half written: every change paceq makes is one transaction",
			"run the command again when you are ready",
		},
	}
}

// internalError is the failure paceq has no better name for. It is deliberately
// loud about being a bug, because the alternative is a user hunting for a
// mistake they did not make.
func internalError(what string, err error) *Error {
	return &Error{
		code: ExitInternal,
		what: fmt.Sprintf("%s: %v", what, err),
		err:  err,
		next: []string{
			"paceq doctor  checks the installation",
			"if the installation is healthy, this is a bug: report it with the output of paceq version",
		},
	}
}

// classify turns any error into one the user can act on and a script can branch
// on. Errors paceq already classified pass through untouched.
//
// The context is read before the error, because a store call cut short by a
// cancelled context returns whatever it was doing at the time, and reporting
// that as an internal failure would tell the operator their installation is
// broken when they pressed Ctrl-C.
func classify(ctx context.Context, err error) *Error {
	var already *Error
	if errors.As(err, &already) {
		return already
	}

	switch {
	case errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled:
		return interruptedError(err)
	case errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded:
		return timeoutError(fmt.Sprintf("the command ran out of time: %v", err), err)
	}

	var locked *store.LockedError
	if errors.As(err, &locked) {
		return busyError(err)
	}

	var perm *store.PermissionError
	if errors.As(err, &perm) {
		// The store's refusal already carries the chmod that fixes it, so the
		// only step worth adding is the one that finds the others.
		return validationError(err.Error(), err,
			"paceq doctor  reports every path whose mode is wider than paceq accepts")
	}

	return internalError("the command failed", err)
}

// renderError writes a failure to stderr and returns the exit code that goes
// with it. Errors never touch stdout: a script that pipes stdout has to be able
// to trust that what arrives there is data.
func renderError(env Env, err *Error) int {
	if err.silent {
		return err.code
	}
	sym := symbols(unicodeOutput(env))
	fmt.Fprintf(env.Stderr, "paceq: %s\n", err.what)
	if err.where != "" {
		fmt.Fprintf(env.Stderr, "  at %s\n", err.where)
	}
	for _, step := range err.next {
		fmt.Fprintf(env.Stderr, "  %s %s\n", sym.arrow, step)
	}
	return err.code
}
