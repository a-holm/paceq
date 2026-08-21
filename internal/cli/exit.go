package cli

import (
	"fmt"
	"strings"
)

// Exit codes are a public contract from the first release (03 section 7.2).
// They never change without a major version: scripts are written against them,
// and the difference between 1 and 5 is what makes moving a crontab to paceq
// safe. 1 means paceq failed. 5 means the job did.
const (
	ExitOK          = 0
	ExitInternal    = 1
	ExitUsage       = 2
	ExitNotFound    = 3
	ExitValidation  = 4
	ExitRunFailed   = 5
	ExitBusy        = 6
	ExitTimeout     = 7
	ExitInterrupted = 8
)

// exitCode is one row of the table. The table is written once and used both by
// the help text and by the test that keeps the constants and the documentation
// in step, because a contract only the source knows is not a contract.
type exitCode struct {
	Code    int
	Meaning string
}

var exitCodes = []exitCode{
	{ExitOK, "success"},
	{ExitInternal, "paceq itself failed"},
	{ExitUsage, "wrong arguments or flags"},
	{ExitNotFound, "the resource asked for does not exist"},
	{ExitValidation, "validation failed"},
	{ExitRunFailed, "the job failed, and paceq was waiting for it"},
	{ExitBusy, "busy: another process holds the state, or a limit was reached"},
	{ExitTimeout, "timed out"},
	{ExitInterrupted, "interrupted"},
}

// exitCodeHelp is the table as it appears under paceq --help.
func exitCodeHelp() string {
	var b strings.Builder
	b.WriteString("Exit codes:\n")
	for _, code := range exitCodes {
		fmt.Fprintf(&b, "  %d  %s\n", code.Code, code.Meaning)
	}
	return b.String()
}
