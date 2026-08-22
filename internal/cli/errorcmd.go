package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// explanation is one entry of the error catalogue: the long form of a message
// that had to be short when it happened.
type explanation struct {
	Code        string   `json:"code"`
	Title       string   `json:"title"`
	Explanation string   `json:"explanation"`
	Next        []string `json:"next,omitempty"`
}

// catalogue holds the codes paceq can raise today, in the series 03 section 8.1
// sets out: PQ1xxx parsing and schema, PQ2xxx semantics, PQ5xxx storage and
// leases, W1xxx warnings.
//
// Every code a command can print has an entry, because every message ends in
// "paceq error <code> for the full explanation". A test walks spec.Codes() and
// fails on one that is missing.
var catalogue = map[string]explanation{
	spec.CodeSyntax: {
		Code:  spec.CodeSyntax,
		Title: "the file is not valid YAML",
		Explanation: "The YAML reader could not turn the file into a document at all, so none of " +
			"the rules about jobs were reached. Three mistakes account for most of these: a tab " +
			"used for indentation, which YAML forbids outright; a missing space after a colon; " +
			"and a list item indented less than the key it belongs to.",
		Next: []string{
			"the message carries the line and column the reader gave up at",
			"expand tabs to spaces: YAML has no valid use for a tab in indentation",
			"a file saved as anything but UTF-8 is refused with this code too",
		},
	},
	spec.CodeFileTooLarge: {
		Code:  spec.CodeFileTooLarge,
		Title: "the job file is too large",
		Explanation: "A job file is refused above 1 MiB, and refused before it is read rather " +
			"than after. The limit is not about disk: a definition that size carries embedded " +
			"data, and paceq reads a job definition as untrusted input from a parser that has to " +
			"stay bounded.",
		Next: []string{
			"split the job into several files, one job per file",
			"move embedded data into a file the job reads at run time",
		},
	},
	spec.CodeBadName: {
		Code:  spec.CodeBadName,
		Title: "the name is missing or is not a name paceq accepts",
		Explanation: "A job, step, schedule and sensor name is lower case letters, digits, " +
			"underscores and dashes, starts with a letter or a digit, and is at most 64 " +
			"characters. The rule exists because a name is typed on a command line, used as a " +
			"directory name and put in a URL, and a name that needs quoting in any of those is " +
			"a name that gets typed wrong at 03:14.",
		Next: []string{
			"lower case it and replace spaces and dots with dashes: nightly-report",
			"the message carries the corrected form, ready to paste",
		},
	},
	spec.CodeMissingField: {
		Code:  spec.CodeMissingField,
		Title: "a required field is missing or empty",
		Explanation: "A job needs a name and at least one step. A step needs a name and a run. " +
			"A schedule needs a name and a cron expression. Nothing here has a default that " +
			"could stand in, so the field is required rather than filled in silently.",
		Next: []string{
			"the message names the field and the block it is missing from",
			"paceq init  writes an example job that has every required field",
		},
	},
	spec.CodeBadValue: {
		Code:  spec.CodeBadValue,
		Title: "a field has the wrong type or a value outside what it accepts",
		Explanation: "Every field in a job has one type. A list written where a block belongs, a " +
			"block where text belongs, or a word where true or false belongs is refused rather " +
			"than coerced. It also covers the enumerated fields, backoff and jitter, and the " +
			"names an environment variable may have.",
		Next: []string{
			"the message says what the field is and what was written instead",
			"yes, no, on and off are text in YAML, not booleans: write true or false",
		},
	},
	spec.CodeTooDeep: {
		Code:  spec.CodeTooDeep,
		Title: "the file nests deeper than paceq reads",
		Explanation: "A job definition nests six levels at most. The limit of 32 is a parser " +
			"limit, not a schema one: it is checked on the syntax tree before anything is " +
			"expanded, and it is one of the four limits that make a hostile YAML file safe to " +
			"read.",
		Next: []string{
			"a structure this deep is data, not a definition: put it in a file the job reads",
		},
	},
	spec.CodeTooManyAliases: {
		Code:  spec.CodeTooManyAliases,
		Title: "the file uses more aliases than paceq resolves",
		Explanation: "An anchor written &name and used as *name is fine, and useful for sharing " +
			"one env block between steps. The limit of 100 is what stops the billion laughs " +
			"file, where a handful of nested aliases expand to more data than a machine has. " +
			"The same code covers an alias with no anchor, an anchor defined twice, and an " +
			"anchor that contains itself.",
		Next: []string{
			"write the shared values out, or move them into fewer anchors",
			"an anchor must appear earlier in the file than the alias that reads it",
		},
	},
	spec.CodeTooLarge: {
		Code:  spec.CodeTooLarge,
		Title: "the file has more in it than paceq reads",
		Explanation: "The file is under the size limit but has more nodes than the parser " +
			"accepts, more flow markers than it will parse, or expands through aliases to more " +
			"than it will build. All three are bounds on work rather than on bytes.",
		Next: []string{
			"split the job into several files",
			"write long lists as block sequences, one entry per line, rather than in brackets",
		},
	},
	spec.CodeTooManySteps: {
		Code:  spec.CodeTooManySteps,
		Title: "the job has more steps than paceq runs",
		Explanation: "A job runs at most 200 steps. It is the same ceiling the dependency graph " +
			"uses, and a graph that size is not something a person reads during an incident.",
		Next: []string{
			"split it into several jobs and let one trigger the next",
		},
	},
	spec.CodeRunNotAList: {
		Code:  spec.CodeRunNotAList,
		Title: "run is written as a string",
		Explanation: "run is argv: a list where the first element is the program and each " +
			"element after it is one argument. There is no string form, and there will not be " +
			"one. paceq starts the process itself, so nothing splits a command on spaces, and a " +
			"file name that carries a space, a quote or a semicolon stays one argument instead " +
			"of turning into several or into a second command.",
		Next: []string{
			`run: ["/bin/sh", "-c", "echo done"]`,
			"or one element per line under run:, each behind a dash",
			"a step that genuinely needs a shell sets shell: true and takes warning W1001",
		},
	},
	spec.CodeRunEmpty: {
		Code:  spec.CodeRunEmpty,
		Title: "run is an empty list",
		Explanation: "The first element of run is the program to start, so an empty list names " +
			"nothing to run.",
		Next: []string{
			`run: ["/bin/echo", "hello"]`,
		},
	},
	spec.CodeRunNotAbsolute: {
		Code:  spec.CodeRunNotAbsolute,
		Title: "the command is not an absolute path",
		Explanation: "paceq starts the process itself. There is no shell to search PATH and no " +
			"working directory for a relative name to resolve against, so the first element of " +
			"run says exactly which program to start. This is deliberate: a job that finds its " +
			"program through PATH runs a different program depending on who started it.",
		Next: []string{
			"command -v <program>  prints the path this machine would use",
			"put the absolute path in run[0]",
			"with shell: true the shell does the lookup, and the step takes warning W1001",
		},
	},
	spec.CodeBadDuration: {
		Code:  spec.CodeBadDuration,
		Title: "the duration is not one paceq reads",
		Explanation: "A duration is a number and a unit, and the units are ms, s, m and h. They " +
			"combine: 1h30m. A bare number is refused rather than guessed at. Days and weeks are " +
			"missing on purpose, because a day is not a fixed length of time in a zone that " +
			"observes daylight saving. Values finer than a millisecond are refused because a " +
			"millisecond is what the job definition stores.",
		Next: []string{
			"add the unit: timeout: 45m",
			"the same rule holds for timeout, retry.initial, retry.max_delay and sensor interval",
		},
	},
	spec.CodeTimeoutTooLong: {
		Code:  spec.CodeTimeoutTooLong,
		Title: "the timeout is over the system ceiling",
		Explanation: "A timeout is mandatory: a job without one gets 1 hour, and no job may set " +
			"more than 24 hours. A timeout is what turns a hung process into a failed run " +
			"instead of a job that never finishes and a schedule that never fires again.",
		Next: []string{
			"split the work into steps with their own timeouts",
			"if it genuinely runs for days it is a service, and a service is supervised, not scheduled",
		},
	},
	spec.CodeBadConcurrency: {
		Code:  spec.CodeBadConcurrency,
		Title: "max_concurrent is not a number of runs",
		Explanation: "max_concurrent is how many runs of one job may be in flight at once, and " +
			"the lowest it goes is 1. The default is 1, which is what a flock wrapper around a " +
			"cron line does: a second run waits rather than overlapping. There is no value that " +
			"means never run.",
		Next: []string{
			"max_concurrent: 1",
			"to stop a job running, remove its schedule rather than setting a concurrency of zero",
		},
	},
	spec.CodeUnknownField: {
		Code:  spec.CodeUnknownField,
		Title: "the field is not one paceq knows",
		Explanation: "An unknown field is an error rather than a warning. A misspelled field " +
			"that is quietly ignored is worse than a refusal: the job runs, it runs without the " +
			"timeout or the retry that was meant to be there, and nothing says so. The message " +
			"names the nearest field paceq does have.",
		Next: []string{
			"the message lists every field the block accepts",
			"paceq validate  after the rename, to see whether the value fits the field",
		},
	},
	spec.CodeDuplicateKey: {
		Code:  spec.CodeDuplicateKey,
		Title: "the same key is set twice in one block",
		Explanation: "A YAML mapping holds each key once. Most readers keep the last one and say " +
			"nothing, which is how a job ends up running with a value nobody can find by reading " +
			"the file. paceq refuses instead of picking.",
		Next: []string{
			"delete the one you did not mean: the message points at the second",
		},
	},
	spec.CodeMergeKey: {
		Code:  spec.CodeMergeKey,
		Title: "a merge key is used",
		Explanation: "<< pulls the contents of another mapping into this one. What wins when " +
			"both define the same key is a rule that is not visible anywhere in the file, so " +
			"paceq does not resolve merge keys.",
		Next: []string{
			"write the fields out",
			"an anchor on a single value still works: timeout: &short 30s, then timeout: *short",
		},
	},
	spec.CodeTag: {
		Code:  spec.CodeTag,
		Title: "an explicit YAML tag is used",
		Explanation: "A tag such as !!str tells a YAML reader to read a value as a given type. " +
			"Every field in a job already has one type, and paceq reads the value as that type, " +
			"so a tag can only disagree with the schema.",
		Next: []string{
			"remove the tag and quote the value instead, when it needs to stay text",
		},
	},
	spec.CodeTooManyProblems: {
		Code:  spec.CodeTooManyProblems,
		Title: "paceq stopped reading the file",
		Explanation: "One file produced more problems than a report carries, so paceq stopped " +
			"and reported the first hundred. A file this far off is usually one mistake near the " +
			"top, such as a list written where a block belongs, and the rest is what that one " +
			"mistake looks like further down.",
		Next: []string{
			"fix the first message and validate again",
		},
	},
	spec.CodeDuplicateStep: {
		Code:  spec.CodeDuplicateStep,
		Title: "two steps have the same name",
		Explanation: "A step name is how a failure says which step failed, how needs points at a " +
			"step and how a retry names one. Two steps with one name make all three ambiguous.",
		Next: []string{
			"rename one of them",
		},
	},
	spec.CodeUnknownNeed: {
		Code:  spec.CodeUnknownNeed,
		Title: "needs names a step that does not exist",
		Explanation: "needs lists the steps that have to succeed before this one starts. Every " +
			"name in it is a step name in the same job. The field parses in M1 and is enforced " +
			"in M4-01, where it becomes the dependency graph; until then the steps run in the " +
			"order they are written.",
		Next: []string{
			"the message lists the steps this job does have, and suggests the nearest name",
		},
	},
	spec.CodeUnknownTimezone: {
		Code:  spec.CodeUnknownTimezone,
		Title: "the time zone is not one this machine knows",
		Explanation: "A schedule's zone is an IANA name, region and city, such as Europe/Oslo. " +
			"Three letter abbreviations are ambiguous and are not accepted. A zone that does not " +
			"load here will not load in the scheduler either, so it is refused now rather than " +
			"at three in the morning.",
		Next: []string{
			"timezone: Europe/Oslo",
			"paceq doctor  reports whether the zone database is readable at all",
			"a machine with no zone database needs one installed: apt install tzdata",
		},
	},
	spec.CodeShell: {
		Code:  spec.CodeShell,
		Title: "the step runs its command through a shell",
		Explanation: "shell: true is an explicit opt in, and it carries a warning wherever it is " +
			"used (08 section 3.2). A shell splits, globs and expands what it is given, so an " +
			"argument carrying a space, a quote or a semicolon stops being one argument. With " +
			"shell off, paceq starts the process itself and nothing touches the arguments.",
		Next: []string{
			`run: ["/bin/sh", "-c", "echo done"]  is explicit and needs no shell flag`,
			"keep it on only where the command genuinely needs a shell, and say why in the description",
			"paceq validate --strict  turns this warning into a failure, for CI",
		},
	},
	spec.CodeInheritEnv: {
		Code:  spec.CodeInheritEnv,
		Title: "the job inherits variables from the environment paceq was started in",
		Explanation: "A job starts from an empty environment on purpose (08 section 3.2): what " +
			"it gets is what the file says, so a run from a terminal and a run from the " +
			"scheduler are the same run. Every name in inherit_env is an exception to that, and " +
			"makes the job depend on how paceq itself was started.",
		Next: []string{
			"set the value in the job's own env block, when it is not a secret",
			"use env_file for a secret: a 0600 file read at run time, never stored in the database",
			"paceq validate --strict  turns this warning into a failure, for CI",
		},
	},
	"PQ5001": {
		Code:  "PQ5001",
		Title: "the state is readable by other users",
		Explanation: "paceq refuses to write to a state directory or a database that anyone but " +
			"the owner can read. Job definitions, run history and lease holders are not public, " +
			"and correcting the mode quietly would hide that they have been readable, possibly " +
			"for weeks.",
		Next: []string{
			"chmod 0700 on the state directory, chmod 0600 on the database",
			"paceq doctor  lists every path whose mode is wider than paceq accepts",
			"consider what had access while the mode was open",
		},
	},
	"PQ5002": {
		Code:  "PQ5002",
		Title: "another process holds the state directory",
		Explanation: "paceq takes an exclusive lock on a state directory before it opens the " +
			"database in it, so exactly one process ever writes. This code means the lock was " +
			"already held. The state is healthy; somebody else is using it.",
		Next: []string{
			"paceq doctor  names the process holding it, when its session row is readable",
			"stop the other process, or point this command at another state directory: --db /other/path/" + store.DatabaseFileName,
		},
	},
}

// seriesDiagnostic and seriesReason label which catalogue a --list entry came
// from. They are part of the JSON contract, so they are fixed strings, never
// derived.
const (
	seriesDiagnostic = "diagnostic"
	seriesReason     = "reason"
)

// catalogueEntry is one row of `paceq error --list -o json` and of a single
// reason code lookup: the stable machine contract for the whole catalogue
// (03 section 7.1). Diagnostic entries carry no level, terminal flag or data
// keys; those three fields belong to the reason series alone.
type catalogueEntry struct {
	Code        string   `json:"code"`
	Series      string   `json:"series"`
	Level       string   `json:"level,omitempty"`
	Terminal    *bool    `json:"terminal,omitempty"`
	Title       string   `json:"title"`
	Explanation string   `json:"explanation"`
	Next        []string `json:"next"`
	DataKeys    []string `json:"data_keys,omitempty"`
}

func newErrorCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "error [code | --list]",
		Short: "Explain an error or reason code",
		Long: `Explain a code, such as PQ1040 or STEP_FAILED_SPAWN.

Every paceq message carries its code. This command is the long form: what the
code means, why paceq refuses, and what to do about it. Diagnostic codes
(PQ1xxx, PQ2xxx, PQ5xxx, W1xxx) come from reading job files and opening state;
reason codes (RUN_, STEP_, TICK_, TRIGGER_) explain what the scheduler and the
runner decided.

With --list the command prints the whole catalogue of both series instead:
human readable on a terminal, one JSON array through a pipe.`,
		Args: func(cmd *cobra.Command, args []string) error {
			list, _ := cmd.Flags().GetBool("list")
			if list {
				return exactArgs(0, "no arguments beside --list")(cmd, args)
			}
			return exactArgs(1, "one error code")(cmd, args)
		},
		RunE: runArgsE(env, g, func(_ context.Context, out *ui, args []string) error {
			return explainOrList(out, args)
		}),
	}
	cmd.Flags().Bool("list", false, "print the whole error catalogue")
	return cmd
}

func explainOrList(out *ui, args []string) error {
	if len(args) == 0 {
		return listCatalogue(out)
	}
	return explainCode(out, args[0])
}

func explainCode(out *ui, code string) error {
	name := strings.ToUpper(strings.TrimSpace(code))
	if e, ok := reason.Lookup(reason.Code(name)); ok {
		return explainReason(out, e)
	}
	found, ok := catalogue[name]
	if !ok {
		next := []string{
			"check the code in the message that sent you here: paceq prints it in front of every refusal",
			"paceq version  says which build this is",
		}
		if s := diag.Suggest(name, knownCodes()); s != "" {
			next = append([]string{"did you mean " + s + "? paceq error " + s + "  explains it"}, next...)
		}
		return notFoundError(
			"no error code "+name+" in this build",
			"the catalogue of "+strings.Join(knownCodes(), ", "),
			next...,
		)
	}

	if out.mode == modeJSON {
		return out.json(found)
	}
	out.print("%s  %s", found.Code, found.Title)
	out.print("")
	out.print("%s", found.Explanation)
	if len(found.Next) > 0 {
		out.print("")
		for _, step := range found.Next {
			out.print("  %s %s", out.symbols.arrow, step)
		}
	}
	return nil
}

// explainReason renders one entry of the runtime catalogue: the short line
// with its level tag, the long explanation, the remediation steps, and the
// reason_data keys the code promises (06 section 2.1).
func explainReason(out *ui, e reason.Entry) error {
	if out.mode == modeJSON {
		return out.json(reasonCatalogueEntry(e))
	}
	out.print("%s  %s  %s", e.Code, e.Short, reasonTag(e))
	out.print("")
	out.print("%s", e.Explanation)
	out.print("")
	out.print("What to do next:")
	for _, r := range e.Remedy {
		out.print("  %s %s", out.symbols.arrow, r)
	}
	if len(e.DataKeys) > 0 {
		out.print("")
		out.print("Promised reason_data keys: %s", strings.Join(e.DataKeys, ", "))
	}
	return nil
}

// reasonTag is the bracketed annotation after the short text: the level, plus
// whether the code ends its object. Terminal is exactly the set of writes the
// schema refuses without a code, so the tag doubles as the rule made visible.
func reasonTag(e reason.Entry) string {
	if e.Terminal {
		return fmt.Sprintf("[%s, ends the object]", e.Level)
	}
	return fmt.Sprintf("[%s]", e.Level)
}

// listCatalogue prints every entry of both catalogues, sorted by code in JSON
// and grouped by series and level as text.
func listCatalogue(out *ui) error {
	if out.mode == modeJSON {
		return out.json(listEntries())
	}
	out.print("Diagnostic codes:")
	for _, e := range listEntries() {
		if e.Series != seriesDiagnostic {
			continue
		}
		out.print("  %s  %s", e.Code, e.Title)
	}
	out.print("")
	out.print("Reason codes:")
	for _, level := range []reason.Level{reason.LevelTick, reason.LevelTrigger, reason.LevelRun, reason.LevelStep, reason.LevelLease} {
		out.print("  %s level", level)
		for _, e := range listEntries() {
			if e.Series != seriesReason || e.Level != string(level) {
				continue
			}
			out.print("    %s  %s", e.Code, e.Title)
		}
	}
	return nil
}

// listEntries is the whole catalogue as one stable array: diagnostics first by
// code, then reason codes by code, then everything sorted together, so the
// order is a function of the content alone.
func listEntries() []catalogueEntry {
	entries := make([]catalogueEntry, 0, len(catalogue)+len(reason.Codes()))
	for code, e := range catalogue {
		entries = append(entries, catalogueEntry{
			Code:        code,
			Series:      seriesDiagnostic,
			Title:       e.Title,
			Explanation: e.Explanation,
			Next:        e.Next,
		})
	}
	for _, e := range reason.All() {
		entries = append(entries, reasonCatalogueEntry(e))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Code < entries[j].Code })
	return entries
}

func reasonCatalogueEntry(e reason.Entry) catalogueEntry {
	terminal := e.Terminal
	return catalogueEntry{
		Code:        string(e.Code),
		Series:      seriesReason,
		Level:       string(e.Level),
		Terminal:    &terminal,
		Title:       e.Short,
		Explanation: e.Explanation,
		Next:        e.Remedy,
		DataKeys:    e.DataKeys,
	}
}

// knownCodes is the union of both catalogues in a stable order, so a refusal
// names the same set every time it is printed and a near miss can be matched
// against everything this build can explain.
func knownCodes() []string {
	codes := make([]string, 0, len(catalogue)+len(reason.Codes()))
	for code := range catalogue {
		codes = append(codes, code)
	}
	codes = append(codes, reason.Codes()...)
	sort.Strings(codes)
	return codes
}
