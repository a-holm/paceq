package cli

import (
	"context"
	"sort"
	"strings"

	"github.com/spf13/cobra"

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

// catalogue holds the codes paceq can raise today. It grows with the code
// series in 03 section 8.1 as the commands that raise them land.
var catalogue = map[string]explanation{
	"PQ1001": {
		Code:  "PQ1001",
		Title: "another process holds the state directory",
		Explanation: "paceq takes an exclusive lock on a state directory before it opens the " +
			"database in it, so exactly one process ever writes. This code means the lock was " +
			"already held. The state is healthy; somebody else is using it.",
		Next: []string{
			"paceq doctor  names the process holding it, when its session row is readable",
			"stop the other process, or point this command at another state directory: --db /other/path/" + store.DatabaseFileName,
		},
	},
	"PQ1002": {
		Code:  "PQ1002",
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
}

func newErrorCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "error <code>",
		Short: "Explain an error code",
		Long: `Explain an error code, such as PQ1001.

Every paceq message carries its code. This command is the long form: what the
code means, why paceq refuses, and what to do about it.`,
		Args: exactArgs(1, "one error code"),
		RunE: runArgsE(env, g, func(_ context.Context, out *ui, args []string) error {
			return explainCode(out, args[0])
		}),
	}
}

func explainCode(out *ui, code string) error {
	name := strings.ToUpper(strings.TrimSpace(code))
	found, ok := catalogue[name]
	if !ok {
		return notFoundError(
			"no error code "+name+" in this build",
			"the catalogue of "+strings.Join(knownCodes(), ", "),
			"check the code in the message that sent you here: paceq prints it in front of every refusal",
			"paceq version  says which build this is",
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

// knownCodes is the catalogue in a stable order, so a refusal names the same
// set every time it is printed.
func knownCodes() []string {
	codes := make([]string, 0, len(catalogue))
	for code := range catalogue {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}
