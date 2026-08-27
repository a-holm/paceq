package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/daemon"
	"github.com/a-holm/paceq/internal/store"
)

// notificationsFlags carries every filter of the list command.
type notificationsFlags struct {
	since   string
	state   string
	subject string
	limit   int
}

func newNotificationsCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "notifications",
		Short: "Inspect and manage the notification outbox",
		Long: `Every alert paceq decided to send lives here first, delivered or not.

The outbox is the audit trail for "did we notify about this?": one row per
(event, rule), written in the same transaction as the state change it is
about. Delivered rows are kept until retention retires them; failed ones stay
until you retry or acknowledge them by hand.`,
	}
	var f notificationsFlags

	list := &cobra.Command{
		Use:   "list",
		Short: "List outbox rows, newest first",
		Long: `List notification rows newest first. Filters narrow the answer:
--since takes a duration like 24h, --state one of pending, delivered or
failed, --subject matches the job or sensor name exactly.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runNotificationsList(ctx, env, g, out, f)
		}),
	}
	list.Flags().StringVar(&f.since, "since", "", "only rows created within this long ago, e.g. 24h")
	list.Flags().StringVar(&f.state, "state", "", "pending, delivered or failed")
	list.Flags().StringVar(&f.subject, "subject", "", "match this job or sensor name exactly")
	list.Flags().IntVar(&f.limit, "limit", 0, fmt.Sprintf("how many rows to show (default %d)", store.DefaultListLimit))

	show := &cobra.Command{
		Use:   "show <id>",
		Short: "Show one notification, payload included",
		Args:  exactArgs(1, "one notification id"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runNotificationsShow(ctx, env, g, out, args[0])
		}),
	}

	retry := &cobra.Command{
		Use:   "retry <id>",
		Short: "Give a failed or pending notification another delivery attempt now",
		Long: `Reset a failed notification back into rotation: available_at moves to
now, attempts keep their count - history does not rewrite itself. A delivered
row refuses, because it did go out.`,
		Args: exactArgs(1, "one notification id"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runNotificationsRetry(ctx, env, g, out, args[0])
		}),
	}

	test := &cobra.Command{
		Use:   "test <notifier>",
		Short: "Send a synthetic event through one configured notifier",
		Long: `Build a synthetic failure event exactly like a real one would have been,
and hand it to the named notifier from config.yaml. Nothing is written to the
outbox: what you see is pure delivery plumbing, exit 0 when the notifier
accepted the event.`,
		Args: exactArgs(1, "the name of one notifier"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runNotificationsTest(ctx, env, g, out, args[0])
		}),
	}

	cmd.AddCommand(list, show, retry, test)
	return cmd
}

// openNotificationsRO opens the read-only path both read verbs share.
func openNotificationsRO(ctx context.Context, env Env, g *globals) (*store.Store, func(), error) {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return nil, nil, err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, statErr := os.Stat(dbPath); errors.Is(statErr, fs.ErrNotExist) {
		return nil, nil, notFoundError(
			fmt.Sprintf("there is no paceq state at %s", stateDir),
			stateDir,
			"paceq init  creates a project with its state directory",
			"run the command inside the project directory, or pass --db",
		)
	}
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return nil, nil, err
	}
	return ro, func() { _ = ro.Close() }, nil
}

type clockNowFn func() time.Time

// clockForNotifications is this group's clock source: whatever Env carries,
// or the real one.
func clockForNotifications(env Env) clockNowFn {
	return func() time.Time { return clkOf(env).Now() }
}

func parseSince(raw string, clk clockNowFn) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return time.Time{}, validationError(
			fmt.Sprintf("--since %q is not a positive duration like 24h", raw),
			nil, "examples: --since 1h, --since 24h, --since 7d")
	}
	return clk().Add(-d), nil
}

// buildFilter turns flags into a store filter, refusing bad states up front.
func buildFilter(f notificationsFlags, now clockNowFn) (store.NotificationFilter, error) {
	since, err := parseSince(f.since, now)
	if err != nil {
		return store.NotificationFilter{}, err
	}
	if !store.ValidListState(f.state) {
		return store.NotificationFilter{}, validationError(
			fmt.Sprintf("--state %q is not one of pending, delivered, failed", f.state),
			nil, "pick --state pending, --state delivered or --state failed")
	}
	return store.NotificationFilter{Since: since, State: f.state, Subject: f.subject, Limit: f.limit}, nil
}

func runNotificationsList(ctx context.Context, env Env, g *globals, out *ui, f notificationsFlags) error {
	filter, err := buildFilter(f, clockForNotifications(env))
	if err != nil {
		return err
	}
	ro, done, err := openNotificationsRO(ctx, env, g)
	if err != nil {
		return err
	}
	defer done()
	rows, err := ro.ListNotifications(ctx, filter)
	if err != nil {
		return internalError("could not list the notifications", err)
	}
	switch out.mode {
	case modeJSON:
		doc := struct {
			Rows []store.NotificationSummary `json:"rows"`
			Now  string                      `json:"now"`
		}{Rows: rowsOrEmpty(rows), Now: clockForNotifications(env)().UTC().Format(time.RFC3339)}
		return out.json(doc)
	default:
		renderNotificationsText(out, rows, false)
		return nil
	}
}

func rowsOrEmpty(rows []store.NotificationSummary) []store.NotificationSummary {
	if rows == nil {
		return []store.NotificationSummary{}
	}
	return rows
}

func runNotificationsShow(ctx context.Context, env Env, g *globals, out *ui, idArg string) error {
	id, ok := notificationID(idArg)
	if !ok {
		return usageError(fmt.Sprintf("%q is not a notification id", idArg),
			"paceq notifications show <id>  ids come from paceq notifications list")
	}
	ro, done, err := openNotificationsRO(ctx, env, g)
	if err != nil {
		return err
	}
	defer done()
	row, err := ro.GetNotification(ctx, id)
	if err != nil {
		return notificationLookupError(err, idArg)
	}
	switch out.mode {
	case modeJSON:
		return out.json(row)
	default:
		out.print("notification %d  [%s]", row.ID, row.State)
		out.print("topic    %s", row.Topic)
		out.print("subject  %s", row.Subject)
		out.print("target   %s", row.Target)
		out.print("created  %s", row.CreatedAt.Format(time.RFC3339))
		if row.Delivered != nil {
			out.print("sent     %s", row.Delivered.Format(time.RFC3339))
		}
		if row.Failed != nil {
			out.print("failed   %s", row.Failed.Format(time.RFC3339))
		}
		out.print("attempts %d", row.Attempts)
		if row.LastError != "" {
			out.print("error    %s", strings.ReplaceAll(strings.TrimRight(row.LastError, "\n"), "\n", "\n          "))
		}
		out.print("payload:")
		for _, line := range strings.Split(row.Payload, "\n") {
			out.print("  %s", line)
		}
	}
	return nil
}

func runNotificationsRetry(ctx context.Context, env Env, g *globals, out *ui, idArg string) error {
	id, ok := notificationID(idArg)
	if !ok {
		return nil
	}
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	// Dual mode, socket first (#29): while the daemon runs, only the daemon
	// writes; without it, flock arbitration makes the direct write safe.
	socketPath := daemonSocket(stateDir)
	if socketPath != "" {
		serr := sockPost(ctx, socketPath, "/v1/notifications/"+strconv.FormatInt(id, 10)+"/retry")
		if serr == nil {
			out.note(1, "notification %d handed back to the daemon for delivery", id)
			return nil
		}
		out.note(1, "daemon unreachable (%v); writing directly to the database", serr)
	}
	st, err := store.OpenState(ctx, stateDir, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	next, err := st.RetryOutbox(ctx, id)
	if err != nil {
		return notificationRetryError(err, idArg)
	}
	switch out.mode {
	case modeJSON:
		return out.json(struct {
			ID    int64  `json:"id"`
			State string `json:"previous_state"`
		}{ID: id, State: next})
	default:
		out.print("notification %d unlocked from %s and due again now", id, next)
		out.note(1, "attempts were kept on purpose; history does not reset")
	}
	return nil
}

func runNotificationsTest(ctx context.Context, env Env, g *globals, out *ui, target string) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	cfgDir := configDirFromEnv(env)
	cfg, err := daemon.LoadNotificationConfig(stateDir, cfgDir)
	if err != nil {
		return validationError(fmt.Sprintf("config.yaml could not be trusted: %v", err), nil,
			"fix the file before trusting any delivery")
	}
	if cfg == nil || !cfgHasTarget(cfg, target) {
		return notFoundError(
			fmt.Sprintf("no notifier named %s is configured", target),
			strings.Join(configuredNames(cfg), ", "),
			"define it under notifiers: in config.yaml",
			"see docs/how-to/notification-recipes.md",
		)
	}
	svc := daemon.NewNotifications(nil, clockForEnv(env), nil, cfg, env.Stderr)
	payload, sendErr := svc.SendTest(ctx, target)

	switch out.mode {
	case modeJSON:
		type testDoc struct {
			OK       bool   `json:"ok"`
			Notifier string `json:"notifier"`
			Payload  string `json:"payload"`
			Error    string `json:"error,omitempty"`
		}
		doc := testDoc{OK: sendErr == nil, Notifier: target, Payload: payload}
		if sendErr != nil {
			doc.Error = sendErr.Error()
		}
		return out.json(doc)
	default:
		out.note(1, "event sent as JSON on stdin plus PULSEQ_* variables; nothing touched the outbox")
		if sendErr != nil {
			return validationError(fmt.Sprintf("notifier %s refused the event: %v", target, sendErr),
				nil, "an event here never wrote an outbox row; fix the notifier script and try again")
		}
		out.print("notifier %s accepted the event", target)
	}
	return nil
}

func cfgHasTarget(cfg *daemon.NotificationConfig, name string) bool {
	if _, ok := cfg.Notifiers[name]; ok {
		return true
	}
	for _, n := range cfg.Stderr {
		if n == name {
			return true
		}
	}
	return false
}

func configuredNames(cfg *daemon.NotificationConfig) []string {
	names := make([]string, 0, len(cfg.Notifiers)+len(cfg.Stderr))
	for n := range cfg.Notifiers {
		names = append(names, n)
	}
	names = append(names, cfg.Stderr...)
	sortStringsSmall(names)
	return names
}

func sortStringsSmall(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// --- small shared helpers ----------------------------------------------------

// configDirFromEnv resolves the system configuration directory the daemon
// also consults: PACEQ_CONFIG_DIR first, then /etc/paceq.
func configDirFromEnv(env Env) string {
	if dir := env.Getenv("PACEQ_CONFIG_DIR"); dir != "" {
		return dir
	}
	return daemon.DefaultNotifierConfigDir
}

// notificationID parses a row id argument.
func notificationID(arg string) (int64, bool) {
	id, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func notificationLookupError(err error, arg string) error {
	if errors.Is(err, store.ErrNotificationNotFound) {
		return notFoundError(
			fmt.Sprintf("no notification carries id %s", arg),
			arg,
			"paceq notifications list --limit 50  shows what exists",
		)
	}
	return internalError("could not read the notification", err)
}

func notificationRetryError(err error, arg string) error {
	switch {
	case errors.Is(err, store.ErrNotificationNotFound):
		return notFoundError(
			fmt.Sprintf("no notification carries id %s", arg),
			arg,
			"paceq notifications list --state failed  lists the rows that need help",
		)
	case errors.Is(err, store.ErrNotificationDelivered):
		return validationError(
			fmt.Sprintf("notification %s was already delivered; history does not resend itself", arg),
			nil,
			"if it must go out again, probe delivery first with: paceq notifications test <notifier>",
		)
	default:
		return internalError("could not retry the notification", err)
	}
}

// notificationStamp is the compact wall-clock text a row shows: RFC3339 in
// UTC, truncated of its seconds-precision suffix noise by json mode anyway.
func notificationStamp(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02T15:04")
}

func truncateCell(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func oneLine(s string) string {
	s = strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", " | ")
	return s
}

// renderNotificationsText draws the audit table. Widths are measured from the
// SAME cell-rendering code the rows use, over the visible rows only (the
// M5-03 column lesson), so a wide subject stretches its own column honestly.
type notificationCell struct{ s string }

func (n notificationCell) width() int { return len(n.s) }

func notificationRowCells(row store.NotificationSummary) []string {
	created := row.CreatedAt
	when := notificationStamp(&created)
	switch {
	case row.Delivered != nil:
		when = notificationStamp(row.Delivered)
	case row.Failed != nil:
		when = notificationStamp(row.Failed)
	}
	return []string{
		strconv.FormatInt(row.ID, 10),
		row.State,
		truncateCell(oneLine(row.Topic), 18),
		truncateCell(row.Subject, 24),
		truncateCell(row.Target, 16),
		when,
		strconv.Itoa(row.Attempts),
	}
}

var notificationsHeader = []string{"ID", "STATE", "TOPIC", "SUBJECT", "TARGET", "SENT_OR_CREATED", "ATT"}

// renderNotificationsText prints either the table or the empty answer.
// verbose adds last_error lines per failed row.
func renderNotificationsText(out *ui, rows []store.NotificationSummary, verbose bool) {
	if len(rows) == 0 {
		out.print("no notifications match")
		out.note(1, "delivered rows stay until retention retires them; failed rows stay for ever")
		return
	}
	widths := make([]int, len(notificationsHeader))
	for i, h := range notificationsHeader {
		widths[i] = len(h)
	}
	for _, r := range rows {
		cells := notificationRowCells(r)
		for i, c := range cells {
			if len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}
	pad := func(s string, w int) string { return s + strings.Repeat(" ", w-len(s)) }
	line := ""
	for i, h := range notificationsHeader {
		line += pad(h, widths[i]) + " "
	}
	out.print(strings.TrimRight(line, " "))
	for _, r := range rows {
		cells := notificationRowCells(r)
		line := ""
		for i := range cells {
			line += pad(cells[i], widths[i]) + " "
		}
		out.print(strings.TrimRight(line, " "))
		if r.LastError != "" && (verbose || r.State == "failed") {
			out.print("    err: %s", oneLine(r.LastError))
		}
	}
}
