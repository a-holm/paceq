# Shadow mode: adopting paceq without letting go of cron

Shadow mode is the migration's trust mechanism (#32 / M6-02). While a plain
`paceq serve` would plan schedules *and* execute runs, shadow mode plans and
records everything - every due evaluation, every skip reason, every catch-up
decision - and executes nothing at all. Not an echo, not an `ls`. The only
difference between the two modes is the last step of materialisation: where
normal mode queues a run, shadow mode writes a tick row that says what the
run **would** have been.

Run it next to your existing crontab for a week or two, then read
`paceq shadow report`. That report answers "what would Pulseq have done
differently than cron?" - and that answer is the whole product argument.

## Start shadowing

    paceq serve --shadow

Optional flags:

- `--shadow` shadows every schedule in the state directory. A single
  schedule can also carry `shadow: true` (or `shadow: true` on its job) in
  YAML; the global switch dominates.
- `--observe journald` reads observed cron starts from the journal
  (`journalctl`), so the report can compare real cron activity against
  Pulseq's decisions.
- `--observe file=/var/log/syslog` does the same from a syslog-format file;
  Debian, Ubuntu and RHEL cron layouts are all parsed.

Observed starts are matched to imported jobs by command, best effort, and
every observation names its source. Nothing else changes: shadow ticks land
in the same database, retention, explain and fsck behave as always.

## Read the report

    paceq shadow report --since 7d        # or --job name, --json

Per job the diff shows:

- **match** - would-run decisions paired with observed cron starts;
- **offset** - one steady shift between both sides, the signature of a
  schedule without a working `timezone:` line; the suggestion carries a
  concrete fix;
- **pulseq_only** - fires Pulseq planned that cron never started (`%`
  escapes, PATH failures, host downtime);
- **cron_only** - observed starts Pulseq would not have made. When overlap
  protection is involved this is the headline finding: with
  `max_concurrent: 1` (the default), every fire-time cron started on top of
  a still-running previous invocation shows up here;
- **unknown** - thin data. A weekly job after three days says nothing yet,
  and neither will we.

The report always states which comparison source fed it. With none recorded
it degrades to an analytic expression diff in the machine's own zone instead
of failing, and it says so plainly.

Admission is simulated too: since nothing executes, earlier shadow fire-times
occupy their job for the average duration of that job's recent finished runs.
That is how overlap stand-downs reproduce under shadow; a job with no history
is treated optimistically and the report discloses when ground is missing.

While any shadow evidence exists, `paceq status` and `paceq explain schedule`
mark it - because the dangerous misunderstanding is believing these jobs are
running. They are not.

## Check status and cut over

    paceq shadow status

shows how long shadow mode has been recording, how many evaluations were
marked, and whether the ground is thick enough for conclusions.

When the report comes back clean, cutover is a separate decision (M6-03):
stop removing crontab lines automatically is out of scope here. Shadow mode
never touches cursors used by other tools, never writes run keys, never edits
a crontab.

## Guarantees

- Zero execution: no process group is ever created; no step runs.
- No run-shaped rows: `runs`, `triggers`, `run_keys`, `steps` stay empty for
  shadowed fire-times; each decision is one tick row.
- Identical decisions: planning and per-fire-time decisions are byte-equal to
  normal mode; only the outcome column differs (`shadow_triggered`).
- Reversible: stop and restart `serve` without `--shadow` and normal
  materialisation resumes from exactly where shadow left off.

Reason codes recorded during shadowing are the same catalogue as normal
mode - see [reason-codes.md](../reference/reason-codes.md).
