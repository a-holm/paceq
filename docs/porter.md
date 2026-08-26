# Ports

This file is the journal of the project's ports and kill criteria (plan
10 §7: "Skriv datoen og svaret ned" - write down the date and the answer).
Every port verdict is committed here, oldest first, so the decisions can be
reread later.

## K1 evaluation - 2026-08-26

Criterion (10 §7): at least 3 of your own, real jobs run in production on
Pulseq, with the crontab lines disabled. If no -> **stop**. You are building
something you would not use yourself.

### Answer: NO

No jobs run in production on Pulseq. The tool is not installed on any machine
yet; no crontab line has been commented out or removed in favour of paceq.
M0-M5 are built all the way to green gates, but cutover was never attempted.

### Jobs

| # | Job | Former crontab line | Migrated | Successful runs since | Impact if it stops |
|---|-----|----------------------|----------|------------------------|---------------------|
| 1 | selvoppdater (kaizer) | `*/15 * * * * cd $HOME/tutorsrv-src && /bin/sh deploy/selvoppdater.sh >> $HOME/selvoppdater-cron.log 2>&1` | No - still active in crontab | 0 | The AI tutor stops self-updating |

One real, own cron job exists on the reachable machines (kaizer, via the ssh
aliases `hjemmeserver` and `mestringsstigen`; same host). It keeps running on
cron. Zero of the required three jobs have been migrated.

### Evidence (machine-readable, collected 2026-08-26)

Development machine (`Nibras`):

    $ crontab -l
    no crontab for johan
    $ command -v paceq pulseq
    (neither exists)
    $ ls ~/.paceq
    (does not exist)

kaizer:

    $ crontab -l
    */15 * * * * cd $HOME/tutorsrv-src && /bin/sh deploy/selvoppdater.sh >> $HOME/selvoppdater-cron.log 2>&1
    $ command -v paceq pulseq
    (neither exists)

systemd user timers: none beyond the distribution default
(`launchpadlib-cache-clean`). Running paceq processes: only orphaned CI test
fixtures (`testscript .../run wobble`, chaos `serve`), no production daemon.

### The demo sentence

"I ask why the backup did not run last night, and it answers" cannot be
verified today: there is no production installation to ask on it, and so no
real or induced case exists either. The demo question stays unanswered until
at least one real job runs on Pulseq.

### Diagnosis (trust / friction / missing value?)

The observable fact is that migration was never attempted - not that it
failed. Therefore:

- **Missing value** (the serious outcome) is *not* demonstrated: `explain`
  has never answered a real "why did X not run?", so the differentiator is
  untried, not disproven.
- What is actually missing is the bridge from finished tool to own use,
  which points at **friction** (the import/cutover path went unused) and
  **trust** (cron stays on until shadow running has shown parity).
- Separating those further needs the owner's judgement; machine evidence
  cannot settle motive.

### Decision

- [ ] YES - K1 passed. M6 starts.
- [x] **NO - STOP. Scope is re-evaluated before M6.**

M6 does not start before this note has been processed by the owner. The
processing must at least decide:

1. Reprioritisation per issue #53's NO path, option 2 (the friction branch):
   shadow mode ([M6-02]) and cutover ([M6-03]) go first; new features wait.
2. A concrete first target: candidate #1 (`selvoppdater` on kaizer) is
   migrated, its crontab line disabled, and K1 is re-evaluated with
   machine-readable evidence (`pulseq status --json`,
   `pulseq explain job <name> --since 168h`, `crontab -l`) after at least 7
   consecutive days of successful runs.
3. If this instead shows missing value: scope is cut to "personal tool" per
   10 K3 - a valid outcome, but it gets documented here.

### Separation from the v0.1 release

The K1 answer stops the start of M6. It does not stop tagging v0.1.0:
milestone M5's value (plan section E: "v0.1-verdien beskyttes alltid" - the
v0.1 value is always protected) is independent of whether the owner already
runs the tool in production, and the release checklist's technical gates were
run and recorded in the issue. The budget deviation measured during the
checklist (core Go 48 764 total lines, 35 824 code lines excluding tests,
against the <= 12 000 budget, SYNTESE §4.9) is a separate port finding that
follows the K4 doctrine ("something is removed, the budget is not raised")
and is handled before 1.0 - it does not change the v0.1 feature set.
