# From crontab to paceq in five minutes

You have a crontab. You want readable job files, real history, and an answer
to "why didn't it run" - without anyone touching your crontab. Import is
read-only towards its source: paceq opens the crontab for reading and never
writes to it, never calls `crontab` with anything but the list flag.

Everything below runs as shown; the tutorial's command blocks are executed
against a real binary on every CI run.

## 1. A project to land in

<!-- run -->
```bash
paceq init
```

A config (`paceq.yaml`), an example job under `jobs/`, the state database
under `.paceq/`, and a `.gitignore` entry keeping state out of version
control.

## 2. Import the crontab

Take this sample crontab as `crontab.txt` (or use `--user`, or point
`--file` at `/etc/crontab`; six-field cron.d files are read too):

<!-- run -->
```bash
cat > crontab.txt <<'EOF'
# m h dom mon dow  command
*/15 * * * * /usr/local/bin/collect-metrics --quiet
0 3 * * * cd /srv/backup && ./run-backup.sh >> /var/log/backup.log 2>&1
@daily flock -n /tmp/report.lock /usr/bin/nightly-report
EOF
```

<!-- run -->
```bash
paceq import crontab --file crontab.txt --tz Europe/Oslo -o jobs-imported/
```

The report goes to stderr, the YAML to files, so piping stays clean:

```
4 lines read -> 3 jobs, 0 to review
  ⚠  1 job wrapped its command in flock -> replaced with max_concurrent: 1
  ⓘ 1 wrote log files
```

What the translation decided, line by line: five-field schedules become
schedule entries with names, `@daily` becomes its cron expression, `cd X &&`
becomes a workdir plus an absolute command, `flock -n` becomes
`max_concurrent: 1` (the same stand-down, now recorded with a reason instead
of silent), output redirection is dropped **with a warning** because the log
is kept by paceq now, and every original line stays as a comment above its
job:

```yaml
# originally: 0 3 * * * cd /srv/backup && ./run-backup.sh >> /var/log/backup.log 2>&1
name: run-backup
workdir: /srv/backup
timeout: 1h                  # explicit so you can see it and adjust it
schedules:
  - name: cron
    cron: "0 3 * * *"
steps:
  - name: main
    run: ["/srv/backup/run-backup.sh"]
```

Commands that leaned on shell syntax stay whole behind `shell: true` with a
TODO comment, so nothing imports half-translated. A percent sign keeps its
cron meaning: text after the first unescaped `%` is carried over verbatim as
the command's standard input.

## 3. Read what you got, then load it

<!-- run -->
```bash
paceq validate jobs-imported/
paceq apply jobs-imported/
```

`validate` checks every rule the parser enforces and names each finding;
`apply` records the batch as immutable versions - one transaction, all three
jobs or none. Applying the same files twice changes nothing: idempotency
lives in the schema.

## 4. Run one now

`paceq run` executes the job immediately, waits for it, and exits 5 if the
job itself failed (exit 1 means paceq had the problem - see
[exit codes](../reference/exit-codes.md)):

```bash
paceq run run-backup
```

(On this tutorial's fake paths that run ends with `STEP_FAILED_SPAWN`, exit
5, which is the contract working: `/srv/backup` does not exist here. On a
real crontab the command exists.)

The run, its steps, its logs and its reason codes are in the history now:

<!-- run -->
```bash
paceq explain job run-backup
paceq status
```

## Where firing stands

Import and apply record ready-to-run state; nothing here has fired a job on a
clock yet, and nothing has touched your crontab. Today you start work
yourself: `paceq run` for a job now, `paceq sensors tick` for one sensor
evaluation now. The [project board](https://github.com/users/a-holm/projects/2)
tracks the activation work that turns recorded schedules into automatic
firing - shadow mode alongside cron first, then cutover - and
[CHANGELOG.md](../../CHANGELOG.md) states where each piece stands at every
release.

## Where to go next

- [Your first sensor](02-first-sensor.md): react to new files in five lines
  of bash.
- [Job file reference](../reference/jobspec.md): every key import may write.
- [Troubleshooting](../troubleshooting.md): when a job did not run.
