# Running paceq under systemd

paceq is one static binary and `paceq serve` is a well-behaved daemon: it
drains on SIGINT/SIGTERM, hands interrupted work back to the queue, answers
`sd_notify` watchdog beats (Type=notify), and refuses a second writer on the
same state directory with exit 6 instead of corrupting anything.

## Quick start

```
sudo paceq install-service          # writes /etc/systemd/system/paceq.service (+ .relaxed variant)
sudo systemctl daemon-reload
sudo systemctl enable --now paceq
systemctl status paceq              # or: journalctl -u paceq -f
```

`install-service` never overwrites without `--force`; `--dry-run` prints the
unit to stdout; `--dest` moves it elsewhere. The unit is embedded in the
binary (`deploy/paceq.service`), so the installed unit matches the binary
that wrote it.

## What the unit does

The shipped unit runs paceq as a dedicated user with a hard sandbox:

- **User/group `paceq`**, `UMask=0077`, private state via
  `StateDirectory=paceq` (`/var/lib/paceq`, mode 0700) and
  `RuntimeDirectory=paceq`.
- **Type=notify + WatchdogSec=30**: the daemon beats the watchdog; if it
  wedges, systemd kills and restarts it (`Restart=always`, `RestartSec=2s`).
- **Graceful stops**: SIGTERM starts a drain - running steps get
  `--drain-timeout` (default 30s per serve; the unit allows 120s) - and
  `KillMode=mixed` escalates to the process group only after that.
- **Hardening that jobs inherit** (the daemon spawns your steps): 
  `NoNewPrivileges`, `ProtectSystem=strict` (read-only filesystem except
  state), `PrivateTmp`, `RestrictAddressFamilies`, `SystemCallFilter`,
  and friends.

### The sandbox your jobs inherit

`ProtectSystem=strict` makes every path read-only except the state
directory. A job that writes `/srv/data/out.csv` will fail with `EROFS`.
That is by design - and it is configurable, not fatal:

1. Add write paths to the unit:
   `ReadWritePaths=/srv/data /var/log/my-jobs`
2. Or switch to the relaxed variant shipped alongside the strict one:
   `paceq.service.relaxed` - same supervision, lighter sandbox - by copying
   it over `paceq.service`.

Run `paceq doctor` to see the effective sandbox as a job would experience
it. The [step contract](../reference/step-contract.md) explains how much of
the environment reaches a step (deny by default; `inherit_env` names what
passes).

## Where state lives

The unit's `ExecStart=/usr/bin/paceq serve` runs with systemd's default
working directory. Give the project explicitly when your project is not the
home directory: point `WorkingDirectory=` at the project root (where
`paceq.yaml` lives), or pass flags:

```ini
[Service]
WorkingDirectory=/srv/my-project
ExecStart=/usr/bin/paceq serve --jobs-dir jobs --metrics-listen 127.0.0.1:9753
```

State always lands under the project's `.paceq/` unless `--db` says
otherwise; see `paceq serve --help` for every flag. Multiple projects mean
multiple units - one per project, each with its own `WorkingDirectory`, each
holding its own lock. A second `serve` on the *same* project exits 6 rather
than racing.

## Watching it

- `journalctl -u paceq -f`: the daemon logs JSON lines to stderr.
- The health endpoints (`/livez`, `/readyz`, `/metrics` and the control
  posts) answer on a unix socket when you pass `--socket <path>`, and
  `/metrics` additionally answers on TCP with `--metrics-listen`. The shipped
  unit passes neither: turn on what you use. See [monitoring](monitoring.md).
- `paceq status` reads the state database directly, so it works from any
  account that can read the state directory - even with the daemon stopped -
  and its exit code is designed for cron/MOTD monitoring.
