# Upgrading

## What is frozen, what moves

The binary you run stamps a schema version into the state database
(`paceq version --json` reports it). Within v0.1 the database format is not
frozen - v1.0 freezes it ([SCOPE](../../SCOPE.md) and the plan both promise
this) - but **migration from v0.1 forward is guaranteed**: every later
release migrates your database in place, forward only, never back.

Public contracts do not move under you at all:

- exit codes, the status JSON document, the sensor contract and the step
  environment are frozen at v0.1;
- generated references stay in lockstep with the code by CI gate:
  [CLI reference](../reference/cli.md),
  [reason codes](../reference/reason-codes.md).

## The upgrade procedure

```bash
paceq version --json                      # note current binary + schema version
systemctl stop paceq                      # or Ctrl-C serve; drains safely
cp -a .paceq /backup/paceq-before-$(date +%F)/   # backup first, always
tar xzf paceq_<new>_linux_amd64.tar.gz && mv paceq /usr/local/bin/
paceq doctor                              # new binary reads old state
systemctl start paceq
paceq fsck                                # prove the world afterwards
```

Notes on each step:

- **One writer at a time.** Migrations need the database; the daemon refuses
  to share with another writer anyway (exit 6). Stop first.
- **Backup first, always.** The daemon takes a consistent copy before each
  schema migration as a matter of correctness; that copy is not a backup
  strategy. See [backup and retention](backup-and-retention.md).
- **Migrations run when the store opens**, not on a timer: the first command
  that opens the state with the new binary migrates it. They are additive -
  new tables and columns, no destructive rewrites - so an interrupted
  migration resumes cleanly rather than corrupting.
- **Downgrades are not supported.** Forward-only migrations mean an old
  binary may refuse a migrated database. This is why the backup step exists.

## After upgrading

1. `paceq doctor` - installation checks, including the sandbox systemd
   applies and the nightly backup's last verified pass.
2. `paceq fsck` - the invariant battery over real state: every guarantee the
   project makes, checked against your data.
3. `paceq explain job <name> --since 48h` around the upgrade window shows
   exactly what ran, what was interrupted by the stop, and how recovery
   handed it back - nothing about an upgrade is silent.

If `fsck` or `doctor` finds anything, the output names the finding with its
reason code; [troubleshooting](../troubleshooting.md) starts from there.
