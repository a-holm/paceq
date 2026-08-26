# Cursor vs run_key

Two concepts carry the dedup story, and they answer two different questions:

- **The cursor** answers *how far have I read?* It belongs to your sensor: you
  invent it, you interpret it, paceq stores it opaquely and hands it back.
- **The run_key** answers *has this unit of work been seen before?* It is the
  identity of one thing the sensor found - a file path, a row id, an ETag -
  and paceq uses it as a key and nothing else.

| | `cursor` | `run_key` |
|---|---|---|
| Owned by | your sensor | paceq |
| Meaning | how far the sensor has read | this unit of work has been seen |
| Interpreted by paceq | never (opaque bytes) | only as a dedup key |
| Written | on a successful tick, in the same transaction as its triggers | when a trigger is accepted |
| Reset by | `paceq sensors reset <s>` or `reset --cursor` | never deleted unless you pass `--forget-run-keys`; otherwise the epoch bump retires them |
| Lifetime | until you change it | 365 days (retention), then treated as new |

## The one warning that matters

**The two reset independently.** Resetting the cursor does not clear run
keys, and forgetting run keys does not move the cursor. There is no implicit
coupling between them, and no command moves both without saying so. Each
command moves exactly what it names:

| Command | cursor | dedup_epoch | run_keys | Effect |
|---|---|---|---|---|
| `paceq sensors reset <s>` | NULL | +1 | kept | Full replay from the start, against a fresh epoch |
| `paceq sensors reset <s> --cursor <v>` | v | +1 | kept | Replay from a chosen point |
| `paceq sensors reset <s> --forget-run-keys` | NULL | +1 | deleted for that sensor | Full replay plus a clean dedup table |
| `paceq sensors cursor set <s> <v>` | v | unchanged | kept | Spool the cursor without replay; old run keys still dedup |

Why the epoch bump instead of deleting rows: the bump is O(1) and reversible
in practice (the old rows stay), while deleting run keys is O(n) and
irreversible. A reset therefore gives true replay without throwing away the
dedup history of a running system.

## The worked example

A dropzone sensor watching for new files:

1. **50 files arrive, the sensor ticks:** 50 triggers, 50 runs. The cursor now
   reads `2026-08-26T10:14:00Z`.
2. **The sensor ticks again immediately:** it finds nothing newer, produces no
   triggers. Even if it re-reported all 50 files, every `run_key` would be
   deduplicated: 0 new runs.
3. **You fix the processing code and want to rerun everything:**
   `paceq sensors reset dropzone`. The cursor goes back to nothing and the
   dedup epoch moves to 1, so all 50 files fire again as fresh runs.

If you had only moved the cursor back (`sensors cursor set dropzone ""`) the
files would re-fire into the *old* epoch and every one would deduplicate away:
0 runs. That asymmetry is the whole page in one line: **the cursor says where
to read, the epoch decides whether reading counts as new.**

## Retention's honest footnote

Run keys are kept for 365 days, longer than runs (90 days). If the same
`run_key` reappears after that window - a file restored from backup, a row
re-imported with its old id - it is treated as new and fires again. That is a
documented consequence, not a silent one: the gate only remembers what it is
still told to remember. See
[backup-and-retention](operations/backup-and-retention.md) for the horizons.

## Where to look from here

- The contract this rests on: [guarantees.md](guarantees.md), G2 and G4.
- Writing a sensor that produces cursors and run keys:
  [your first sensor](tutorials/02-first-sensor.md).
- The frozen wire format both travel over:
  [sensor contract](reference/sensor-contract.md).
