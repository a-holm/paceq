# ADR-0003: SQLite driver and the write throughput gate

Status: accepted
Date: 2026-08-21
Decision maker: Johan Holm

## Context

Two decisions rest on numbers rather than on argument, and both get cheaper to reverse the earlier they are taken.

The first is the driver. `modernc.org/sqlite` is a pure Go translation of SQLite, chosen so the binary stays cgo free, statically linked and cross buildable. The cost is speed: it is a translation, not the C library. If the cost is too high, the alternative is `mattn/go-sqlite3`, which is the C library behind cgo, and which trades all three properties away.

The second is the write model: one write connection, `_txlock=immediate` on every write transaction, `busy_timeout` kept as a safety net for other processes rather than as a strategy. The model claims `SQLITE_BUSY` is impossible inside this process, not rare.

Both claims are measured before the domain code exists, while the codebase is small enough that reversing either is a day of work.

## Decision

`modernc.org/sqlite` stays. The write model stands as implemented.

The measurements are the argument. Every number below comes from `make gate` and `make bench` on this machine, with the load conditions stated, because a throughput number without them cannot be compared to the next one.

## Measurements

Conditions: Intel i7-3770K, 4 cores and 8 threads, Linux 6.17, ext4, Go 1.26.5, `modernc.org/sqlite` v1.57.0, `synchronous=NORMAL`, one minute load average between 3 and 10 (a shared development machine, not an idle one). Every figure is a single run, without the race detector.

| Measurement | Value | Where it comes from |
|---|---|---|
| Write throughput, 32 concurrent writers, 10 s window | 3125 tx/s | `TestConcurrentWriters` |
| Write throughput, same, with the writers pinned to 2 cores | 2616 tx/s | `taskset -c 0,1` |
| Write throughput, single writer | 4027 tx/s (248 µs/op) | `BenchmarkWriteTx` |
| Write lock hold time, 32 writers | p50 137 µs, p99 2.7 ms, max 46 ms | `TestConcurrentWriters` |
| Queue wait for the write connection, 32 writers | p50 4.4 ms, p99 64 ms | `TestConcurrentWriters` |
| Read latency under that write load | p50 1.2 ms, p99 5.0 ms | `TestConcurrentWriters` |
| Read latency, single reader, writer saturating the lock | 48.6 µs/op | `BenchmarkReadUnderWriteLoad` |
| Write ahead log after 10 s of saturated writing | 4.2 MB against a 64 MiB limit | `TestConcurrentWriters` |
| `SQLITE_BUSY` reaching a caller | 0 | `TestConcurrentWriters` |
| Committed transactions surviving SIGKILL intact | 25 of 25 kills, `integrity_check` ok | `TestWALRecoveryUnderKill` |

Against the gate's floor of 500 tx/s, the margin is 6× on this machine and 5× with the writers confined to two cores. Against the capacity that actually matters, under 1 write per second in production, the margin is over three thousandfold. The floor exists as regression cover, not as a capacity plan.

Two numbers are worth more than their margin:

**Lock hold time is the metric that predicts trouble**, not throughput. A p50 of 137 µs and a p99 of 2.7 ms are what let 32 writers queue behind one connection and still see a median queue wait of 4.4 ms. The retention work in M5 turns this into an assertion of its own.

**Readers are not blocked by writers.** 5428 reads per second complete while the writers saturate the lock, at a p99 of 5 ms. That is the WAL claim, measured rather than assumed.

## The price of synchronous=FULL

`BenchmarkWriteTxSyncFull` measures 160 tx/s (6.26 ms/op) against 4027 tx/s for `NORMAL`, a factor of 25 on this hardware. `NORMAL` writes a commit into the write ahead log without an fsync; `FULL` waits for the disk on every commit.

The consequence is that the 500 tx/s floor describes the shipped default only. An operator who turns `FULL` on buys durability against a power cut at the cost of two orders of magnitude of write throughput, which is still 160 times the load this system is designed for. The gate does not assert the floor for `FULL`, and no configuration check should treat the number as a limit.

## The escape hatch

If a later measurement puts the driver below the floor, the replacement is `github.com/mattn/go-sqlite3` behind a `sqlite_cgo` build tag: the same `database/sql` interface, so the store's DSN, PRAGMA verification and retry policy carry over unchanged. `isBusySnapshot` already checks the error's `Code` method rather than a concrete driver type, and the gate's busy detection also matches on the message, so neither has to be rewritten for a different driver.

Taking that hatch is not free, and the cost is why it stays closed:

- The binary needs cgo, so it is no longer statically linked and no longer builds for another platform from a laptop. The cross build job in CI asserts cgo free binaries and would have to be rewritten.
- A C toolchain becomes a build requirement for anyone building from source.
- The escape hatch is documented, not implemented. Opening it is a new issue with its own ADR, not a flag someone flips.

## The gate and where it runs

`TestConcurrentWriters` is the existence proof: 32 writers and 8 readers against a real file in `t.TempDir()`, never an in-memory database, for 10 seconds. It asserts zero busy errors reaching a caller, exactly one write transaction in flight at a time, a read that never blocks, a sequence with no lost update, a write ahead log under its size limit, and the throughput floor.

The gate is split across two CI steps, which is a deliberate trade:

- `make test` runs the whole suite with the race detector, where the concurrency assertions run over a 2 second window and the throughput floor is **not** asserted. A number measured under the race detector describes the detector.
- `make gate` runs the same tests without the detector over the full 10 second window, asserts the floor, and prints every measurement into the CI log.

Two tests skip themselves under the race detector because they would only be paying for it: the SIGKILL recovery test, which watches a file left by a dead process, and the harness calibration, whose subject is timing.

The harness that produces these numbers is itself under test. `TestLoadHarnessMeasuresTheWorkItRuns` drives it with workloads whose duration is known in advance, one that does nothing and one that takes 5 ms, and fails if the measurement does not tell them apart. A performance gate whose measurement is unverified is a gate that stays green through anything.

## Consequences

- The driver decision is closed for M0. Reopening it needs a measurement below the floor, and the gate is what produces one.
- CI pays roughly 25 seconds per run for the gate and the concurrency assertions. That is the price of the existence proof being green continuously rather than nightly.
- The floor is absolute rather than relative. It does not track the machine, so a fast runner hides a 5× regression. The p50 and p99 numbers in the CI log are what a reviewer compares against this table.
- 1000 SIGKILL iterations run when `PACEQ_NIGHTLY` is set; a pull request runs 25.
