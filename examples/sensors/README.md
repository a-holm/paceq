# Example sensors

Four real, small sensors and one blank form. They are the documentation that
cannot rot: every example runs in CI against fixtures, so a broken contract
breaks the build before it reaches a user. Copy one and adapt it.

All sensors are POSIX shell (`sh`, works in `dash`), read the last committed
cursor from `PACEQ_CURSOR`, and answer with exactly one JSON object on
stdout. That object is the whole contract: a `cursor` for the next run, a
`triggers` array for the runs to create, and an optional `skip_reason` for a
quiet run. The reference contract is `docs/reference/sensor-contract.md`.

| Example | Cursor strategy | run_key | The gap it shows |
|---|---|---|---|
| `mal.sh` | none yet, a blank form | none | the shape of the answer |
| `fs-watermark.sh` | highest `mtime` | `<path>:<mtime>` | half written files |
| `http-etag.sh` | ETag, or body hash | the ETag/hash | a service with no ETag |
| `sql-watermark.sh` | last id over a monotone column | `<table>:<id>` | non monotone columns |
| `s3-listing.sh` | last key in lexical order | the object key | pagination and truncation |

## The five minute idea

A sensor is a program that answers one question: what is new since the cursor?
You do not move files, you do not mark them read. You say what is new and let
paceq's run_keys dedup + cursor give you at most one run per trigger identity.
That frees you from
the classic dropbox bug where moving the file and crashing lose data.

`mal.sh` is the blank form. Run it through the linter to see a valid answer:

```sh
echo '{}' | sh examples/sensors/mal.sh | examples/sensors/bin/contract-lint
```

## The shared environment

paceq starts a sensor as a subprocess with a fixed `PATH`, the `HOME`/`TZ`/
`LANG` it was given, the eight reserved `PACEQ_` contract variables, and
anything the sensor declares in its own definition. The sensor reads its
position from `PACEQ_CURSOR` and the cap from `PACEQ_MAX_TRIGGERS`. The
example scripts use extra `WATCH_*` variables for their configuration; in a
real deployment those come from the sensor's `env` block, not from the shell.

## Existing sensors need `WATCH_*` variables

Run them against a fixture with:

```sh
WATCH_DIR=/path/to/watch sh examples/sensors/fs-watermark.sh
WATCH_URL=https://example.com sh examples/sensors/http-etag.sh
WATCH_DB=events.db WATCH_TABLE=events sh examples/sensors/sql-watermark.sh
WATCH_BUCKET=my-bucket sh examples/sensors/s3-listing.sh
```

Set `PACEQ_CURSOR` to replay from a position. Set `PACEQ_MAX_TRIGGERS` to cap
one tick; the SQL and S3 examples only advance the cursor to the last item
they actually emitted, so a capped tick re-lists the rest next poll.

## CI

The test `run_examples_test.go` runs every example both standalone (input in,
valid JSON out, checked against `bin/contract-lint`) and the store proof in
`internal/store` drives the same scripts through the real production path:
the atomic commit and the run_keys dedup gate used by the daemon. The suite
needs no network and no installed `sqlite3` or `aws` CLI: the HTTP example
talks to an in process test server, the S3 example to the `bin/aws` stub, and
the SQL example to the `bin/sqlite3` stub, over `testdata/`. `sh -n` and,
where installed, `shellcheck` are part of the build (see Makefile
`sensors-examples`, wired into `make ci`).