# Your first sensor

A schedule asks "is it time yet?". A sensor asks "did something new arrive?".
It is your own program: paceq hands it a cursor and a deadline as JSON, the
program answers with one JSON object, and paceq does the deduplication,
history and explaining. No SDK, no library - this tutorial's sensor is five
lines of bash.

Everything below runs as shown; CI executes the tutorial's command blocks
against a real binary on every run.

## 1. The sensor: react to new files in a directory

Create a project, then write `sensors/new-files.sh` into it:

<!-- run -->
```bash
paceq init
mkdir -p sensors
cat > sensors/new-files.sh <<'EOF'
#!/usr/bin/env bash
# sensors/new-files.sh - five lines, no SDK, no library.
cursor=$(jq -r '.cursor // ""')
files=$(find "$WATCH_DIR" -type f -newermt "${cursor:-1970-01-01T00:00:00Z}" | sort)
newest=$(stat -c '%Y' "${files##*$'\n'}" 2>/dev/null); [ -z "$newest" ] && newest=0
jq -cn --arg c "$(date -u -d "@${newest:-0}" +%Y-%m-%dT%H:%M:%SZ)" \
   --argjson t "$(echo "$files" | jq -Rn '[inputs | select(length>0) | {run_key: ., params: {file: .}}]')" \
   '{cursor:$c, triggers:$t}'
EOF
```

Make it executable, create the dropzone, and try it by hand:

<!-- run -->
```bash
chmod +x sensors/new-files.sh
mkdir -p dropzone
touch dropzone/a.txt dropzone/b.txt
WATCH_DIR=$PWD/dropzone ./sensors/new-files.sh
```

That last line prints:

```json
{"cursor":"2026-08-26T10:14:00Z","triggers":[{"run_key":"dropzone/a.txt","params":{"file":"dropzone/a.txt"}},{"run_key":"dropzone/b.txt","params":{"file":"dropzone/b.txt"}}]}
```

What happened, line by line:

- stdin carried the **contract JSON**; line 3 reads its `cursor` (empty on
  first run). The same facts are also set as `PACEQ_*` environment variables -
  see the [sensor contract](../reference/sensor-contract.md).
- Line 4 finds everything newer than the cursor. The cursor is yours: an
  mtime here, a row id, an ETag anywhere else.
- Lines 5-7 answer with one JSON object: the new cursor, plus one trigger per
  file. Each trigger carries a `run_key` - the file path, the natural
  identity of "this file" - and `params` that ride along to the run.

## 2. Wire it to a job

One job, one sensor, one step. Write a small worker script, then the job
that owns the sensor:

<!-- run -->
```bash
mkdir -p bin
cat > bin/process-one <<'EOF'
#!/usr/bin/env bash
# The trigger's params arrive as JSON in PACEQ_PARAMS; this is where the
# real work goes. Here we just show the file that started this run.
jq -r '.file' <<<"$PACEQ_PARAMS"
EOF
chmod +x bin/process-one

cat > jobs/process-file.yaml <<EOF
name: process-file
description: React to files landing in dropzone/
timeout: 10m
sensors:
  - name: new-files
    run: ["./sensors/new-files.sh"]
    interval: 30s
    env:
      WATCH_DIR: dropzone   # relative to the project root; the sensor subprocess
                            # sees only what it declares, deny by default
steps:
  - name: main
    run: ["$PWD/bin/process-one"]   # steps start with an absolute path on purpose:
                                    # paceq runs the process itself, there is no
                                    # shell to search PATH (PQ1012 guards this)
EOF
```

The step's `run` is **argv** - paceq starts the process itself, so nothing is
split or expanded by a shell unless you opt in with `shell: true`, and the
first element must be an absolute path because there is no shell to search
`PATH`. The trigger's `params` arrive as JSON in `$PACEQ_PARAMS`; the full
environment a step runs under is frozen in the
[step contract](../reference/step-contract.md). And note where `WATCH_DIR`
lives: on the sensor, not the job. The sensor subprocess is deny-by-default -
it sees the fixed baseline, its contract keys, and what it declares.

## 3. Dry-run it

<!-- run -->
```bash
paceq apply
paceq sensors test new-files
```

`sensors test` runs against real state but writes nothing - the database is
bit-identical before and after:

```
Dry run of sensor new-files - no runs created, cursor not saved.
  command:  ./sensors/new-files.sh
  duration: 16ms  exit: 0
  outcome:  triggered
  2 trigger(s):
    dropzone/a.txt  ✓ new
    dropzone/b.txt  ✓ new
  cursor out "2026-08-26T10:14:00Z"
```

Pipe debugging works too: `paceq sensors test new-files --print-input |
./sensors/new-files.sh | jq` shows exactly what the contract round trip does.

## 4. Fire it for real

<!-- run -->
```bash
paceq sensors tick new-files
```

One real evaluation: triggers are accepted or deduplicated, runs are queued,
the cursor advances - all in one transaction. Run it twice and the second is
quiet: every `run_key` was seen before.

## 5. The wow moment

<!-- run -->
```bash
paceq explain sensor new-files
```

Every evaluation, verdict, trigger and dedup decision is stored with its
reason code. When someone asks "did the dropzone sensor run last night?",
the answer is read back from the database, not reconstructed from memory.

## Where automatic firing stands

Today sensors are evaluated when you ask: `sensors test` (dry),
`sensors tick` (real). The `interval` you set reserves how often the daemon
will evaluate once catalog activation lands - tracked on the
[project board](https://github.com/users/a-holm/projects/2) ahead of the
v0.1 cut; [CHANGELOG.md](../../CHANGELOG.md) states where things stand at
each release.

## Where to go next

- [Cursor vs run_key](../cursor-vs-run-key.md): why resetting gives replay
  without losing dedup, and why the two reset independently.
- [Reason codes](../reference/reason-codes.md): what every stored verdict means.
