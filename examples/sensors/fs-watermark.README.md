# fs-watermark: filesystem watermark

Trigger one run per file that is newer than the cursor, then move the cursor
to the highest mtime seen.

Cursor strategy
: the highest `mtime` (unix seconds) already committed.

run_key
: `<path>:<mtime>`. A file that changes again gets a new run because its
  mtime moved past the cursor.

The gap
: a file still being uploaded looks newer than the cursor before it is
  finished. This example does not wait for quiescence; the duplicate run is a
  no-op at commit time thanks to run_keys. A "stable for N seconds" rule and
  inotify are the M7-03 answer.

Jobspec

```yaml
sensors:
  - name: dropzone
    run: ["./examples/sensors/fs-watermark.sh"]
    env: {WATCH_DIR: /srv/incoming}
    interval: 30s
```

