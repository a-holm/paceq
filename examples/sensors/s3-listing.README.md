# s3-listing: new objects in lexical key order

Trigger one run per object whose key is lexicographically greater than the
cursor.

Cursor strategy
: the last object key seen.

run_key
: the object key, so dedup and cursor agree.

The gap
: pagination and truncation. `aws s3 ls --recursive` returns keys in order
  but a huge bucket is truncated. `PACEQ_MAX_TRIGGERS` caps one tick; the
  cursor only ever advances to the last key actually emitted, so a capped
  tick re-lists the rest next poll instead of skipping them.

Uses the `aws s3 ls --recursive` form; `bin/aws` is a directory stub because
CI runs without network and without the AWS CLI.

Jobspec

```yaml
sensors:
  - name: new-objects
    run: ["./examples/sensors/s3-listing.sh"]
    env: {WATCH_BUCKET: my-bucket}
    interval: 60s
```

