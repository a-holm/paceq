# sql-watermark: new rows over a monotone column

Trigger one run per row whose id (or updated_at) is newer than the cursor.

Cursor strategy
: the highest committed id in the column.

run_key
: `<table>:<id>` so one row is fired at most once per epoch.

The gap
: a column that is not monotone (updates rewritting an old id, or clock skew
  between app servers) will replay or under report. An auto increment id
  beats `updated_at` on almost every database.

Exit 75 (transient)
: a database that is briefly unreadable counts nothing against the breaker.

Uses the `sqlite3` CLI as the billing example; the same pattern works with
`psql` and `mysql`.

Jobspec

```yaml
sensors:
  - name: new-events
    run: ["./examples/sensors/sql-watermark.sh"]
    env: {WATCH_DB: /var/lib/events.db, WATCH_TABLE: events}
    interval: 15s
```

