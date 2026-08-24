# http-etag: poll a URL for change

Trigger when a URL changes its ETag, or its body hash when the server sends
no ETag.

Cursor strategy
: the ETag value, or a sha256 of the body as fallback.

run_key
: the ETag/hash itself, so cursor and dedup key agree.

The gap
: a service with no ETag forces a full body download every poll. Fine for
  small payloads, wasteful for big ones; the M7-03 refinement is a proper
  conditional GET.

Exit 75 (transient)
: a network blip maps to exit 75 so the breaker sees a transient fault, not a
  real outage. The test verifies the breaker does not trip.

Jobspec

```yaml
sensors:
  - name: release-notes
    run: ["./examples/sensors/http-etag.sh"]
    env: {WATCH_URL: "https://example.com/releases.atom"}
    interval: 60s
```

