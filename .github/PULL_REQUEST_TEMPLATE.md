## What

<!-- What does this PR change, and which issue closes? -->

## Review checklist

- [ ] A new code path that declines, skips or cancels work has (a) a reason
      code in internal/reason and (b) a scenario row in
      internal/explain/scenarios_test.go (`make explain-checklist`).
- [ ] Schema or golden-fixture changes carry a `FIXTURE-CHANGE:` line with the
      reason.
- [ ] `make ci` is green (shellcheck vendored on PATH; no SKIP lines).
