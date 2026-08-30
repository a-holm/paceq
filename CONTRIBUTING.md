# Contributing

Thank you for working on paceq. The short version:

1. `make ci` is the whole gate; every PR runs it. `make test` alone is not
   enough for changes that touch store schemas, reason codes or CLI surfaces.
   A second `make ci` on an unchanged tree skips what it already proved and
   returns in under a second; `PACEQ_GATE_STAMP=0 make ci` runs everything.
2. Commit subjects follow `#<issue>: <lower-case phrase>`, matching the log.

## The explain checklist rule

Every code path that declines, skips, defers or cancels work owes an
explanation a person can read later. That is two artifacts, not one:

- **(a)** a reason code in `internal/reason/codes.go`, with its explanation,
  remedies and data keys filled in; and
- **(b)** a named scenario row in `internal/explain/scenarios_test.go`, which
  asserts that the decision is stored, that its code is a catalogue member,
  and that `paceq explain` shows both in `--json` and text form.

`TestEveryTerminalReasonHasScenario` crosses the scenario table against the
catalogue and fails when a terminal code has no scenario. A code may sit out
the checklist only by being marked `ScenarioExempt` in `internal/reason`
**with** a non-empty `ExemptReason` arguing why no scenario can exist yet;
the catalogue tests reject an empty excuse. If you add a writer for an exempt
code, add the scenario in the same commit and drop the exemption.

Pull requests carry the same rule as a checkbox in the template.

## Generated documentation

Two reference pages are generated, never hand-edited:
[docs/reference/cli.md](docs/reference/cli.md) from the cobra command tree and
[docs/reference/reason-codes.md](docs/reference/reason-codes.md) from the
reason catalogue. Their staleness tests run inside `make test`, so changing
help text or a catalogue entry without regenerating is red. Run

```
make docs
```

and commit the regenerated pages together with your change. The same gate
keeps the prose honest: code blocks marked `<!-- run -->` in the README and
tutorials execute against a freshly built binary on every test run, relative
links in every markdown file must resolve, the phrase "exactly-once" is never
written as a promise, only inside a negation, and user-facing material stays
in English (see internal/cli/docsexamples_test.go).
