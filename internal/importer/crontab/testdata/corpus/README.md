# Crontab corpus

Fixtures the import gate runs against. Two hard gates read every file:

- `TestCorpusInterpretationRate` - more than 90 percent of all lines must be
  interpreted on the first pass (plan 09, risk R6: a first run that answers
  "could not interpret" twelve times kills the product).
- `TestCorpusRoundTripsThroughSpec` - every job emitted from every file must
  parse through internal/spec.

## Provenance

The fixtures are representative constructions, not verbatim copies: each one
models a shape collected from public dotfiles repositories, configuration
management roles (Ansible-style crontab templates), and this project's own
production machines. No private data was copied; host names, addresses and
paths are generic or invented. They are maintained as code, so a new
construct found in the wild lands here as a fixture plus the fix that makes
it translate.

- `dotfiles-*.crontab`: personal user crontabs in the five-field form.
- `ansible-role-*.crontab`: configuration-management managed entries,
  including system-style lines that carry a user column.
- `egne-*.crontab`: this project's own jobs, the ones milestone M5 exit
  criterion K1 will import for real.
- `hostile-*.crontab`: edge cases on purpose: CRLF line endings, tab
  separators, percent storms, quote forms, comment-only files.
