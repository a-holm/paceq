# ADR-0002: Product name paceq

Status: accepted
Date: 2026-08-21
Decision maker: Johan Holm

## Context

ADR-0001 decision 2 kept `pulseq` as a working title and left the naming question open. The name is load-bearing for more than the module path: it ends up in the binary, the domain, the package registries, the search results a stuck operator types into, and the `PULSEQ_*` environment contract that M8-01 freezes for v1.0. Every milestone that ships user-facing surface writes the name into something that costs more to change later.

The six criteria come from the product plan (09 §3.4): a free `.dev` domain, a free GitHub org, an unambiguous first-page search result, pronounceable, a free binary name in Debian and Homebrew, and free package names in the language registries the project may publish to.

All checks below were run on 2026-08-21.

## Decision

The product is named **paceq**. It is spelled lowercase in every context, headings included, the way `systemd` and `kubectl` are. One spelling keeps the binary, the module path, the documentation and the search term the same string.

| Criterion | paceq | pulseq (rejected) | planq (rejected) |
|---|---|---|---|
| GitHub org | free | taken by a third party, created 2026-06-02, 14 public repos | taken, `PlanQ`, since 2017 |
| `.dev` domain | `paceq.dev` unregistered, RDAP 404 via rdap.org | `pulseq.dev` free | `planq.dev` free |
| PyPI | free | not checked, blocked earlier on GitHub and search | `planq` exists: a transport-agnostic async task queue, latest release 0.3.0 |
| npm | free | not checked | not checked |
| crates.io | free | not checked | not checked |
| Homebrew formula | free | free | not checked |
| Debian sources | free | free | not checked |
| Search | one hit, ETS PACEQ-1, an analog audio surveillance preamp; hardware, unrelated field | permanent first-page collision with the MRI pulse-sequence framework `pulseq.github.io` and PyPulseq | not checked |
| Pronounceable | yes, in English and Norwegian | yes | yes |

`pulseq` fails on two independent criteria, and the search collision is the one we cannot fix. The MRI pulse-sequence framework owns the term in every search index, and an orchestrator cannot outrank an established scientific toolchain on its own name. The GitHub org was taken by a third party while the naming issue was open.

`planq` fails harder than its checklist suggests: the PyPI package of that name is an async task queue, which is the same product field. A user who installs the wrong one gets something that looks plausible.

Intermediate candidates `taktd` and `taktfast` passed every registry check. They lost on language: the owner chose a name built from English, since the documentation, the CLI and the audience are English. `chimeq` and `steadyq` passed most checks and lost on the last one: `chimeq` sits too close to Chime, the consumer fintech brand, in search, and `steadyq` is taken on npm.

## Consequences

1. The environment variable prefix is `PACEQ_*`. No `PULSEQ_*` variable exists in the code yet, so there is nothing to migrate. M8-01 freezes the prefix under the new name.
2. The rename is effectuated now rather than in M8-07 (#79). The surface was two merged issues, so the cost was under an hour. M8-07 keeps only the residual work that is not code: buying `paceq.dev`, reserving the GitHub organisation, reserving the package names in the registries listed above, and the SEO measures.
3. The deadline pressure recorded in ADR-0001 decision 2 and in M8-01 is gone. The name is settled before any user-facing contract is frozen.
4. The GitHub repository was renamed in place, so old URLs redirect and issues, pull requests and project links survive.
