# The run and step state machines

Generated from internal/model. Regenerate with:

    go test ./internal/model -run TestGoldenTransitions -update

A cross table cell holds the states an event can lead to, over every combination of the guards. A dash is a pair the machine refuses as an illegal transition. A pair that leads back to the state it started in is a transition all the same: it writes something.

## The run machine

| state | claim | deferred | step_started | step_succeeded | step_failed | upstream_failed | all_steps_done | cancel_observed | lease_expired | operator_retry |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| queued | running, cancelled | queued | - | - | - | - | - | - | - | - |
| running | - | - | - | - | - | - | succeeded, failed | cancelled | queued, failed | - |
| succeeded | - | - | - | - | - | - | - | - | - | queued |
| failed | - | - | - | - | - | - | - | - | - | queued |
| cancelled | - | - | - | - | - | - | - | - | - | queued |

### Transitions

| from | event | to | case | effects |
| --- | --- | --- | --- | --- |
| queued | claim | running | a claim starts an available run | bump_epoch, take_lease, set_started, emit(run.started) |
| queued | claim | running | a run available exactly now is available | bump_epoch, take_lease, set_started, emit(run.started) |
| queued | claim | cancelled | a claim on a run somebody cancelled cancels it instead | set_finished, emit(run.cancelled) |
| queued | claim | cancelled | cancellation beats a deferral | set_finished, emit(run.cancelled) |
| queued | deferred | queued | a deferral records why and stays queued | set_available_at, set_defer_reason(concurrency_limit), emit(run.deferred) |
| running | all_steps_done | succeeded | a run whose steps all succeeded succeeds | set_finished, release_lease, emit(run.succeeded) |
| running | all_steps_done | failed | a run with a failed step fails | set_finished, release_lease, emit(run.failed) |
| running | cancel_observed | cancelled | an observed cancellation kills the process group and finishes the run | kill_process_group, set_finished, release_lease, emit(run.cancelled) |
| running | lease_expired | queued | an expired lease requeues the run while the crash budget lasts | bump_epoch, inc_crash_count, set_defer_reason(reconciled_after_crash), emit(run.requeued) |
| running | lease_expired | failed | a run out of crash budget is poisoned | set_finished, emit(run.poisoned) |
| succeeded | operator_retry | queued | an operator reopens a succeeded run | bump_epoch, emit(run.reopened) |
| failed | operator_retry | queued | an operator reopens a failed run | bump_epoch, emit(run.reopened) |
| cancelled | operator_retry | queued | an operator reopens a cancelled run | bump_epoch, emit(run.reopened) |

### Refusals

| from | event | case | error |
| --- | --- | --- | --- |
| queued | claim | a claim on a deferred run is refused without moving it | run is not available yet |
| queued | claim | cancelling at claim time needs a reason code | missing reason code |
| queued | deferred | a deferral without a reason is refused | missing defer reason |
| running | all_steps_done | a run cannot finish while a step of it is still active | steps are not all terminal |
| running | all_steps_done | a writer without the lease cannot finish the run | lease is not held |
| running | all_steps_done | finishing a run needs a reason code | missing reason code |
| running | cancel_observed | a writer without the lease cannot cancel the run | lease is not held |
| running | cancel_observed | cancelling a running run needs a reason code | missing reason code |
| running | lease_expired | poisoning a run needs a reason code | missing reason code |
| running | lease_expired | a lease that has not expired cannot expire | lease has not expired |

## The step machine

| state | claim | deferred | step_started | step_succeeded | step_failed | upstream_failed | all_steps_done | cancel_observed | lease_expired | operator_retry |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pending | - | - | running | - | - | skipped | - | - | - | - |
| running | - | - | - | succeeded | pending, failed | - | - | cancelled | - | - |
| succeeded | - | - | - | - | - | - | - | - | - | - |
| failed | - | - | - | - | - | - | - | - | - | - |
| skipped | - | - | - | - | - | - | - | - | - | - |
| cancelled | - | - | - | - | - | - | - | - | - | - |

### Transitions

| from | event | to | case | effects |
| --- | --- | --- | --- | --- |
| pending | step_started | running | a pending step opens an attempt | inc_attempt, set_started, emit(step.started) |
| running | step_succeeded | succeeded | a running step succeeds | set_finished, emit(step.succeeded) |
| running | step_failed | pending | a failed attempt with retries left goes back to pending | set_next_attempt_at, emit(step.retry_scheduled) |
| running | step_failed | failed | a failed attempt with no retries left fails the step | set_finished, emit(step.failed) |
| pending | upstream_failed | skipped | a failed upstream skips a pending step | set_finished, emit(step.skipped) |
| running | cancel_observed | cancelled | an observed cancellation kills the process group and cancels the step | kill_process_group, set_finished, emit(step.cancelled) |

### Refusals

| from | event | case | error |
| --- | --- | --- | --- |
| running | step_succeeded | a step that succeeded needs a reason code too | missing reason code |
| running | step_failed | scheduling a retry needs the failure's reason code | missing reason code |
| running | step_failed | failing a step needs a reason code | missing reason code |
| pending | upstream_failed | skipping a step needs a reason code | missing reason code |
| running | cancel_observed | cancelling a step needs a reason code | missing reason code |
