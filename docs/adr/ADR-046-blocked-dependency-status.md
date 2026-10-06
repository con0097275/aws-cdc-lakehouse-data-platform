# ADR-046 — BLOCKED_DEPENDENCY: rejected as a status, answered as a reason

- Status: **ACCEPTED** (Session 38)
- Related: ADR-035, ADR-037, ADR-038, ADR-039, ADR-043,
  `docs/REPORTING_TARGET_ARCHITECTURE.md` (the ten-state contract)

## Context

The reporting master prompt asks, in §19/§28/§35, for a `BLOCKED_DEPENDENCY` execution
status: when turn 1 has `A = SUCCEEDED` and `B = FAILED`, the job in turn 2 that depends on
B "must become something explicit such as `BLOCKED_DEPENDENCY` or approved equivalent".

`docs/REPORTING_TARGET_ARCHITECTURE.md:437-441` already fixes the vocabulary at ten states
and does not include it. That document is normative, so the prompt does not simply win.

**The behaviour the prompt is protecting already holds.** Verified live on 2026-08-24, run
`s36_eod_fixed`, with two real EMR Serverless submissions that failed:

```
s36_eod_fixed:EOD:mart_account_balance_daily:2026-08-22:1    FAILED  ENGINE_FAILED
s36_eod_fixed:EOD:mart_account_balance_monthly:2026-08-22:1  WAITING_DEPENDENCY
```

The downstream job did not run, and no watermark advanced. Gating reads the upstream's
**watermark**, not its execution status (`gates.upstream_watermarks_for`), and ADR-043
forbids a failed run from advancing one — so the child is held without the coordinator ever
needing to reason about the upstream's status. Correctness was never the issue.

**What is missing is distinguishability.** `Coordinator.gate` returns exactly two values,
`READY` or `WAITING_DEPENDENCY` (`coordinator.py:266-271`). A job whose upstream FAILED and
a job whose upstream is merely STILL RUNNING land in the same state. At 02:00 those need
opposite responses: one resolves itself, the other is dead until someone intervenes.

## Options

| Option | Verdict |
|---|---|
| **Distinguish in the GATE; terminate via the existing `block()` → `SKIPPED` + reason** | **CHOSEN** |
| Add `BLOCKED_DEPENDENCY` as an eleventh status | Rejected |
| Add a `blocked_reason` field alongside `WAITING_DEPENDENCY` | Rejected |
| Do nothing | Rejected |

**An eleventh status is rejected** because it costs more than it returns. It touches the
transition table in `status.py`, every consumer that switches on status, the metrics
vocabulary in §36, the runbooks, and `summary_config_v1` — and having paid all that, it
still answers only "blocked", not *which* upstream or *why*. The operator's real question is
"which dependency, and is it dead or slow"; an enum cannot carry that.

**A parallel `blocked_reason` field on `WAITING_DEPENDENCY` is rejected** because it leaves
the job in a non-terminal state that Airflow will keep retrying, which is exactly wrong for
a dependency that will never arrive in this coordinator run.

**Doing nothing is rejected** because the conflation is a real operability defect, observed
in a real run.

## Decision

**No new status. The distinction is made where it belongs — in the gate — and expressed
through machinery that already exists.**

`Coordinator.block(task, reason=...)` (`coordinator.py:326-334`) already terminates a job
whose required upstream will not arrive, and it deliberately records `SKIPPED` rather than
`FAILED`: the job did not fail, it was never eligible. That is precisely the prompt's
"approved equivalent", and it predates the prompt.

The change is to let the gate reach it:

| Situation | Status | Carries |
|---|---|---|
| Upstream not yet at the required date, still eligible | `WAITING_DEPENDENCY` | unmet dependency list |
| Upstream reached a TERMINAL FAILED state in this coordinator run | `SKIPPED` | `reason` naming the upstream and its status |
| All required dependencies satisfied | `READY` | resolved snapshot ids |

`SKIPPED` is terminal, so Airflow stops retrying a run that cannot succeed — and
`_surface()` maps it to `AirflowSkipException`, so the UI shows a skip rather than a
false green or a misleading failure.

The ten-state contract in `REPORTING_TARGET_ARCHITECTURE.md` is unchanged.

## Consequences

- The prompt's §19 requirement is met without reopening an accepted vocabulary.
- An operator can tell the two causes apart from the execution record alone, and gets the
  offending upstream's identity, which an enum would not have given them.
- `SKIPPED` now has two distinct causes — "disabled / not a working day" and "upstream
  dead" — separated only by `reason`. That is the cost of this choice, and it is why the
  reason string is mandatory on the blocked path rather than optional.
- Success rates computed from status stay meaningful: a blocked job is not counted as a
  failure it did not cause, nor as a success it did not achieve.

## Cost

**Zero.** No new AWS resource, no new table, no extra query. The upstream's `ExecutionRecord`
was already fetched by `dependency_state` to decide satisfaction; `dead_upstreams` reads the
same record and asks a second question of it.

It is mildly cost-*reducing*: a dependent that used to sit in `WAITING_DEPENDENCY` was
retried by Airflow on its normal schedule (2 retries, exponential backoff) for a run that
could never succeed. Those task pods are no longer scheduled.

## Security

**No change.** No IAM, no new principal, no new data path. The `reason` string names an
upstream `job_id` and its status — both already present in the execution history and in the
compiled plan, and neither is sensitive.

## Rollback

Delete `Coordinator.dead_upstreams` and the four-line block in `Coordinator.gate` that calls
it. The gate returns to `WAITING_DEPENDENCY` for every unsatisfied dependency and nothing
else changes: `block()` predates this ADR and keeps working for its original callers.

No data migration either way — this decides a status at gate time and rewrites no history.
Records already written as `SKIPPED` remain valid under the old contract, because `SKIPPED`
is a state the old contract also had.

## Validation

Four tests, all offline (no AWS, no Spark):

| Test | Asserts |
|---|---|
| `test_failed_upstream_blocks_downstream` | gate returns `SKIPPED`, not `FAILED`, and the reason NAMES the upstream |
| `test_a_skipped_upstream_does_not_satisfy_a_dependency` | a SKIPPED upstream blocks rather than parks |
| `test_a_failed_branch_does_not_block_an_unrelated_branch` | §19 in full — `mart_c` READY while `mart_d` is blocked, and neither dead branch advances a watermark |
| `test_02_dependency_fails` (phase-15 recovery matrix) | the documented recovery path matches the new status |

The third is the one that matters: it is the first test in the suite that proves the failure
propagates DOWN its own branch and nowhere else. Full suite 938 passed after the change.
