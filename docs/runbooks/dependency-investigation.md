# Runbook — dependency investigation

**When:** a job sits at `WAITING_DEPENDENCY` and you need to know why.

## 1. Ask the coordinator, not the YAML

```python
ready, snapshots, unmet = coordinator.dependency_state(task)
```

`unmet` names each unsatisfied dependency. The job YAML holds only **direct** edges; the
transitive closure and the turn numbers are computed by the compiler and never authored
(ADR-037/038), so reading the YAML tells you what was declared, not what is blocking.

## 2. The two kinds of dependency

| Kind | Blocks on | Fix |
|---|---|---|
| `JOB` | an upstream reporting job succeeding | run the upstream, or SKIP it if genuinely not required |
| `EOD_TABLE` | a layer close — a **gate**, not an ordering constraint | close the day ([eod-retry.md](eod-retry.md)) |

A **FAILED** upstream does not satisfy a dependency. That is deliberate: a downstream that
ran on a failed upstream would publish numbers derived from data nobody certified.

## 3. dbt refs are NOT authored here

`ref()` edges are derived from the dbt manifest by `make reporting-compile-dbt`. If a
dependency you expect is missing, re-run dependency-sync before editing YAML — hand-authored
ref edges drift from the model and are the usual cause of a "phantom" missing dependency.

## 4. Turn order

```bash
make reporting-validate     # prints each flow's turns
```

Every dependency must land in a **strictly earlier** turn than its dependent. A retry keeps
its original turn — a retried job must not jump ahead of what it depends on, nor be replanned
into a wave whose dependents have already run.

## 5. A cycle never reaches runtime

Cycles fail at **compile** time with the cycle named. If you are debugging a cycle at run
time, something bypassed the compiler.
