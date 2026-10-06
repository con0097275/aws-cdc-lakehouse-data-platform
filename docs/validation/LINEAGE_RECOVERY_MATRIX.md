# LINEAGE ↔ RECOVERY MATRIX

- Phase: DRP11 · What each lineage edge permits a recovery to do, and what it does not.
- Companions: `LINEAGE_SOURCE_MATRIX.md`, `DATA_INCIDENT_RECOVERY_MODEL.md`,
  `LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md`

---

## 1. The rule, once

> A recovery subgraph may execute automatically **only if every edge in it is `observed` or
> `derived`.** One `declared` edge downgrades the whole plan to
> `RECOVERY_PLAN_REQUIRES_APPROVAL`.

`decide_approval()` enforces it, and `plan.weakest_evidence` is the value it reads.

## 2. Per hop, today

| Hop | Evidence now | Auto-recoverable now | Becomes when |
|---|---|---|---|
| source DB → Kafka | `declared` | **no** | never — Debezium emits no telemetry |
| Kafka → FULL_CDC | `declared` | **no** | `observed`, when the Spark listener runs |
| FULL_CDC → REALTIME | `derived` (`ops.realtime_run`) | **yes** | `observed` with the listener |
| FULL_CDC → EOD | `derived` (`ops.eod_run`) | **yes** | `observed` with the listener |
| EOD → CURATED entity | `derived` (`curated/entities.yaml`) | **yes** | stays derived |
| CURATED entity → dim | `derived` | **yes** | stays derived |
| CURATED → fact | **`declared`** (Python only) | **no** | `derived`, when the fact builders read a config contract |
| dbt model → model | `derived` (manifest) | **yes** | stays derived |
| MART → Power BI | absent | n/a | `declared` at best |

## 3. The consequence, stated plainly

**Every mart recovery is operator-approved today**, because every mart is downstream of a
Kimball fact whose producer is declared only in Python. Verified on the real graph:

```
plan rp1:…  radius 14  turns [1,1,2,2,5,3]  weakest declared  cost medium
approval REQUIRES_APPROVAL
  the weakest lineage edge on this path is declared; automatic recovery may only
  traverse derived, observed
```

That is not a gap to work around. It is the gate refusing to rewrite data on the strength of
a human's reading of a Python function. Closing it means giving `spark/facts/` a config
contract like `reporting/curated/entities.yaml` — **not** lowering the gate.

## 4. What is auto-recoverable today

Only a recovery whose whole subgraph lives between FULL_CDC and the curated dimensions:

- rebuild `eod:<table>` for one COB from FULL_CDC
- rebuild `realtime:<table>` over a bounded window
- rebuild a conformed entity and its dimension

Anything reaching a fact or a mart requires approval.

## 5. The other nine conditions

Evidence is condition 4 of ten. The rest apply regardless:

| # | Condition | Enforced by |
|---|---|---|
| 1 | root cause known | `root_cause_status != unknown` |
| 2 | interval bounded | `AffectedScope.bounded` |
| 3 | failure class repairable by rerunning | `REPAIRABLE_BY_RERUN` |
| 5 | at least one registered executable job | `executable_descendants` non-empty |
| 6 | blast radius within limit | `max_blast_radius` |
| 7 | cost class within limit | `max_cost_class` |
| 8 | attempts not exhausted | `max_attempts` → `EXHAUSTED` |
| 9 | incident not terminal | `TERMINAL_STATUSES` |
| 10 | plan targets this incident | id match |

A `SOURCE_DEFECT` produces **no plan at all** — `WAITING_SOURCE_CORRECTION`, and nothing is
ever written back to Oracle or SQL Server.

## 6. Repair mechanism by layer

| Layer | Mechanism | Forbidden |
|---|---|---|
| FULL_CDC | approved CDC replay and reconcile | **offset or checkpoint reset** |
| REALTIME | bounded rebuild from FULL_CDC | full history rebuild |
| EOD | rebuild the same COB, same cutoff | closing a different COB |
| CURATED | conformance rebuild for the COB | rebuilding all dimensions |
| MART | dbt/Spark rerun over the affected interval | `dbt build` with no `--select` |
| STREAM_BATCH | bounded watermark window after analysis | resetting the watermark |
| AUTO_CORRECT | bounded keys and dates | `ALL_KEYS_IN_DATE` when per-key would do |
| FULFILL | the missing date only | the whole history |
| BI | **marked impacted, never refreshed** | an automatic refresh |

The FULL_CDC row is not theoretical. On 2026-09-29 a checkpoint outlived its MSK cluster and
the ingest reported `SUCCESS` with `batches: 0, rows: 0`. A recovery permitted to reset
checkpoints would have hidden that rather than fixed it.

## 7. Status

Every row above is **modelled and unit-tested; none has executed**. There is no incident
store, no plan store and no coordinator run. See `DATA_RELIABILITY_E2E.md` §5.
