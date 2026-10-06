# DATA RELIABILITY — OBSERVABILITY

- Phase: DRP10 · Code: `cdc/reliability_metrics.py`
- **18 metrics, 7 SLOs (3 enforceable, 4 proposed), 6 alerts.**

---

## 1. Two planes, two SLO sets

> **A metadata SLO is not a data SLO.**

"The catalogue is 30 minutes stale" and "the mart is 30 minutes stale" are different promises
with different consequences. Merging them means a lineage backlog pages the person on call
for a late close.

| Plane | Asks | Example |
|---|---|---|
| **data** | is the number right, and on time? | `certification_age_seconds`, `dq_failures_by_layer` |
| **metadata** | does the catalogue describe reality? | `metadata_ingestion_age_seconds`, `orphan_critical_assets` |

## 2. An unmeasured target is not a target

Every SLO carries a `basis`, and the type refuses a `measured`/`inherited` target with no
evidence:

| Basis | Meaning | Count |
|---|---|---|
| `measured` | a real run produced it | 2 |
| `inherited` | an existing, enforced config value | 1 |
| `proposed` | **nobody has measured it** | 4 |

```
certification_age_seconds   a COB certifies by 06:00 UTC on D+1
                            MEASURED — close of 2026-09-30: closed=10 certified=5 rows=3404
freshness_violations        per-table SLA (60m default, 15m for transaction, digital_event)
                            INHERITED — cdc/registry/sources.yaml
metadata_publish_degraded   never fails a data flow; worst case 31.5 s per call
                            MEASURED — ClientSettings.worst_case_seconds, test-asserted

metadata_ingestion_age      24 h                    PROPOSED
impact_plan_latency         60 s                    PROPOSED
mttd / mttr                 one close cycle / COB   PROPOSED
```

**A `proposed` target may be reported against; it may not page anyone.** `alertable()`
enforces it. Paging on a guess wakes people for nothing, and the second time it happens the
alert is muted — which is how a real one gets missed later.

## 3. Alerts are deduplicated, not rate-limited

An alert storm is not "too many alerts". It is **one condition arriving as fifty**. Every rule
declares a `group_by`, and a rule without one is refused at construction.

| Alert | Severity | Grouped by | Runbook |
|---|---|---|---|
| `blocker_dq` | PAGE | cob_date, layer, check_id | RECOVERY_RUNBOOK §2 |
| `reconciliation_failure` | PAGE | cob_date, source_dataset | RECOVERY_RUNBOOK §1b |
| `late_certification` | PAGE | cob_date | EOD_SNAPSHOT_RUNBOOK |
| `recovery_failed` | PAGE | incident_id | RECOVERY_RUNBOOK §6 |
| `critical_lineage_stale` | TICKET | asset | LINEAGE_TROUBLESHOOTING §1 |
| `metadata_plane_outage` | TICKET | environment | DATAHUB_OPERATIONS_RUNBOOK §9 |

Ten tables failing the same check on the same COB is **one** page. A PAGE with no runbook is
refused: waking someone with no instructions is worse than not waking them.

Note which two are TICKET. A metadata-plane outage does not page, because by design it cannot
corrupt or block business data — the outage policy makes every data flow `BEST_EFFORT`.

## 4. The metrics

**Metadata plane (9)** — emission totals and failures, degraded publishes, ingestion age,
DataHub availability, orphan critical assets, lineage audit findings, impact-plan size and
latency.

**Data plane (9)** — DQ failures by layer and severity, freshness violations, reconciliation
failures, open incidents, auto-recovery attempts and success rate, MTTD, MTTR, certification
age.

`datahub_available` is a tri-state: `None` means **NOT CHECKED**, never `False`. A health
check that reports "down" when it never looked is the same defect as a DQ check reporting
PASS on an empty table.

## 5. What is not instrumented yet

| | Why |
|---|---|
| every metric above | nothing emits — no DataHub, no listener, no DQ run against a live table |
| `mttd` / `mttr` | require `ops.data_incident`, which is declared and not created |
| `auto_recovery_*` | require `ops.recovery_execution`, same |
| the Prometheus wiring | `spark/ops/metrics_push.py` exists for the data plane; the metadata-plane counters are not yet pushed |

The metric *catalogue* is the deliverable of this phase. The numbers arrive with DRP11.
