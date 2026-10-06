# Lineage

- Session: 14
- Date: 2026-08-15
- Status: **configuration and graph static-validated; no events emitted**

---

## 1. What is tracked

```
Oracle / SQL Server → Debezium → Kafka → FULL_CDC (canonical, append-only)
                                              |
                              +---------------+---------------+
                              v                               v
                        REALTIME (T-N → T)              EOD (as-of T-1)
                              |                               |
                              +---------------+---------------+
                                              v
                              Athena / Power BI ← Data Mart ← Kimball

CORRECTED 2026-08-21: FULL_CDC is written straight from Kafka; REALTIME and EOD are
siblings derived from it. The old "L1 STREAM → L2 FULL CDC → L3 SNAPSHOT" chain had an
intermediate landing layer in the reporting path that ADR-033 forbids reporting from.
See docs/TARGET_ARCHITECTURE.md §3.
```

## 2. What OpenLineage adds that dbt does not

dbt already emits complete lineage for the models it owns (Session 11: `manifest.json`,
53 nodes, 7 sources, 2 exposures). **Rebuilding that here would be a second lineage system
restating the first** — the S11-3 objection.

What dbt *cannot* see is everything upstream of the marts: Debezium, Kafka, and the Spark
jobs building L1 → L2 → L3 → Kimball are invisible to a SQL compiler. That gap is what
OpenLineage covers, and the two graphs are joined at the marts.

## 3. Declared vs observed

Every edge carries an `evidence` marker, because they are not equally trustworthy:

| Evidence | Meaning | Which edges |
|---|---|---|
| `observed` | the Spark listener emits real START/COMPLETE/FAIL events | L1, L2, L3, Kimball |
| `dbt_manifest` | dbt already emits it; referenced, not duplicated | marts |
| **`declared`** | **asserted from config — no telemetry exists** | Debezium, Athena→Power BI |

**Debezium does not emit OpenLineage.** Recording that edge as `observed` would claim
telemetry that does not exist — the graph would look complete and one of its links would be
an assumption. `test_declared_edges_are_marked_as_declared` enforces the distinction.

## 4. The parent link

```
spark.openlineage.parentRunId:   ${AIRFLOW_RUN_ID}
spark.openlineage.parentJobName: ${AIRFLOW_DAG_ID}.${AIRFLOW_TASK_ID}
```

Without it every Spark job is an island and *"which DAG run produced this table"* is
unanswerable — which is the question anyone actually asks during an incident. Acceptance
criterion: *lineage events capture parent Airflow run and Spark job.*

## 5. Transport: an Iceberg table, not a service

`ops.lineage_event`, partitioned by `business_date`, queryable from Athena.

Marquez is the usual OpenLineage backend, but it is a **service that must run 24/7** to
receive events. An Iceberg table costs storage only and needs nothing running. Marquez stays
a feature flag, **off** (`CLAUDE.md` §4.10).

**Emission never fails the job it observes** (`fail_on_error: false`). A lineage backend
outage that takes down the pipeline has inverted the priority — lineage is metadata about
the work, not the work.

## 6. Querying it

```sql
-- What produced this table, and under which DAG run?
SELECT event_time, job_name, run_id, parent_job, parent_run_id, inputs
FROM   ops.lineage_event
WHERE  business_date = DATE '2026-08-14'
  AND  contains(outputs, 'mart.fact_transaction')
ORDER  BY event_time DESC;

-- Impact analysis: everything downstream of a source table.
SELECT job_name, outputs FROM ops.lineage_event
WHERE  contains(inputs, 'stream.oracle_corebank_customer');

-- Jobs that FAILED for a date.
SELECT job_name, run_id, parent_run_id FROM ops.lineage_event
WHERE  business_date = DATE '2026-08-14' AND event_type = 'FAIL';
```

## 7. Evidence

`test_governance.py::TestLineageCoversTheWholeChain` — 6 tests: the chain is connected from
source database to Power BI, declared edges are marked declared, the Airflow parent is
configured, emission cannot fail the job, managed backends are off with a recorded reason,
and dbt lineage is referenced rather than rebuilt.

**Not tested:** a single real lineage event. Nothing is deployed, so the listener has never
run. The configuration is validated; the telemetry is `NOT_TESTED`.
