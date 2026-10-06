# LINEAGE SOURCE MATRIX

- Phase: **DRP0** (audit only — nothing below is emitted yet)
- Date: **2026-09-30**
- Companion: `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md` §2 row 13, ADR-087

This matrix answers one question per hop: **where would a trustworthy lineage edge come
from, and what is it today?** It is not a design — it is the inventory the design must
respect.

---

## 1. Evidence classes

`docs/LINEAGE.md` already established that edges are not equally trustworthy, and that
distinction is kept and extended here:

| Class | Meaning | May a recovery plan act on it? |
|---|---|---|
| `observed` | a runtime emitted a real event for this specific run | **yes** |
| `derived` | computed from an artifact the producer itself wrote (dbt manifest, Iceberg metadata, OPS ledger) | **yes** |
| `declared` | asserted from config; no telemetry and no producer artifact | **no — plan only, never auto-execute** |
| `absent` | no edge exists at all | n/a |

The distinction is the safety rule. An impact plan built on a `declared` edge is a
hypothesis about what the platform does; acting on it automatically means deleting or
rewriting data on the strength of a YAML file nobody verified.

---

## 2. The matrix

| # | Hop | Producer | Metadata that exists **today** | Today's class | Achievable class | How it gets there |
|---|---|---|---|---|---|---|
| 1 | Oracle `coredb.corebank.*` → Debezium | `oracle-corebank-source` connector | `cdc/registry/sources.yaml` (5 tables, PKs), connector template, `docker/source-lab/oracle/02-enable-cdc.sql` | `declared` | **`declared`** | Debezium emits no OpenLineage. This hop can never be better than declared, and must stay marked as such. |
| 2 | SQL Server `digital.dbo.*` → Debezium | `sqlserver-digital-source` connector | same, 5 tables | `declared` | **`declared`** | as above |
| 3 | Debezium → Kafka topics | Kafka Connect | Connect REST `/connectors/{n}/topics` gives the **actual** topic list per connector | `declared` | **`derived`** | poll the Connect REST API; it reports what the connector really produced, not what the config asked for |
| 4 | Kafka → FULL_CDC | `spark/jobs/full_cdc/stream_job.py` (Structured Streaming) | `ops.streaming_batch_ledger`, `ops.streaming_app_state` — **live, 14 ops tables** | `absent` (no lineage event) / `derived` (from OPS) | **`observed`** | OpenLineage Spark listener. Note: streaming jobs emit per-micro-batch, which is a volume decision DRP3 must make explicitly. |
| 5 | FULL_CDC → REALTIME | `spark/jobs/realtime/realtime_engine.py` | `ops.realtime_run` (31 columns) and `ops.realtime_info` carry input snapshot, output snapshot, rows, read mode, fallback reason — **per run, live** | `derived` | **`observed` + `derived`** | this is the single best-instrumented hop in the platform; OPS already records exactly what OpenLineage would, per table per run |
| 6 | FULL_CDC → EOD | `spark/jobs/eod/eod_engine.py` | `ops.eod_run`, `ops.eod_run_hist`, `ops.eod_info`, `ops.eod_watermark` — live; last close `closed=10 certified=5 rows=3404` | `derived` | **`observed` + `derived`** | as above |
| 7 | REALTIME/EOD → dbt models | dbt-spark 1.9.x on EMR | `dbt/target/manifest.json` — 695 KB, regenerated 2026-09-29; `graph_summary.json` | `derived` | **`derived` + `observed`** | the manifest is already authoritative for node→node; `openlineage-dbt` adds the per-**run** dimension the manifest cannot have |
| 8 | STREAM_BATCH flow → its CDC sources | `reporting/jobs/*.yaml` | ADR-086 `source_tables` + `source_policy {preferred, fallback}` | `declared` | **`declared` (checked)** | the compiler cannot see through dbt `ref()` to a CDC registry entry, which is exactly why ADR-086 made it declared-and-validated. It stays declared; the validation is what makes it usable. |
| 9 | dbt → MART | dbt-spark | manifest + `run_results.json` | `derived` | **`derived` + `observed`** | as row 7 |
| 10 | MART → Athena | Athena workgroup | query history in the workgroup | `absent` | **`derived`** | Athena query history → parsed to table references. Retention window is **open question 1** in the audit. |
| 11 | Athena/MART → Power BI | — | none; no Power BI implementation exists | `absent` | **`declared`** at best | there is no Power BI workspace in this account. Out of scope until one exists. |
| 12 | Any job → quarantine | `spark/jobs/l1_stream/quarantine.py` | the `quarantine` Glue database exists and holds **0 tables** | `absent` | **`observed`** | the code path has never executed, so there is no edge to observe yet |
| 13 | Iceberg table → its own history | Iceberg / Glue | snapshot history, `start-snapshot-id`/`end-snapshot-id`, parent lineage | `derived` | **`derived`** | already exploited by ADR-083's cursor. This is lineage the storage layer gives away free, and the REALTIME cursor proves it is trustworthy. |

---

## 3. What the existing `openlineage.yml` gets wrong

Reported, not corrected — fixing it is a DRP3 action.

| Claim in `governance/lineage/openlineage.yml` | Reality |
|---|---|
| Spark hops marked `evidence: observed` | ~~Nothing emits~~ — **12 real events** from a local Spark 3.5.0 run, producer string matching the pin. `spark.extraListeners` is now derived by `cdc/lineage_runtime.py::spark_conf()` and injected by `scripts/emr-submit.sh` under `OPENLINEAGE=1`. Emission from **EMR** is still pending. |
| transport `iceberg_table: glue_catalog.ops.lineage_event` | the table has DDL (`spark/ops/ddl/governance_tables.sql:34`) and **does not exist** in the live `ops` database |
| `airflow.provider: apache-airflow-providers-openlineage` | ~~not installed~~ — **validated live 2026-09-30**: 2.20.2 on the deployed Airflow 3.2.2 emitted **7 real events** with `parent`/`root` run facets. The stock image already ships the provider at **2.17.0** and the plugin is registered; what the deployed release lacks is the **config** (no `AIRFLOW__OPENLINEAGE__*` set). The custom image exists to reach the pinned 2.20.2 / openlineage-python 1.53.0, matching the Spark jar. The env vars are in `airflow/helm/values.yaml`, and the transport value had to be corrected from the bare string `console` to `{"type":"console"}` — see ADR-091. |
| topic `kafka://cdc.oracle.corebank.CUSTOMER` | `cdc/naming.py:84` upper-cases schema and table for Oracle, so the real topic is `cdc.oracle.COREBANK.CUSTOMER`. The declared edge points at a topic that does not exist. |
| datasets `stream.oracle_corebank_customer`, `full_cdc.oracle_corebank_customer`, `snapshot.banking_customer`, `mart.dim_customer` | none exist. Live names are `stream.rt_oracle_coredb_corebank_customer`, `full_cdc.cdc_oracle_coredb_corebank_customer`, `snapshot.eod_oracle_coredb_corebank_customer`, `curated.dim_customer`. |
| 8-job graph covering 3 Oracle + 2 SQL Server tables | the platform runs **10** CDC tables across 6 layers |

The file is not wrong in *concept* — its declared/observed distinction is the right idea
and is preserved above. It is wrong in *every identifier*, which is what happens to a
hand-maintained graph when the platform underneath it becomes config-driven.

---

## 4. The asymmetry that shapes the design

Reading the matrix down the "Today's class" column:

- Rows 5, 6, 7, 13 are already `derived` from artifacts the producers themselves wrote.
  **OPS, the dbt manifest and Iceberg metadata are the platform's real lineage today**,
  and they are trustworthy because nothing had to be maintained by hand to keep them true.
- Rows 1, 2, 8 can never be better than `declared`, and saying so is the point.
- Rows 4, 10, 12 are genuinely absent.

So the work is **not** "install a lineage system". It is: publish the three derived
sources that already exist into one graph, add `observed` where a listener can honestly
produce it (rows 4–7, 9), and keep `declared` labelled so the recovery planner refuses to
auto-execute across it.

---

## 5. Consequence for DRP8/DRP9

The safety rule in the brief — *do not blindly rerun all descendants* — resolves through
this matrix into a concrete gate:

> A recovery subgraph may be executed automatically **only if every edge in it is
> `observed` or `derived`.** One `declared` edge on the path downgrades the whole plan to
> `RECOVERY_PLAN_REQUIRES_APPROVAL`.

Row 8 (STREAM_BATCH `source_tables`) is the interesting case: it is `declared` and
*validated*, and it is the only thing connecting a mart to the CDC table that feeds it.
A recovery that crosses it is therefore always operator-approved — which is the correct
outcome, because ADR-086 exists precisely because that link was once invisible and a mart
served a stale number while looking healthy.
