# GOVERNANCE — CURRENT STATE AUDIT

- Phase: **DRP0** (audit only — no AWS mutation, no install, no pipeline change)
- Date: **2026-09-30**
- Account `111122223333` · profile `my-aws-profile` · region `ap-southeast-1` · env `dev`
- Evidence class per CLAUDE.md §9.7: `static` (read from the repo), `live-read`
  (read-only AWS call made in this session), `doc` (asserted by a document only).

---

## 0. The headline finding

**The governance plane and the data plane have completely diverged.**

The three governance files — `governance/catalog/domains.yml`,
`governance/dq/rules.yml`, `governance/lineage/openlineage.yml` — were authored in
Sessions 14–15 against the old `stream. / full_cdc. / snapshot. / mart.` logical naming.
The platform then became config-driven from `cdc/registry/sources.yaml` (ADR-067…ADR-073)
and its physical names changed to `cdc_<engine>_<db>_<schema>_<table>`, `rt_*`, `eod_*`.
**The governance files were never migrated.**

Reconciled in this session against the live Glue Data Catalog (`live-read`):

| File | datasets declared | resolve to a live table | drift |
|---|---|---|---|
| `governance/catalog/domains.yml` | 12 | **1** (`mart.mart_customer_360_daily`) | 11 do not exist |
| `governance/dq/rules.yml` | 7 | **1** | 6 do not exist |
| `governance/lineage/openlineage.yml` | 11 Iceberg datasets (+9 external URIs) | **2** | 9 do not exist |

Inverted, which is the number that matters:

> **72 of the 73 live Glue tables have no owner, no domain, no classification, no PII
> marking, no retention, no freshness SLA and no DQ rule.**

Live census (`live-read`, `aws glue get-tables`):

| Glue database | tables |
|---|---|
| `kafka_dev_lab_dev_full_cdc` | 11 |
| `kafka_dev_lab_dev_stream` (REALTIME + L1) | 11 |
| `kafka_dev_lab_dev_snapshot` (EOD) | 10 |
| `kafka_dev_lab_dev_curated` | 20 |
| `kafka_dev_lab_dev_mart` | 7 |
| `kafka_dev_lab_dev_ops` | 14 |
| `kafka_dev_lab_dev_quarantine` | 0 |
| `kafka_dev_lab_dev_serving` | 0 |
| **total** | **73** |

### Why this is not cosmetic

1. **The AI assistant's access guard runs off the drifted registry.** `ai/guards.py:266`
   and `ai/agent_tools/catalog.py:92` read `domains.yml` as the authority for
   *"is this dataset BI-readable?"*. Against the live catalog that guard can only answer
   **deny** — for every real table — while happily describing eleven datasets that do not
   exist. A governance control that describes a phantom platform is worse than none,
   because it still returns confident answers. The drift has also been **indexed**:
   `ai/knowledge/build_index.py:35-36` ingests `governance/catalog/` and `governance/dq/`
   into `ai/knowledge/corpus.json`, so the phantom dataset names are now retrievable
   answers, not merely an unread file.
2. **`dq_engine.py` reads the drifted rules file at runtime** (`--rules
   /opt/governance/dq/rules.yml`, `spark/ops/dq_engine.py:317`). A run today evaluates
   seven non-existent datasets, every check returns `NOT_EVALUATED`, and
   `suite_blocks_publish()` correctly treats that as blocking. The engine is right; its
   input is three naming generations stale.
3. **It explains an absence that otherwise looks like a bug.** `ops.dq_result`,
   `ops.data_certification`, `ops.reconciliation_run`, `ops.contract_change`,
   `ops.metric_variance`, `ops.layer_watermark`, `ops.maintenance_run` and
   `ops.job_master` are all referenced in code and **none of them exists in the live
   `ops` database** (`live-read`). DQ, reconciliation and certification have never been
   executed against this platform. The EOD layer records its own truth in
   `ops.eod_run` / `ops.eod_info`, which do exist.

### Where the fix belongs

`cdc/registry/sources.yaml` **already carries per-table** `classification`,
`primary_key`, `dq.not_null`, `dq.freshness_sla_minutes`, `eod.retention_days` and
`maintenance` (`static`). It is the file the platform is actually driven from, so it is
the only registry that cannot drift without the pipeline noticing. It is missing only
`owner`, `domain`, `description`, `glossary_term` and `pii_columns`.

`domains.yml` was created because — in its own words — *"a second list of PII columns
somewhere else is exactly how masking and policy drift apart while both look
maintained."* That is precisely what happened to it. The conclusion is not to re-author
it; it is to **derive** it from the registry the platform already obeys, and to make
DataHub ingest from Glue so the catalog follows the platform instead of describing it
from memory. See ADR-087.

---

## 1. Architecture — confirmed against the code

```
Oracle / SQL Server → Debezium → Kafka (MSK, KRaft) → CDC Normalizer → FULL_CDC
                                                                          │
                                                  ┌───────────────────────┴──────────────────┐
                                                  ▼                                          ▼
                                              REALTIME                                      EOD
                                                  └───────────────────┬──────────────────────┘
                                                                      ▼
                                                              dbt-Spark → MART → Athena → Power BI
```

**Confirmed: REALTIME and EOD are siblings of FULL_CDC, not a chain.** `docs/LINEAGE.md`
carries the explicit 2026-08-21 correction that removed the old
`L1 STREAM → L2 FULL CDC → L3 SNAPSHOT` chain, and the physical layout agrees —
`stream.rt_*` and `snapshot.eod_*` are both derived from `full_cdc.cdc_*`.

Flow policies present in `reporting/schema/job.schema.json` (`static`): `EOD`,
`AUTO_CORRECT`, `FULFILL`, `STREAM_BATCH`, `STREAMING_RT` — all five, as documented.

---

## 2. Governance capability inventory

| # | Capability | Verdict | Evidence and reasoning |
|---|---|---|---|
| 1 | **Catalog** | **CONFLICT** | Two catalogs disagree. Glue holds 73 real tables (`live-read`); `domains.yml` describes 12, of which 1 exists. Neither is a superset of the other. |
| 2 | **Owner** | **PARTIAL → effectively MISSING** | `domains.yml` assigns owners for 12 datasets; 11 are phantoms. 72/73 live tables are unowned. |
| 3 | **Domain** | **PARTIAL → effectively MISSING** | `banking` / `digital` / `ops` domains are defined and sound; they are attached to datasets that do not exist. |
| 4 | **Description** | **MISSING** | No description on any live Glue table (`live-read` — `Description` absent). |
| 5 | **Glossary** | **MISSING** | No business glossary anywhere in the repo. No `glossary` key in any governance file. |
| 6 | **PII / classification** | **CONFLICT** | `domains.yml` marks `pii_columns` on phantom datasets. `cdc/registry/sources.yaml` carries a real per-table `classification` (default `internal`) for all 10 CDC tables — but nothing propagates it to Glue, Lake Formation or the masking views. |
| 7 | **Retention** | **PARTIAL** | Real and enforced for the CDC layers: `eod.retention_days: 365`, REALTIME `retention_hours`, `maintenance.expire_snapshots_days: 7` (`static`, executed by `cdc/maintenance.py`). Absent for `curated`, `mart`, `ops`. |
| 8 | **Contracts** | **PARTIAL** | `cdc/schema_guard.py` is a real ALLOW/PLAN/BLOCK evaluator with an asymmetry argument behind it, and `docs/DATA_CONTRACTS.md` exists. But `ops.contract_change` does not exist live — no contract decision has ever been recorded. |
| 9 | **Freshness SLA** | **PARTIAL** | Declared twice — `cdc/registry/sources.yaml` `dq.freshness_sla_minutes: 60` and `domains.yml` `freshness_sla_minutes: 1440`. **Two different values, two different files, neither measured.** No freshness check has ever run. |
| 10 | **Data quality** | **PARTIAL (engine PASS, execution MISSING)** | `spark/ops/dq_engine.py` is genuinely good: six check types, and the three-verdict design (`PASS`/`FAIL`/`NOT_EVALUATED`) that refuses to call an unevaluated check a pass. It has **never run against the live platform** — `ops.dq_result` does not exist. |
| 11 | **Reconciliation** | **PARTIAL** | `spark/common/reconcile.py` + `spark/ops/reconcile_job.py` exist and are unit-tested; `ops.reconciliation_run` does not exist live. |
| 12 | **Certification** | **PARTIAL** | Live and working for EOD — `closed=10 certified=5 rows=3404` on 2026-09-30 with the readiness gate enforced. But it certifies on *completeness of the close*, not on a DQ verdict: `ops.data_certification` does not exist and `dq_result_run_id` is passed as a literal `None` by its only producer (`spark/reporting/ops_client.py:359`). **Certified today means "the close finished", not "the data was checked".** |
| 13 | **Lineage** | **CONFLICT** | `governance/lineage/openlineage.yml` declares 8 jobs and marks the Spark hops `evidence: observed`. **Nothing emits them.** No `spark.extraListeners` appears in any script, Terraform file, Airflow DAG or job config (`static`, exhaustive grep). `ops.lineage_event` has DDL (`spark/ops/ddl/governance_tables.sql:34`) and **does not exist in Glue** (`live-read`). The `observed` marker claims telemetry that has never been produced. |
| 14 | **Column-level lineage** | **MISSING** | Nothing in the repo derives column lineage. `spark/features/lineage.py` projects the dbt manifest for *feature* lineage only, and is explicit that it is a projection, not a graph. |
| 15 | **Impact analysis** | **MISSING** | No forward-closure from a dataset to its consumers exists. `reporting/compile.py` and `spark/reporting/graph.py` compute a topological order over *reporting jobs*, which is the closest thing and covers only the mart layer. |
| 16 | **Incident** | **MISSING** | No incident object, store, state machine or lifecycle anywhere. |
| 17 | **Recovery** | **PARTIAL** | `airflow/dags/dag_recovery.py` is real and thoughtfully bounded (three shapes, refuses to default the business date). But it is **manual-trigger, hand-scoped and still written against the retired L1/L2/L3 naming** — it is not derived from lineage and has no blast-radius limit. |
| 18 | **Dependency truth** | **PASS** | Three real sources: the dbt manifest (695 KB, regenerated 2026-09-29), `reporting/compile.py` topological turns, and ADR-086's `source_tables` + `source_policy` declaration on STREAM_BATCH flows. |
| 19 | **OPS operational truth** | **PASS** | 14 live `ops` tables including `eod_run`, `eod_run_hist`, `eod_info`, `realtime_run`, `realtime_info`, `streaming_batch_ledger`, `streaming_app_state`. This is the healthiest part of the reliability plane. |
| 20 | **Quarantine / DLQ** | **PARTIAL** | Implemented in `spark/jobs/l1_stream/quarantine.py` and referenced by the FULL_CDC and REALTIME jobs. The `quarantine` Glue database exists and holds **0 tables** — never written, so never proven. |
| 21 | **Power BI metadata** | **NOT_APPLICABLE (today)** | No Power BI implementation exists outside docs and one test. There is no workspace to ingest from. |
| 22 | **Athena usage metadata** | **UNKNOWN** | The workgroup exists and is the default engine; whether query history is retained long enough to ingest was not checked in this audit (would require a `live-read` not performed). |

### Score

| Verdict | Count |
|---|---|
| PASS | 3 |
| PARTIAL | 9 |
| MISSING | 6 |
| CONFLICT | 3 |
| NOT_APPLICABLE | 1 |
| UNKNOWN | 1 |

---

## 3. DQ inventory — every check that exists today

| Where | Kind | Status |
|---|---|---|
| `spark/ops/dq_engine.py` | freshness, completeness, uniqueness, validity, referential integrity, reconciliation | code PASS, **never executed live** |
| `governance/dq/rules.yml` | 7 datasets, ERROR/WARN severities, `${business_date}` predicates | **drifted — 1 of 7 datasets exists** |
| `cdc/registry/sources.yaml` | per-table `dq.not_null`, `dq.event_date_null_tolerance`, `dq.freshness_sla_minutes` | **live and config-driven, but not executed by `dq_engine`** |
| dbt tests | `dbt/models/**` schema tests | run with dbt; results in `run_results.json`, not published anywhere |
| `cdc/schema_guard.py` | contract change ALLOW/PLAN/BLOCK | pure, unit-tested, no recorded decisions |
| `spark/common/reconcile.py` | cross-layer variance | unit-tested, no live run |
| `spark/jobs/l1_stream/quarantine.py` | poison-record capture | implemented, zero rows ever written |
| `spark/reporting/eod_flow.py:77` | treats `NOT_EVALUATED` as blocking | correct, but never fed a real DQ result |

**Two DQ systems exist and do not know about each other**: the rules file `dq_engine`
reads, and the per-table `dq:` block the CDC platform is actually driven from. The second
one is the live one.

---

## 4. Runtime versions — measured, not assumed

| Component | Version | How obtained |
|---|---|---|
| Python (workstation) | 3.10.8 | `python3 --version` |
| Java | OpenJDK 17.0.20.1 | `java -version` |
| PySpark (workstation) | 3.5.0 | `pyspark.__version__` |
| EMR Serverless release | **emr-7.2.0** | `terraform/envs/dev/variables.tf:418` |
| Spark (EMR 7.2.0) | 3.5.x, Scala **2.12** | EMR release mapping |
| Iceberg | **1.5.2** | `scripts/dbt-verify.sh:42`, `scripts/layout-benchmark.py:93` |
| Airflow (deployed) | **3.2.2**, Helm chart 1.22.0, `KubernetesExecutor` | `docs/VERSIONS.md:100-101` |
| Airflow (workstation package) | 2.9.3 | `airflow.__version__` — **workstation only; does not match the deployed cluster** |
| dbt-core / dbt-spark (pinned for EMR) | **1.9.11 / 1.9.3** | `docs/VERSIONS.md:104` |
| dbt-core (workstation) | 1.9.4 | `dbt --version` — **also does not match the pin** |
| MSK Kafka | 3.9.x KRaft | `docs/VERSIONS.md:81` |
| `openlineage-*`, `acryl-datahub` | **not installed** | `pip list` — nothing present |

Two mismatches are recorded deliberately: the workstation's Airflow (2.9.3) and dbt
(1.9.4) are **not** the deployed versions. Any OpenLineage provider compatibility decided
by testing on this workstation would be decided against the wrong Airflow major. That is
a DRP3 constraint, written down now so it is not discovered later.

### Version selection constraints this imposes (do NOT pin yet — DRP2/DRP3)

| Integration | Constraint derived here | Command that must resolve the pin |
|---|---|---|
| OpenLineage Spark | must be the **Scala 2.12** artifact for **Spark 3.5**, and must be verified against **Iceberg 1.5.2** | resolve `io.openlineage:openlineage-spark_2.12`, verify the release notes name Spark 3.5, record the SHA256 |
| OpenLineage Airflow | **Airflow 3.2.2**, so the provider must be an Airflow-3-compatible major — an Airflow-2-era provider will not load | resolve `apache-airflow-providers-openlineage` against Airflow 3.2.2, not against the workstation's 2.9.3 |
| OpenLineage dbt | must match **dbt-core 1.9.11** and be added to the EMR wheelhouse, which `docs/VERSIONS.md:104` says must be rebuilt whenever that pin moves | resolve `openlineage-dbt`, rebuild the wheelhouse |
| DataHub | server and `acryl-datahub` CLI must be the **same** release line; the OpenLineage-ingestion endpoint must exist in the chosen release | resolve the DataHub chart/image, confirm the OpenLineage endpoint is present in that exact version |

CLAUDE.md §3.9 forbids `latest`. Every one of these is left **unpinned on purpose** —
DRP0 does not install, and a pin chosen from a prompt rather than from a resolve is
exactly the defect `docs/VERSIONS.md` already records for dbt 1.8.9.

---

## 5. Cost and security constraints DataHub must satisfy

DataHub is not a small deployment: GMS, frontend, Elasticsearch, a metadata store and
Kafka. Under `lab_low_cost` (CLAUDE.md §4.1) that is the largest always-on component the
project would have, and §4.10 already keeps **Marquez** off for exactly this reason —
*"a 24/7 service; `ops.lineage_event` needs nothing running."* Rejecting Marquez on cost
and then accepting a heavier service without a decision would be inconsistent.

Therefore any DataHub adoption must carry, as ADR-087 requires:

1. `enable_datahub` feature flag, **default `false`** (CLAUDE.md §4.12).
2. A destroy + verify script, like every other module.
3. Private networking only — no `0.0.0.0/0`, SSM port-forward for the UI (§3.3, §3.4).
4. No static credentials; IRSA/instance profile for the Glue/Athena/S3 ingestion sources (§3.1, §3.2).
5. All six required tags including `AutoDestroyAfter` (§4.11).
6. **An offline path**, so that DRP4–DRP7 can be built and tested at $0 with the server
   down: DataHub ingestion recipes can emit to a `file` sink, and the resulting metadata
   events can be schema-validated in CI and loaded later. This keeps the checkpoint chain
   from becoming "we cannot test anything until an expensive service is up."

---

## 6. Open questions this audit could not answer

| # | Question | Why it is open |
|---|---|---|
| 1 | Does Athena retain enough query history to ingest usage metadata? | needs a `live-read` not performed in DRP0 |
| 2 | Which Glue/Iceberg table properties survive `register_tables.py` re-adoption? | matters because DataHub's Glue source reads exactly those properties; re-registration after the 2026-09-29 rebuild may have dropped comments |
| 3 | Can the k3s EC2 node host DataHub at all under `lab_low_cost`? | needs the node's instance type and free memory vs the chart's requests — a DRP2 sizing question |
| 4 | Is the 60-minute vs 1440-minute freshness SLA disagreement a real difference or a copy error? | both are declared; neither is measured, so there is no evidence to prefer one |

---

## 7. What DRP0 did NOT do

- No AWS mutation. The only AWS calls were `sts get-caller-identity`, `glue get-databases`
  and `glue get-tables`, all read-only.
- No install of `openlineage-*` or `acryl-datahub`.
- No pipeline, DAG, job or config change.
- No lineage edge created, and **no `evidence` marker corrected** — the `observed` claims
  in `openlineage.yml` are reported here and left in place, because changing them is a
  DRP3 action, not an audit action.
- No version pinned.
