# Reporting / Datamart Framework — Discovery Report

- Date: 2026-08-19
- Status: **DISCOVERY ONLY. No code changed, no infrastructure touched, no AWS call made.**
- Scope: design a metadata-driven reporting/datamart framework above the three CDC layers.
- Stop point: `REPORTING_ARCHITECTURE_DISCOVERY_COMPLETE` — awaiting review.

Everything below is derived from reading the actual source of the four reference
codebases and this repository. Where I state a fact about a reference project, the file
path is given so it can be checked.

---

## 1. Reference project inventory

| Project | Size | What it actually is | Runtime |
|---|---:|---|---|
| `data_pipelines-tutorials/data_pipelines` | 951 MB | ACB (bank) batch/reporting platform. ~110 pipelines, Airflow 2 + Impala/Kudu config tables + Iceberg on HDFS + YARN Spark. | On-prem CDH/CDP |
| `plstream_merged` | 27 MB | ACB near-real-time P&L reporting. Spark Structured Streaming from Kafka + Avro/Schema Registry → Iceberg, with an Airflow lifecycle DAG set. | On-prem YARN |
| `TTC-Repo code/ttc-dlh-ops-dbt-library` | — | dbt-spark project (`ttc_dbt`), Glue catalog, Iceberg, dual target spark/redshift. | AWS |
| `TTC-Repo code/ttc-dlh-ops-data-platform` | — | Terraform: EMR Serverless + Step Functions + Lambda orchestrator + DynamoDB job registry. **No Airflow.** | AWS |
| `TTC-Repo code/ttc-dlh-ops-etl-library` | — | `express_data_transform` Python package: Spark node primitives, extraction, OpenMetadata client. Packaged to S3 via Terraform `null_resource`. | AWS |
| `TTC-Repo code/ttc-dlh-shared-service-base-infra` | — | Base account/network IaC. Nothing reporting-specific. | AWS |
| `aws-cdc-lakehouse` (target) | 1.7 GB (mostly `.terraform`) | This project. 481 tracked files. | AWS |

Key files actually read:

```
data_pipelines/src/scheduler/airflow/plugins/job_scheduler/functions.py      1480 lines
data_pipelines/src/scheduler/airflow/plugins/job_scheduler/settings.py         59
data_pipelines/src/scheduler/airflow/dags/pipelines/pipeline_builder.py      1920
data_pipelines/src/scheduler/airflow/dags/pipelines/pipeline_auto_generator.py 156
data_pipelines/src/scheduler/airflow/dags/orchestration/coordinator.py        116
data_pipelines/src/scheduler/airflow/dags/orchestration/coordinator_graph_builder.py 67
data_pipelines/src/c2pp/init/data_lake_init.sql                (the config-table DDL)
data_pipelines/src/lib_common/lib/helpers.py                                 941
data_pipelines/src/pipelines/f_acct_depo_by_day/code/*_auto_correct_generate.py 1462
plstream_merged/.../1_phase_a/flow3_pl_stream_main/src/main.py              1128
plstream_merged/.../v9/airflow/dags/bcn_realtime_dags.py                     555
plstream_merged/.../v9/airflow/configs/bcn_pipeline.yaml                     175
TTC/.../data_platform/lambda/orchestration_lambda/execute_job_logic.py       232
TTC/.../data_platform/emr-serverless/run_dbt_job.py
TTC/ttc_dbt/dbt_project.yml, profiles.yml, macros/*
```

---

## 2. ACB patterns found

### 2.1 The config/runtime table set (`src/c2pp/init/data_lake_init.sql`)

Six tables, all in Impala/Kudu (upsertable, low-latency):

```
job_master                    PK(job_name)                     ← mutable current state
job_master_execution_hist     PK(date_of_data, job_name, turn) ← per-attempt runtime
job_resource_config           PK(job_name)                     ← Spark resources + app path
job_master_run_mode           PK(run_mode)
job_master_coordinator_mode   PK(job_type)                     ← per-job-type coordinator policy
summary_config (v1)           PK(job_name)                     ← the resolved coordinator plan
summary_config_hist_v1        append-only                      ← planning/decision history
```

**`job_master` is NOT pure config.** It carries `date_of_data` and is mutated by
`sync_job_master_hist()` (`functions.py:1119`) — an `UPDATE ... FROM` that pushes the
`date_of_data` of non-EOD jobs forward to follow their `before_full` upstreams. So the
"master" table is really *current position + config*, which is precisely the mixing the
new design is asked to undo.

**Dependencies are comma-separated strings.** `before_direct`, `after_direct`,
`before_full`, `after_full`, `dwh_dependencies` are all `VARCHAR(1000)`. They are matched
in SQL with `instr(concat(",", b.after_full, ","), concat(",", a.job_name, ",")) > 0`
(`functions.py:107`). Splitting happens in Python at load time. There is no graph
structure and no cycle detection anywhere in the codebase.

**`order_by_default` / `order_by_mode` are hand-maintained integers**, not computed. The
coordinator groups jobs by that integer into "turns"
(`coordinator_graph_builder.py:22-28`) — so a wrong integer silently produces a wrong
execution order with no error.

### 2.2 EOD

EOD readiness is a **three-way gate** (`lib_common/lib/helpers.py:530 get_ready_eod`):

1. `dlpt.eod_info` has a row with `cob_date = execution_date` for each dependency table;
2. the Iceberg source snapshot table has a **ref/tag named `TAG_<execution_date>`**
   (`select * from integration.<tbl>.refs where name = 'TAG_<date>'`);
3. upstream *jobs* (not tables) have already advanced their `summary_config_v1.execution_date`
   past `execution_date`.

`is_ready_eod = 1` only if all dependencies are covered by (1)+(3) **and** the tag count
matches. Non-working days short-circuit to ready (`dim_times.is_working_day = 'N'`).

This is the single best idea in the ACB codebase: **the EOD close signal is an Iceberg
tag, atomic with the data**, which also gives the consumer a snapshot to time-travel to.

`get_eod_range_time()` (`helpers.py:321`) then derives a per-source-table window
`[prev_datelastmaint, datelastmaint)` plus a resolved `<tbl>_snp_id` per dependency, so
the job reads a **pinned snapshot**, not "whatever is current".

### 2.3 AUTO_CORRECT

`f_acct_depo_by_day_level_acct_auto_correct_generate.py` (1462 lines) runs on a cadence
during the day. Its actual shape:

1. read `summary_config_v1` → `execution_date`, `dependencies`, `is_first_running`;
2. `get_ready_eod()` → is today's source data closed yet;
3. **cherry-pick rollback**: read `auto_correct_snapshot_id` for the previous run of this
   `execution_date`, then
   `CALL system.set_current_snapshot('dlpt.<tbl>', <snapshot_id>)` on ~8 target tables
   before recomputing (line ~200). That is how the job is made idempotent — it *rewinds
   the target* and rebuilds.
4. recompute the day, write, update PIT tables;
5. append to `summary_config_hist_v1` with `indicator = 'automation' | 'manual'`;
6. if `is_ready_eod` → advance `summary_config_v1.execution_date` to the next working day
   and set `is_first_running = 1`;
7. record the new Iceberg snapshot ids into `auto_correct_snapshot_id` for the next run's
   rollback point.

So in ACB, **AUTO_CORRECT and EOD are the same job**; "EOD" is just the run of
auto-correct that happens to find the sources closed and therefore rolls the date forward.

### 2.4 FULFILL / LOAD_HIST

Two distinct things, both mapped to FULFILL in the new design:

- `*_auto_correct_generate_fulfilled.py` — a *catch-up close* for a business date whose
  EOD arrived late. Same shape as auto-correct but keyed on the pending date.
- `job_type = 'load_hist'` → `build_alone_job_dag()` — a standalone historical backfill
  DAG, `is_paused_upon_creation=True`, `use_execution_hist: False`, run manually with an
  explicit date range.

There is **no gap detection**. Nobody computes "which dates are missing". An engineer
picks the dates by hand.

### 2.5 STREAM (micro-batch)

`f_acct_depo_by_day_level_acct_streaming_generate.py` — Airflow-triggered, `job_type =
'stream'` / `'stream_ref'`, `build_stream_job_dag()`. Watermark is a single-row Iceberg
table `dim_max_time_<flow>` holding `max_systime_rawvault` (+ `dv_kaf_ofs`), read as
`start_system_time` with a `before_system_date_time = 3` minute safety rewind
(line 192-203). Written only after success. This is a proper watermark, just per-flow
rather than per-job/mode.

### 2.6 Coordinator

`coordinator.py` builds **one coordinator DAG per `job_type`**. Each job is a
`TriggerDagRunOperator(wait_for_completion=True, deferrable=True, poke_interval=15)`
against a *separate per-job DAG*. `coordinator_mode` selects the wiring:

| mode | wiring |
|---|---|
| `default` / `custom` | turn *i-1* fan-out to turn *i* (bipartite, coarse) |
| `native` | true edges from `after_direct` |
| `sequential` | `chain(*tasks)` — everything serial |

Config is loaded once at DAG-parse time from Impala and **cached to a JSON file**
(`/home/dlpt/tmp/airflow/config/job_master_config.json`, 7-day TTL), with an Airflow
Variable `config_source ∈ {database, cache_file}` flipping between them
(`functions.py:28-95`). After the first load the coordinator *sets the variable back to
`cache_file`*. This exists because Airflow re-parses DAG files continuously and hitting
the warehouse each parse is unacceptable. **This pattern is directly relevant to us and
more so on AWS, where every parse would be an Athena/Glue call that costs money.**

### 2.7 Execution history and status lifecycle

`JobStatus` (`settings.py:44`): `INITIALIZED, PROCESSING, KILLED,
KILLED_BY_DEACTIVATION, SUCCEEDED, SUCCEEDED_BY_SKIPPING, FAILED`.

`turn` is the attempt counter *within a `date_of_data`*. `gen_new_execution_hist_for_job()`
(`functions.py:953`) computes the next turn as
`CASE WHEN status IS NULL THEN turn ELSE turn + 1 END` — i.e. an un-started row is reused,
a finished row spawns a new turn.

`run_mode` gates dependency satisfaction (`pipeline_builder.py:543 handle_run_mode`):

- `cross_turn` — a dependency satisfied at *any* turn ≥ mine counts; the satisfying turn
  is recorded in `turn_watermark` (a JSON dict `{job: turn}`) so the next attempt does not
  re-check from scratch;
- `turn_by_turn` — the dependency must have succeeded at *exactly* my turn.

Spark tracking: `spark_app_id`, `spark_app_status`, `spark_app_tracking_url`, plus
`get_spark_error_msg()` which greps YARN logs and contains a genuinely alarming
heuristic — it looks for the literal string `JOB_DONE` in the log and, if found, *flips a
FAILED run to SUCCEEDED* (`functions.py:363`, used at `pipeline_builder.py:659`). Do not
port that.

### 2.8 The `c2pp` sub-framework (the good part)

`src/c2pp/` is ACB's own second attempt: **one YAML per report in Git** →
`dag_generator.py` → dynamic DAG. The YAML (`configs/tiennv02/yamls/rpt_abc_123.yaml`)
carries job identity, four dependency classes (`rawvault_/bizvault_/dwh2cdp_/dwh_`),
Spark resources, an ordered `procedures:` map (`source_view → sink_table`,
`loading_engine`, `write_type`, `business_keys`), and mail config. No database row is
edited to ship a report.

That is the direction the reference project itself was moving in, and it is the direction
this design should take.

---

## 3. `plstream_merged` patterns found

Application: `1_phase_a/flow3_pl_stream_main/src/main.py`.

| Aspect | What it does |
|---|---|
| Source | Kafka **directly** (8 Debezium topics), Avro + Schema Registry, `starting_offsets: latest`, `max_offsets_per_trigger: 100000`, `fail_on_data_loss: false` |
| Shape | `readStream` → `.foreachBatch(handler.process)` → `.trigger(processingTime="30 seconds")` → `awaitTermination()` |
| Checkpoint | `hdfs://.../warehouse/_checkpoint/pl_init_bcn_sodu` — one stable path per app, config-driven |
| Restart safety | `_warn_if_stale_checkpoint()` (line 813) warns but **never auto-deletes** the checkpoint; it explains that `starting_offsets: latest` only applies when the checkpoint is empty |
| Failure handling | Explicit and correct: "with foreachBatch, returning quietly = Spark commits the offset = data loss" (line 299, 540). Enrichment failures are audited, not swallowed |
| Late/missing dims | `PendingQueue` (bounded, 10 000, 60 s max wait, `insert_null_with_flag` on timeout) + `dim_pending_timeout_acctnbrs` table |
| Dim refresh | `DimensionService` background thread; three priority classes with different refresh intervals (65 s / 65 s / 4020 s — deliberately a prime to avoid beating against the 300 s compaction) |
| Change detection | `DimChangeUpdater` **detect-only**: writes an HDFS event marker + `dim_dim_change_audit`; the *batch* autocorrect is the only writer of dim-driven corrections |
| Watermark | `dim_autocorrect_watermark`, also used to detect "first run after EOD" and force a full cache reload |
| Compaction | inline in the batch autocorrect, not in the stream: `target_file_size` 64 MB stream / 128 MB base, `min_input_files: 5`, `expire_snapshots_days: 7`, `retain_last_snapshots: 5` |

**Lifecycle is Airflow's job, processing is not** (`v9/airflow/dags/bcn_realtime_dags.py`,
`configs/bcn_pipeline.yaml`):

```
05:00  bcn_eod            resolve cob_date from dim_times (nearest working day BEFORE run
                          date — explicitly fixed away from "run date minus N")
                          → poll eod_info (48 × 5 min) → kill leftover YARN apps
                          → eod_clean → eod_build_base → write HDFS marker eod_done_<date>
08:00  bcn_stream_start   wait for marker → nohup submit → poll YARN for RUNNING (15 min)
08:00  bcn_datamart_start same, independent of stream
09–19  bcn_autocorrect    every 3 h; refuses to run unless the stream app is RUNNING
20:00  bcn_shutdown       yarn kill by app NAME
```

The author chose an **HDFS marker over `ExternalTaskSensor`** explicitly, to avoid
execution-date skew between DAGs on different schedules. That reasoning holds on AWS too.

**Conclusion for §33:** `job_master_execution_hist + job_watermark_state` cannot represent
this cleanly. `restart_count`, `last_batch_id`, `last_progress_at`, `deployment_id` and
"is the app up right now" are properties of a *process*, not of a run or a position.
`ops.streaming_app_state` is justified.

---

## 4. TTC dbt-spark patterns found

**Execution model — this is the important one.** `run_dbt_job.py` runs on **EMR
Serverless**, and:

1. downloads `s3://<code>/dbt/ttc_dbt.zip` and unzips it;
2. reads the job row from DynamoDB: `DBT_JOB_TBL.get_item(Key={"id": jobId})`;
3. **writes `profiles.yml` at runtime** (secrets from Secrets Manager, never in Git);
4. invokes dbt through the **Python API**, not the CLI:

```python
dbt = dbtRunner()
cli_args = ["run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
            "--target", job["target"], "-s", job["modelFqn"],
            "--vars", "{data_date: " + args.dataDate + "}"]
result: dbtRunnerResult = dbt.invoke(cli_args)
```

5. on success registers table metadata + lineage in OpenMetadata;
6. retries up to 8 times in a `while not is_successful` loop.

The dbt profile is **`type: spark, method: session`** — dbt runs *inside the EMR
Serverless Spark driver*. There is no Thrift server, no always-on endpoint, no JDBC. The
Glue catalog is reached through `spark.sql.catalog.glue` +
`spark.sql.defaultCatalog=glue`, and `spark.sql.catalog.glue.glue.id` allows cross-account
catalog access.

Other patterns worth taking:

- `dbt_project.yml` nests models by **database → schema → layer** (`agrc.harvest.marts`)
  and sets `+database` per node via a Jinja conditional on `target.name`, so the same model
  tree materialises to Glue/Iceberg on the spark target and to Redshift on the redshift
  target. `+file_format: iceberg` on staging/intermediate, plain tables in marts.
- `macros/get_custom_schema.sql` derives the schema from `node.fqn` rather than requiring
  each model to declare one.
- `macros/spark_temp_table.sql` — a custom `temp_table` materialization that emits
  `CREATE OR REPLACE TEMPORARY VIEW`, i.e. an ephemeral that stays a real view within the
  session.
- Orchestration is **Step Functions + Lambda**, with the Lambda holding all the job
  registry logic and DynamoDB tables `-job`, `-dbt-job`, `-etl-execution`, `-jobrerun`.
  Status polling is a `Wait 20 Seconds → Get Job Status → Choice` loop.
- EMR Serverless application: `emr-7.0.0`, `auto_start`, `auto_stop idle 15 min`,
  `maximum_capacity 400 vCPU / 3000 GB`, plus a small `initial_capacity` warm pool.
- The Python library is packaged to S3 by a Terraform `null_resource` triggered on
  `sha1` of the source tree — deployment is part of `terraform apply`.

What to **not** copy: DynamoDB as the *only* config store with no Git representation;
OpenMetadata coupling; the 8-attempt retry loop around `dbt.invoke` (retrying a
non-transient model failure 8 times is 8× the EMR bill).

---

## 5. Current `aws-cdc-lakehouse` state

### 5.1 Layers as actually implemented

```
L1  glue db  <prefix>_stream      warehouse/stream/     raw CDC events, all I/U/D
L2  glue db  <prefix>_full_cdc    warehouse/full_cdc/   immutable full history
L3  glue db  <prefix>_snapshot    warehouse/snapshot/   dedup as-of T-1 + _history variant
    glue db  <prefix>_curated     warehouse/curated/
    glue db  <prefix>_mart        warehouse/mart/       Kimball facts/dims + dbt marts
    glue db  <prefix>_ops         warehouse/ops/        ledgers
    glue db  <prefix>_quarantine  warehouse/quarantine/
```

`checkpoints/` is a **top-level S3 sibling** of `warehouse/`, enforced by a Terraform
variable validation (`modules/data_lake/variables.tf:110`) because
`remove_orphan_files` would otherwise eat streaming state.

### 5.2 Four flows already exist

`spark/common/flows.py` + `flow_runner.py` + `run_flow.py` implement NRT, AUTO_CORRECT,
EOD, FULL_FILL over **one** transformation (`transform.py::build_fact_transaction`), with:

- an accuracy ladder `CERTIFIED(4) > RECONCILED(3) > PROVISIONAL_CORRECTED(2) > PROVISIONAL_NRT(1)`;
- `may_overwrite()` and `merge_condition_sql()` — a guarded MERGE where a higher tier
  always wins and, within a tier, the later `input_cutoff` wins. A test asserts the Python
  and the generated SQL agree on every ordered pair;
- `FlowContext` refusing a naive `input_cutoff` (ADR-024, UTC).

This is a **better idempotency mechanism than ACB's snapshot cherry-pick** and should be
kept.

### 5.3 ops ledgers that already exist

| Table | Source | Shape |
|---|---|---|
| `ops.layer_watermark` | `spark/eod/ddl/ops_tables.sql` | layer, table, watermark_ts, run_id |
| `ops.reconciliation_run` | same | append-only, per run counts + status |
| `ops.eod_watermark` | same | **written only after write + validation both pass** |
| `ops.data_certification` | `spark/common/ddl/mart_and_certification.sql` | append-only, per flow run |
| `ops.metric_variance` | same | provisional vs certified variance |
| `ops.dq_result` | `spark/ops/ddl/governance_tables.sql` | PASS / FAIL / **NOT_EVALUATED**, ERROR blocks |
| `ops.lineage_event` | same | OpenLineage shape, carries `parent_run_id` |
| `ops.contract_change`, `ops.quarantine_invalid` | same | |

`ops.eod_watermark` is already the EOD-close gate this design needs. It does not need to
be invented.

### 5.4 dbt project

`dbt/` — dbt-core `>=1.9,<1.10`, profile `aws_cdc_lakehouse`, no `packages.yml` on
purpose. 3 staging + 3 intermediate (all `ephemeral`) + 4 marts (`incremental`,
`insert_overwrite`, partitioned by `business_date`, `file_format: iceberg`). 8 singular
tests, 47 dbt tests passing. `vars: business_date` defaulted to `1970-01-01` and
overridden per run. Reads Spark-produced tables through `sources.yml`; explicitly does
**not** own the transformation (`docs/DBT_SPARK.md §2`).

`dbt/profiles.yml.example` offers two targets: `local` (`method: session`) and
`emr_serverless` (**`method: thrift`**, requiring `DBT_SPARK_HOST`/`PORT`).

### 5.5 Airflow

5 DAGs, all orchestration-only and test-enforced (`airflow/tests/test_dags.py`):
`eod_certified_pipeline` (`30 1 * * *`), `flow_nrt_mart` (`*/5`), `flow_auto_correct`
(`*/30`), `dag_l1_stream`, `dag_recovery`, `dag_cdc_health`. `common.py` establishes
pools (`spark_jobs`, `maintenance`), `emr_submit()`, and a **deterministic** run id
`{{ dag.dag_id }}__{{ ds_nodash }}__{{ ti.try_number }}` — chosen over a UUID precisely so
retries are recognised as repeats.

### 5.6 Deployment reality (from `PROJECT_STATE.md`, Session 20/21)

- **Applied:** S3 lake (13 prefixes, SSE-KMS, versioned, TLS-only), 7 Glue databases,
  Athena workgroup with a 10 GiB cutoff *proven enforced*, lake KMS CMK, 6 lifecycle
  rules, tag-filtered $30 budget. 32 resources, drift 0, idle ~$1.01/month.
- **Not created:** MSK, the EMR Serverless application, Airflow, **and every Iceberg
  table** — table creation is deliberately deferred to the metered CDC window because
  Spark creates them on first write.
- **Flag discrepancy to resolve:** `envs/dev/terraform.tfvars` commits
  `enable_kafka_platform = true`, `enable_emr_serverless = true`, `enable_airflow = false`,
  but `PROJECT_STATE.md` records the applied cheap tier as having run with
  `enable_kafka_platform=false` (hence 32 resources and no MSK). Since
  `emr_serverless`, `lake_iam` and `airflow_k3s` are all `count`-gated on
  `enable_kafka_platform`, none of them exist today regardless of their own flags.
- 417 Python + 47 dbt tests pass locally against real Spark + Iceberg.

---

## 6. Current infrastructure that can be reused

| Need | Reuse | Status |
|---|---|---|
| Ops metadata database | Glue db `<prefix>_ops` | **exists, deployed** |
| Ops table storage | `s3://<lake>/warehouse/ops/` | **exists, deployed** |
| Streaming checkpoints | `s3://<lake>/checkpoints/` (top-level, 14-day noncurrent expiry) | **exists, deployed** |
| Spark compute | `modules/emr_serverless` (auto-stop, max capacity, private subnets) | written, not applied (gated on `enable_kafka_platform`) |
| Orchestration | `modules/airflow_k3s` + 5 DAGs + pools | written, not applied (`enable_airflow=false`) |
| dbt | `dbt/` project, macros, tests | exists |
| Catalog | Glue Data Catalog | **exists, deployed** |
| Query/validation | Athena workgroup with bytes cutoff | **exists, deployed, live-tested** |
| IAM | `modules/lake_iam` per-workload roles | written, not applied (gated on kafka+athena) |
| Cost guardrails | `modules/budget_guardrails`, `scripts/plan_cost.py`, `verify-destroy.sh` | **exists** |
| Run-id / idempotency contract | `airflow/dags/common.py`, `flows.py` | exists |
| Business-date discipline | `{{ params.business_date or ds }}` everywhere | exists |

Nothing in §39's "do not create" list is needed.

---

## 7. Gap analysis

### G1 — Layer naming conflict (**blocking, must be settled before any code**)

The master prompt and the 3-layer prompt name the canonical layers
`FULL_CDC / EOD / REALTIME`. The deployed repository names them
`stream / full_cdc / snapshot`, and `full_cdc` means something *different* in each:

| Prompt term | Definition in prompt | Nearest thing in repo |
|---|---|---|
| `FULL_CDC` | append-only complete CDC history, source of truth | **`stream` ∪ `full_cdc`** — the repo splits raw landing (L1) from the immutable history (L2) |
| `EOD` | dedup stable full business state as of COB T-1 | **`snapshot`** (`L3`, as-of T-1, deletes applied, opt-in `_history`) |
| `REALTIME` | bounded rolling recent CDC window T-N → T | **does not exist** |

Three Glue databases are already created in AWS with the old names; renaming them is a
destroy/recreate of catalog objects and a rewrite of every `sources.yml`, DDL and doc.

**Recommendation:** do not rename anything physical. Introduce a **logical layer name**
that the framework resolves to a physical table, and build `REALTIME` as a genuinely new
table set. Recorded as an ADR. The logical→physical map lives in one config file, which is
exactly the `resolve_source_layer()` abstraction §27 asks for.

### G2 — Iceberg is the wrong store for hot mutable job state

Every ACB config/runtime table lives in **Kudu via Impala** — millisecond upserts,
row-level updates, no metadata churn. On this platform the equivalent would be Iceberg on
S3 through Glue, and that is a poor fit for the runtime tables:

- a status transition per task (`PLANNED → READY → SUBMITTED → RUNNING → …`) is a
  single-row update; each one creates an Iceberg snapshot and a metadata file;
- a coordinator polling 40 jobs every 15 s would produce thousands of snapshots a day per
  table and force constant `expire_snapshots`;
- reads from Airflow would be Athena queries — **billed per query, seconds of latency**,
  and CLAUDE.md §4.7 puts a bytes-scanned cutoff on the workgroup precisely to catch this
  class of access.

TTC solved the same problem with DynamoDB (`-etl-execution`, `-dbt-job`, PAY_PER_REQUEST).

**Recommendation:** split by access pattern —
**DynamoDB for hot mutable state, Iceberg `ops.*` for append-only audit.** Cost is cents;
it removes the whole snapshot-churn class of problem. Detailed in §9.

### G3 — No `REALTIME` layer

Nothing bounded-and-rolling exists. `STREAM_BATCH` currently reads L1 for the whole day
(`flow_runner.py`, `FLOW_AUTO_CORRECT`) or incrementally by watermark (`FLOW_NRT`). A
REALTIME table is needed as a first-class artefact, or `source_layer_policy = REALTIME`
has nothing to point at.

### G4 — No job registry, no dependency graph, no turn model

There is no `job_master`, `job_flow_config`, `job_dependency` or coordinator anywhere. The
DAGs hardcode their job lists (`dag_eod_pipeline.py` iterates a literal tuple of entities;
`MAINTAINED_TABLES` is a literal string). Adding a mart today means editing a DAG — the
exact thing §29 forbids.

### G5 — dbt is invoked by nothing

`dbt/` exists and is tested, but no DAG, script or Spark job calls it. `run-flow.sh`
drives the four Spark flows only. There is no manifest sync, no artifact upload, no
`--vars` contract, no selector strategy.

### G6 — dbt `emr_serverless` target uses `method: thrift`

`dbt/profiles.yml.example` requires `DBT_SPARK_HOST`/`DBT_SPARK_PORT`. EMR Serverless does
not expose a Thrift endpoint; providing one means an always-on Spark app, which CLAUDE.md
§4.5/§4.6 rule out. TTC's `method: session` inside an EMR Serverless job is the shape that
actually works and costs only the seconds it runs. **This is a correction to existing repo
config, not a new feature.**

### G7 — No FULFILL gap detection

`spark/full_fill/full_fill.py` is a *dimension-key repair* flow (rows carrying
`UNKNOWN_SK = -1`). It is **not** the FULFILL of §8 (missing business dates). Both are
legitimate; they need different names, or one will silently be used for the other.
ACB has the same confusion (`fulfilled` = late close, `load_hist` = backfill, and neither
detects gaps).

### G8 — No STREAMING_RT anything

No long-running app, no `ops.streaming_app_state`, no lifecycle DAGs, no restart policy.
`flow_runner.py`'s docstring records a *deliberate* decision against always-on streaming
on cost grounds. That decision must be revisited explicitly, not contradicted quietly.

### G9 — No resource profiles

`common.py::emr_submit()` hardcodes three Spark confs for every job. No `small/medium/large`.

### G10 — Airflow is not deployed

`enable_airflow = false`, and it is gated behind `enable_emr_serverless && enable_kafka_platform
&& enable_athena`. Any orchestration work is `static-validated` at best until a metered
window opens.

---

## 8. Proposed final reporting architecture

```
                        Git  (source of truth)
                 reporting/jobs/*.yaml   dbt/models/**
                            │
                    make reporting-compile
       (validate → resolve graph → detect cycles → assign turns
        → merge dbt manifest deps → emit plan.json + checksum)
                            │
              s3://<lake>/ops/config/<version>/plan.json
                            │
        ┌───────────────────┼─────────────────────────┐
        │                   │                         │
   Airflow DAGs        Coordinator              ops registry
  (parse plan.json    (per flow_mode:          DynamoDB: hot state
   from local cache;   resolve → gate →        Iceberg  ops.*: audit
   never queries        submit → track)
   the warehouse)
                            │
                     EMR Serverless
              run_dbt_job.py  (dbtRunner, method: session)
              run_spark_job.py (existing spark/ entrypoints)
                            │
        ┌───────────────────┴───────────────────┐
   read layers                             write mart
   FULL_CDC / EOD / REALTIME               mart.<table>  (guarded MERGE)
        │                                        │
   resolve_source_layer(table, flow_mode)   validation (dbt tests + ops.dq_result)
                                                 │
                                          advance watermark  ← only here
```

### 8.1 Logical layer names (settles G1)

| Logical (framework + config + ADR) | Physical (deployed, unchanged) |
|---|---|
| `FULL_CDC` | `<prefix>_full_cdc.*` (L2, immutable history) |
| `FULL_CDC_RAW` | `<prefix>_stream.*` (L1, landing; not a reporting source) |
| `EOD` | `<prefix>_snapshot.*` (L3, as-of T-1) |
| `REALTIME` | `<prefix>_realtime.*` (**new**) |
| `CURATED` | `<prefix>_curated.*` |
| `MART` | `<prefix>_mart.*` |
| `OPS` | `<prefix>_ops.*` |

Business SQL never writes a physical database name; it calls `resolve_source_layer()`.

### 8.2 Consumption policy (§11), as configuration not convention

```yaml
source_layer_policy:
  EOD:           EOD                    # certified daily marts
  AUTO_CORRECT:  EOD_PLUS_REALTIME      # baseline + recent changes
  FULFILL:       FULL_CDC_OR_EOD        # by target date vs EOD availability
  STREAM_BATCH:  REALTIME               # bounded window
  STREAMING_RT:  FULL_CDC_APPEND        # or KAFKA_DIRECT, per job, justified in ADR
```

A job that does not declare a policy for a mode **cannot run that mode** — validation
fails at compile time, not at 02:00.

### 8.3 EOD close gate

Combine what already exists with ACB's best idea:

1. the producing job writes `ops.eod_watermark` (already: only after write **and**
   validation pass);
2. **and** tags the Iceberg table `EOD_<business_date>` — atomic with the data and giving
   consumers a snapshot to pin, exactly as ACB's `TAG_<date>` does;
3. the consumer's gate = row present **and** tag resolvable **and** upstream jobs'
   watermarks past the date.

A tag is superior to plstream's HDFS marker here because it is transactional with the
commit and it makes FULFILL deterministic (`FOR VERSION AS OF`).

---

## 9. Final metadata table design

**Storage split** (settles G2):

| Store | Tables | Why |
|---|---|---|
| **DynamoDB** (PAY_PER_REQUEST) | `job_execution` (hot), `job_watermark_state`, `streaming_app_state`, `coordinator_plan` | single-row updates at task cadence; millisecond reads from Airflow; no snapshot churn; no per-query billing |
| **Iceberg `ops.*`** | `job_execution_hist`, `summary_config_hist`, plus existing `data_certification`, `dq_result`, `lineage_event`, `reconciliation_run`, `eod_watermark` | append-only audit, joinable in Athena with the data it describes |
| **Git (+ compiled `plan.json` in S3)** | `job_master`, `job_flow_config`, `job_dependency`, `resource_profile` | version controlled, reviewable, rollback = `git revert` |

A terminal `job_execution` row is copied to `ops.job_execution_hist` once, at completion —
one Iceberg write per run instead of one per status transition.

### 9.1 `job_master` (Git YAML → `plan.json`)

Exactly the §13 schema, with these deltas:

- `job_id` — **stable string slug**, not `bigint`. An auto-increment integer in Git is a
  merge conflict generator, and `job_id` appears in S3 checkpoint paths where a renumber
  would orphan state.
- add `logical_target_layer` (`MART` | `CURATED`) — used to resolve `target_database`.
- `only_eod` kept (ACB semantic: "this job may only run when sources are closed").
- `config_version` = the compile checksum, injected at compile time, not hand-written.
- **not present:** `date_of_data`, `status`, `spark_app_*`, `started_at`, `ended_at`,
  `error_msg`, `turn`, `turn_watermark`.

### 9.2 `job_flow_config` — key `(job_id, flow_mode)`

§14 schema as written. Additions:

- `validation_selector` — the dbt test selector that must pass before SUCCEEDED;
- `eod_gate_mode` — `TAG_AND_WATERMARK | WATERMARK_ONLY | NONE`;
- `cost_guard` — `max_bytes_scanned_gb`, `max_execution_minutes` (a runaway STREAM_BATCH
  every 5 min is the realistic way to burn this project's $30 budget);
- `checkpoint_location` **derived**, never hand-written:
  `s3://<lake>/checkpoints/reporting/<env>/<job_id>/<flow_mode>/`. Validation rejects any
  literal path, and any path under `warehouse/` (the `remove_orphan_files` hazard already
  encoded in `modules/data_lake/variables.tf`).

### 9.3 `job_dependency` — normalized, computed

§15 schema. `dependency_scope = DIRECT` rows only are authored; `TRANSITIVE` is computed
at compile time and materialised **into `plan.json`**, never into the authored YAML.
`source_of_definition ∈ {DBT_MANIFEST, MANUAL_CONFIG}`. Cycle detection is a compile-time
hard failure.

`before_direct / after_direct / before_full / after_full` become **derived arrays in
`plan.json`**, `ARRAY<STRING>` — never comma-separated strings, never hand-maintained.

### 9.4 `job_execution` (DynamoDB, hot) + `ops.job_execution_hist` (Iceberg, audit)

§17 schema, with `ARRAY`/`MAP`/`STRUCT` where §17 asks. Notes:

- PK `execution_id`; GSIs on `(job_id, flow_mode, date_of_data)` and on `status`;
- `attempt_number` replaces ACB's `turn` for retries; `turn` is retained but means
  **topological turn within the coordinator run**, which is what §23 defines. These are
  two different concepts that ACB conflated into one column — keeping both names for one
  column would guarantee the confusion is inherited;
- `iceberg_snapshot_id_before/after` per input and output table (`MAP<STRING,BIGINT>`) —
  ACB's `auto_correct_snapshot_id` idea, kept for audit and selective rollback, but **not**
  used as the idempotency mechanism (see §9.7);
- `resolved_dependencies` as `ARRAY<STRUCT<job_id, flow_mode, execution_id, satisfied_at>>`;
- TTL on the DynamoDB item (e.g. 30 days) — the Iceberg copy is the permanent record.

### 9.5 `job_watermark_state` (DynamoDB) — key `(job_id, flow_mode)`

§19 as written. Invariant, enforced in one function: **written only inside the same
success path that writes the terminal SUCCEEDED status, after validation.** A conditional
write on `last_success_execution_id` prevents a stale retry from moving it backwards.

### 9.6 `coordinator_plan` (DynamoDB) + `ops.summary_config_hist` (Iceberg)

Replaces `summary_config_v1` / `summary_config_hist_v1`. Generated per coordinator run
from `plan.json` + watermarks + execution state (§20). Never hand-edited — which is the
one thing ACB's `summary_config_v1` most needed and never got (its `execution_date` is
mutated by the Spark jobs themselves, from inside the transformation).

The three `is_first_running_after_eod_in_*_flow` flags collapse to one derived boolean
`is_first_run_after_eod` plus the `flow_mode` already on the row.

### 9.7 Idempotency — keep the repo's mechanism, not ACB's

ACB rewinds target tables with `set_current_snapshot` before recomputing. That is unsafe
under concurrency, discards work committed by other jobs between the two runs, and is
unrecoverable if the job dies mid-rewind.

This repo already has the better mechanism: the guarded MERGE in `spark/common/flows.py`
(`may_overwrite` / `merge_condition_sql`), where a higher accuracy tier always wins and,
within a tier, the later `input_cutoff` wins. It generalises to the five modes directly:

| flow_mode | stamps | effect on an existing row |
|---|---|---|
| `STREAMING_RT` | `PROVISIONAL_NRT` | may not overwrite anything higher |
| `STREAM_BATCH` | `PROVISIONAL_NRT` | same |
| `AUTO_CORRECT` | `PROVISIONAL_CORRECTED` | overwrites NRT, not CERTIFIED |
| `EOD` | `CERTIFIED` | overwrites everything; a later EOD rebuild wins on `input_cutoff` |
| `FULFILL` | `CERTIFIED` (or the tier of its source layer) | same rule |

### 9.8 `resource_profile` (Git)

`small / medium / large / streaming_small / streaming_medium`, mapping to EMR Serverless
`sparkSubmitParameters` + driver/executor sizing. Per-job override allowed but
**recorded in `plan.json` and echoed into execution history**, so "why was this run slow"
is answerable.

---

## 10. Legacy field mapping

| Legacy (`job_master` / `job_master_execution_hist`) | Disposition | Where it goes |
|---|---|---|
| `job_name` | **KEEP** | `job_master.job_name` |
| `job_id` | **KEEP (retype)** | stable string slug, not int |
| `job_type` | **MOVE + split** | `job_master.execution_engine` + `job_flow_config.flow_mode`. ACB overloaded one column for both |
| `is_active` | **KEEP** | `job_master.is_active` (job exists at all) |
| `is_skipped` | **MOVE** | `job_flow_config.is_enabled` (per mode — skipping EOD but not STREAM_BATCH is a real need ACB could not express) |
| `coordinator_mode` | **MOVE** | `job_flow_config.coordinator_mode` |
| `run_mode` | **MOVE** | `job_flow_config` (`cross_turn` / `turn_by_turn`) |
| `date_of_data` | **MOVE** | `job_execution.date_of_data` + `job_watermark_state.last_success_date_of_data`. **Never in master** |
| `before_direct` / `after_direct` | **DERIVE** | from `job_dependency`; materialised into `plan.json` as `ARRAY<STRING>` |
| `before_full` / `after_full` | **DERIVE** | transitive closure at compile time |
| `*_by_mode` (4 columns) | **DERIVE** | `job_dependency.flow_mode` filter over the same graph |
| `order_by_default` | **DERIVE** | topological turn |
| `order_by_mode` | **DERIVE** | topological turn computed on the mode-filtered subgraph |
| `turn_watermark` | **MOVE (retype)** | `job_execution.resolved_dependencies` as a struct array, not a JSON string |
| `is_rerun` | **KEEP** | `job_execution.is_rerun` |
| `is_eod` | **DERIVE** | `flow_mode == 'EOD'`, or the EOD gate result on the run |
| `estimated_duration` | **DERIVE** | rolling median of `ops.job_execution_hist.duration_seconds`; not authored |
| `status` | **MOVE** | `job_execution.status` (10-state model, §18) |
| `note_for_tracing` | **KEEP** | `job_execution.note_for_tracing` |
| `spark_app_id` / `_status` / `_tracking_url` | **MOVE** | `job_execution.*`; on AWS these become the EMR Serverless `applicationId/jobRunId` and the console URL |
| `driver_cores` / `driver_memory` / `executor_*` | **MOVE + snapshot** | authored in `resource_profile`; the *resolved* values snapshot into `job_execution` |
| `airflow_dag_id` | **MOVE** | `job_execution.airflow_dag_id` (+ `dag_run_id`, `task_id` — ACB had none of these, so correlating a run to a task was manual) |
| `application` / `application_args` | **MOVE** | template in `job_master`, resolved value in `job_execution` |
| `started_at` / `ended_at` / `updated_at` | **MOVE** | `job_execution` |
| `error_msg` | **MOVE** | `job_execution.error_code` + `error_msg` |
| `trigger_type` | **KEEP** | `job_execution.trigger_type` |
| `only_eod` | **KEEP** | `job_master.only_eod` |
| `flow_name` | **KEEP** | `job_master.flow_name` (the domain grouping) |
| `non_eod` | **REMOVE** | it is `NOT only_eod`; two columns that must agree will eventually disagree |
| `schedule` | **MOVE** | `job_flow_config.schedule` + `timezone` |
| `dwh_dependencies` | **MOVE** | `job_dependency` rows with `dependency_type = EXTERNAL` |
| `owner` | **KEEP** | `job_master.owner` |
| `job_resource_config.*` | **MOVE** | `resource_profile` + per-job override |
| `job_master_run_mode` / `job_master_coordinator_mode` | **REMOVE as tables** | enumerations belong in code + config validation, not in a database that can drift |
| `summary_config_v1.execution_date` | **DERIVE** | `job_watermark_state.last_success_date_of_data` + calendar. **Critical:** in ACB the Spark job itself advances this from inside the transformation; the framework must own it |
| `summary_config_v1.dependencies` | **DERIVE** | `job_dependency` |
| `summary_config_v1.kickoff_condition` | **KEEP** | `coordinator_plan.kickoff_condition` |
| `is_first_running_after_eod_in_{auto_correct,streaming,streaming_ref}_flow` | **COLLAPSE** | one `is_first_run_after_eod` + the row's `flow_mode` |
| `auto_correct_snapshot_id` / `streaming_snapshot_id` | **MOVE, repurposed** | `job_execution.iceberg_snapshot_id_before/after` — **audit only**, not the rerun mechanism (§9.7) |
| `dim_max_time_<flow>` | **MOVE** | `job_watermark_state` |
| `engine_selection` (spark vs impala per job) | **REMOVE** | single engine here |
| `dim_times` | **KEEP as a concept** | a business calendar table is required (see R4) |
| `eod_info` | **REPLACE** | `ops.eod_watermark` + the Iceberg `EOD_<date>` tag |
| YARN-log `JOB_DONE` string heuristic | **REMOVE** | flipping FAILED to SUCCEEDED on a log grep is how a silently-wrong day gets certified |

---

## 11. Proposed Airflow architecture

**Evaluated:**

**A. Config-driven dynamic DAG factory (one DAG per mart).** ACB's actual choice —
`pipeline_auto_generator.py` builds one DAG per row of `job_master`, and a coordinator DAG
per `job_type` triggers them with `TriggerDagRunOperator(wait_for_completion=True)`.
Rejected: with N marts × 5 modes you get 5N DAGs plus coordinators, each re-parsed
continuously. `airflow_k3s` targets a small node; Airflow 3 + KubernetesExecutor there
cannot afford that parse load. It also doubles every run into two DAG runs.

**B. Fixed flow DAGs + runtime coordinator.** Four DAGs (one per batch mode), each
reading the compiled plan and expanding tasks over the jobs in turn order.

**C. One DAG per logical mart.** Rejected outright: it is the thing §29 exists to prevent.

**Chosen: B, with A's config-driven expansion inside it.**

```
datamart_eod          schedule: daily after EOD close
datamart_auto_correct schedule: every 30 min inside the window
datamart_fulfill      schedule: None (manual/param, dry_run first)
datamart_stream_batch schedule: every 5–15 min inside the window

streaming_rt_start / streaming_rt_monitor / streaming_rt_stop   (separate lifecycle)
```

Each batch DAG is:

```
resolve_plan   (read plan.json from the local cache; refresh from S3 if stale)
    ↓
[ turn_1 ]  TaskGroup — dynamic task mapping .expand() over jobs in turn 1
[ turn_2 ]  ...
[ turn_N ]
    ↓
finalize      (write ops.summary_config_hist, emit metrics)
```

Per job, the mapped task chain is
`gate_dependencies → submit → track → validate → commit_watermark`, mirroring ACB's
proven `check_is_processing → check_ready_eod → check_is_skipped → handle_run_mode →
submit_spark_job → handle_final_job_status` but with the watermark commit made explicit
and last.

**DAG parsing must never query the warehouse.** ACB's `config_source ∈ {database,
cache_file}` with a JSON cache is imported wholesale, and matters more here: an Athena
query per parse is billed and slow. The DAG file reads a local `plan.json`; a small
`sync_plan` DAG refreshes it from S3 on a schedule and on demand.

Turn ordering uses **`default` (bipartite fan-out) as the safe default** and `native`
(true edges) as an opt-in, exactly as ACB does — a bipartite join between turns is
strictly more conservative and cannot execute a job before a dependency.

To be recorded as **ADR — Airflow Dynamic DAG vs Coordinator Architecture**.

---

## 12. Proposed dbt-spark architecture

```
dbt/
├── dbt_project.yml            (extend the existing one; do not fork)
├── profiles.yml.example       (FIX: emr_serverless target → method: session)
├── macros/
│   ├── audit_columns.sql          exists
│   ├── generic_tests.sql          exists
│   ├── status_priority.sql        exists   ← already encodes the accuracy ladder
│   ├── resolve_source_layer.sql   NEW  logical layer → physical relation
│   ├── flow_window.sql            NEW  (window_start, window_end) per flow_mode
│   ├── incremental_filter.sql     NEW  the WHERE clause per flow_mode
│   ├── merge_predicate.sql        NEW  wraps merge_condition_sql's ladder rule
│   └── business_date.sql          exists as business_date()
├── models/
│   ├── staging/  intermediate/  marts/    exist
└── tests/                                  exist (8 singular + schema tests)
```

**Execution: `dbtRunner` inside an EMR Serverless job, `method: session`** — TTC's
`run_dbt_job.py` shape, adapted:

- project shipped as a zip to `s3://<lake>/artifacts/dbt/<version>/`;
- `profiles.yml` written at runtime (no secrets in Git — CLAUDE.md §3.1/§3.6);
- job config read from the compiled plan, not from a DynamoDB row edited by hand;
- **no 8-attempt retry loop** — retries belong to Airflow, with backoff, and a
  non-transient model failure retried 8× is 8× the EMR bill.

**The standard variable contract** (§26), passed to every model:

```
flow_mode  cob_date  from_date  to_date  window_start  window_end
watermark_before  watermark_after  run_id  execution_id
is_rerun  is_backfill  source_layer  input_cutoff
```

One model per mart, **not four**. The mode differences are entirely inside
`resolve_source_layer()` + `incremental_filter()`. This mirrors the invariant the repo
already enforces for the Spark flows
(`test_all_four_flows_produce_identical_business_columns`) — and for the same reason: if
EOD and STREAM_BATCH have separate SQL, a variance between provisional and certified
numbers has two possible causes and no way to tell them apart.

**Manifest → dependency sync**: `dbt parse` → `target/manifest.json` → parse
`nodes[].depends_on.nodes` → emit `job_dependency` rows with
`source_of_definition = DBT_MANIFEST`. Runs at compile time; a mismatch between the
manifest and hand-authored `MANUAL_CONFIG` rows is a compile warning, and a *missing*
manifest dependency that is not declared manually is a compile **failure**.

---

## 13. Proposed five flow designs

### EOD
```
gate: ops.eod_watermark(EOD, <cob_date>) present  AND  Iceberg tag EOD_<cob_date> resolvable
      AND every upstream job's watermark >= cob_date
read: EOD layer, pinned to the tagged snapshot (FOR VERSION AS OF)
run:  dbt build --select <model> --vars '{flow_mode: EOD, cob_date: ...}'
write:MERGE, stamps CERTIFIED
gate: dbt tests (validation_selector) + ops.dq_result has no ERROR FAIL
then: watermark advance  →  status SUCCEEDED  →  Iceberg tag on the mart
```
Idempotent for `(job_id, cob_date, EOD)`: rerunning re-merges CERTIFIED over CERTIFIED and
wins on the later `input_cutoff`.

### AUTO_CORRECT
```
window: [cob_date - lookback_days, now)  bounded by job_flow_config.lookback_days
read:   EOD baseline  +  REALTIME (or FULL_CDC where REALTIME's window is too short)
impact: changed source rows → affected business keys → affected business dates
        → affected downstream marts (from the dependency graph)
run:    only the affected (key, date) ranges — not a blind full rebuild
write:  MERGE, stamps PROVISIONAL_CORRECTED (cannot overwrite CERTIFIED)
record: affected_dates, affected_key_count into job_execution
```
The impact analysis is what ACB never had — it reruns the whole day every time.

### FULFILL
```
plan:  expected business dates (calendar)
       MINUS dates with a SUCCEEDED execution
       MINUS dates present and complete in the target mart
       → {missing, failed, incomplete}
       --dry-run prints the plan and exits (default posture)
exec:  per date, choose the source layer:
         date <= last EOD close  → EOD   (deterministic, tagged snapshot)
         otherwise               → FULL_CDC (rebuild the state at that cutoff)
       skip dates already SUCCEEDED unless --force
write: MERGE, stamps the tier of the source layer
```
`--dry-run` defaults to true. A backfill that starts before anyone has read the plan is
the expensive mistake in this mode.

**Naming:** the existing `spark/full_fill/` (unresolved-SK repair) is renamed in
documentation to **`DIMENSION_REPAIR`** to stop it colliding with FULFILL. It stays a
Spark flow; it is not one of the five reporting modes.

### STREAM_BATCH
```
watermark_before ← job_watermark_state(job_id, STREAM_BATCH)
window = [watermark_before - safety_overlap, bounded_now)
read:  REALTIME
run:   dbt incremental / Spark
write: MERGE on affected keys, stamps PROVISIONAL_NRT
validate → advance watermark  (FAILURE ⇒ NO ADVANCE, unconditionally)
```
`safety_overlap` is not optional: the repo already applies
`NRT_SAFETY_OVERLAP_MINUTES = 2` for exactly this reason (an event written microseconds
before the recorded watermark otherwise falls through the gap between two runs and is
never picked up), and ACB independently uses a 3-minute rewind.

### STREAMING_RT
```
long-running Spark Structured Streaming, foreachBatch → MERGE into the mart
source:      FULL_CDC append stream (Iceberg incremental read) by default.
             KAFKA_DIRECT only where the latency budget cannot tolerate the Iceberg
             commit interval, and then FULL_CDC remains the canonical durable truth
             and a reconciliation job against it is mandatory.
checkpoint:  s3://<lake>/checkpoints/reporting/<env>/<job_id>/STREAMING_RT/   (derived)
state:       ops.streaming_app_state (DynamoDB)
lifecycle:   Airflow start / monitor / stop DAGs; the app owns its own trigger loop
failure:     foreachBatch must raise on partial failure — a quiet return commits the
             offsets and loses the batch (plstream main.py:299, learned the hard way)
```

**Cost posture — explicit.** `enable_streaming_rt = false` by default. CLAUDE.md §4
forbids always-on compute, and `flow_runner.py` records a deliberate prior decision
against continuous streaming for these flows. STREAMING_RT ships as framework +
lifecycle + a bounded demo window (start 08:00 / stop 20:00, as plstream does), not as a
24/7 application.

### STREAM_BATCH vs STREAMING_RT (§35) — normative

| | STREAM_BATCH | STREAMING_RT |
|---|---|---|
| Trigger | Airflow schedule | its own trigger loop |
| Lifetime | starts and exits per run | runs until stopped |
| Cadence | 5–15 min | seconds |
| Position | `job_watermark_state` | Spark checkpoint (+ mirrored to `streaming_app_state`) |
| History | one `job_execution` per run | one per *deployment*; progress in `streaming_app_state` |
| Cost | seconds of EMR per run | continuous EMR capacity |
| Failure | Airflow retry | app restart, checkpoint recovery |

Distinct prefixes everywhere (`sb_` / `rt_`), distinct checkpoint roots, distinct pools.

---

## 14. Terraform changes required

Small. Nothing in §39's prohibition list.

| Change | Module | Justification | Cost |
|---|---|---|---|
| DynamoDB tables `job_execution`, `job_watermark_state`, `streaming_app_state`, `coordinator_plan` — PAY_PER_REQUEST, PITR off, SSE with the lake CMK, TTL on `job_execution` | **new** `modules/reporting_ops` | G2: Iceberg cannot serve hot single-row state | ~$0.01–0.50/mo at lab volume |
| S3 prefixes `artifacts/dbt/`, `ops/config/`, `checkpoints/reporting/` | `modules/data_lake` (`lake_prefixes` list) | dbt bundle, compiled plan, RT checkpoints | $0 |
| IAM role `reporting` (EMR job role): read layers, write `mart`/`ops`, RW the four DDB tables, read `artifacts/dbt/`, Glue catalog | `modules/lake_iam` | CLAUDE.md §3.7 per-workload roles | $0 |
| Glue database for `realtime` (8th) | `modules/glue_catalog` | G3. the `variables.tf:24-33` validation is a `setsubtract` **subset** check (all seven required layers present), so an eighth key passes unchanged — verified, no relaxation needed | $0 |
| Lifecycle rule for `ops/config/` (keep last N versions) | `modules/data_lake` | plan.json is versioned | $0 |
| EMR Serverless: no change | — | existing app, auto-stop 15 min, max capacity set | $0 |
| Airflow: no change | — | existing `airflow_k3s`; new DAGs are files | $0 |
| Tags `Project/Environment/ManagedBy/Owner/CostCenter/AutoDestroyAfter` on all new resources | all | CLAUDE.md §4.11 | — |
| `enable_reporting_framework`, `enable_streaming_rt` flags + destroy/verify | `envs/dev` + `scripts/verify-destroy.sh` | CLAUDE.md §4.12 | — |

Open question for review: DynamoDB is a new service class in this project. The
alternative — everything in Iceberg with batched status writes and a longer poll interval
— avoids it at the cost of snapshot churn, Athena query spend on every coordinator poll,
and seconds-scale latency in the gate loop. **Recommendation: DynamoDB**, but this is a
decision for the operator, not for me, because it adds a service to the account.

---

## 15. Code changes required

**New:**

```
reporting/
├── jobs/                       one YAML per logical job (job_master + flow_config + deps)
├── profiles/resource_profiles.yaml
├── layers.yaml                 logical → physical layer map (settles G1)
└── compile.py                  validate → graph → cycles → turns → dbt manifest merge
                                → plan.json + checksum

spark/reporting/
├── ops_client.py               the ONLY writer of execution/watermark state
├── coordinator.py              resolve → gate → order → emit the plan
├── graph.py                    DIRECT → transitive, before/after, turns, cycle detection
├── gates.py                    EOD gate (watermark + Iceberg tag), dependency gate
├── source_resolver.py          resolve_source_layer(table, flow_mode) → relation + snapshot
├── run_dbt_job.py              dbtRunner entrypoint for EMR Serverless
├── run_spark_job.py            existing spark/ entrypoints under the same contract
├── fulfill_planner.py          gap detection + dry-run
├── autocorrect_impact.py       changed rows → keys → dates → downstream marts
└── streaming_rt/               long-running app + app-state reporting

airflow/dags/
├── datamart_eod.py  datamart_auto_correct.py  datamart_fulfill.py  datamart_stream_batch.py
├── streaming_rt_lifecycle.py
├── sync_reporting_plan.py      refresh plan.json into the local cache
└── reporting_common.py         plan loading, mapped-task factory, gates

dbt/macros/  resolve_source_layer.sql  flow_window.sql  incremental_filter.sql
             merge_predicate.sql

scripts/     reporting-compile.sh  reporting-validate.sh  create-datamart.sh
             reporting-dryrun.sh
```

**Modified:**

- `dbt/profiles.yml.example` — `emr_serverless` target `method: thrift` → `method: session`
  (G6). This is a correction; the current target cannot work on EMR Serverless.
- `dbt/dbt_project.yml` — add the standard `vars` contract with safe defaults.
- `terraform/modules/data_lake/variables.tf` — three prefixes.
- `terraform/modules/glue_catalog` — `realtime` database.
- `docs/DATA_CONTRACTS.md` §8 — the `realtime` layer and the logical-name map.
- `Makefile` — `reporting-compile`, `reporting-validate`, `create-datamart`.
- `spark/full_fill/` — documentation rename to DIMENSION_REPAIR (G7). No behaviour change.

**Explicitly NOT modified:** `spark/common/flows.py`, `transform.py`, `mart_writer.py`,
`spark/eod/`, `spark/snapshot/`, `spark/jobs/l1_stream/`. The reporting framework sits
*above* the layers and must not alter their semantics (§49).

### New-mart developer experience (§29)

```
scripts/create-datamart.sh --name mart_x --flows EOD,AUTO_CORRECT,FULFILL,STREAM_BATCH
  → dbt/models/marts/mart_x.sql          (templated skeleton)
  → dbt/models/marts/mart_x.yml          (tests: unique key, not_null, freshness)
  → reporting/jobs/mart_x.yaml           (job + per-mode flow config)
make reporting-compile     validate, graph, cycles, turns, manifest sync
make reporting-dryrun      what would run, for which dates, reading which layer
```

No DAG is written. That property is the acceptance test for the whole design.

---

## 16. Prompt / guide changes required

`aws-cdc-lakehouse-claude-guide-v2/prompts/` currently has 00–19 + `13B` + `13C`, plus two
loose files: `architect kafka event to s3 3layer.md` and `master promt.md`.

| Prompt | Verdict | Action |
|---|---|---|
| 00–08 | **VALID** | none |
| 09 (four flows) | **NEEDS_UPDATE** | the four flows become the substrate for the five reporting modes; FULL_FILL's name collides with FULFILL (G7) |
| 10 (Kimball) | **VALID** | |
| 11 (dbt-spark) | **NEEDS_UPDATE** | thrift → session; add the variable contract, manifest sync, selectors |
| 12 (Airflow) | **NEEDS_UPDATE** | add the four flow DAGs + the plan-cache rule (no warehouse queries at parse time) |
| 13 / 13B / 13C | **VALID** | |
| 14 (governance) | **NEEDS_UPDATE** | `ops.dq_result` becomes a publish gate for reporting jobs |
| 15–19 | **VALID** | 17 gains the new destroy/verify paths; 18 gains the framework in the CV evidence |
| `architect kafka event to s3 3layer.md` | **CONFLICTING** | its `FULL_CDC / EOD / REALTIME` naming contradicts the deployed `stream / full_cdc / snapshot` (G1). Keep as the upstream architectural prerequisite; add a **normative mapping section**, do not delete |
| `master promt.md` (this task) | **VALID** | becomes prompt `20` |

**New, in order:** `20 REPORTING_METADATA_FRAMEWORK` → `21 PILOT_DATAMART` →
`22 EOD_FLOW` → `23 AUTO_CORRECT_FLOW` → `24 FULFILL_FLOW` → `25 STREAM_BATCH_FLOW` →
`26 STREAMING_RT_OPTIONAL` → `27 REPORTING_MIGRATION_AND_SCALE`.

`prompts/README.md` gets the index and the ordering; nothing is deleted without a
documented replacement.

### ADRs to write (§46)

`ADR-033` Reporting metadata architecture · `034` Job master vs runtime history separation ·
`035` Dependency graph and dbt manifest integration · `036` Airflow flow DAGs +
coordinator (vs per-mart DAG factory) · `037` Five reporting execution modes ·
`038` STREAM_BATCH vs STREAMING_RT · `039` Datamart incremental/merge strategy ·
`040` Config source of truth (Git YAML → compiled plan → runtime store) ·
`041` Watermark and idempotency strategy · **`042` Logical vs physical layer naming (G1)** ·
**`043` Runtime state store: DynamoDB vs Iceberg (G2)**.

The last two are not in §46's list; they are the two decisions this discovery found that
must be settled *before* anything is written.

---

## 17. Implementation phases

| Phase | Deliverable | AWS spend | Gate |
|---:|---|---|---|
| 0 | ADR-042 (layer naming) + ADR-043 (state store) approved | $0 | **operator decision — blocks everything** |
| 1 | Metadata framework: YAML schema, `compile.py`, graph/cycles/turns, `plan.json`, ops client, tests | $0 (all local) | tests green, `make reporting-compile` clean |
| 2 | Terraform: DDB tables, prefixes, IAM, `realtime` Glue db · `terraform plan` only | $0 | plan reviewed, cost preview ≈ $0.00/hr |
| 3 | Pilot mart, EOD flow only, static-validated + local Spark | $0 | pilot proves the §44 checklist locally |
| 4 | EOD flow live in a metered CDC window | metered | certified numbers reconcile |
| 5 | AUTO_CORRECT + impact analysis | metered | a late event provably corrects the right dates only |
| 6 | FULFILL + gap detection + dry-run | metered | a deliberately failed date is detected and repaired |
| 7 | STREAM_BATCH + watermark | metered | watermark provably does not advance on failure |
| 8 | REALTIME layer + STREAM_BATCH on it | metered | bounded window reconciles against FULL_CDC |
| 9 | STREAMING_RT (flag, bounded window) | metered | restart from checkpoint loses nothing |
| 10 | Migrate the remaining 3 marts; retire hardcoded DAG lists | metered | new mart = SQL + YAML only |

Phases 1–3 are entirely free and are where the design risk actually lives.

### Pilot (§44)

**`mart_account_balance_daily`.** It exists, has one source
(`fact_account_daily_snapshot`), a two-column grain (`account_sk + business_date`), no
fan-out join, and a semi-additive measure that already forced a documented correctness
decision (S10-8). `mart_customer_360_daily` joins three intermediates and is the wrong
place to debug a new framework.

It exercises: config registration, dependency resolution (1 upstream), dbt execution,
EMR submission, Iceberg MERGE, Airflow orchestration, execution history, watermark,
retry, rerun, and validation — i.e. every item in §44.

### Tests (§45)

All 17 required test classes map to Phase 1–3 work and run locally against real Spark +
Iceberg, alongside the existing 417 Python + 47 dbt tests. The three that matter most and
are easiest to get wrong: **watermark non-advancement after failure**, **cycle detection**,
and **turn calculation on the mode-filtered subgraph**.

---

## 18. Risks and assumptions

### Risks

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | **Layer naming (G1)** settled late, or settled by renaming deployed Glue databases | rewrite of every DDL, `sources.yml`, doc and 3 deployed databases | ADR-042 in Phase 0; logical alias layer; rename nothing physical |
| R2 | **Iceberg as hot state store (G2)** | snapshot churn, Athena spend per coordinator poll, seconds of latency in the gate loop | ADR-043; DynamoDB for mutable, Iceberg for audit |
| R3 | Airflow DAG parsing queries the warehouse | recurring Athena cost + slow parse + a scheduler that stalls when the catalog is slow | `plan.json` local cache; a test asserts no DAG module imports a query client |
| R4 | **No business calendar.** ACB depends on `dim_times` everywhere ("nearest working day before the run date"); this repo has none | EOD/FULFILL pick wrong dates around weekends and holidays — silently | build `curated.dim_date` in Phase 1; make the calendar an explicit dependency |
| R5 | STREAMING_RT left on | always-on EMR capacity; the $30 budget gone in days | flag off by default; bounded window; `AutoDestroyAfter`; budget alert already live |
| R6 | STREAM_BATCH every 5 min × N marts | frequency dominates cost, not size (already recorded in `flow_runner.py`) | `cost_guard` per flow config; pause outside the demo window |
| R7 | dbt manifest and hand-authored dependencies drift | a mart runs before its input | manifest sync at compile; missing-and-undeclared = compile failure |
| R8 | Nothing is deployed (Airflow off, EMR off, zero Iceberg tables) | the whole framework stays `static-validated` until a metered window | Phases 1–3 are free and prove the design; live proof needs an operator window |
| R9 | Porting ACB's target-rewind idempotency | data loss under concurrency | use the repo's guarded MERGE (§9.7); rewind is audit-only |
| R10 | AUTO_CORRECT impact analysis wrong | corrected marts silently miss affected dates | fall back to the full lookback window when impact is not derivable; record which path was taken |
| R11 | The five modes drift into five copies of the SQL | provisional/certified variance becomes uninterpretable | one model per mart; a test asserting identical business columns across modes, mirroring `test_all_four_flows_produce_identical_business_columns` |
| R12 | DynamoDB is a new service in this account | new IAM surface, new destroy path, new cost line | `modules/reporting_ops` with `enable_*`, destroy verification in `verify-destroy.sh`, cost in `docs/COST.md` |

### Assumptions (each needs confirmation)

1. **The deployed layer names stay.** Physical `stream / full_cdc / snapshot` are not
   renamed; the prompt's names become logical. — **needs operator confirmation.**
2. **A `realtime` layer is genuinely wanted.** It does not exist and is real work
   (build, maintain, reconcile against FULL_CDC, expire). If AUTO_CORRECT and
   STREAM_BATCH can read `full_cdc` with a date predicate, REALTIME is a performance
   optimisation, not a requirement. — **needs confirmation.**
3. **DynamoDB is acceptable.** Otherwise everything goes in Iceberg and R2 is accepted.
4. **dbt runs on EMR Serverless via `dbtRunner` + `method: session`.** The current thrift
   target is unusable there.
5. **STREAMING_RT is flag-off by default** and demonstrated in a bounded window.
6. Business-date semantics are UTC per ADR-024, and the calendar (R4) will be built.
7. `mart_account_balance_daily` is an acceptable pilot.
8. Reference code is studied, not copied. No ACB/TTC/plstream source is vendored; the
   patterns are reimplemented against this platform's constraints.

---

```
REPORTING_ARCHITECTURE_DISCOVERY_COMPLETE
```

Awaiting review. Nothing will be implemented until Phase 0 (ADR-042 layer naming,
ADR-043 runtime state store) is decided.
