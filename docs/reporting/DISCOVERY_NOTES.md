# Reporting Framework — Discovery Notes (Part 1: reference projects)

Status: PARTIAL. Sections 1-4 of the discovery brief done (ACB, plstream, TTC).
Sections 5-18 (current aws-cdc-lakehouse state, gap analysis, proposed design) NOT started.
Nothing in this file is a design decision yet — it is verified observation of reference code.

## 1. ACB — data_pipelines-tutorials/data_pipelines

Metadata store: Impala, database `dlpt`.

Tables actually observed in code:
- `dlpt.job_master` — MIXES config + runtime. Carries `date_of_data` (mutable,
  advanced by coordinator), `before_direct/after_direct/before_full/after_full`
  as COMMA-SEPARATED STRINGS, `order_by_default`, `order_by_mode`, `job_type`,
  `run_mode`, `coordinator_mode`, `is_active`, `is_skipped`, `only_eod`,
  `flow_name`, `dwh_dependencies`, `non_eod`, `schedule`.
- `dlpt.job_resource_config` — ALREADY SEPARATE: airflow_dag_id, driver_cores,
  driver_memory, executor_cores, executor_memory, executor_instances,
  application, application_args. (Resource split already exists upstream.)
- `dlpt.job_master_coordinator_mode` — (job_type, coordinator_mode, parallel).
- `dlpt.job_master_execution_hist` — PK (job_name, date_of_data, turn).
  Snapshot of master config + runtime (status, spark_app_id, started/ended,
  error_msg, note_for_tracing, turn_watermark, is_rerun, is_eod).
- `dlpt.summary_config_v1` — resolved plan: job_name, execution_date,
  dependencies (csv), execution_date_ref, is_first_running.
- `dlpt.eod_info` — EOD close registry: target_table, cob_date,
  pre_datelastmaint, datelastmaint, end_time.
- `dlpt.dim_times` — working-day calendar. Used everywhere; NOT optional.
- `dlpt.auto_correct_snapshot_id` / `eod_snapshot_id` / `dummy_snapshot_id` —
  Iceberg snapshot bookmarks per (flow, table_name, execution_date).
- `dlpt.log_tag_eod`, `dlpt.dwh2cdp_pulled_log`.

### EOD close mechanism (MOST REUSABLE FINDING)
`get_ready_eod()` (lib/helpers.py:530) declares a dependency closed only when BOTH:
  1. a row exists in `eod_info` with cob_date = execution_date, AND
  2. the Iceberg table carries tag `TAG_{execution_date}` (`.refs` metadata table).
Then `get_eod_range_time()` resolves, per dependency:
  - `{tbl}_snp_id` = snapshot id behind that tag (falls back to `main` if absent)
  - `start_system_time_{tbl}` / `end_system_time_{tbl}` = (pre_datelastmaint, datelastmaint]
EOD SQL reads `FOR SYSTEM_VERSION AS OF {tbl}_snp_id` => reproducible/pinned.
DAILY + STREAMING SQL read unpinned, bounded only by dv_src_ldt window => fresh.
That pinned-vs-unpinned split IS the EOD/REALTIME distinction in the reference.

### Mode dispatch
One Spark app per mart; branch at runtime on `is_ready_eod`:
  is_ready_eod -> `<job>_eod.sql` ; else -> `<job>_daily.sql`
Both SQL files ~600-700 lines and ~90% identical => the exact duplication the
new framework must avoid.
job_type values in use: eod, auto, stream, stream_ref, load_hist, report.
Maps to the five target modes: eod=EOD, auto=AUTO_CORRECT, load_hist=FULFILL,
stream/stream_ref=STREAM_BATCH. No STREAMING_RT here.

### turn / run_mode / turn_watermark (verified semantics)
- `turn` = attempt sequence within a `date_of_data`. Incremented by
  `sync_job_master_hist` only when the previous turn has a non-NULL status.
  NULL status = "planned, not yet run".
- `turn_watermark` = JSON dict {upstream_job_name: last_consumed_turn} stored on
  the execution_hist row. This is a PER-DEPENDENCY CURSOR, not a time watermark.
- `run_mode` = `cross_turn` (accept any newer SUCCEEDED turn of each upstream;
  advance the cursor) or `turn_by_turn` (upstream must have SUCCEEDED at the
  SAME turn number; otherwise mark self FAILED to break the poll loop).
- `order_by_default` / `order_by_mode` = topological level, computed offline.

### Dependency graph construction (functions.py:fulfill_job_master)
Source of truth is an EXCEL FILE, not the DB. A Node class computes:
  order = 1 if no parents else max(parent.order)+1
  after_full = transitive closure (recursive)
  before_full = DIRECT PARENTS ONLY -- NOT transitive. Asymmetric with
  after_full. Looks like a latent bug; do not replicate.
No cycle detection anywhere (cached_property + recursion would hang/RecursionError).

### Coordinator (dags/orchestration/coordinator.py)
One DAG per job_type: `coordinator_{job_type}`. Inside, a TaskGroup of
TriggerDagRunOperator (one per job, wait_for_completion, deferrable).
coordinator_mode:
  - `default` : group by order level, barrier between levels (level-to-level
    cross product => over-synchronised but simple)
  - `native`  : real edges from after_direct (true DAG)
  - `sequential`: chain everything
Each job ALSO has its own generated DAG (pipeline_auto_generator.py) built from
job_master. So the architecture is BOTH a dynamic DAG factory (per job) AND a
coordinator (per job_type). Not either/or.

### Per-job DAG skeleton (pipeline_builder.py)
check_if_working_day -> check_anti_dag -> read_config -> check_is_processing
 -> check_ready_eod -> check_is_skipped -> [update_skip_hist | submit_spark_job]
 -> handle_final_job_status -> raise_exception

- `check_anti_dag`: MUTUAL EXCLUSION between the flows of the same mart
  (auto vs eod vs stream vs stream_ref must never run concurrently), plus a
  global "EOD is a priority" gate. Essential concept; keep, but drive it from
  (job_id, flow_mode) instead of string surgery on dag_id.
- `check_is_processing`: reconciles a stale PROCESSING row against the real
  YARN app state before doing anything else. Crash-recovery path.
- `handle_final_job_status`: re-queries YARN, greps the driver log for a
  `JOB_DONE` marker, and will FLIP a FAILED row to SUCCEEDED if found.
  Compensates for the submit client dying while the app succeeded.

### Config source-of-truth pattern (worth keeping)
Airflow Variable `config_source` in {database, cache_file}. Coordinator reads
from DB once, writes a JSON cache, then flips the Variable to `cache_file` so
every subsequent DAG PARSE reads the file, not Impala. `handle_eod` flips it
back to `database` when the business date advances. This is what keeps DAG
parsing fast with DB-backed config. Cache TTL 7d, and DB errors fall back to
the cache unconditionally.

### Anti-patterns observed (do NOT carry forward)
- Related jobs found by string surgery: `dag_id.replace('_eod','')`,
  `.replace('auto_correct','streaming')`, `+ '_ref'`.
- Hardcoded job-name lists in DAG code (sg_acct_auto_list, fact_snp_auto_list,
  fact_sg_auto_mapping) and per-job `if dag_id == ...` branches.
- Business-date state (`date_of_data`) living on the config table and being
  mutated in place.
- Local-filesystem coordination files under /home/dlpt/tmp/airflow/config
  (working-day flags, ready-eod flags) — breaks with >1 scheduler node.
- SQL built by f-string concatenation of job names throughout.

## 2. plstream_merged — STREAMING_RT reference
Path: plstream_merged/plstream_merged/v9/flow3_pl_ceo_v2

Four applications sharing `_shared_src/`: flow3_pl_init, flow3_pl_stream_main,
flow3_pl_batch_eod, flow3_pl_batch_autocorrect. Same code, mode-parametrised —
this is the shape the new framework should have.

- Source: Kafka DIRECT (readStream) + Avro decode, NOT a lakehouse read path.
- Sink: `foreachBatch` -> `MERGE INTO <target>_STREAM` (Iceberg v2,
  merge-on-read; write.delete/merge/update.mode=merge-on-read).
- Target split: `*_BASE` (full state, rebuilt by EOD) + `*_STREAM` (recent
  changes) + a view unioning them. Directly analogous to EOD + REALTIME.
- Checkpoint: HDFS dir per app, config-driven (`streaming.checkpoint_dir`).
  Startup emits a STALE CHECKPOINT warning if offsets/ is non-empty, and
  deliberately refuses to auto-delete it.
- Trigger 30s, max_offsets_per_trigger ~100k.
- CRITICAL CORRECTNESS LESSON (documented in main.py): if foreachBatch swallows
  an exception and returns normally, Spark COMMITS THE OFFSET and the batch is
  silently lost. They therefore write an audit row on enrichment failure and
  re-raise. Any STREAMING_RT design must specify this explicitly.
- Inline compaction every N batches inside foreachBatch (rewrite_position_delete
  _files + expire_snapshots) — avoids a separate maintenance job.
- Late-arriving dimension joins: bounded in-memory PendingQueue with
  max_wait_seconds -> on timeout insert NULL + flag + row in
  `dim_pending_timeout_acctnbrs`. Explicit late-data policy, auditable.
- `dim_autocorrect_watermark` (single row, max source_ts processed) used to
  detect the first run after EOD and force a full dimension reload.
- Operational state tables: dim_snapshot_status, dim_dim_change_audit.

### Streaming lifecycle via Airflow (airflow/dags/bcn_realtime_dags.py)
5 DAGs generated from ONE yaml (configs/bcn_pipeline.yaml); no schedule/table/
resource hardcoded in the DAG file.
  05:00 bcn_eod            check eod_info + DWH gate -> kill stragglers ->
                           clean+build BASE -> write HDFS marker eod_done_{date}
  08:00 bcn_stream_start   wait marker -> spark-submit long-running stream app
  08:00 bcn_datamart_start wait marker -> submit long-running datamart app
  09-19h/2h bcn_autocorrect check stream RUNNING -> run autocorrect batch
  20:00 bcn_shutdown       yarn kill both long-running apps
Cross-DAG dependency is an HDFS MARKER FILE, chosen over ExternalTaskSensor to
avoid execution_date skew between DAGs on different schedules and to keep manual
runs possible. Polling is done with task retries + retry_delay, not Sensors, to
avoid holding a slot.
The daily start/stop of the streaming app is also the cost-control mechanism —
directly relevant to the lab_low_cost profile.
cob_date is resolved from dim_times (last working day < run date), NOT by
subtracting a fixed offset — a fixed offset breaks when T-1 is a holiday.

## 3. TTC — dbt-spark reference
Repos: ttc-dlh-ops-dbt-library (dbt project `ttc_dbt`), ttc-dlh-ops-etl-library
(`express_data_transform` python pkg), ttc-dlh-ops-data-platform (IaC:
emr-serverless, iam, lambda, stepfunction, redshift modules),
ttc-dlh-shared-service-base-infra.

- `profiles.yml`: dbt-spark `method: session` => dbt runs INSIDE the Spark
  driver. This is the key enabler for running dbt on EMR Serverless.
  Glue catalog wired via `spark.sql.catalog.glue.*` server_side_parameters.
  Second target `redshift` (method: iam) over the SAME models — matches the
  "Athena/Glue core, Redshift optional flag" rule in CLAUDE.md §8.
- Model tree: `models/<domain>/<subject>/{staging,intermediate,marts}`.
  `dbt_project.yml` sets `+file_format: iceberg` and routes `+database` per
  layer and per target (marts -> business catalog, staging/intermediate ->
  awsdatacatalog).
- Naming: `{domain}_{subject}_{stg|int|dm}__{source}__{entity}`.
- `generate_schema_name` derives schema from `node.fqn` — schema is a function
  of directory position, so a new model needs no schema config.
- Incremental pattern actually used: `materialized='incremental'`,
  `incremental_strategy='append'` + `pre_hook` deleting the target business date
  (`delete from {{ this }} where pr_date = '{{ var("data_date") }}'`).
  i.e. delete+append idempotency keyed on a dbt var. Simple, works, but a real
  Iceberg MERGE on a declared unique_key is stronger and should be preferred.
- Custom `temp_table` materialization (creates a temporary view) — useful for
  intermediate steps that must not be persisted.
- Only 4 incremental models out of the whole project; the rest are full
  `table` rebuilds. So TTC gives layout/config/execution patterns, NOT a
  proven incremental/merge framework.

## Open question carried forward
Whether `ops.streaming_app_state` is needed (brief §33) — based on plstream,
YES: a long-running app has restart_count / last_batch_id / last_progress_at /
checkpoint_location with no natural (date_of_data, turn) key, and
job_master_execution_hist is keyed per finite run. Confirm against the current
project's Spark runtime before finalising.
