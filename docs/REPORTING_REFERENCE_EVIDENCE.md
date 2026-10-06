# Reporting Reference Discovery — Evidence Map

- Date: 2026-08-19
- Scope: **discovery and comparison only.** No file modified in any of the nine paths, no
  AWS call made, no Terraform executed.
- Purpose: locate, with exact paths and construct names, where each of eleven reporting
  concerns is implemented in the reference codebases and in the target repository.

Every row below was read. Line numbers are from the files as they stand on disk today.
Where a repository has **no** implementation of a concern, that is stated as `ABSENT`
rather than left blank — an absent capability is the finding.

---

## 0. Inventory and coverage

| # | Path | What it is | Coverage of this pass |
|---|---|---|---|
| 1 | `data_pipelines-tutorials/data_pipelines/src` | ACB on-prem batch/reporting platform. Airflow 2 + Impala/Kudu config tables + Iceberg on HDFS + YARN. ~110 pipeline dirs. | Orchestration, plugins, config DDL, `lib_common`, `c2pp` read in full. The ~110 per-pipeline dirs sampled, not exhaustively read. |
| 2 | `.../src/pipelines/f_acct_depo_by_day` | The reference pilot mart. | All 4 live `code/*.py` + `sqls/` + `additional/` + `load_hist/` read. `code/archive/` and `code/bk/` are byte-identical duplicates — skipped after diffing names. |
| 3 | `plstream_merged` | ACB near-real-time P&L reporting: Spark Structured Streaming + Airflow lifecycle. | `v9/flow3_pl_ceo_v2` phase A/C, `v9/airflow`, configs read. `_shared_src/` is duplicated verbatim into each pipeline dir — read once. |
| 4 | `TTC-Repo code/ttc-dlh-ops-dbt-library` | dbt-spark project `ttc_dbt`. | `dbt_project.yml`, `profiles.yml`, all 3 macros read. `spark_venv/`, `redshift_venv/`, `aws/`, `target/`, `logs/` are vendored artefacts — not source. |
| 5 | `TTC-Repo code/ttc-dlh-ops-data-platform` | Terraform: EMR Serverless + Step Functions + Lambda orchestrator + DynamoDB registry. | `IaC/modules/data_platform` read (TF + lambda + emr-serverless scripts). `modules/xdata` (OpenMetadata/chatbot) skimmed — out of scope. |
| 6 | `TTC-Repo code/ttc-dlh-ops-etl-library` | `express_data_transform` Python package. | `core/` read in full; `sparknode/writer.py` and `reader.py` read by signature + the Iceberg/merge paths. `openmetadata/schema/**` is generated Pydantic — skipped. |
| 7 | `TTC-Repo code/ttc-dlh-shared-service-base-infra` | CI/CD + Terraform state prerequisites. | Read in full (42 files). **Contains nothing reporting-specific** — see §12. |
| 8 | `aws-cdc-lakehouse` | Target repository. | `spark/`, `airflow/`, `dbt/`, `terraform/modules`, `terraform/envs/dev`, `docs/` read. |
| 9 | `aws-cdc-lakehouse-claude-guide-v2` | Prompt/guide set. | Prompt index, `MASTER_PLAN.md`, the 3-layer architecture prompt structure read. |

**Naming caution applied throughout.** Path 8 uses `stream`/`full_cdc`/`snapshot` for
L1/L2/L3. Path 9's architecture prompt uses `FULL_CDC`/`EOD`/`REALTIME`. `full_cdc` denotes
different things in the two. Rows below say which repository's vocabulary is in use.

---

## 1. EOD

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `src/lib_common/lib/helpers.py:530` | `get_ready_eod(spark, dependencies, execution_date, job_name)` — the readiness gate. Three conditions must all hold: a row in `dlpt.eod_info` with `cob_date = execution_date` per dependency (`:544`), an Iceberg **ref named `TAG_<execution_date>`** on each `sat_snp_*` table (`:566-575`), and upstream jobs' `summary_config_v1.execution_date` already past the date (`:534-542`). Sets `is_ready_eod=1` only when `len(range_rows) == len(table_have_tag)` **and** counts cover all dependencies (`:577`). Writes a `dlpt.log_tag_eod` audit row on success (`:583-585`). |
| 1 | `src/lib_common/lib/helpers.py:321` | `get_eod_range_time(...)` — derives per-source window `[prev_datelastmaint, datelastmaint)` **and resolves `<tbl>_snp_id` per dependency**, so the job reads a pinned Iceberg snapshot, not "current". Falls back to the `main` ref when no tag exists (`:344-353`). |
| 1 | `src/scheduler/airflow/plugins/job_scheduler/functions.py:1387` | `impala_get_ready_eod(job_name)` — the Airflow-side duplicate of the same gate, reading `dlpt.summary_config_v1` + `dlpt.eod_info`. **Two implementations of one rule** (Spark-side and Airflow-side); `f_acct_depo_by_day_level_acct_auto_correct_generate.py:141` raises if they disagree. |
| 1 | `src/scheduler/airflow/dags/pipelines/pipeline_builder.py:1514` | `build_eod_job_dag(...)` — the `job_type='eod'` DAG shape (332 lines, the largest builder). |
| 1 | `src/scheduler/airflow/dags/orchestration/trigger_eod.py`, `rerun_eod.py` | Manual EOD trigger and rerun entry points. |
| 1 | `src/c2pp/init/data_lake_init.sql:40` | `job_master.only_eod INT` — "this job may run only when sources are closed". |
| 2 | `code/f_acct_depo_by_day_level_acct_auto_correct_generate.py:133` | `is_ready_eod_value, _ = get_ready_eod(spark, dependencies, execution_date)`; a non-working day forces ready (`:135-136`); a conflict with the Airflow-side flag raises (`:140`). |
| 2 | same file `:1281-1315` | The EOD-only branch: `if is_ready_eod_value == 1:` deletes and rebuilds the PIT rows for the main stream table. |
| 2 | same file `:1359-1372` | **On EOD only** (`if is_ready_eod_value == 1:` at `:1359`), advances `summary_config_v1.execution_date` to the next working day and sets `is_first_running = 1`. This is how the business date rolls forward. |
| 3 | `v9/airflow/dags/bcn_realtime_dags.py:84` | `_resolve_cob_date(dim_times_table, working_flag, **context)` — resolves COB as the nearest **working day strictly before** the run date, explicitly replacing a fixed `run_date - N` offset (rationale in `configs/bcn_pipeline.yaml:52-68`). |
| 3 | `v9/airflow/dags/bcn_realtime_dags.py:132` / `:170` | `_check_eod_info(dependencies_cdp)` / `_check_dwh_ready(dependencies_dwh)` — poll gates, 48 × 5 min. |
| 3 | `v9/airflow/dags/bcn_realtime_dags.py:397` | `publish_eod_done_marker` — writes an HDFS marker `eod_done_<date>`. `configs/bcn_pipeline.yaml:29-31` records the choice of a marker over `ExternalTaskSensor` to avoid execution-date skew between DAGs on different schedules. |
| 3 | `v9/flow3_pl_ceo_v2/0_phase_eod/src/eod_clean.py`, `eod_build_base.py`; `config/eod_config.yaml` | EOD as two Spark applications; DDL for 16 tables externalised verbatim to `sql/ddl_all_tables.sql`. `eod_config.yaml:28-31` — checkpoint/state dirs must be deleted **before** dropping tables, else a restarted stream reads a checkpoint pointing at a dropped table. |
| 4 | ABSENT | dbt project has no EOD gate concept. |
| 5 | ABSENT | Step Functions orchestration is time/queue-driven (`orchestration_lambda.py` `startTimeBasedJob`), not close-gated. |
| 6 | ABSENT | |
| 7 | ABSENT | |
| **8** | `spark/eod/ddl/ops_tables.sql:42` | `glue_catalog.ops.eod_watermark(layer, table_name, business_date, completed_at, run_id, row_count)` — comment: written **only after write + validation both pass**. This is already an EOD-close signal. |
| **8** | `spark/eod/stream_to_full_cdc.py`, `spark/eod/window.py`, `spark/eod/audit.py` | The L1→L2 EOD transition and its window/audit. |
| **8** | `spark/common/flows.py:47`, `:52` | `FLOW_EOD = "EOD"`; `FLOW_STATUS[FLOW_EOD] = "CERTIFIED"`. |
| **8** | `spark/common/flow_runner.py:86-93` | EOD reads **L3 snapshot**, not L1, filtered on `snapshot_date`. |
| **8** | `airflow/dags/dag_eod_pipeline.py:52` | DAG `eod_certified_pipeline`, `schedule="30 1 * * *"`. |
| **8 gap** | — | No Iceberg **tag** is written at EOD close anywhere in `spark/`. `grep -rn "create_tag\|system.create_tag"` over `spark/` returns nothing. The gate is watermark-row-only. |

---

## 2. AUTO_CORRECT

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `src/scheduler/airflow/dags/pipelines/pipeline_builder.py:1236` | `build_auto_job_dag(...)` — the `job_type='auto'` DAG. |
| 1 | `.../pipeline_builder.py:1333` | `build_first_running_job_dag(...)` — a second DAG per auto job, for the first run after EOD. |
| 1 | `.../pipeline_builder.py:1108` | `_read_first_running_config(**context)`. |
| 1 | 78 files named `*_auto_correct_generate*.py` under `src/pipelines/` | The dominant job type in the platform. |
| 2 | `code/f_acct_depo_by_day_level_acct_auto_correct_generate.py:98-120` | Reads `summary_config_v1` for `job_id, job_name, is_first_running, execution_date, dependencies, execution_date_ref`. |
| 2 | same file `:194-215` | **The idempotency mechanism**: reads `auto_correct_snapshot_id` for the previous run of this `execution_date` (`:196`), then `CALL spark_catalog.system.set_current_snapshot('dlpt.<tbl>', <id>)` over 8 target tables (`:211`) — i.e. it *rewinds the targets* and recomputes. Two further rewinds at `:493` and `:618`. |
| 2 | same file `:1390-1415` | After the run, captures `snapshot_id` from `dlpt.<tbl>.history` for each target and appends to `auto_correct_snapshot_id` (`:1415`) — the next run's rollback point. |
| 2 | same file `:1331-1356` | Appends to `summary_config_hist_v1` with `indicator ∈ {'automation','manual'}`. |
| 2 | `additional/submit_auto.sh` | The spark-submit for this flow: `--driver-cores 3 --driver-memory 8g --executor-cores 3 --executor-memory 8g --num-executors 5`. |
| 2 | — | **No impact analysis.** The job recomputes the whole business date every cadence; there is no "which keys changed" step. |
| 3 | `v9/flow3_pl_ceo_v2/1_phase_a/flow3_pl_batch_autocorrect/src/main_autocorrect.py:1113` | `main()` — a 6-step + 4-step-compaction pipeline (architecture comment at `:4`). **This is a real impact analysis**, which repo 1 lacks. |
| 3 | same file `:472` / `:493` / `:514` / `:535` / `:585` | The five scan sources that define the affected set: `_get_stream_today_acctnbrs`, `_get_incomplete_dim_acctnbrs`, `_get_pending_timeout_acctnbrs`, `_get_missing_accounts` (anti-join silver vs BASE), `_get_dim_change_acctnbrs`. Each toggled by `pipeline_config.yaml` `autocorrect.scan_*`. |
| 3 | same file `:835` | `_merge_into_base(...)` — re-enrich then MERGE, restricted to the affected accounts. |
| 3 | same file `:946` / `:1045` | `_update_pending_timeout_resolved`, `_update_dim_change_processed` — closes the metadata loop so a correction is not re-applied. |
| 3 | `.../flow3_pl_batch_autocorrect/config/pipeline_config.yaml` `autocorrect:` block | `cron_interval_minutes: 30`, `cdc_window_start_hour: 16` (window = 16:00 T-1 → now, with the reason stated: EOD completes ~03:00 T, so events between 16:00 T-1 and 03:00 T can precede the silver close), `audit_retention_days: 7`. |
| 3 | `v9/airflow/dags/bcn_realtime_dags.py:473-511`; `configs/bcn_pipeline.yaml` `AUTOCORRECT:` | `SCHEDULE: "0 9-19/3 * * 1-6"`, `EXECUTION_TIMEOUT_MINUTES: 110`, `REQUIRE_STREAM_RUNNING: true` — refuses to run if the streaming app is not up. |
| 4–7 | ABSENT | |
| **8** | `spark/common/flows.py:46`, `:53` | `FLOW_AUTO_CORRECT`; stamps `PROVISIONAL_CORRECTED`. |
| **8** | `spark/common/flow_runner.py:81-84` | Reads the **whole** of day T from L1, deliberately ignoring the watermark ("a correction pass that trusted the watermark would inherit the very gap it exists to close"). Same posture as repo 1; no impact analysis. |
| **8** | `spark/correct/auto_correct.py` (15 lines) | Thin driver over `run_flow.run`. |
| **8** | `airflow/dags/dag_intraday_flows.py:31-41` | `flow_auto_correct`, `schedule="*/30 * * * *"`, `execution_timeout=25min`. |
| **8** | `spark/common/flows.py:127` `may_overwrite` / `:150` `merge_condition_sql` | Idempotency by **guarded MERGE** (higher tier wins; within a tier the later `input_cutoff` wins) rather than repo 2's target rewind. |

---

## 3. FULFILL

Three distinct things exist across the references under overlapping names.

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `src/scheduler/airflow/dags/pipelines/pipeline_builder.py:1417` | `build_alone_job_dag(...)` for `job_type='load_hist'`; created with `is_paused_upon_creation=True` and `spark_op_params={"use_execution_hist": False}` (`pipeline_auto_generator.py:31-46`) — historical backfill, deliberately outside the execution-history machinery. |
| 1 | `src/pipelines/*/load_hist/` (e.g. `f_acct_depo_by_day/load_hist/f_acct_depo_auto_correct_generate_load_hist.py`, 780 lines) | Per-mart backfill applications. |
| 1 | `src/scheduler/airflow/plugins/job_scheduler/functions.py:790` | `fulfill_job_master(file_path, date_of_data, sheet_name)` — **loads job master rows from an Excel worksheet**. Confirmed by `src/scheduler/airflow/configs/Danh_sach_bang_TEST_DWH1.xlsx` and `dags/orchestration/fulfill_job_master.py`. |
| 1 | `.../functions.py:878` | `fulfill_job_resource(file_path, sheet_name)` — same, for Spark resources. |
| 2 | `code/f_acct_depo_by_day_level_acct_auto_correct_generate_fulfilled.py` (793 lines) | "Fulfilled" here = the **late close** of a business date whose EOD arrived after the normal window; reads `summary_config_v1` where `job_name = '..._fulfilled'` (`:53`). Not a date-range backfill. |
| 2 | `additional/submit_fullfilled.sh` | Its submit: `--driver-memory 10g --executor-memory 20g --num-executors 4` — a heavier profile than the daily run. |
| 1/2 | — | **No gap detection anywhere.** No code computes expected-vs-successful business dates. Date selection is manual. No dry-run. |
| 3 | `v9/flow3_pl_ceo_v2/0_phase_eod/scripts/submit_eod_build_base.sh`; `manual_eod_spark_submit/run_eod_manual.py` | Manual re-close for a named date. Again no gap detection. |
| 4–7 | ABSENT | |
| **8** | `spark/full_fill/full_fill.py` (18 lines), `spark/common/run_flow.py:53` `_run_full_fill` | **A different concern with a colliding name.** `flow_runner.py:107 unresolved_rows(...)` selects mart rows where `account_sk/channel_sk/merchant_sk == UNKNOWN_SK (-1)` or `amount_base IS NULL`; the flow re-resolves dimension keys through the same transform. `flows.py:99-108` raises if anyone asks it for a `status` — it deliberately stamps none, so it cannot downgrade a certified row. This is **dimension repair**, not business-date backfill. |
| **8 gap** | — | No business-date backfill flow, no gap detector, no dry-run planner. |

---

## 4. STREAM_BATCH (Airflow-scheduled micro-batch)

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `src/scheduler/airflow/dags/pipelines/pipeline_builder.py:1460` | `build_stream_job_dag(...)` — `job_type ∈ {'stream','stream_ref'}`, `schedule=None`, `max_active_runs=1`, `is_paused_upon_creation=True` (`pipeline_auto_generator.py:47-81`). Driven by the coordinator, not by cron. |
| 1 | `.../pipeline_builder.py:1193` / `:1206` | `_pause_stream_dag` / `_unpause_stream_dag` — the EOD job pauses the micro-batch DAG while it closes the day. |
| 2 | `code/f_acct_depo_by_day_level_acct_streaming_generate.py:186-203` | **The watermark**: `read_dim_thin(...)` returns the single-row table `d_0_<fact>`; `before_system_date_time = 3` minutes is subtracted to give `start_system_time`. Sentinel `-1` → `default_system_date_time = '1991-01-31 23:59:59.000001'` for a cold start. |
| 2 | same file `:250-257` | Cold-start fallback: `to_back = 60*24*2` minutes (2 days) back from `execution_date`. |
| 2 | same file `:387`, `:508`, `:517` | The watermark applied as source predicates (`abh.dv_ldt >= '{start_system_time}'`, `rtxn_dv_ldt >= ...`, and one with an `- interval 8 hours` skew allowance). |
| 2 | same file `:1380` | `update summary_config_v1 ...` after success. |
| 2 | `.../additional/1.create_tables_level_acct.sql:556` (sibling mart) | `dim_max_time_auto_f_acct_loan_by_day_incremental(max_systime_rawvault TIMESTAMP, dv_kaf_ofs BIGINT)` — the watermark table shape: timestamp **plus Kafka offset**. |
| 3 | — | Repo 3's "autocorrect" is a scheduled batch, but it is a correction pass, not a watermark-advancing incremental. See §2. |
| 4–7 | ABSENT | |
| **8** | `spark/common/flow_runner.py:62` | `NRT_SAFETY_OVERLAP_MINUTES = 2` — same idea as repo 2's 3-minute rewind, same stated reason (an event written just before the recorded watermark otherwise falls between two runs). |
| **8** | `spark/common/flow_runner.py:71-79` | NRT source selection: `[max(watermark - overlap, day_start), input_cutoff)`. |
| **8** | `spark/common/flow_runner.py:95` `_from_l1` | Half-open `[low, high)` on `source_commit_ts` — business time, not ingest time. |
| **8** | `spark/streaming/nrt_mart.py` (18 lines); `airflow/dags/dag_intraday_flows.py:26-33` | `flow_nrt_mart`, `schedule="*/5 * * * *"`, `execution_timeout=8min`, `max_active_runs=1`. |
| **8** | `spark/eod/ddl/ops_tables.sql:7` | `ops.layer_watermark(layer, table_name, watermark_ts, run_id)` — per-layer, **not** per (job, flow_mode). |
| **8 gap** | — | No per-job/per-mode watermark table; the NRT watermark is passed in as a function argument (`run_flow.run(..., watermark=...)`, `run_flow.py:32`) with no persistent store keyed by job. |

---

## 5. STREAMING_RT (long-running application)

Only repo 3 implements this.

| Repo | Evidence | Construct |
|---|---|---|
| 3 | `v9/flow3_pl_ceo_v2/1_phase_a/flow3_pl_stream_main/src/main.py:1028-1031` | `.writeStream.foreachBatch(handler.process).option("checkpointLocation", config.streaming.checkpoint_dir).trigger(processingTime=config.streaming.trigger_interval)`; `:1049` `query.awaitTermination()`. |
| 3 | same file `:254` | `MicrobatchHandler.process(self, batch_df, batch_id)` — the foreachBatch entry point. |
| 3 | same file `:299`, `:540-545` | The failure contract, stated explicitly: with `foreachBatch`, returning quietly = Spark commits the offsets = the batch is lost (`~100K offsets`, `max_offsets_per_trigger`). Enrichment failures are audited and raised, not swallowed. |
| 3 | same file `:813` | `_warn_if_stale_checkpoint(spark, config)` — inspects `offsets/` under the checkpoint dir and warns that `starting_offsets: latest` applies **only** when the checkpoint is empty. Deliberately does **not** auto-delete (`:822`). |
| 3 | same file `:361` | `_maybe_compact(batch_id)` — compaction inline in the trigger loop, not in a thread. |
| 3 | same file `:672` `_enqueue_partial`, `:726` `_detect_missing_dims` | Late-dimension handling into `PendingQueue`. |
| 3 | `.../_shared_src/pipeline_config.yaml` `streaming:` | `trigger_interval: "30 seconds"`, `checkpoint_dir: "hdfs://.../warehouse/_checkpoint/pl_init_bcn_sodu"` — one stable path per app, config-driven. |
| 3 | same file `kafka:` | `starting_offsets: latest`, `max_offsets_per_trigger: 100000`, `fail_on_data_loss: false`, `consumer_group: pl_stream_main_bcn_sodu`, 8 topics, SSL truststore/keystore. |
| 3 | same file `iceberg.tables` | `watermark_table: dlpt.dim_autocorrect_watermark`, `pending_timeout_table: dlpt.dim_pending_timeout_acctnbrs`, `dim_change_audit_table: dlpt.dim_dim_change_audit` — the app's own state tables. |
| 3 | same file `cache.detect_first_after_eod: true` | Detects the first run after EOD via the watermark table and forces a full cache reload instead of an incremental one. |
| 3 | `v9/airflow/dags/bcn_realtime_dags.py:223` / `:236` / `:250` | `bash_kill_app(app_name)`, `bash_wait_eod_marker()`, `bash_start_longrun(app_name, submit_rel, submit_args, ...)` — the lifecycle primitives. Airflow starts, monitors and stops; it does not drive the batches. |
| 3 | `configs/bcn_pipeline.yaml` `STREAM:` / `SHUTDOWN:` | Start `"0 8 * * 1-6"`, `STARTUP_TIMEOUT_MINUTES: 15`, stop `"0 20 * * 1-6"` by **app name** — a bounded 12-hour daily window, not 24/7. |
| 3 | `1_phase_a/flow3_pl_stream_main/scripts/submit_stream.sh` | `nohup bash scripts/submit_stream.sh > .../logs/stream_main_$(date +%F).log 2>&1 &`; zips `src/*.py` to `/tmp/pl_stream_src.zip`; `kinit` from keytab; kill = `yarn application -kill`. |
| 3 | `3_phase_c/flow3_pl_ceo_report/config/datamart_config.yaml` | A **second** long-running app (the datamart), separate checkpoint `_checkpoint/pl_datamart`, `SUBMIT_ARGS: "--override-mode polling"`. |
| 1, 2, 4, 5, 6, 7 | ABSENT | Repo 1's `job_type='stream'` is Airflow-triggered batch (§4), despite the name. |
| **8** | ABSENT | No long-running application, no `streaming_app_state`, no restart policy. |
| **8** | `spark/common/flow_runner.py:12-18` | A recorded decision **against** always-on streaming: "Structured Streaming with an always-on cluster would hold EMR Serverless capacity for 24h/day to serve a demo that runs for an hour." |
| **8** | `terraform/modules/data_lake/variables.tf:92-95`, `:110` | `checkpoints/` is a top-level S3 prefix, and a variable validation **rejects** any checkpoint path under `warehouse/` — because `remove_orphan_files` would delete it. `outputs.tf:26` exposes `checkpoints_uri`. The convention exists; nothing writes to it yet. |
| **8** | `terraform/modules/data_lake/variables.tf:46-56` | `checkpoint_expiration_days >= 2` validation, set to `14` in `envs/dev/terraform.tfvars`. |

---

## 6. DBT-SPARK

| Repo | Evidence | Construct |
|---|---|---|
| 4 | `ttc_dbt/profiles.yml:1-16` | `ttc_dbt_profile`, `target: spark`, `type: spark`, **`method: session`**, `schema: temp_db`, `host: NA`, `server_side_parameters: {spark.sql.catalog.glue.glue.skip-name-validation, spark.sql.catalog.glue.glue.id}` (cross-account Glue). Second target `redshift` with `method: iam`, `user: "USEIAM"`, `ra3_node: true`. |
| 4 | `ttc_dbt/dbt_project.yml:38-...` | Model tree nested **database → schema → layer** (`agrc.harvest.marts` / `.intermediate` / `.staging.frm_mssql`). `+database: "{{ 'agrc' if target.name in ('redshift','spark') else target.database }}"` — the same model tree materialises to Glue/Iceberg or Redshift by target. `+file_format: iceberg` on staging/intermediate. |
| 4 | `ttc_dbt/macros/get_custom_schema.sql` | `generate_schema_name(custom_schema_name, node)` derives the schema from `node.fqn[1] + "_" + node.fqn[2] + "_" + ("marts" if fqn[3]=="marts" else "staging")` — no per-model schema declaration. |
| 4 | `ttc_dbt/macros/spark_temp_table.sql` | `{% materialization temp_table, adapter='spark' %}` — emits `CREATE OR REPLACE TEMPORARY VIEW`. |
| 4 | `ttc_dbt/target/manifest.json` (present) | The compiled artefact exists; **nothing in repos 4–6 consumes it for dependency registration**. |
| 5 | `IaC/modules/data_platform/emr-serverless/run_dbt_job.py` | The execution wrapper. Downloads `s3://{CODE_BUCKET}/dbt/ttc_dbt.zip`, unzips, reads Redshift creds from Secrets Manager, **writes `profiles.yml` at runtime** (`:92-121`), then `dbt = dbtRunner(); dbt.invoke(cli_args)` with `["run","--project-dir",...,"--target",job["target"],"-s",job["modelFqn"],"--vars","{data_date: "+args.dataDate+"}"]`. Retry loop `while not is_successful and number_of_retries < 8`. |
| 5 | `.../lambda/orchestration_lambda/execute_job_logic.py:223` | `execute_dbt_job(data_date, jobId, engine)` → `execute_emr_serverless("run_dbt_job", ["--openmetadataSecrets",...,"--jobId",jobId,"--dataDate",data_date])`. |
| 5 | `.../ochestration.tf:61`, `:213` | `DBT_JOB_TABLE = "${local.general_prefix}-dbt-job"`; `aws_dynamodb_table "dbt-job"` with `hash_key = "id"`, `PAY_PER_REQUEST`. The dbt job registry is **DynamoDB, with no Git representation**. |
| 6 | `src/express_data_transform/sparknode/writer.py:1013` | `iceberg_upsert_by_primary_key(data, spark, database_name, table_name, primary_key, hash_column)` — builds `MERGE INTO ... USING temp_table AS source ON <pk eq> WHEN MATCHED [AND target.<hash> != source.<hash>] THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...` (statement at `:1045-1050`). The optional `hash_column` is a change-detection short-circuit. |
| 6 | `.../writer.py:1104` | `iceberg_writer(..., mode)` — `overwrite | overwrite_partitions | append | upsert` (docstring `:1127`); `upsert` dispatches to the above at `:1210`. |
| 6 | `.../writer.py:827` | `redshift_writer_v2(..., mode="upsert", primary_key=...)` — the same strategy vocabulary for the Redshift target. |
| 1 | `src/c2pp/configs/*/yamls/*.yaml` | ACB's dbt-less equivalent: `procedures:` maps `source_view → sink_table` with `loading_engine ∈ {spark,impala}`, `write_type ∈ {incremental,overwrite,append}`, `business_keys`. Same concept, hand-rolled. |
| 2, 3, 7 | ABSENT | |
| **8** | `dbt/dbt_project.yml` | `name: aws_cdc_lakehouse`, `require-dbt-version: [">=1.9.0","<1.10.0"]`, **no `packages.yml`** (deliberate — no network fetch at build time). `vars: {business_date: "1970-01-01", reconciliation_tolerance_pct: 5.0}`. Marts: `+materialized: incremental`, `+incremental_strategy: insert_overwrite`, `+partition_by: ["business_date"]`, `+file_format: iceberg`. Staging and intermediate are `ephemeral` with a recorded reason (Iceberg VIEW support varies by catalog). |
| **8** | `dbt/profiles.yml.example` | Target `local`: `method: session`. Target `emr_serverless`: **`method: thrift`**, requiring `DBT_SPARK_HOST` / `DBT_SPARK_PORT`. |
| **8** | `dbt/models/` | 3 staging + 3 intermediate + 4 marts + `sources.yml` + `marts/schema.yml`; 8 singular tests in `dbt/tests/`. `dbt/macros/`: `audit_columns.sql`, `generic_tests.sql`, `status_priority.sql`. |
| **8** | `docs/DBT_SPARK.md` §2 | The boundary: dbt owns SQL modelling, marts, tests, docs, lineage; it does **not** own the canonical transformation, the guarded MERGE, SCD2, or the L1/L2/L3 jobs. |
| **8 conflict** | — | Repo 5 proves `method: session` inside an EMR Serverless job run. Repo 8's `emr_serverless` target needs a Thrift host/port, which EMR Serverless does not expose; supplying one implies an always-on Spark application, contradicting `CLAUDE.md` §4.5/§4.6. Nothing in repo 8 currently invokes dbt at all (`grep -rn "dbt" airflow/dags scripts/*.sh` finds only `scripts/dbt-verify.sh`, `dbt_check_counts.py`, `dbt_seed_fixture.py` — verification helpers, not an orchestrated run). |

---

## 7. DEPENDENCY

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `src/c2pp/init/data_lake_init.sql:29-36` | `before_direct`, `after_direct`, `before_full`, `after_full`, `before_direct_by_mode`, `after_direct_by_mode`, `before_full_by_mode`, `after_full_by_mode` — all `VARCHAR(1000)`, **comma-separated strings**. Plus `dwh_dependencies VARCHAR(1000)` (`:44`). |
| 1 | `src/scheduler/airflow/plugins/job_scheduler/functions.py:107` | Graph traversal in SQL by substring match: `instr(concat(",", b.after_full, ","), concat(",", a.job_name, ",")) > 0`. |
| 1 | `.../functions.py:73-75`, `:1072-1078` | `list_cols = ["before_direct","after_direct","before_full","after_full"]` split on `,` at load; the `_by_mode` variants added in `_preprocessing_conf`. |
| 1 | `src/c2pp/init/data_lake_init.sql:37-38` | `order_by_default INT`, `order_by_mode INT` — **hand-maintained turn numbers**, not computed. |
| 1 | `.../coordinator_graph_builder.py:15` | `build_task_graph(task_group_name, job_type, job_config, job_coordinator_mode)`; groups jobs into turns by that integer (`:18-28`); three wirings at `:44-66`. |
| 1 | `.../pipeline_builder.py:1023` | `_check_before_full(**context)` — the runtime dependency gate. |
| 1 | `.../functions.py:301` | `get_target_and_downstream_jobs(jobs_name)` — downstream traversal for kill/deactivate. |
| 1 | — | **No cycle detection anywhere.** `grep -rn "topological\|cycle\|networkx\|nx\." src/scheduler` returns nothing. |
| 1 | `src/c2pp/configs/*/yamls/*.yaml` | The four typed dependency classes: `rawvault_dependencies`, `bizvault_dependencies`, `dwh2cdp_dependencies`, `dwh_dependencies`, `import_data_dependencies` — still comma-separated, but **typed by source system**, which the flat `job_master` columns are not. |
| 1 | `.../c2pp_dags/dag_builder.py:108-125` | One `_check_if_ready_eod` task **per dependency class**, wired conditionally on whether that class is non-empty. |
| 3 | `configs/bcn_pipeline.yaml` `EOD.DEPENDENCIES_CDP` / `DEPENDENCIES_DWH` | Dependencies as **YAML lists** (not strings), with the instruction "add a table = add one line here, nothing else changes". `DEPENDENCIES_DWH: []` and the check auto-disables. |
| 6 | `src/express_data_transform/core/base.py:36` | `class Job(nx.DiGraph)` — a real graph. `:58` `terminal_node` (nodes with no successors); `:108` `run()` executes `list(nx.topological_sort(self))` in order, registering each node's DataFrame as a temp view named after the node, and setting `_status = "FAIL"` + `_error_message` on the first exception. |
| 6 | `.../core/base.py:142` `Node`, `:178` `Root`, `:185` `Starter`, `:189` `SingleSource`, `:197` `MultipleSource` | The node taxonomy that defines arity. |
| 6 | `.../core/nodes.py:5` `ReaderNode`, `:16` `WriterNode`, `:73` `MergeNode`, `:86` `MultipleNode` | Node kinds. |
| 6 | `.../core/base.py:8-33` | `State` / `Success` / `Failed` / `Skipped` — a per-node result type, not a string. |
| 4 | `ttc_dbt/target/manifest.json` | `depends_on.nodes` is the natural dependency source; unused for scheduling. |
| 5 | `.../stepfunction/orchestration_sfn.json` | Linear state machine (`Run Job → Get Job Status → Job Complete? → …`). **No inter-job dependency graph** — ordering lives in the Lambda's queue logic, not in a DAG. |
| 2, 7 | ABSENT | |
| **8** | `airflow/dags/dag_eod_pipeline.py:20-27` | Dependencies expressed as **prose and DAG edges**, with each arrow justified as a correctness constraint (L2 before L3; dimensions before facts; facts before dbt; reconciliation before maintenance). |
| **8** | `airflow/dags/dag_eod_pipeline.py:44-50` | `MAINTAINED_TABLES` — a hardcoded literal list. `:89-90` iterates a literal entity tuple. |
| **8** | `dbt/models/sources.yml`, `ref()` in `dbt/models/**` | dbt's own graph, e.g. `mart_account_balance_daily.sql:16` `FROM {{ ref('stg_fact_account_daily_snapshot') }}`. |
| **8 gap** | — | No `job_dependency` table, no graph resolver, no cycle detection, no turn computation. Adding a mart requires editing a DAG file. |

---

## 8. JOB HISTORY

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `src/c2pp/init/data_lake_init.sql:49-95` | `job_master_execution_hist`, `PRIMARY KEY (date_of_data, job_name, turn)`, 45 columns: `turn`, `turn_watermark`, `is_rerun`, `is_eod`, `estimated_duration`, `status`, `note_for_tracing`, `spark_app_id`, `spark_app_status`, `spark_app_tracking_url`, `driver_cores`, `driver_memory`, `executor_cores`, `executor_memory`, `executor_instances`, `airflow_dag_id`, `application`, `application_args`, `started_at`, `ended_at`, `updated_at`, `error_msg VARCHAR(5000)`, `trigger_type`, `dwh_dependencies`, plus all eight dependency strings copied from master. |
| 1 | `src/c2pp/init/data_lake_init.sql:20-47` | `job_master`, `PRIMARY KEY (job_name)` — **carries `date_of_data DATE` (`:28`)**, so it is current-state, not pure config. |
| 1 | `src/c2pp/init/data_lake_init.sql:2-17` | `summary_config`, `PRIMARY KEY (job_name)`: `execution_date`, `start_date`, `end_date`, `status`, `dependencies`, `kickoff_condition`, `active`, `is_first_running`, `execution_date_ref`, and three `is_first_running_after_eod_in_*_flow` flags. |
| 1 | `.../functions.py:953` | `gen_new_execution_hist_for_job(job_name)` — next turn = `CASE WHEN status IS NULL THEN turn ELSE turn + 1 END` (`:986-988`); an un-started row is reused, a finished one spawns a new turn. |
| 1 | `.../functions.py:1119` | `sync_job_master_hist(job_type, ...)` — first an `UPDATE dlpt.job_master SET date_of_data = ...` that pushes non-EOD jobs' dates forward to follow their `before_full` upstreams (`:1121-1158`), then the bulk upsert of new turns. |
| 1 | `.../functions.py:1053` | `load_unfinished_execution_hists(job_name, ...)` — finished set is `["SUCCEEDED","SUCCEEDED_BY_SKIPPING","FAILED","KILLED","FINISHED"]`. |
| 1 | `.../settings.py:44` | `class JobStatus(enum.Enum)`: `INITIALIZED, PROCESSING, KILLED, KILLED_BY_DEACTIVATION, SUCCEEDED, SUCCEEDED_BY_SKIPPING, FAILED`. Note the strings actually written (`"PROCESSING"`, `"SUCCEEDED"`) are literals in SQL, not this enum. |
| 1 | `.../functions.py:770` `update_job_master`, `:779` `update_execution_hist(job_name, date_of_data, turn, update_dict)`, `:331` `update_execution_hist_for_killed_job`, `:730` `update_hist_for_skipping_job` | The mutation surface. |
| 1 | `.../pipeline_builder.py:659` `handle_final_job_status`, with `.../functions.py:363` `get_spark_error_msg` | **Defect to avoid:** greps the YARN log for the literal string `JOB_DONE` and, if found, flips a `FAILED` run to `SUCCEEDED` (`pipeline_builder.py:686-689`). A second heuristic treats "`summary_config.execution_date` moved past `date_of_data`" as proof of success (`:690-697`). |
| 2 | `code/..._auto_correct_generate.py:1331` | `insert into summary_config_hist_v1(job_id, job_name, execution_date, is_first_running, is_first_running_after_eod_in_auto_correct_flow, is_first_running_after_eod_in_streaming_flow, etl_date, execution_date_ref, indicator)`. `indicator = 'manual' if is_manual_configs else 'automation'`. |
| 2 | `.../additional/submit_auto.sh` | Spark app **name** encodes the history key: `--name f_acct_depo_by_day_level_acct_auto_correct_generate__2024-06-21__1` = `<job_name>__<date_of_data>__<turn>`. That string is the correlation handle between YARN and the history table. |
| 5 | `.../ochestration.tf:184-211` | `aws_dynamodb_table "etl_execution"`, `hash_key = "id"`, GSI `status-index` on `status`, `PAY_PER_REQUEST`. |
| 5 | `.../lambda/orchestration_lambda/job_run_table_logic.py` (377 lines), `db.py` (121) | The execution-record CRUD layer. |
| 5 | `.../stepfunction/orchestration_sfn.json` | `updateStatus` with `status: running | failed | success` — a **three-value** status model. |
| 6 | `.../core/base.py:97-101`, `:140-141` | `Job.get_status()` / `get_error_message()`; `_status ∈ {"RUNNING","FAIL","SUCCESS"}`. In-memory only — nothing is persisted. |
| 3 | `.../_shared_src/pipeline_config.yaml` `iceberg.tables` | `dim_autocorrect_watermark`, `dim_pending_timeout_acctnbrs`, `dim_dim_change_audit` — per-application state tables, no generic run history. |
| 7 | ABSENT | |
| **8** | `spark/common/ddl/mart_and_certification.sql:80` | `ops.data_certification(run_id, source_flow, processing_status, business_date, input_cutoff, row_count, amount_sum, unknown_account, unresolved_amount, rows_before, rows_after, status, notes, recorded_at)` — **APPEND-ONLY**, `status ∈ {SUCCEEDED, FAILED}`. Written by `spark/common/flow_runner.py:117 record_certification`. |
| **8** | `spark/eod/ddl/ops_tables.sql:20` | `ops.reconciliation_run(run_id, layer, table_name, business_date, started_at, finished_at, input_count, output_count, duplicate_count, quarantine_count, watermark_low, watermark_high, status, failure_reason)`, `status ∈ {RUNNING, SUCCEEDED, FAILED}`. |
| **8** | `spark/ops/ddl/governance_tables.sql:9` / `:34` | `ops.dq_result` with a **three-value verdict** `PASS | FAIL | NOT_EVALUATED` and `severity ∈ {ERROR, WARN}`; `ops.lineage_event` carrying `parent_run_id` / `parent_job`. |
| **8** | `spark/common/ddl/mart_and_certification.sql:108` | `ops.metric_variance` — provisional vs certified. |
| **8** | `airflow/dags/common.py:86` | `run_id_arg()` = `"{{ dag.dag_id }}__{{ ds_nodash }}__{{ ti.try_number }}"` — deterministic, chosen over a UUID so a retry reuses the id. Structurally the same three-part key as repo 2's Spark app name. |
| **8 gap** | — | No per-job execution record: no `job_id`, `flow_mode`, `attempt_number`, `turn`, `spark_app_id`, resolved resources, or Airflow `dag_run_id`/`task_id`. The ledgers are keyed by `run_id` + `business_date` + flow, not by a registered job. |

---

## 9. AIRFLOW COORDINATOR

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `.../dags/orchestration/coordinator.py:65` | `build_job_coordinator(dag_id, job_type, dag_default_args, job_config, job_coordinator_mode)` — **one coordinator DAG per `job_type`** (`:110-116`), `schedule=None`. |
| 1 | `.../coordinator_graph_builder.py:31-42` | Each job becomes `TriggerDagRunOperator(trigger_dag_id=job["job_name"], wait_for_completion=True, poke_interval=15, deferrable=True, allowed_states=['success'], failed_states=['failed'], conf=job)` — pointing at a **separate per-job DAG**. Two DAG runs per job execution. |
| 1 | `.../coordinator_graph_builder.py:44-66` | `coordinator_mode`: `native` wires true `after_direct` edges; `sequential` is `chain(*tasks)`; `default`/`custom` fans turn *i-1* out to all of turn *i*. |
| 1 | `.../dags/pipelines/pipeline_auto_generator.py:23-155` | The DAG factory: **one DAG per row of `job_master`**, dispatched by `job_type` to `build_alone_job_dag` / `build_stream_job_dag` / `build_auto_job_dag` + `build_first_running_job_dag` / `build_eod_job_dag` / `build_report_job_dag` / `build_job_dag`. Every one gets `pool=job_type`, `pool_slots=1`, `max_active_runs=1`, `is_paused_upon_creation=True`, `render_template_as_native_obj=True`. |
| 1 | `.../functions.py:28` `change_config_source`, `:42` `load_full_job_master`, `:97` `load_active_job_config`, `:164` `load_job_coordinator_mode`, `:1445` `load_eod_jobs` | **The DAG-parse caching pattern.** Each reads from Impala, writes a JSON cache (`/home/dlpt/tmp/airflow/config/*.json`) with a `cached_at` and a 7-day (1-day for `eod_jobs`) TTL, and falls back to the cache on error. An Airflow `Variable` `config_source ∈ {database, cache_file}` selects the path; `coordinator.py:41-42` and `pipeline_auto_generator.py:20-21` **set it back to `cache_file`** immediately after the first load. |
| 1 | `.../functions.py:106-115` | `load_active_job_config` excludes any job that is transitively downstream of an inactive one (`left anti join` on `after_full`). |
| 1 | `.../coordinator.py:20` | `COORDINATOR_IGNORED_JOB_TYPES` from an Airflow Variable. |
| 1 | `.../dags/orchestration/` (16 DAGs) | The operational surface as DAGs rather than a CLI: `flow_activate_jobs`, `flow_deactivate_jobs`, `flow_skip_jobs`, `flow_unskip_jobs`, `flow_change_run_mode`, `flow_change_coordinator_mode`, `flow_change_eod_mode`, `flow_job_rollback`, `fulfill_job_master`, `fulfill_job_resource_config`, `rerun_eod`, `trigger_eod`, `rewrite_summary_config`, `rewrite_table`. |
| 1 | `.../pipeline_builder.py:453` `read_config` → `:483` `check_is_processing` → `:519` `check_ready_eod` → `:529` `check_is_skipped` → `:543` `handle_run_mode` → submit → `:659` `handle_final_job_status` → `:757` `update_turn_watermark` → `:770` `handle_eod` | The per-job task chain. |
| 1 | `.../pipeline_builder.py:543` | `handle_run_mode`: `cross_turn` accepts a dependency satisfied at any turn ≥ mine and records the satisfying turn into `turn_watermark` (`:656`); `turn_by_turn` requires exactly my turn and **writes FAILED with `note_for_tracing="FAILED_BY_UNSATISFIED_RUN_CONDITION"`** to break an infinite re-check loop (`:644-655`). |
| 1 | `.../c2pp_dags/dag_generator.py` (65 lines), `dag_builder.py` (252), `callable_functions.py` (236), `submit_yaml_config.py` (91) | The **YAML-in-Git → dynamic DAG** alternative the same team built later. |
| 3 | `v9/airflow/dags/bcn_realtime_dags.py` (555 lines) + `configs/bcn_pipeline.yaml` (175) | Five fixed DAGs driven entirely by one YAML: `bcn_eod`, `bcn_stream_start`, `bcn_datamart_start`, `bcn_autocorrect`, `bcn_shutdown`. The file header states the intent: "everything adjustable is here; you never edit the DAG file". |
| 5 | ABSENT (Airflow) | Orchestration is Step Functions + Lambda + EventBridge (`ochestration.tf:130-181`: `startTimeBasedJob`, `checkJobQueueAgain`, `cleanDynamodbLog`, `startExtractionJob`). |
| 2, 4, 6, 7 | ABSENT | |
| **8** | `airflow/dags/common.py:31-32` | `POOL_SPARK = "spark_jobs"`, `POOL_MAINTENANCE = "maintenance"`, with the reason recorded: `max_active_tasks` bounds tasks per DAG, not across DAGs. |
| **8** | `airflow/dags/common.py:34-46` | `DEFAULT_ARGS`: `retries: 2`, `retry_exponential_backoff: True`, `max_retry_delay: 30min`, `execution_timeout: 30min`, `email_on_failure: False` (Alertmanager owns alerting). |
| **8** | `airflow/dags/common.py:96` `default_dag_kwargs` | `catchup=False` everywhere. |
| **8** | `airflow/dags/` | 5 DAGs, all static: `dag_eod_pipeline.py`, `dag_intraday_flows.py` (2 DAGs from one file), `dag_l1_stream.py`, `dag_recovery.py`, `dag_cdc_health.py`. |
| **8** | `airflow/tests/test_dags.py` | Enforces orchestration-only (no transformation logic in a DAG). |
| **8 gap** | — | No coordinator, no config-driven DAG generation, no plan cache. Every DAG's job list is a literal. |

---

## 10. SPARK SUBMISSION

| Repo | Evidence | Construct |
|---|---|---|
| 1 | `.../plugins/job_scheduler/functions.py:443` | `submit_spark_job(cmd, job_hist, env, update_hist_func, raise_error_when_failed_or_killed, reduce_polling_log, update_hist_retries=20, update_hist_retry_delay_sec=180)`. Spawns the client and **regex-scrapes stdout**: `pattern1` for `(application_\d+_\d+)` + `state:`, `pattern2` for `final status:`, `pattern3` for `tracking URL:`, `pattern4` for `Caused by:` (`:452-455`). Writes `status="PROCESSING"` + `spark_app_id` + `started_at` as soon as the app id appears (`:520-526`); on exit writes the final status, `ended_at`, `error_msg`. `trigger_type = "scheduled" if started_at is None else "manual"` (`:468`). |
| 1 | `.../functions.py:577` `submit_spark_job_v2` | A rewrite using a reader thread + `Queue` (`:595`). |
| 1 | `.../functions.py:346` `kill_spark_job(app_id)`, `:416` `get_spark_job_status(app_id)`, `:400` `persist_spark_job_log(app_id, ...)` | Lifecycle helpers. |
| 1 | `.../plugins/job_scheduler/settings.py:20-24` | Resource defaults: `driver_cores 1`, `driver_memory "4g"`, `executor_cores 2`, `executor_memory "4g"`, `num_executors 2`. |
| 1 | `.../settings.py:26-42` | Submission environment: `spark3-submit` binary, keytab, principal, `lib.zip` via `--py-files`, `spark.tar.gz#spark` via `--archives`, `SPARK_CONF_DIR`, `--files` for keytab + JAAS. All overridable by env var. |
| 1 | `src/c2pp/init/data_lake_init.sql:97-111` | `job_resource_config(job_name PK, job_id, job_type, driver_cores, driver_memory, executor_cores, executor_memory, executor_instances, airflow_dag_id, application, application_args, updated_at)` — resources are **per job**, joined into the config load (`functions.py:52-60`). There is no reusable profile. |
| 2 | `additional/submit_auto.sh`, `submit_fullfilled.sh`, `submit_yarn_cluster.sh`, `spark-submit.txt` | The concrete commands. `--master yarn --deploy-mode cluster`, `--name <job>__<date>__<turn>`, `--conf spark.yarn.tags=airflow`, `--conf spark.yarn.submit.waitAppCompletion=true`, SQL files shipped via `--files`. Per-flow sizing: auto `3c/8g` × 5 executors; fulfilled `3c/10g` driver, `3c/20g` × 4 executors. |
| 3 | `1_phase_a/flow3_pl_stream_main/scripts/submit_stream.sh` | `nohup ... &` for the long-running app; zips `src/*.py` at submit time; `kinit` from keytab; documents kill and log retrieval by `application_id`. |
| 3 | `configs/bcn_pipeline.yaml` `STREAM.APP_NAME`, `SHUTDOWN.APP_NAMES` | Apps addressed **by name**, not id, so the lifecycle DAGs can find them across restarts. |
| 5 | `.../lambda/orchestration_lambda/execute_job_logic.py:29` | `execute_emr_serverless(script_name, job_args, spark_args, file_name)` → `emr_svless.start_job_run(applicationId, executionRoleArn, name, configurationOverrides={...}, jobDriver={"sparkSubmit": {...}}, executionTimeoutMinutes=240)`. Returns `{"engineRunId": "/applications/{app}/jobs/{run}"}`. |
| 5 | same file `:44-82` | The Spark configuration block: `spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.defaultCatalog=glue`, `spark.sql.extensions=...IcebergSparkSessionExtensions`, `catalog-impl=...GlueCatalog`, `spark.archives=s3://.../pyspark_venv.tar.gz#environment` + `PYSPARK_PYTHON=./environment/bin/python`, and per-job env via `spark.emr-serverless.driverEnv.*`. |
| 5 | same file `:105-115`, `:148` | `class EMRServerlessJobStatus(str, Enum)` — `SUBMITTED, PENDING, SCHEDULED, RUNNING, SUCCESS, FAILED, CANCELLING, CANCELLED`; `get_emr_serverless_job_status` collapses them to `running | success | failed` via `_STATUS_MAP`. |
| 5 | same file `:117` | `check_extraction_agent_is_running(job_name)` — paginates `list_job_runs` over the four in-flight states to enforce single-instance. |
| 5 | `.../emr_serverless_job_submit.json` | A standalone reference payload for the same call. |
| 5 | `.../emr_serverless.tf:2-48` | `aws_emrserverless_application`: `release_label "emr-7.0.0"`, `maximum_capacity {400 vCPU, 3000 GB, 20000 GB disk}`, `auto_start`, `auto_stop {idle_timeout_minutes = 15}`, `initial_capacity` 1 driver + 3 executors, `network_configuration` into private subnets. |
| 6 | `.../emr-serverless/run_edt_script.py` | The entrypoint contract: `express_data_transform_run_job(sys.argv)` → `job.run()` → `job.engine.stop()`; re-raises `job._error_message` as `RuntimeError` if `_status == "FAIL"` so the engine sees a non-zero exit. |
| 6 | `IaC/main/prod/main.tf:38-56` | Packaging as infrastructure: a `null_resource` whose triggers include `sha1(join("", [for f in fileset(".../src/express_data_transform","**"): filesha1(...)]))`, running `run_setup_docker.sh` to build and upload the venv to the code bucket. |
| 4, 7 | ABSENT | |
| **8** | `airflow/dags/common.py:52` | `emr_submit(job_name, entrypoint, args)` → `{"name", "entryPoint", "entryPointArguments", "sparkSubmitParameters"}`. Returned as a dict rather than an operator so tests can assert on it. `sparkSubmitParameters` hardcodes exactly three confs: Iceberg extensions, `spark.sql.session.timeZone=UTC` (ADR-024), `spark.dynamicAllocation.enabled=true`. |
| **8** | `airflow/dags/dag_eod_pipeline.py:76-86` etc. | Actual submission today is `BashOperator("spark-submit /opt/spark/...")`, not `EmrServerlessStartJobOperator`. |
| **8** | `scripts/run-flow.sh` | The flow entrypoint invoked by `dag_intraday_flows.py:66-73` with `--flow/--date/--execute` and `FLOW_RUN_ID` in the env. |
| **8** | `spark/common/run_flow.py:92` `main()` | `--flow`, `--business-date`, `--run-id` argument contract. |
| **8** | `terraform/modules/emr_serverless/main.tf:15` | `aws_emrserverless_application "spark"`; variables `release_label`, `max_vcpu`, `max_memory_gb`, `max_disk_gb`, `auto_stop_idle_minutes`. |
| **8 gap** | — | No resource profiles (one hardcoded conf set for all jobs); no job-status polling loop; no app-id capture into any ledger. |

---

## 11. TERRAFORM

| Repo | Evidence | Construct |
|---|---|---|
| 5 | `IaC/main/{prod,dev}/main.tf`, `config.tf`; `IaC/modules/{data_platform,redshift,xdata}` | Layout: thin per-env roots over shared modules. |
| 5 | `IaC/modules/data_platform/ochestration.tf:184` / `:213` | `aws_dynamodb_table "etl_execution"` (GSI `status-index`) and `"dbt-job"` — both `PAY_PER_REQUEST`, both `lifecycle { ignore_changes = [stream_enabled] }`. Referenced by name through Lambda env vars (`:56-70`). |
| 5 | `.../ochestration.tf:26-49` | `aws_lambda_layer_version "xdata-layer"` pinned to a checked-in zip; `aws_lambda_function "orchestration"` on `python3.11`/`arm64`, in-VPC, `source_code_hash` from `archive_file`. |
| 5 | `.../ochestration.tf:72-...` , `stepfunction/orchestration_sfn.json`, `extraction_sfn.json` | `aws_sfn_state_machine` built with `templatefile(...)`. |
| 5 | `.../ochestration.tf:232` | `aws_sns_topic "job_alert"`. |
| 5 | `.../etl_job.tf:1-12` | `aws_s3_object` `for_each = fileset("${path.module}/emr-serverless", "**.py")` with `source_hash = filemd5(...)` — job scripts deployed by Terraform, redeployed on content change. |
| 5 | `.../emr_serverless.tf` | See §10. |
| 6 | `IaC/main/prod/main.tf:1-20` | `provider "aws" { assume_role { role_arn = ".../service-role-cicd" } }`; `backend "s3"` with `bucket`, `key = "ttc-dlh-prod-etl-library/terraform.tfstate"`, `dynamodb_table = "ttc-dlh-ops-tfstate-lock"`, `encrypt = true`. |
| 6 | `IaC/main/prod/main.tf:22-37` | `locals` naming convention: `general_prefix = "${master}-${app}-${env}"`; account ids, vpc, subnets, domain as locals. |
| 6 | `IaC/main/prod/main.tf:38-56` | The `null_resource` packaging build (see §10). |
| 7 | `IaC/00-manual/00-terraform-prereq/s3.tf` | The state bucket: versioning enabled, full public-access block, `aws_s3_bucket_server_side_encryption_configuration` with `sse_algorithm = "AES256"` (**SSE-S3, not KMS**). |
| 7 | `IaC/00-manual/00-terraform-prereq/dynamodb.tf` | `aws_dynamodb_table "terraform_state_lock"`, `hash_key = "LockID"`, `PAY_PER_REQUEST`. |
| 7 | `IaC/00-manual/00-terraform-prereq/cicd_iam/` | 11 JSON policy documents split by service (`cicd-etl`, `cicd-s3-readwrite`, `cicd-dynamodb-readwrite`, `cicd-msk`, `cicd-rds`, `cicd-ecs`, `cicd-secret-manager`, `cicd-assume-roles`, …) loaded as files rather than inline HCL. |
| 7 | `IaC/00-manual/01-pipeline-factory/pipelines/main.tf:2` / `:27` / `:50` | `aws_codebuild_project "tf-plan"` + `"tf-apply"` with `buildspec = templatefile(var.plan_buildspec_path, var.variables)`, and `aws_codepipeline "cicd_pipeline"`; stages gated by `for_each = var.enable_plan_stage ? [1] : []` (`:79`) and `var.enable_manual_approval` (`:98`). |
| 7 | `.../01-pipeline-factory/buildspec/tf-plan-buildspec.yml`, `tf-apply-buildspec.yml` | The plan/apply separation. |
| 7 | — | **Nothing reporting-specific.** This repository contributes CI/CD and state-backend conventions only. |
| 1, 2, 3, 4 | ABSENT | On-prem; no IaC. |
| **8** | `terraform/modules/` (11) | `data_lake`, `glue_catalog`, `athena`, `emr_serverless`, `lake_iam`, `airflow_k3s`, `kafka_platform`, `cdc_runtime_ec2`, `source_lab_ec2`, `vpc_endpoints`, `budget_guardrails`. |
| **8** | `modules/glue_catalog/variables.tf:16-33` | `databases` map; a validation asserts `setsubtract(["stream","full_cdc","snapshot","curated","mart","ops","quarantine"], keys(var.databases)) == 0`. It is a **subset** check, so an additional layer key passes without editing the validation. `main.tf:10` names them `${replace(name_prefix,"-","_")}_${each.key}`. |
| **8** | `modules/data_lake/variables.tf:78-112` | `lake_prefixes` — 7 `warehouse/*` prefixes plus top-level `checkpoints/`, with two validations: every prefix ends in `/`, and none may start with `warehouse/checkpoints`. |
| **8** | `modules/data_lake/variables.tf:46-56`, `main.tf:203-210` | `checkpoint_expiration_days >= 2`; `expire-old-checkpoints` lifecycle rule on `checkpoints/`. `main.tf:245` records that there is deliberately **no** expiration on `warehouse/`. |
| **8** | `envs/dev/terraform.tfvars` | `project_name="kafka-dev-lab"`, `environment="dev"`, `monthly_budget_usd=30`, `athena_bytes_scanned_cutoff_gb=10`, `checkpoint_expiration_days=14`, `allow_destroy_with_data=false`, `enable_nat_gateway=false`. Flags: `enable_kafka_platform=true`, `enable_emr_serverless=true`, `enable_airflow=false`, `enable_redshift_serverless=false`, `enable_trino=false`, `allow_multiple_optional_query_engines=false`. |
| **8** | `envs/dev/main.tf:105-381` | Module wiring. `emr_serverless` (`:345`), `lake_iam` (`:210`) and `airflow_k3s` (`:372`) are all `count`-gated on `enable_kafka_platform`; `airflow_k3s` additionally on `enable_emr_serverless && enable_athena`. |
| **8** | `envs/dev/validations.tf:83` | A precondition failing the plan if `enable_storage_autoscaling` is true. |
| **8** | `scripts/plan_cost.py`, `scripts/tf.sh`, `scripts/verify-destroy.sh`, `Makefile` | Cost preview and destroy verification are part of the workflow. |
| **8 discrepancy** | `PROJECT_STATE.md:12-15` vs `envs/dev/terraform.tfvars` | State records the applied cheap tier as 32 resources with **MSK never created**; the committed tfvars sets `enable_kafka_platform = true`. Because EMR/IAM/Airflow are gated on that flag, none of them exist today regardless. Resolve before planning any reporting infrastructure. |

---

## 12. Cross-cutting findings

**F1 — Repo 7 contributes nothing to reporting.** It is CI/CD scaffolding: state bucket,
lock table, CodePipeline factory, CI/CD IAM. Its only transferable ideas are the
file-per-service IAM policy layout (`cicd_iam/iam/policies/*.json`) and the plan/apply
buildspec split. Repo 8 already has an equivalent state-backend design
(`docs/adr/ADR-021-terraform-state-backend.md`, `scripts/bootstrap-state-backend.sh`), and
its bucket is KMS-encrypted where repo 7's is SSE-S3.

**F2 — Two references independently reached "config in Git, not in the database".**
Repo 1's own second-generation framework (`src/c2pp/`) and repo 3's `bcn_pipeline.yaml`
both moved configuration out of the warehouse and into version-controlled YAML, after
repo 1's first generation put it in Impala and ended up loading `job_master` from an Excel
workbook (`functions.py:790`). Repo 5 went the other way (DynamoDB-only, no Git
representation of `-dbt-job` rows).

**F3 — Only repo 6 has a real dependency graph.** `Job(nx.DiGraph)` with
`nx.topological_sort` (`core/base.py:108`). Repo 1 uses comma-separated strings plus
hand-numbered turns and has **no cycle detection**. Repo 8 has neither.

**F4 — Only repo 3 does impact analysis for corrections.** Five explicit scan sources
(`main_autocorrect.py:472-585`) versus repo 1's and repo 8's "recompute the whole day".

**F5 — Nobody does FULFILL gap detection.** Not repo 1, 2, 3, 5 or 8. Every backfill in
every reference is a human choosing dates.

**F6 — The dbt manifest is produced but never consumed for scheduling.** Repo 4 has
`target/manifest.json`; repo 5's `run_dbt_job.py` passes `-s job["modelFqn"]` from a
DynamoDB row that a human maintains. The dependency graph exists twice and is reconciled
by nobody.

**F7 — Repo 8's idempotency mechanism is stronger than repo 2's.**
`flows.py:127 may_overwrite` / `:150 merge_condition_sql` (tier ranking + `input_cutoff`
tie-break, with a test asserting the Python and generated SQL agree) versus repo 2's
`set_current_snapshot` target rewind, which discards anything committed between the two
runs and has no recovery path if it dies mid-rewind.

**F8 — Correction to a claim in `docs/REPORTING_ARCHITECTURE_DISCOVERY.md` (R4).** That
document states this project has "no business calendar". It has one:
`spark/common/ddl/kimball.sql:84` defines `mart.dim_date`, generated by
`spark/dimensions/dim_builder.py:92 build_dim_date` over an explicit date range. What it
lacks is the **working-day/holiday semantics** the references depend on — `dim_date` has
`is_weekend BOOLEAN` but no `is_working_day` equivalent to repo 1's `dlpt.dim_times`
(`helpers.py:384`, `:651 check_is_first_running`, `:714 get_num_working_days`) or repo 3's
`_resolve_cob_date` (`bcn_realtime_dags.py:84`). The gap is narrower and cheaper to close
than that document implies: extend an existing generated dimension rather than build one.

**F9 — Two defects in repo 1 that must not be ported.**
(a) `functions.py:363 get_spark_error_msg` + `pipeline_builder.py:686-697` — a `FAILED`
run is flipped to `SUCCEEDED` if the literal `JOB_DONE` appears in the YARN log, or if
`summary_config.execution_date` happens to have moved.
(b) The EOD readiness rule is implemented twice, in Spark (`helpers.py:530`) and in
Airflow (`functions.py:1387`); `f_acct_depo_by_day_level_acct_auto_correct_generate.py:140`
raises `'#### ERROR: Conflict eod status between airflow and spark job'` when they
disagree, which means the divergence was observed in production.

**F10 — Vocabulary collisions to settle before writing any code.**

| Term | Repo 1/2 | Repo 3 | Repo 8 | Repo 9 prompt |
|---|---|---|---|---|
| `stream` | Airflow micro-batch (`job_type='stream'`) | long-running Spark app | L1 raw CDC layer | — |
| `full_cdc` | — | — | L2 immutable history | L1, source of truth |
| `fulfilled` | late EOD close | — | — | — |
| `full_fill` | — | — | dimension-key repair | — |
| `FULFILL` | ≈ `load_hist` | — | ABSENT | date backfill |
| `EOD` | close event + job type | 05:00 DAG | flow stamping CERTIFIED | L2 dedup layer |
| `turn` | retry counter within a date | — | — | topological order (§23) |

---

```
REPORTING_REFERENCE_DISCOVERY_COMPLETE
```
