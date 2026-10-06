# Reporting / Datamart Framework — Target Architecture

- Date: 2026-08-19
- Status: **APPROVED PENDING IMPLEMENTATION.** No code written, no infrastructure deployed.
- Decisions: ADR-033 … ADR-045 (13 ADRs, all `ACCEPTED`)
- Evidence base: `docs/REPORTING_REFERENCE_EVIDENCE.md` (discovery), `docs/REPORTING_ARCHITECTURE_DISCOVERY.md` (comparison)

Every design choice below traces to an ADR, and every ADR traces to a file and line in one
of the four reference codebases or this repository. Nothing here is preference.

---

## 1. Decisions at a glance

| Question | Decision | ADR |
|---|---|---|
| Layer naming | Logical names in config; physical Glue databases unchanged | 033 |
| Config source of truth | **Hybrid** — Git YAML is truth → compiled `plan.json` → mirrored to Iceberg `ops.*` | 034 |
| Master vs history | Three planes: config (immutable) / position (framework-owned) / runtime (append-only) | 035 |
| Runtime state store | **Split by access pattern** — DynamoDB hot, Iceberg audit | 036 |
| dbt manifest → dependencies | `dbt parse` → `manifest.json` → `depends_on.nodes` → auto-registered edges | 037 |
| Turn / ordering | Computed `nx.topological_generations` per mode-filtered subgraph; cycles fail compile | 038 |
| Airflow shape | **Hybrid** — 4 fixed flow DAGs + runtime coordinator + dynamic task mapping | 039 |
| Five modes | Each defined by source layer + window + tier; one model per mart | 040 |
| STREAM_BATCH vs STREAMING_RT | Distinct in every dimension; STREAMING_RT flag-off, bounded window | 041 |
| Merge semantics / idempotency | Guarded MERGE on the accuracy ladder; two distinct idempotency keys | 042 |
| Watermark / rerun | One writer, advanced only after validation, conditional write | 043 |
| Spark execution | `dbtRunner` + `method: session` inside EMR Serverless; no Thrift | 044 |
| Streaming checkpoint | Derived path under top-level `checkpoints/`; deletion is an operator action | 045 |

---

## 2. System shape

```
                     Git — the only editable surface
        reporting/jobs/*.yaml   reporting/layers.yaml   dbt/models/**
                                │
                    make reporting-compile   (CI, $0, no AWS)
        validate → build graph → detect cycles → assign turns
        → merge dbt manifest edges → resolve profiles → emit plan.json + sha256
                                │
              s3://<lake>/ops/config/<config_version>/plan.json
                                │
        ┌───────────────────────┼───────────────────────────┐
        ▼                       ▼                           ▼
  local plan cache        ops.job_master              coordinator
  (Airflow parse;         ops.job_flow_config         resolve → gate → order
   never queries          ops.job_dependency          → submit → track
   the warehouse)         (Iceberg, by config_version)  → validate → commit
                                                          │
                                                    EMR Serverless
                                    run_dbt_job.py  │  run_spark_job.py
                                                          │
        ┌─────────────────────────────────────────────────┴──────────┐
        ▼                                                            ▼
  read (logical → physical via layers.yaml)                    write mart
  FULL_CDC · EOD · REALTIME · CURATED                    guarded MERGE (ladder)
                                                                     │
                                                    validation (dbt tests + ops.dq_result)
                                                                     │
                                                    watermark advance ← ONLY here
                                                                     │
                                                          status → SUCCEEDED
```

---

## 3. Logical layers (ADR-033)

| Logical | Physical | Reporting source? |
|---|---|---|
| `FULL_CDC` | `<prefix>_full_cdc.*` | yes — recovery, history, replay, correction |
| `FULL_CDC_RAW` | `<prefix>_stream.*` | **no** — rejected at compile |
| `EOD` | `<prefix>_snapshot.*` | yes — certified daily marts |
| `REALTIME` | `<prefix>_realtime.*` | yes — bounded recent window |
| `CURATED` | `<prefix>_curated.*` | yes — dimensions, facts |
| `MART` | `<prefix>_mart.*` | target only |
| `OPS` | `<prefix>_ops.*` | framework only |

`REALTIME` does not exist yet. Until the upstream layer work builds it, `layers.yaml` maps
it to `full_cdc` with a mandatory bounded date predicate and `status: SUBSTITUTED`; every
execution records which mapping it used.

---

## 4. Metadata tables — final

Storage per ADR-036. `PK` = DynamoDB partition key, `SK` = sort key.

### 4.1 `ops.job_master` — Git → `plan.json` → Iceberg (append-only, partitioned by `config_version`)

One row per **logical job**. Never one row per execution.

```
job_id                    STRING   NOT NULL   -- stable slug; immutable; appears in checkpoint paths
job_name                  STRING   NOT NULL
flow_name                 STRING              -- domain grouping (e.g. "account_reporting")
description               STRING   NOT NULL
owner                     STRING   NOT NULL

is_active                 BOOLEAN  NOT NULL
only_eod                  BOOLEAN  NOT NULL   -- may run only when sources are closed

coordinator_mode          STRING   NOT NULL   -- default | native | sequential
default_run_mode          STRING   NOT NULL   -- cross_turn | turn_by_turn

logical_target_layer      STRING   NOT NULL   -- MART | CURATED
target_catalog            STRING   NOT NULL
target_database           STRING   NOT NULL   -- resolved from logical_target_layer
target_table              STRING   NOT NULL

transformation_type       STRING   NOT NULL   -- SQL | PYTHON | HYBRID
execution_engine          STRING   NOT NULL   -- DBT_SPARK | SPARK_BATCH | SPARK_STREAMING | PYTHON

dbt_model                 STRING
dbt_selector              STRING
application               STRING              -- entrypoint for non-dbt engines
application_args_template STRING

primary_key               ARRAY<STRING> NOT NULL  -- the MERGE key
business_date_column      STRING   NOT NULL
partition_spec            ARRAY<STRING> NOT NULL  -- no high-cardinality column; no PK
                                                  -- column except business_date_column (ADR-042)
merge_strategy            STRING   NOT NULL   -- MERGE_LADDER | INSERT_OVERWRITE_PARTITION | APPEND
delete_strategy           STRING   NOT NULL   -- SOFT | HARD | NONE

default_resource_profile  STRING   NOT NULL

created_at                TIMESTAMP
updated_at                TIMESTAMP
config_version            STRING   NOT NULL   -- injected by the compiler
```

**Absent by rule** (compile fails if present): `date_of_data`, `status`, `turn`,
`turn_watermark`, `spark_app_id`, `started_at`, `ended_at`, `error_msg`, `is_rerun`,
`estimated_duration`, `is_skipped` (moved to per-mode `is_enabled`), `schedule` (per mode),
`non_eod` (derivable from `only_eod`), and all eight dependency string columns.

### 4.2 `ops.job_flow_config` — key `(job_id, flow_mode)`

```
job_id                    STRING   NOT NULL
flow_mode                 STRING   NOT NULL   -- EOD | AUTO_CORRECT | FULFILL | STREAM_BATCH | STREAMING_RT

is_enabled                BOOLEAN  NOT NULL   -- per mode; repo 1's single is_skipped could not express this
schedule                  STRING              -- cron; NULL for coordinator-driven / manual
timezone                  STRING   NOT NULL   -- UTC (ADR-024); validation rejects anything else

source_layer_policy       STRING   NOT NULL   -- EOD | EOD_PLUS_REALTIME | FULL_CDC_OR_EOD
                                              -- | REALTIME | FULL_CDC_APPEND | KAFKA_DIRECT
source_justification      STRING              -- REQUIRED when source_layer_policy = KAFKA_DIRECT
eod_gate_mode             STRING   NOT NULL   -- TAG_AND_WATERMARK | WATERMARK_ONLY | NONE

lookback_days             INT                 -- AUTO_CORRECT
late_arrival_days         INT
default_from_offset_days  INT
default_to_offset_days    INT
safety_overlap_minutes    INT      NOT NULL   -- STREAM_BATCH; compile fails on 0

watermark_type            STRING   NOT NULL   -- TIMESTAMP | SNAPSHOT_ID | KAFKA_OFFSET | NONE
incremental_strategy      STRING   NOT NULL
merge_strategy            STRING              -- overrides job_master when a mode differs
delete_strategy           STRING

coordinator_mode          STRING              -- overrides job_master
run_mode                  STRING              -- cross_turn | turn_by_turn (attempts, not waves)
order_by_default          INT                 -- DERIVED, written by the compiler
order_by_mode             INT                 -- DERIVED, written by the compiler

retry_count               INT      NOT NULL
retry_delay_seconds       INT      NOT NULL
execution_timeout_seconds INT      NOT NULL
max_active_runs           INT      NOT NULL
max_concurrent_tasks      INT      NOT NULL

resource_profile          STRING              -- overrides job_master.default_resource_profile
dbt_selector_override     STRING
application_args_override STRING
validation_selector       STRING   NOT NULL   -- dbt tests that must pass before SUCCEEDED

stream_trigger_interval   STRING              -- STREAMING_RT only
checkpoint_location       STRING              -- DERIVED (ADR-045); a literal fails compile

cost_guard                STRUCT<max_bytes_scanned_gb: INT, max_execution_minutes: INT>

created_at                TIMESTAMP
updated_at                TIMESTAMP
config_version            STRING   NOT NULL
```

### 4.3 `ops.job_dependency` — DIRECT edges only (ADR-037)

```
dependency_id             STRING   NOT NULL   -- deterministic hash of the edge
job_id                    STRING   NOT NULL
upstream_job_id           STRING              -- when dependency_type = JOB
upstream_dataset          STRING              -- when dependency_type is a table/dataset kind

dependency_type           STRING   NOT NULL   -- JOB | DATASET | EOD_TABLE | REALTIME_TABLE
                                              -- | FULL_CDC_TABLE | EXTERNAL
dependency_scope          STRING   NOT NULL   -- DIRECT only; TRANSITIVE exists in plan.json alone
flow_mode                 STRING              -- NULL = applies to every mode
required                  BOOLEAN  NOT NULL
kickoff_condition         STRING   NOT NULL   -- SUCCEEDED | COMPLETED | WATERMARK_PAST
lag_days                  INT      NOT NULL
dependency_order          INT                 -- DERIVED
source_of_definition      STRING   NOT NULL   -- DBT_MANIFEST | MANUAL_CONFIG
is_active                 BOOLEAN  NOT NULL

created_at                TIMESTAMP
updated_at                TIMESTAMP
config_version            STRING   NOT NULL
```

Computed into `plan.json`, never authored: `before_direct`, `after_direct`, `before_full`,
`after_full` as `ARRAY<STRING>`; the per-mode variants by filtering the graph on
`flow_mode`, not as four more columns.

### 4.4 `ops.job_master_execution_hist` — DynamoDB (live) + Iceberg (audit)

DynamoDB: `PK execution_id`; GSI-1 `(job_id#flow_mode, date_of_data)`; GSI-2 `status`; TTL 30 days.
Iceberg: partitioned by `date_of_data`, **one append per completed execution**.

```
execution_id                STRING    NOT NULL  -- ULID
coordinator_run_id          STRING    NOT NULL
job_id                      STRING    NOT NULL
job_name                    STRING    NOT NULL
flow_mode                   STRING    NOT NULL
run_mode                    STRING

date_of_data                DATE      NOT NULL
execution_date_ref          DATE
turn                        INT       NOT NULL  -- topological wave (ADR-038)
attempt_number              INT       NOT NULL  -- retry counter (ADR-038)
is_rerun                    BOOLEAN   NOT NULL
is_eod                      BOOLEAN   NOT NULL
trigger_type                STRING    NOT NULL  -- SCHEDULED | MANUAL | BACKFILL | RECOVERY

status                      STRING    NOT NULL  -- see §5
started_at                  TIMESTAMP
ended_at                    TIMESTAMP
duration_seconds            BIGINT

airflow_dag_id              STRING
airflow_dag_run_id          STRING
airflow_task_id             STRING              -- repo 1 had none of the last two

spark_app_id                STRING              -- EMR Serverless applicationId/jobRunId
spark_app_status            STRING
spark_app_tracking_url      STRING

driver_cores                INT                 -- RESOLVED values, snapshotted
driver_memory               STRING
executor_cores              INT
executor_memory             STRING
executor_instances          INT
resource_profile            STRING
application                 STRING
application_args            ARRAY<STRING>

resolved_source_layer       STRING    NOT NULL
source_layer_substituted    BOOLEAN   NOT NULL  -- true while REALTIME maps to full_cdc
window_start                TIMESTAMP
window_end                  TIMESTAMP
input_cutoff                TIMESTAMP NOT NULL  -- the MERGE tie-breaker (ADR-042)

watermark_before            TIMESTAMP
watermark_after             TIMESTAMP
source_position_before      STRING
source_position_after       STRING
kafka_offsets_before        MAP<STRING,BIGINT>
kafka_offsets_after         MAP<STRING,BIGINT>
iceberg_snapshot_id_before  MAP<STRING,BIGINT>  -- per table, audit only
iceberg_snapshot_id_after   MAP<STRING,BIGINT>

resolved_dependencies       ARRAY<STRUCT<job_id:STRING, flow_mode:STRING,
                                         execution_id:STRING, satisfied_at:TIMESTAMP>>
before_direct               ARRAY<STRING>
after_direct                ARRAY<STRING>
before_full                 ARRAY<STRING>
after_full                  ARRAY<STRING>

affected_dates              ARRAY<DATE>         -- AUTO_CORRECT / FULFILL
affected_key_count          BIGINT
impact_analysis_path        STRING              -- DERIVED | FULL_WINDOW_FALLBACK

rows_read                   BIGINT
rows_inserted               BIGINT
rows_updated                BIGINT
rows_deleted                BIGINT
rows_rejected               BIGINT
bytes_read                  BIGINT
bytes_written               BIGINT

validation_status           STRING              -- PASSED | FAILED | NOT_EVALUATED
dq_result_run_id            STRING              -- joins ops.dq_result

error_code                  STRING
error_msg                   STRING              -- truncated and scrubbed
note_for_tracing            STRING

config_version              STRING    NOT NULL
code_version                STRING    NOT NULL
created_at                  TIMESTAMP NOT NULL
updated_at                  TIMESTAMP NOT NULL
```

### 4.5 `ops.job_watermark_state` — DynamoDB, `PK job_id#flow_mode` (ADR-043)

```
job_id                      STRING    NOT NULL
flow_mode                   STRING    NOT NULL
last_success_execution_id   STRING    NOT NULL
last_success_date_of_data   DATE
watermark_ts                TIMESTAMP
source_position             STRING
kafka_offsets               MAP<STRING,BIGINT>
iceberg_snapshot_id         BIGINT
last_success_started_at     TIMESTAMP
last_success_ended_at       TIMESTAMP
updated_at                  TIMESTAMP NOT NULL
```

Written only by `ops_client.commit_watermark()`, only after validation passes, under a
condition on `last_success_execution_id` and monotonic `watermark_ts`.

### 4.6 `ops.summary_config_v1` — DynamoDB, `PK job_id#flow_mode`, `SK coordinator_run_id`

The **current resolved coordinator plan**, generated per coordinator run from
`plan.json` + `job_watermark_state` + execution state. Never hand-maintained; never the
source of any configuration.

```
coordinator_run_id          STRING    NOT NULL
job_id                      STRING    NOT NULL
job_name                    STRING    NOT NULL
flow_mode                   STRING    NOT NULL
execution_date              DATE      NOT NULL
execution_date_ref          DATE
start_date                  DATE
end_date                    DATE
status                      STRING    NOT NULL
active                      BOOLEAN   NOT NULL
turn                        INT       NOT NULL
dependencies                ARRAY<STRUCT<job_id:STRING, flow_mode:STRING,
                                         kickoff_condition:STRING, satisfied:BOOLEAN>>
kickoff_condition           STRING
is_first_running            BOOLEAN
is_first_run_after_eod      BOOLEAN            -- collapses repo 1's three per-flow flags
resolved_watermark          STRING
resolved_source_layer       STRING
config_version              STRING    NOT NULL
updated_at                  TIMESTAMP NOT NULL
```

Repo 1 carried `is_first_running_after_eod_in_auto_correct_flow`,
`..._in_streaming_flow` and `..._in_streaming_ref_flow` as three columns
(`data_lake_init.sql:12-14`). One boolean plus the row's own `flow_mode` says the same
thing and cannot disagree with itself.

### 4.7 `ops.summary_config_hist_v1` — Iceberg, append-only, partitioned by `execution_date`

```
coordinator_run_id          STRING    NOT NULL
job_id                      STRING    NOT NULL
job_name                    STRING    NOT NULL
flow_mode                   STRING    NOT NULL
execution_date              DATE      NOT NULL
execution_date_ref          DATE
is_first_running            BOOLEAN
is_first_run_after_eod      BOOLEAN
etl_date                    TIMESTAMP NOT NULL
indicator                   STRING    NOT NULL  -- AUTOMATION | MANUAL | RERUN | BACKFILL
                                                -- | REPAIR | RECOVERY
turn                        INT
resolved_dependencies       ARRAY<STRUCT<...>>
resolved_watermark          STRING
status                      STRING    NOT NULL
decision_reason             STRING              -- why the coordinator chose this
config_version              STRING    NOT NULL
```

`decision_reason` is new. Repo 1 recorded *what* was decided and never *why*, which is the
first question asked when a job did not run.

### 4.8 `ops.streaming_app_state` — DynamoDB, `PK job_id`, `SK deployment_id` (ADR-041)

**Justified.** `job_master_execution_hist` is per run and `job_watermark_state` is a single
row per key with no lifecycle; neither can express restart count, liveness, or per-batch
progress of a process that outlives both.

```
job_id                      STRING    NOT NULL
deployment_id               STRING    NOT NULL  -- one per start
application_name            STRING    NOT NULL
spark_app_id                STRING
status                      STRING    NOT NULL  -- STARTING | RUNNING | DEGRADED
                                                -- | STOPPING | STOPPED | FAILED
source_type                 STRING    NOT NULL  -- FULL_CDC_APPEND | KAFKA_DIRECT
source_name                 STRING
checkpoint_location         STRING    NOT NULL
checkpoint_state_at_start   STRING    NOT NULL  -- EMPTY | RESUMING_FROM_BATCH_<n>
last_progress_at            TIMESTAMP
last_batch_id               BIGINT
last_source_offset          STRING
last_event_time             TIMESTAMP
last_watermark              TIMESTAMP
output_table                STRING    NOT NULL
rows_written_total          BIGINT
started_at                  TIMESTAMP NOT NULL
restarted_at                TIMESTAMP
restart_count               INT       NOT NULL
error_msg                   STRING
updated_at                  TIMESTAMP NOT NULL
```

### 4.9 `resource_profile` — Git only

```yaml
small:            {driver_cores: 1, driver_memory: 4g,  executor_cores: 2, executor_memory: 8g,  executor_instances: 2}
medium:           {driver_cores: 2, driver_memory: 8g,  executor_cores: 4, executor_memory: 16g, executor_instances: 4}
large:            {driver_cores: 4, driver_memory: 16g, executor_cores: 4, executor_memory: 32g, executor_instances: 8}
streaming_small:  {driver_cores: 2, driver_memory: 8g,  executor_cores: 2, executor_memory: 8g,  executor_instances: 2}
streaming_medium: {driver_cores: 2, driver_memory: 8g,  executor_cores: 4, executor_memory: 16g, executor_instances: 4}
```

Repo 1 stored resources per job in `job_resource_config` with no reusable profile
(`data_lake_init.sql:97-111`); the resolved values are still snapshotted per execution.

---

## 5. Status model

```
PLANNED → WAITING_DEPENDENCY → READY → SUBMITTED → RUNNING → VALIDATING → SUCCEEDED
                                                                   ↓
                                                                 FAILED
   any → SKIPPED        (disabled, not a working day, gate says not applicable)
   any → CANCELLED      (operator, or coordinator shutdown)
```

`SUCCEEDED` requires **all four**: engine completed, target commit confirmed, required
validation passed, watermark committed. Nothing else may set it.

Explicitly rejected: repo 1's `SUCCEEDED_BY_SKIPPING` (conflates two outcomes — `SKIPPED`
is a distinct state here), and the `JOB_DONE` log-grep and `execution_date`-moved
heuristics that flip `FAILED` to `SUCCEEDED` (`pipeline_builder.py:686-697`).

---

## 6. The five flows — final

### 6.1 EOD

```
gate    ops.eod_watermark(EOD, cob_date) present
        AND Iceberg tag EOD_<cob_date> resolvable on each source table   [eod_gate_mode]
        AND every upstream job's watermark >= cob_date
read    EOD layer, pinned:  FOR VERSION AS OF <tag snapshot>
run     run_dbt_job.py --select <model> --vars '{flow_mode: EOD, cob_date: ...}'
write   guarded MERGE, stamps CERTIFIED
verify  dbt build tests + ops.dq_result has no ERROR-severity FAIL
commit  watermark → last_success_date_of_data = cob_date
finish  status SUCCEEDED; tag the mart EOD_<cob_date>
```

The tag is the addition to what exists. `ops.eod_watermark` already carries the "written
only after write + validation" contract (`spark/eod/ddl/ops_tables.sql:42`); an Iceberg
tag is atomic with the commit and gives the reader a snapshot to pin — repo 1's
`TAG_<date>` mechanism (`helpers.py:566-577`), which is what makes FULFILL deterministic.

Idempotent for `(job_id, EOD, cob_date)`: a rerun re-merges CERTIFIED over CERTIFIED and
wins on the later `input_cutoff`.

### 6.2 AUTO_CORRECT

```
window  [cob_date - lookback_days, now)              bounded by job_flow_config
read    EOD baseline + REALTIME  (FULL_CDC where the REALTIME window is too short)
impact  changed source rows → affected business keys → affected business dates
        → affected downstream marts (from the dependency graph)
        FALLBACK: full lookback window; impact_analysis_path records which ran
run     only the affected (key, date) ranges
write   guarded MERGE, stamps PROVISIONAL_CORRECTED — cannot overwrite CERTIFIED
verify  same gate
commit  watermark → run's input_cutoff; affected_dates recorded
```

Impact analysis is repo 3's contribution (`main_autocorrect.py:472-585`, five scan
sources). Repo 1 and this repo's current `FLOW_AUTO_CORRECT` both recompute the whole day
(`flow_runner.py:81-84`); that becomes the fallback, not the default.

### 6.3 FULFILL

```
plan    expected business dates (calendar)
          MINUS dates with a SUCCEEDED execution for (job_id, FULFILL|EOD)
          MINUS dates present and complete in the target mart
        = {missing, failed, incomplete}
        --dry-run prints the plan and exits.  DEFAULTS TO TRUE.
select  per date:  date <= last EOD close → EOD (at that date's tag)
                   otherwise              → FULL_CDC (rebuild at an explicit cutoff)
skip    dates already SUCCEEDED, unless --force
run     one execution per date; turn ordering still applies across jobs
write   guarded MERGE, stamps the tier of the source layer
commit  per date; watermark records max(date) only, never moved backwards
```

Gap detection exists in no reference (finding F5). Parameters: `from_date`, `to_date`,
`specific_dates`, `force`, `dry_run`.

### 6.4 STREAM_BATCH

```
read    watermark_before ← job_watermark_state(job_id, STREAM_BATCH)
window  [watermark_before - safety_overlap_minutes, bounded_now)
source  REALTIME
run     dbt incremental / Spark
write   guarded MERGE on affected keys, stamps PROVISIONAL_NRT
verify  validation_selector
commit  watermark → bounded_now
        FAILURE ⇒ NO ADVANCE, unconditionally
```

`safety_overlap_minutes` defaults to 2 (this repo's `NRT_SAFETY_OVERLAP_MINUTES`,
`flow_runner.py:62`); repo 2 independently chose 3. Zero fails compile.

### 6.5 STREAMING_RT — `enable_streaming_rt = false` by default

```
app     long-running Spark Structured Streaming
        readStream → foreachBatch(handler) → trigger(processingTime=<interval>)
source  FULL_CDC_APPEND (default) | KAFKA_DIRECT (requires source_justification)
sink    guarded MERGE into the mart, stamps PROVISIONAL_NRT
ckpt    s3://<lake>/checkpoints/reporting/<env>/<job_id>/STREAMING_RT/   (derived)
state   ops.streaming_app_state, mirrored per batch
fail    foreachBatch MUST raise on partial failure — a quiet return commits offsets
life    Airflow: streaming_rt_start (08:00) / _monitor / _stop (20:00), by app name
```

Bounded window, matching the only reference that actually operates one
(`bcn_pipeline.yaml`: start 08:00, kill 20:00, six days a week).

---

## 7. Proposed repository structure

Only paths that are **new** or **modified** are marked; everything else exists today.

```
aws-cdc-lakehouse/
├── reporting/                                   NEW — the only editable config surface
│   ├── layers.yaml                              logical → physical layer map (ADR-033)
│   ├── profiles/
│   │   └── resource_profiles.yaml               small | medium | large | streaming_* (§4.9)
│   ├── jobs/
│   │   └── mart_account_balance_daily.yaml       one file per logical job (pilot)
│   ├── schema/
│   │   ├── job.schema.json                      JSON Schema for a job YAML
│   │   └── layers.schema.json
│   └── compile.py                               validate → graph → cycles → turns
│                                                → dbt manifest merge → plan.json + sha256
│
├── spark/
│   ├── reporting/                               NEW
│   │   ├── __init__.py
│   │   ├── ops_client.py                        THE only writer of execution/watermark state
│   │   ├── coordinator.py                       resolve → gate → order → emit plan
│   │   ├── graph.py                             DIRECT → closure, turns, cycle detection
│   │   ├── gates.py                             EOD gate (watermark + tag), dependency gate
│   │   ├── source_resolver.py                   resolve_source_layer(table, flow_mode)
│   │   ├── run_dbt_job.py                       dbtRunner entrypoint (ADR-044)
│   │   ├── run_spark_job.py                     Spark entrypoint under the same contract
│   │   ├── fulfill_planner.py                   gap detection + dry-run
│   │   ├── autocorrect_impact.py                changed rows → keys → dates → marts
│   │   ├── validation.py                        dbt tests + ops.dq_result gate
│   │   ├── ddl/
│   │   │   └── reporting_ops_tables.sql         Iceberg: job_master, job_flow_config,
│   │   │                                        job_dependency, job_master_execution_hist,
│   │   │                                        summary_config_hist_v1
│   │   └── streaming_rt/
│   │       ├── app.py                           readStream → foreachBatch → MERGE
│   │       ├── handler.py                       the batch handler (must raise on failure)
│   │       └── app_state.py                     ops.streaming_app_state writer
│   ├── common/                                  UNCHANGED — flows.py, transform.py,
│   │                                            mart_writer.py, flow_runner.py, run_flow.py
│   ├── full_fill/                               UNCHANGED behaviour; documented as
│   │                                            DIMENSION_REPAIR (ADR-040)
│   ├── eod/  snapshot/  dimensions/  facts/  jobs/  ops/  correct/  streaming/   UNCHANGED
│   └── tests/
│       ├── test_reporting_graph.py              NEW — turns, cycles, mode filtering
│       ├── test_reporting_config.py             NEW — schema, compile failures
│       ├── test_reporting_gates.py              NEW — EOD gate, dependency gate
│       ├── test_reporting_watermark.py          NEW — non-advance on failure, conditional write
│       ├── test_reporting_fulfill.py            NEW — gap detection, dry-run
│       ├── test_reporting_merge.py              NEW — ladder across five modes
│       ├── test_reporting_manifest_sync.py      NEW — dbt manifest → job_dependency
│       └── test_streaming_rt.py                 NEW — restart, checkpoint recovery
│
├── airflow/
│   ├── dags/
│   │   ├── reporting_common.py                  NEW — plan cache, mapped-task factory, gates
│   │   ├── datamart_eod.py                      NEW
│   │   ├── datamart_auto_correct.py             NEW
│   │   ├── datamart_fulfill.py                  NEW
│   │   ├── datamart_stream_batch.py             NEW
│   │   ├── streaming_rt_lifecycle.py            NEW — start / monitor / stop
│   │   ├── sync_reporting_plan.py               NEW — refresh the local plan cache
│   │   ├── common.py                            MODIFIED — add POOL_REPORTING, POOL_STREAMING
│   │   └── dag_eod_pipeline.py  dag_intraday_flows.py  dag_l1_stream.py
│   │       dag_recovery.py  dag_cdc_health.py                              UNCHANGED
│   └── tests/
│       └── test_dags.py                         MODIFIED — assert no warehouse call at parse
│
├── dbt/
│   ├── dbt_project.yml                          MODIFIED — standard vars contract
│   ├── profiles.yml.example                     MODIFIED — emr_serverless: thrift → session
│   ├── macros/
│   │   ├── audit_columns.sql  generic_tests.sql  status_priority.sql        UNCHANGED
│   │   ├── resolve_source_layer.sql             NEW
│   │   ├── flow_window.sql                      NEW
│   │   ├── incremental_filter.sql               NEW
│   │   └── merge_predicate.sql                  NEW — wraps merge_condition_sql
│   ├── models/                                  UNCHANGED shape; marts gain mode-awareness
│   └── tests/                                   UNCHANGED
│
├── terraform/
│   ├── modules/
│   │   ├── reporting_ops/                       NEW — 4 DynamoDB tables + IAM policy
│   │   │   ├── main.tf  variables.tf  outputs.tf
│   │   ├── data_lake/variables.tf               MODIFIED — 3 prefixes
│   │   ├── glue_catalog/variables.tf            MODIFIED — realtime database
│   │   ├── lake_iam/                            MODIFIED — reporting role
│   │   └── (athena, emr_serverless, airflow_k3s, …)                        UNCHANGED
│   └── envs/dev/
│       ├── main.tf                              MODIFIED — wire reporting_ops
│       ├── variables.tf                         MODIFIED — enable_reporting_framework,
│       │                                        enable_streaming_rt
│       └── terraform.tfvars                     MODIFIED — both default false
│
├── scripts/
│   ├── reporting-compile.sh                     NEW
│   ├── reporting-validate.sh                    NEW
│   ├── reporting-dryrun.sh                      NEW
│   ├── create-datamart.sh                       NEW — scaffolding (§9)
│   ├── streaming-reset.sh                       NEW — confirm_destructive(), audited
│   ├── verify-destroy.sh                        MODIFIED — DynamoDB + checkpoints
│   └── (lib.sh, tf.sh, run-flow.sh, …)                                     UNCHANGED
│
├── docs/
│   ├── REPORTING_TARGET_ARCHITECTURE.md         THIS FILE
│   ├── REPORTING_REFERENCE_EVIDENCE.md          discovery evidence
│   ├── REPORTING_ARCHITECTURE_DISCOVERY.md      comparison
│   ├── REPORTING_RUNBOOK.md                     NEW — operate the five flows
│   ├── adr/ADR-033 … ADR-045                    NEW — 13 decisions
│   ├── DATA_CONTRACTS.md                        MODIFIED — §8 logical names + realtime
│   ├── FOUR_FLOWS.md                            MODIFIED — map to the five modes
│   ├── DBT_SPARK.md                             MODIFIED — execution mechanism
│   ├── COST.md                                  MODIFIED — DynamoDB, STREAMING_RT
│   └── (all other docs)                                                    UNCHANGED
│
└── Makefile                                     MODIFIED — reporting-compile,
                                                 reporting-validate, create-datamart
```

---

## 8. Terraform changes required

Nothing in the master prompt's §39 prohibition list. No new EKS, EMR cluster, Airflow
deployment, Kafka cluster or RDS.

| # | Change | Module | Reason | Cost |
|---|---|---|---|---|
| 1 | 4 DynamoDB tables — `job_execution`, `job_watermark_state`, `summary_config`, `streaming_app_state`; `PAY_PER_REQUEST`, SSE with the lake CMK, PITR off, TTL on `job_execution` | **NEW** `modules/reporting_ops` | ADR-036 | ~$0.00 idle; <$0.01/mo in use |
| 2 | S3 prefixes `artifacts/dbt/`, `ops/config/`, `checkpoints/reporting/` | `data_lake` (`lake_prefixes`) | dbt bundle, compiled plan, RT checkpoints | $0 |
| 3 | IAM role `reporting`: read logical source layers, write `mart` + `ops`, RW the 4 DDB tables (no `Scan`, no wildcard ARN), read `artifacts/dbt/`, RW `checkpoints/reporting/*`, Glue catalog | `lake_iam` | `CLAUDE.md` §3.7 | $0 |
| 4 | Glue database `realtime` (8th) — the `setsubtract` validation is a subset check, so it passes unchanged | `glue_catalog` | ADR-033 | $0 |
| 5 | Lifecycle rule on `ops/config/` (retain last N versions) | `data_lake` | `plan.json` is versioned | $0 |
| 6 | Flags `enable_reporting_framework`, `enable_streaming_rt` — **both default `false`** | `envs/dev` | `CLAUDE.md` §4.12 | — |
| 7 | Tags `Project`, `Environment`, `ManagedBy`, `Owner`, `CostCenter`, `AutoDestroyAfter` on every new resource | all | `CLAUDE.md` §4.11 | — |
| 8 | Destroy verification for the DynamoDB tables and the reporting checkpoint prefix | `scripts/verify-destroy.sh` | `CLAUDE.md` §4.12 | — |

Unchanged and reused: EMR Serverless (auto-stop 15 min, max capacity, private subnets),
`airflow_k3s`, Athena workgroup with its 10 GiB cutoff, the lake bucket and its KMS CMK,
`budget_guardrails`.

**Blocking discrepancy to resolve before any `terraform plan`.**
`envs/dev/terraform.tfvars` commits `enable_kafka_platform = true`, but `PROJECT_STATE.md`
records the applied cheap tier as 32 resources with MSK never created. Because
`emr_serverless`, `lake_iam` and `airflow_k3s` are all `count`-gated on that flag, none of
them exist today regardless of their own flags — so nothing this framework needs to run on
is currently deployed.

---

## 9. New-mart developer experience

```
scripts/create-datamart.sh --name mart_x --flows EOD,AUTO_CORRECT,FULFILL,STREAM_BATCH
  → dbt/models/marts/mart_x.sql        skeleton using resolve_source_layer + incremental_filter
  → dbt/models/marts/mart_x.yml        unique key, not_null, freshness, row-count tests
  → reporting/jobs/mart_x.yaml         job + per-mode flow config

make reporting-compile     schema, graph, cycles, turns, manifest sync
make reporting-validate    policy checks (partition spec, overlap, merge strategy, layers)
make reporting-dryrun      what would run, for which dates, reading which layer
make test                  the suite, including the new reporting tests
```

**No Airflow DAG is written.** That property is the acceptance test for the whole design;
a test asserts DAG count is independent of job count.

---

## 10. Pilot

**`mart_account_balance_daily`** — it exists (`dbt/models/marts/mart_account_balance_daily.sql`,
`spark/common/ddl/kimball.sql:177`), has one source (`stg_fact_account_daily_snapshot`), a
two-column grain (`account_sk + business_date`), no fan-out join, and a semi-additive
measure that already forced a documented correctness decision (S10-8).
`mart_customer_360_daily` joins three intermediates and is the wrong place to debug a new
framework.

It exercises every §44 item: config registration, dependency resolution, dbt execution,
EMR submission, Iceberg MERGE, Airflow orchestration, execution history, watermark, retry,
rerun, and validation.

---

## 11. Open items carried into implementation

| # | Item | Owner | Blocking? |
|---|---|---|---|
| 1 | Operator sign-off on ADR-036 (DynamoDB adds a service class to the account) | operator | before `terraform apply`, not before code |
| 2 | `enable_kafka_platform` discrepancy between tfvars and applied state (§8) | operator | before any `terraform plan` |
| 3 | `mart.dim_date` has `is_weekend` but no `is_working_day` / holiday flag; EOD and FULFILL date selection needs it (`kimball.sql:84`, `dim_builder.py:92`) | implementation, Phase 1 | before EOD flow |
| 4 | REALTIME layer does not exist; substituted mapping is an interim (ADR-033) | upstream layer work | before STREAM_BATCH is meaningful |
| 5 | Guide-v2 3-layer prompt marked `CONFLICTING`; needs a normative mapping section | docs | before the next guide session |
| 6 | Nothing is deployed — Airflow off, EMR off, zero Iceberg tables. Phases 1–3 are free and prove the design; live proof needs a metered window | operator | for `live-tested` claims only |

---

```
REPORTING_TARGET_ARCHITECTURE_APPROVED_PENDING_IMPLEMENTATION
```
