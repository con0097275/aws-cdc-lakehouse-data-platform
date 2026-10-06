# ADR-035 — Job master vs runtime history separation

- Status: **ACCEPTED** (Session 22)
- Related: ADR-034, ADR-036, ADR-043

## Context

Repo 1's `job_master` is not configuration. `src/c2pp/init/data_lake_init.sql:28` declares
`date_of_data DATE` on it, and `plugins/job_scheduler/functions.py:1119
sync_job_master_hist` opens with an `UPDATE dlpt.job_master SET date_of_data = ...` that
pushes every non-EOD job's business date forward to follow its `before_full` upstreams.

The consequences are visible throughout that codebase:

- `job_master_execution_hist` re-declares 20 columns copied from master on every turn
  (`data_lake_init.sql:49-95`) so history survives a config edit — a copy that can drift.
- The business date is advanced by **the Spark job itself**, from inside the
  transformation (`f_acct_depo_by_day_level_acct_auto_correct_generate.py:1359-1372`
  updates `summary_config_v1.execution_date`). Position is owned by business logic.
- `handle_final_job_status` compensates for the resulting confusion by treating "the
  execution_date in summary_config moved" as evidence that a FAILED Spark run actually
  succeeded (`pipeline_builder.py:690-697`).

Each of those is a symptom of one cause: config and position live in the same row.

## Options

| Option | Verdict |
|---|---|
| **Strict separation: config immutable, position and runtime elsewhere** | **CHOSEN** |
| Repo 1's shape — mutable master carrying `date_of_data` | Rejected |
| One wide table with a `record_type` discriminator | Rejected |

## Decision

Three planes, no overlap:

| Plane | Contains | Store | Mutability |
|---|---|---|---|
| **Config** | `job_master`, `job_flow_config`, `job_dependency`, `resource_profile` | Git → `plan.json` → Iceberg `ops.*` | immutable per `config_version` |
| **Position** | `job_watermark_state`, `summary_config_v1` | DynamoDB | mutable, one row per key, framework-owned |
| **Runtime** | `job_master_execution_hist`, `summary_config_hist_v1` | DynamoDB (live) → Iceberg (terminal) | append-only audit |

`job_master` **must not** contain: `date_of_data`, `status`, `turn`, `turn_watermark`,
`spark_app_id`, `started_at`, `ended_at`, `error_msg`, `is_rerun`, `estimated_duration`.
A compile-time schema check rejects any of those keys in a job YAML.

**Position is framework-owned.** No Spark job and no dbt model writes
`job_watermark_state` or `summary_config_v1`. Only `spark/reporting/ops_client.py` does,
and only from the success path (ADR-043). This is the single most important reversal of
repo 1's design: business logic reads position and never advances it.

**History does not copy config.** `job_master_execution_hist` carries `config_version` and
`code_version`, not twenty duplicated config columns. The config that produced a run is
recovered by joining on `config_version` — which is possible precisely because
`ops.job_master` is append-only and partitioned by it (ADR-034).

The exception is **resolved runtime values**, snapshotted deliberately: `driver_cores`,
`driver_memory`, `executor_cores`, `executor_memory`, `executor_instances`,
`application_args`, `resolved_source_layer`. These record what the run actually used after
profile resolution and per-job override, which is not derivable from config alone.

## Consequences

- "Why was this run slow" is answerable: resolved resources are on the row.
- "What config produced this number" is answerable: `config_version` join.
- Editing config never rewrites history, and never moves a business date.
- A rerun cannot be inferred from a mutated master row; it is an explicit
  `attempt_number` (ADR-043).

## Cost

One Iceberg write per completed execution instead of one per status transition
(ADR-036). Lower, not higher.

## Security

None beyond ADR-036's IAM split.

## Rollback

Config and history are separate objects; reverting would mean merging them, which nothing
depends on. No data migration exists to undo.

## Validation

- Compile fails if a job YAML declares any runtime key.
- A test asserts `ops.job_master` has no `date_of_data`, `status` or `spark_app_*` column.
- A test asserts no module outside `ops_client.py` writes the watermark table.
- A test reconstructs the config for a historical execution by `config_version` join and
  asserts it matches the `plan.json` archived in S3 for that version.
