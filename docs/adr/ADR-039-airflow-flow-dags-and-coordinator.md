# ADR-039 — Airflow flow DAGs and runtime coordinator

- Status: **ACCEPTED** (Session 22)
- Related: ADR-034, ADR-038, ADR-010 (KubernetesExecutor), ADR-011 (k3s)

## Context

Three shapes were evaluated against evidence, not preference.

**A — per-mart dynamic DAG factory.** Repo 1's actual choice:
`pipeline_auto_generator.py:23-155` builds one DAG per row of `job_master`, and
`coordinator.py:65` builds one coordinator DAG per `job_type` that triggers them with
`TriggerDagRunOperator(wait_for_completion=True, poke_interval=15, deferrable=True)`
(`coordinator_graph_builder.py:31-42`).

**B — fixed flow DAGs + runtime coordinator.** Repo 3's shape: five DAGs driven entirely
by one YAML (`v9/airflow/dags/bcn_realtime_dags.py` + `configs/bcn_pipeline.yaml`), whose
header states "everything adjustable is here; you never edit the DAG file".

**C — one DAG per mart, hand-written.** The thing the requirement exists to prevent.

Option A's cost is structural: with N marts × 5 modes it produces 5N job DAGs plus
coordinators, every one re-parsed on the scheduler's parse loop, and every job execution
becomes **two** DAG runs. ADR-011 puts Airflow on k3s on a small node and
`airflow/dags/common.py:31` already records that pool limits, not DAG limits, are the real
concurrency control there. Option A also inherits repo 1's `_check_if_prev_job_running`
/ `check_is_processing` machinery, which exists to reconcile two DAG runs' views of one
job's state.

## Options

| Option | Verdict |
|---|---|
| **Hybrid: fixed flow DAGs, config-driven task expansion, runtime coordinator** | **CHOSEN** |
| A — per-mart dynamic DAG factory (repo 1) | Rejected |
| C — one hand-written DAG per mart | Rejected |

## Decision

**Four batch flow DAGs, plus a separate streaming lifecycle. One DAG run per flow run.**

```
datamart_eod            after the EOD close gate
datamart_auto_correct   every 30 min inside the window
datamart_fulfill        schedule=None; params; dry_run defaults true
datamart_stream_batch   every 5–15 min inside the window

streaming_rt_start / streaming_rt_monitor / streaming_rt_stop   (ADR-041)
sync_reporting_plan     refreshes the local plan cache from S3
```

Each batch DAG has one shape:

```
resolve_plan
    └── turn_1  (TaskGroup)  →  .expand() over the jobs in turn 1
    └── turn_2               →  ...
    └── turn_N
finalize      writes ops.summary_config_hist_v1, emits metrics
```

Per job, the mapped chain is:

```
gate_dependencies → submit → track → validate → commit_watermark
```

which is repo 1's proven sequence (`read_config → check_is_processing → check_ready_eod →
check_is_skipped → handle_run_mode → submit_spark_job → handle_final_job_status`,
`pipeline_builder.py:453-770`) with two corrections: `validate` is a distinct step that
must pass before success, and `commit_watermark` is last and separate (ADR-043).

**Dynamic task mapping, not dynamic DAGs.** The DAG *files* are static and few; the
*tasks* expand from `plan.json`. This is what buys option A's config-driven property
without its parse cost.

**DAG parsing never queries the warehouse.** DAG files read a local `plan.json`. Repo 1
built the same cache for the same reason (`functions.py:42-95`, with an Airflow Variable
switching `database` ↔ `cache_file`), and the argument is stronger here: a parse-time
Athena call is billed and `envs/dev/terraform.tfvars` sets a bytes-scanned cutoff exactly
to catch that pattern. `sync_reporting_plan` refreshes the cache on a schedule and on
demand.

**Operational overrides are Variables, not config edits.** Disabling a job at 02:00 sets
an Airflow Variable (`reporting_disabled_jobs`, `reporting_ignored_flow_modes`) that the
coordinator consults. Config stays a pull request (ADR-034); the break-glass path does not
require one. Repo 1 exposed the same surface as sixteen `orchestration/flow_*.py` DAGs;
Variables are the smaller equivalent.

## Consequences

- Adding a mart adds **zero** DAG files.
- The scheduler parses ~8 DAG files regardless of mart count.
- One DAG run per flow run, so `dag_run_id` is a usable correlation key.
- `airflow/tests/test_dags.py`'s orchestration-only rule extends to the new DAGs.
- Existing `flow_nrt_mart` / `flow_auto_correct` DAGs stay as-is; they orchestrate the
  Spark flows, not the reporting framework, and are not migrated in this phase.

## Cost

No new infrastructure. Fewer DAG-parse cycles than option A. Pools
(`POOL_SPARK`, `POOL_MAINTENANCE`, `airflow/dags/common.py:31-32`) gain
`POOL_REPORTING` so reporting cannot starve the CDC path.

## Security

None new; DAGs submit external jobs under the existing `reporting` role (ADR-036).

## Rollback

The plan-driven expansion is contained in `airflow/dags/reporting_common.py`. Reverting to
hand-written DAGs means writing them; nothing has to be migrated.

## Validation

- A test asserts each flow DAG imports and expands from a fixture `plan.json`.
- A test asserts no `airflow/dags/**` module opens a warehouse connection at import time.
- A test asserts DAG count is independent of the number of jobs in the plan.
- A test asserts a job disabled by Variable is skipped, and the skip is recorded with
  status `SKIPPED`, not `SUCCEEDED`.
