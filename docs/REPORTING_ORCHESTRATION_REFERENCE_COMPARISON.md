# Reference orchestration patterns — KEEP / ADAPT / REJECT

Phase E §9. Three reference files were read **read-only**; nothing was copied. Each pattern
below is judged against the architecture this repository already has: a bounded set of
generic flow DAGs, a config-driven coordinator, and dynamic task mapping (ADR-039, ADR-071,
ADR-075/076).

Sources:

* `data_pipelines/airflow_pipelines/airflow_dim_model.py` (344 lines)
* `data_pipelines/airflow_pipelines/airflow_dwh2cdp_extract.py` (117 lines)
* `data_pipelines/src/scheduler/airflow/dags/pipelines/pipeline_builder.py` (1,922 lines)

---

## 1. `airflow_dim_model.py` — the readiness query

```python
sql = f"""
    select distinct target_table as table_name, cast(pre_datelastmaint as string), ...
    from dlpt.eod_info
    where cob_date = '{execution_date}'
    and target_table in (
        {','.join([f"'{tbl.replace('sat_', 'sat_snp_')}'" for tbl in dependencies])}
    )
"""
range_rows = impala_exec_sql(sql, as_dict=True)
if len(range_rows) + len(range_eod_foundation_tables) == len(dependencies_transform):
    is_ready_eod = 1
```

| verdict | element | reasoning |
|---|---|---|
| **KEEP** | **One batched query** — `target_table IN (...) AND cob_date = ...` for every dependency at once | A wave of twelve jobs over eight shared datasets is one query, not ninety-six. `EodInfoReader.fetch` does exactly this and caches per coordinator run. |
| **KEEP** | Readiness is asked of a **control table**, never of the target's row count | Producing data and declaring a day closed are different events. Only the second is safe to depend on. |
| **KEEP** | Two sources of readiness (`eod_info` **and** `summary_config_v1`) reconciled into one verdict | Our equivalent: a dataset may be closed by the EOD layer *or* be a job the reporting plan owns. The gate answers one question either way. |
| **ADAPT** | Readiness = "a row exists for this `cob_date`" | Our `eod_info` carries `certification_status` (ADR-076), and a close that BUILT but failed DQ is deliberately left readable. Existence is therefore not enough: `EodTableReadyGate` requires `CERTIFIED` unless a job writes `certification: ANY` down. |
| **ADAPT** | `pre_datelastmaint` / `datelastmaint` read directly | Kept as a **view** over the unambiguous watermark pair (ADR-076 §9), not as canonical column names. |
| **REJECT** | `tbl.replace('sat_', 'sat_snp_')` — deriving a dependency's physical table by string surgery on a prefix | A derived name maintained by hand. This repository hit that exact class three times this month (a health gate asserting `= "4"`, a dead EMR application id, a stale writer-schema export). `required_datasets` names the **canonical registry id**; the physical table is resolved by the catalog. |
| **REJECT** | Readiness by **counting** matches (`len(rows) == len(deps)`) | Counting says "three of four are ready" and never says *which* one is missing — the only thing an operator needs at 02:00. Our verdict carries `ready` / `waiting` / `failed` lists and names the gap. |
| **REJECT** | f-string SQL interpolating an execution date and a table list | One quoted identifier away from breaking, and untestable without a warehouse. The SQL lives in `EodInfoReader.query_for`, which a test asserts on directly. |

---

## 2. `airflow_dwh2cdp_extract.py` — one DAG per config file

```python
for path in config_paths:
    config = json.load(open(path))
    TARGET_TABLE = config["TARGET_TABLE"]
    dag_id = f"extract_{TARGET_TABLE}"
    if "_oram1_" in TARGET_TABLE:
        schedule_interval = "0 6 * * 2-6"
    else:
        schedule_interval = "0 1 * * 2-6"
    dag = DAG(dag_id=dag_id, schedule_interval=schedule_interval, ...)
```

| verdict | element | reasoning |
|---|---|---|
| **REJECT** | **One DAG object per config file** | This is the decision Phase E §1 asks to justify, and the evidence is against it here. N configs become N DAGs, N schedules and N UI entries; a change to the shared shape is N reviews. Our four flow DAGs plus dynamic task mapping already scale to any number of jobs, and `test_the_realtime_dag_is_built_per_cadence_not_per_table` fails if a per-table DAG appears. **Nothing in current operational requirements shows the fixed-flow coordinator is insufficient**, which is the bar §1 sets for generating per-pipeline DAGs. |
| **REJECT** | Cadence chosen by **substring match on a table name** (`if "_oram1_" in TARGET_TABLE`) | The schedule is a property of the data contract, not of a naming convention. Renaming a table silently reschedules it. Ours is a validated cron in the registry (ADR-075/076), and one DAG per *cadence* — so a table that needs 02:30 gets it without a file per table. |
| **KEEP** | Per-config resource sizing (`DRIVER_CORES`, `EXECUTOR_MEMORY`, `NUM_EXECUTORS`) | Right instinct: a 4-row reference table and a 3,000-event one should not share an envelope. Adopted as the **named** `resource_profile` (small/medium/large) rather than four loose numbers per config, so the choice is reviewable and bounded. |
| **KEEP** | A `ShortCircuitOperator` control check before the work | The same shape as a readiness gate: decide cheaply, then skip rather than fail. Ours returns a verdict with a reason instead of a bare boolean. |
| **ADAPT** | Config discovered by `glob(...)` at **parse time** | Discovery is right; doing it in the DAG file is not (§8). Ours globs in the **compiler**, and the DAG reads one precompiled plan — so adding a YAML changes the runtime plan with no Python edit and no scan during import. |

---

## 3. `pipeline_builder.py` — the generic builder

Assessed in full during Phase A (`docs/CDC_RUNTIME_PRODUCTION_REFINEMENT.md` §19). Carried
forward here for completeness.

| verdict | element | reasoning |
|---|---|---|
| **KEEP** | `impala_get_ready_eod(job_name, running_date)` as a **named readiness function** resolving declared dependencies against `eod_info` for one `cob_date` | Became `EodTableReadyGate`. The value is that readiness has a name and one implementation instead of being inlined per DAG. |
| **KEEP** | Declared dependencies in **config** (`summary_config_v1`), not in the DAG | Already how ADR-034 works; `required_datasets` extends it to layer readiness. |
| **KEEP** | Explicit skip/processing branches (`check_is_processing`, `check_is_skipped`) | A rerun must distinguish "already running" from "already done" from "deliberately skipped". Our status vocabulary does this (ADR-046). |
| **KEEP** | Watermark advanced on a **branch**, never unconditionally | Exactly ADR-076 §10: `eod_info` moves only after certification. |
| **ADAPT** | `eod_info` carrying `(pre_datelastmaint, datelastmaint, cob_date)` | Adopted as `prev_watermark_ts` / `watermark_ts`, with the old spelling available through a view. |
| **REJECT** | `os.system(f'rm /home/dlpt/tmp/airflow/config/check_if_ready_eod_*_{job_name}.json')` | Shelling out to `rm` with a glob and an interpolated job name, on the scheduler's local disk. Unsafe, untestable, and a cache belongs in the coordinator run — which is where ours lives (`EodInfoReader`, per instance). |
| **REJECT** | Impala/Hive coupling, `set timezone=UTC` session hacks, trust-store password reads | Engine-specific and credential-handling patterns this platform already solves (Athena + SSM). |
| **REJECT** | `signal`-based `TimeoutException` around SQL | Signals do not work off the main thread and interact badly with Airflow's own timeouts. The client's timeout is the right mechanism. |
| **REJECT** | Per-job DAG construction | Same reasoning as §2. |

---

## What this changes in this repository

Nothing structural. The architecture §1 prefers is the one already deployed, and the
reference files confirm the cost of the alternative rather than the benefit. Phase E adds one
gate, one config key and one batched reader:

```yaml
# reporting/jobs/<mart>.yaml -- external LAYER readiness only
required_datasets:
  - layer: EOD
    table_id: oracle.coredb.corebank.account   # canonical registry id
    certification: CERTIFIED
```

dbt `ref()` edges are **not** repeated here: `manifest_sync` registers them from
`target/manifest.json` and a test asserts no `required_datasets` entry names a job.
