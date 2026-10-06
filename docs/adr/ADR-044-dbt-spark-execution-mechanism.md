# ADR-044 — dbt-spark execution mechanism

- Status: **ACCEPTED** (Session 22)
- Supersedes: the `emr_serverless` target in `dbt/profiles.yml.example`
- Related: ADR-008 (Spark runtime), `docs/DBT_SPARK.md`, `CLAUDE.md` §4.5/§4.6

## Context

`dbt/profiles.yml.example` declares two targets: `local` (`method: session`) and
`emr_serverless` (**`method: thrift`**, requiring `DBT_SPARK_HOST` and `DBT_SPARK_PORT`).

EMR Serverless does not expose a Thrift endpoint. Providing one means standing up and
holding a Spark Thrift server, i.e. always-on compute — which `CLAUDE.md` §4.5 rules out
by default and §4.6 requires auto-stop for. The current target therefore cannot work as
written, and the way to make it work is prohibited.

Repo 5 solved the same problem on the same service.
`IaC/modules/data_platform/emr-serverless/run_dbt_job.py` runs dbt **inside** the EMR
Serverless Spark driver: it downloads the project zip from S3, writes `profiles.yml` at
runtime with secrets from Secrets Manager, and invokes the Python API —

```python
dbt = dbtRunner()
result: dbtRunnerResult = dbt.invoke([
    "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
    "--target", job["target"], "-s", job["modelFqn"],
    "--vars", "{data_date: " + args.dataDate + "}"])
```

with the profile set to `type: spark, method: session` and the Glue catalog reached
through `spark.sql.catalog.glue` + `spark.sql.defaultCatalog=glue`
(`execute_job_logic.py:44-82`).

## Options

| Option | Verdict |
|---|---|
| **`dbtRunner` inside an EMR Serverless job, `method: session`** | **CHOSEN** |
| `method: thrift` against a Spark Thrift server | Rejected |
| dbt CLI in an Airflow task against a remote endpoint | Rejected |
| Abandon dbt; hand-write Spark SQL | Rejected |

Running dbt in the Airflow task would put Spark on the k3s orchestration node, which
`airflow/dags/common.py:52-62` already rules out for the Spark jobs and which ADR-011's
node sizing cannot carry.

## Decision

**`dbt/` is packaged to `s3://<lake>/artifacts/dbt/<code_version>/dbt.zip` and executed by
`spark/reporting/run_dbt_job.py` as an EMR Serverless job.**

```
EMR Serverless job
  ├─ download + unzip dbt.zip                (code_version pinned by the plan)
  ├─ write profiles.yml at runtime            (no secret in Git — CLAUDE.md §3.1/§3.6)
  ├─ dbtRunner().invoke([...])                (method: session, in-driver)
  ├─ parse run_results.json → row metrics
  └─ return non-zero on failure               (so the engine reports FAILED)
```

`dbt/profiles.yml.example`'s `emr_serverless` target is rewritten to `method: session`
with `server_side_parameters` carrying the Glue catalog settings. The `local` target is
unchanged and remains what the 47 existing dbt tests run against.

**Selection and variables** come from the plan, not from a hand-maintained registry row:

```
--select   {{ job_flow_config.dbt_selector_override | default(job_master.dbt_selector) }}
--vars     flow_mode, cob_date, from_date, to_date, window_start, window_end,
           watermark_before, watermark_after, run_id, execution_id,
           is_rerun, is_backfill, source_layer, input_cutoff, config_version
```

This is repo 5's shape with the registry replaced by ADR-034's compiled plan.

**`dbt build`, not `dbt run`.** Repo 5 invokes `run`, which skips tests. Validation is a
gate here (ADR-043 step 3), so the framework invokes `build` — or `run` followed by
`test --select <validation_selector>` where a mode needs them separated. A model whose
tests fail must not reach SUCCEEDED.

**No retry loop inside the job.** Repo 5 wraps `dbt.invoke` in
`while not is_successful and number_of_retries < 8`. A non-transient model failure retried
eight times is eight times the EMR bill. Retries belong to Airflow, with backoff, bounded
by `job_flow_config.retry_count` (`airflow/dags/common.py:34-46` already sets
`retry_exponential_backoff` and `max_retry_delay`).

**Artefacts are captured.** `run_results.json` and `manifest.json` are written back to
`s3://<lake>/artifacts/dbt/<code_version>/<execution_id>/`, and per-model timings and row
counts land in `job_master_execution_hist`. Repo 5 discards both.

## Consequences

- dbt runs on the same engine, catalog and IAM role as the Spark jobs — one execution
  path, one status model, one place to look.
- `docs/DBT_SPARK.md` §2's boundary is unchanged: dbt still does not own the canonical
  transformation, the guarded MERGE or SCD2.
- `dbt deps` remains unnecessary; there is still no `packages.yml`, so the job needs no
  network egress beyond S3 and Glue (ADR-022).
- The dbt project must be zipped in CI and its `code_version` pinned in the plan.

## Cost

One EMR Serverless job per dbt invocation, auto-stop at 15 min idle already configured
(`modules/emr_serverless`). No standing endpoint, no idle charge.

## Security

`profiles.yml` is generated at runtime; nothing authenticating is committed. Glue and S3
are reached with the `reporting` role through the default credential chain
(`CLAUDE.md` §3.2). No password exists to leak — the lab uses IAM throughout.

## Rollback

`method: session` also works locally, so reverting the execution wrapper leaves the models
runnable. The `local` target is untouched.

## Validation

- The 47 existing dbt tests still pass on the `local` target.
- A test asserts `dbt/profiles.yml.example` contains no `method: thrift`.
- A test asserts `run_dbt_job.py` exits non-zero when `dbtRunnerResult.success` is false.
- A test asserts no retry loop wraps `dbt.invoke`.
- A test asserts `run_results.json` is uploaded and its model count matches the selector.
