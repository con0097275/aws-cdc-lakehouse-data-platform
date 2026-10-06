# ADR-071 — Config-driven CDC orchestration: tables are data, not DAGs

* **Status**: Accepted (implementation: Phase 8 acceptance closure)
* **Date**: 2026-09-10
* **Extends**: ADR-062…ADR-070; master brief §56 ("no custom Airflow DAG per table")
* **Evidence**: `airflow/dags/cdc_table_platform.py`,
  `airflow/tests/test_dags.py::TestCdcTablePlatformIsConfigDriven`

## Context

Phases 1–7 built generic engines that each take `--table` and proved them live. Nothing
scheduled them. Every REALTIME materialisation, every EOD close and every maintenance run in
this project has been a hand-typed `scripts/emr-submit.sh`.

That made the Phase 8 acceptance point **"no custom Airflow DAG is needed"** pass for the
wrong reason: no DAG orchestrated the per-table platform *at all*. The check was satisfied by
absence rather than by genericity, which is the same shape as a green test that asserts
nothing. It was recorded as open issue #7 rather than quietly claimed.

`dag_eod_pipeline.py` does hardcode three table names, but they are the **legacy** ones
(`full_cdc.oracle_corebank_customer`, …) driving the pre-platform path.

## Decision

### 1. Three generic DAGs, and the task list is a function of the registry

```
cdc_realtime      */10 * * * *   materialise each enabled table's rolling window
cdc_eod           30 2 * * *     close each enabled table for its own COB
cdc_maintenance   0 4 * * *      metric-driven Iceberg maintenance, bounded
```

Every task is produced by **dynamic task mapping over the compiled plan**. Onboarding a table
adds a registry entry and nothing else: measured live, 7 → 8 REALTIME tasks and 10 → 11 EOD
tasks appeared from one YAML entry with the DAG file untouched.

The mapping reads `realtime_policy.enabled` per table, so a near-static reference table with
`realtime: {enabled: false}` gets **no task** rather than a scheduled no-op.

### 2. The DAG reads the same plan artifact the jobs read

Not its own copy of the registry. If Airflow and the Spark jobs resolved the table list
separately they would disagree the moment one was redeployed, and the disagreement would
present as a table that "sometimes runs".

A missing plan is a **refusal**, not an empty list. A DAG that silently produced zero tasks
would report SUCCESS every run while materialising nothing — the green-and-idle failure this
platform has now hit three separate ways (ADR-070).

### 3. EOD does not compute the COB date here

The date is derived **inside the engine** from each table's own `business_timezone` and
`business_date_lag_days` (ADR-065 §2). Computing it in the DAG would apply the *scheduler's*
timezone to every table and certify an hour of events into the wrong business date for any
table that is not UTC. Asserted by a test on the absence of `--cob-date`.

### 4. Three things these DAGs must never do

| | why |
|---|---|
| **ingest** | FULL_CDC is written by a long-running streaming app whose lifecycle is `streaming_rt_lifecycle.py` (ADR-041/045). A task per micro-batch is a micro-batch architecture wearing a streaming name |
| **provision** | an orchestrator that auto-creates targets brings unowned, unclassified, unretained data products into existence *on a timer* (ADR-063 §F) |
| **update connectors** | that gate requires a human to type a confirmation phrase, which is the entire point of it |

Enforced by a test that greps the executable body — not the docstring, which names the banned
things while explaining that they are banned.

### 5. The coordination callables are registered deliberately

`test_no_python_operator_carries_transformation_logic` refused the three new adapters until
they were added to `COORDINATION_CALLABLES` by name. That is the allowlist working: each one
builds arguments and calls the shared submitter, and **no row of data passes through any of
them** — the transformation is entirely inside the Spark job (CLAUDE.md §7).

### 6. Off by default, bounded, and paused on creation

`ENABLE_CDC_TABLE_PLATFORM` defaults false, so deploying the file starts nothing and the DAGs
carry no schedule. `--max-tables` bounds maintenance, because maintenance rewrites data and
commits: an unbounded run turns the maintenance window into the platform's peak load, on the
same capacity the ingest uses (ADR-068).

### 7. The submission is bound to the submitter's real signature by test

The first version of `_submit_callable` called
`submit(name=..., entry_point=..., arguments=...)`. That reads perfectly and is wrong:
`EmrServerlessSubmitter.submit` takes one positional `SubmissionRequest`, so it would have
raised `TypeError` the first time a task ran — and for a DAG that ships off by default,
"the first time a task ran" could be weeks after review and merge.

Every other check in the DAG suite reads the file, so none of them would have caught it.
`test_the_submit_call_matches_the_real_submitter_signature` uses `inspect.signature` to bind
the DAG to the actual contract: the parameter list of `submit`, the field names the DAG sets
on the request, and the handle attribute the callable returns. A rename on either side now
fails a test instead of a run.

## Options

* **A DAG per table** (`account_cdc_dag.py`, …). Rejected — master brief §56. A per-table DAG
  is a per-table deployment, a per-table review, and a per-table way to drift from its
  siblings. `test_no_per_table_dag_exists` fails if one appears, checking every registry
  table name against every DAG filename.
* **A literal table list in the DAG.** Rejected: it would drift from the registry exactly as
  the connector capture list and the Kafka topic list both did (ADR-070), and with the same
  silent result. A test asserts no registry table name appears in the executable body.
* **Keep running everything by hand.** Rejected — it is what made the acceptance point pass
  vacuously, and it does not scale past a demo.
* **Have the DAG also run the ingest.** Rejected — §4.
* **One DAG with three branches.** Rejected: the three have genuinely different cadences
  (10 minutes / daily / daily-off-peak) and different failure meanings. A single `max_active_runs`
  across them would let a slow maintenance run block every REALTIME window.

## Consequences

* Open issue #7 is closed, and acceptance point 3 now passes for the right reason.
* `dag_eod_pipeline.py` is untouched and still drives the legacy path. It is superseded for
  registered tables, not deleted — the same treatment `spark/jobs/eod/job.py` received.
* The DAGs need `CDC_TABLE_PLAN`, `REPORTING_LAKE_BUCKET`, `REPORTING_EMR_APPLICATION_ID` and
  `REPORTING_JOB_ROLE_ARN`. All are already set for the reporting DAGs.
* **Not yet live-tested.** These DAGs have never run on the deployed Airflow: the platform's
  Airflow instance is not currently running the CDC flows, and enabling them is a scheduling
  and cost decision. What is proven is that they parse, that they satisfy every DAG policy
  test, and that the task list derives from the registry.

## Cost

**$0 as shipped** — `ENABLE_CDC_TABLE_PLATFORM=false`, so the DAGs carry no schedule and fire
nothing. Once enabled the cost is exactly the jobs they submit, which are the same bounded
EMR Serverless runs an operator was launching by hand; the change is who types the command.

The one new cost lever is cadence: `cdc_realtime` at `*/10` is 144 submissions/day across the
enabled tables. That is why the cron is an env var and why REALTIME is per-table disableable —
three of the ten shipped tables already opt out.

## Security

* **No new resource, no IAM change.** The DAGs submit to the existing EMR Serverless
  application with the existing job role, via the same shared submitter the reporting DAGs use.
* They cannot create a table, change a connector, or read a source database.
* No credential appears in the DAG: the role ARN and application id come from the
  environment, and the submitter uses the default credential chain (CLAUDE.md §3.1–3.2).

## Rollback

Delete the file, or leave `ENABLE_CDC_TABLE_PLATFORM` unset — the DAGs are paused on creation
and carry no schedule, so an unwanted deploy does nothing. Everything they submit remains
runnable by hand through `scripts/emr-submit.sh`, which is how it was done up to this point.

## Validation

1. `pytest airflow/tests/test_dags.py` — **51 passed**, including the pre-existing policy
   suite (paused-on-creation, dagrun timeout, execution timeout, no transformation in a
   PythonOperator) and `TestCdcTablePlatformIsConfigDriven`: the three DAGs exist, no
   per-table DAG exists, the task list comes from the plan and names no registry table
   literally, the DAGs do not ingest/provision/touch connectors, EOD passes no `--cob-date`,
   maintenance is bounded, all three are off by default, and the submission call is bound
   by `inspect.signature` to the submitter's real contract.
1b. `pytest spark/tests airflow/tests -q` — **2,283 passed, 0 failed** (4m21s).
2. **Measured**: with the shipped registry the mapping yields 7 REALTIME tasks (10 tables
   less the 3 that disable it) and 10 EOD tasks. Adding one registry entry took those to
   **8 and 11** with no change to the DAG file.
