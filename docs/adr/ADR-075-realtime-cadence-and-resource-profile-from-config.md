# ADR-075 — REALTIME cadence and sizing come from the registry, one DAG per cadence

* **Status**: Accepted (implementation: Phase C)
* **Date**: 2026-09-20
* **Extends**: ADR-064 (config-driven realtime window), ADR-071 (config-driven orchestration)
* **Evidence**: `cdc/realtime.py`, `cdc/table_plan.py`, `airflow/dags/cdc_table_platform.py`,
  `spark/tests/test_realtime_phase_c.py`, `airflow/tests/test_dags.py`

## Context

Phase C's brief asks for a config-driven REALTIME materialization. An audit found almost all
of it already built and covered by **89 tests**: one generic engine (`realtime_engine.py`
taking `--table`), a frozen upper bound supplied by the caller, a half-open
`[lower, upper)` window, FULL_CDC as the only source, source and target snapshot ids
captured, per-table Iceberg targets, deterministic rebuild without Kafka, and the
`ops.realtime_run` ledger. Both retention invariants
(`physical_retention_days >= lookback_days + late_arrival_grace_days`, and the hours
equivalent) were already enforced at compile.

Two things were missing, and the first is the interesting one.

**`realtime.schedule` was read by nothing.** It was accepted by the loader, carried into the
compiled plan, and the actual cadence came from `CDC_REALTIME_CRON` in the Airflow
environment. Editing `schedule:` in the registry moved the plan hash and `make cdc-verify`
went green, so every signal said the change had landed. Config that *appears* live and is not
is worse than config that is absent, because an absence is visible.

**`resource_profile` did not exist.** Every REALTIME submission used one hardcoded envelope
(`timeout_minutes=40`, 2 cores / 4g driver, 2 × 2-core executors), applied identically to a
4-row reference table (`channel`, `BRANCH`) and to `digital_event`, which is 3,000 of the
lab's events.

## Decision

### 1. The cadence is a validated cron in the registry, resolved at compile

```yaml
realtime:
  schedule: "*/5 * * * *"     # omitted -> the platform default, */10
  resource_profile: medium    # omitted -> small
```

`normalise_schedule` refuses anything it cannot read — wrong field count, junk characters, a
non-positive minute step — and refuses Airflow **aliases** (`@daily`) so that two tables
asking for one cadence produce one identical string to group on. Validation happens at
compile because a bad cron discovered by Airflow is a DAG that fails to import, and a DAG
that fails to import disappears from the UI.

The plan carries `schedule` **resolved** plus `schedule_declared`, because "resolved to the
default" and "explicitly asked for `*/10`" are the same string and different facts.

### 2. One DAG per CADENCE — never per table, and never one DAG for all

Airflow has exactly one schedule per DAG, so per-table cadence cannot live in a single DAG
without a scheduler-side skip, which would fire a task only to do nothing. `schedule_groups`
turns the plan into `{cron: [table_id, ...]}` and the module builds one DAG per group,
dynamic-mapped over that group's tables:

* seven tables on one cadence → **one** DAG, seven mapped tasks (today's registry)
* one table moved to `*/5` → `cdc_realtime` (six) + `cdc_realtime_every_5m` (one)
* the default cadence keeps the id `cdc_realtime`, so runbooks and pools do not move when a
  second cadence appears

`test_no_per_table_realtime_dag_exists` fails if a per-table DAG ever appears.

### 3. The resource profile names sizing *and* the pool

`small` / `medium` / `large`, resolved into the plan and read by the submitter. The pool
belongs with the size because "how big is it" and "which queue does it compete in" are one
decision: a large job in the small pool starves it. `large` uses `spark_jobs`, the others
`reporting_jobs`. The default is **small** — a profile nobody chose must not be the one that
bills most.

### 4. Plan schema 9

A schema-8 consumer has a `schedule` it must default itself and no sizing at all — exactly
the split-brain this ADR closes.

## Options

* **Keep the cadence in the DAG env var.** Rejected: it makes `schedule:` a lie.
* **One DAG, skip tasks that are not due.** Rejected: it fires a task per table per tick to
  decide to do nothing, and turns the cadence into scheduler-side logic nobody can read from
  the DAG list.
* **One DAG per table.** Rejected — brief §9, and the rule ADR-071 already enforces.
* **A cron alias vocabulary (`@hourly`).** Rejected: two spellings of one cadence would split
  a group in two, producing two DAGs where one was meant.
* **Sizing from table row counts at submit time.** Rejected: it makes the cost of a run
  depend on data that changes, and an unreviewed number is not a decision.
* **Profile without a pool.** Rejected — §3.

## Consequences

* Changing `schedule:` in the registry now changes the cadence, and the DAG id may change
  with it (a new cadence = a new DAG). An operator moving a table between cadences must
  unpause the new DAG; the old one keeps running the tables that remain.
* `CDC_REALTIME_CRON` survives only as the fallback when the plan cannot be read at parse
  time, and as the id anchor for the default group.
* Airflow must be able to import the `cdc` package (`CDC_PACKAGE_ROOT`, default
  `/opt/airflow`). If it cannot, the module falls back to the single legacy group rather than
  failing to import.

## Cost

**$0 added.** Every REALTIME DAG remains `is_paused_upon_creation=True` and gated on
`ENABLE_CDC_TABLE_PLATFORM`. The default profile is *smaller* than the previous hardcoded
envelope (1 core / 2g × 1 executor against 2 / 4g × 2), so when these DAGs are enabled the
shipped tables cost **less** per run than before this ADR, not more. A table that needs more
must now say so in a reviewed file.

## Security

No IAM, network or credential change. The cadence and profile are configuration; neither
reaches a secret or widens an access path.

## Rollback

Remove `schedule:` and `resource_profile:` from the registry: the loader defaults to
`*/10 * * * *` and `small`, which is one DAG with the previous cadence. Reverting the DAG
module restores the env-var cadence, and `_FALLBACK_PROFILE` already reproduces the old
hardcoded sizing.

## Validation

1. `spark/tests/test_realtime_phase_c.py` — **28 passed**: cadence defaulting, declared
   override, six malformed crons refused at compile, alias refused, grouping (one DAG for
   seven tables, a second cadence splits, disabled joins none, deterministic), profile
   ordering/pool/refusal/resolution, and schema 9.
2. `airflow/tests/test_dags.py::TestRealtimeCadenceComesFromConfig` — **6 passed**: one
   cadence DAG, no per-table DAG, cadence read from the plan, dag-id derivation, sizing and
   pool from the profile, and importability with no plan.
3. The brief's §10 matrix: 11 of 13 cases were already covered by the 89 existing realtime
   tests (3-day window, 5-day/per-table override, late inside grace, late outside the
   logical window, physical retention, frozen upper, rerun, rebuild, deletes, schema add,
   disabled). The two that were not are now covered on real Iceberg:
   * `test_two_tables_run_in_parallel_without_touching_each_other` — concurrent runs, each
     target a subset of its OWN FULL_CDC, two ledger rows
   * `test_a_failure_leaves_the_previous_target_contents_intact` — a failure injected after
     the window resolves leaves the served rows unchanged, and the next run converges
4. `make check-all` exit 0 — **2,524 passed**, 14/14 static, shellcheck clean, plan not
   stale (`cdccfg-d2b45f36c02a5e5c`).
5. **Not claimed**: no REALTIME DAG has executed on the deployed Airflow, and no
   materialization has run against the live catalog (which currently holds 0 tables). The
   Glue catalog must be re-provisioned first.
