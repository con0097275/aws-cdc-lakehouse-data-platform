# Handoff — dbt/Airflow dependency orchestrator phase

Written 2026-08-22 at the end of session 34, so the next session starts from evidence
rather than re-auditing. Findings below are verified with file:line, not recalled.

## THE FINDING THAT MATTERS — start here

**Table dependencies are declared but never enforced.**

`reporting/schema/job.schema.json` permits six dependency types:
`JOB, DATASET, EOD_TABLE, REALTIME_TABLE, FULL_CDC_TABLE, EXTERNAL`.

But `spark/reporting/dependency_engine.py:115`:

```python
if (not dep.is_active or not dep.required
        or dep.dependency_type is not DependencyType.JOB):
    continue
```

Only `JOB` survives. And `coordinator.dependency_state` iterates only
`task["required_upstreams"]` — a list of job_ids. So:

- `reporting/jobs/mart_account_balance_daily.yaml` declares an `EOD_TABLE` dependency on
  `kafka_dev_lab_dev_snapshot.fact_account_daily_snapshot`
- that dependency is **silently ignored at gate time**
- the job can be gated READY while its declared table dependency is unmet

The EOD flow separately calls `check_eod_ready` (ledger row + Iceberg tag), but only for
`EOD` mode and only for `source_tables` passed in by the caller — NOT from the declared
dependency block. So the declaration and the enforcement are two unconnected mechanisms.

This is precisely §33/§34 of the prompt (external/dataset gates, `DependencyGate` returning
READY / WAITING / FAILED / NOT_APPLICABLE). It is a **P1-class correctness gap**, not a
feature request.

## Gap analysis already done (§5 of the prompt)

| Component | Status | Evidence |
|---|---|---|
| Config compiler | **COMPLETE** | `reporting/compile.py`; deterministic `config_version` |
| plan.json | **COMPLETE** | `make reporting-compile-dbt` writes `target/reporting/plan.json` |
| job_master / job_flow_config | **COMPLETE** | in the compiled plan |
| job_dependency | **PARTIAL** | declared, compiled, but only JOB edges enforced (above) |
| job_watermark_state | **COMPLETE** | DynamoDB, proven live |
| summary_config_v1 / _hist | **COMPLETE** | `kafka-dev-lab-dev-summary-config` |
| dbt manifest dependency parser | **COMPLETE** | `manifest_sync.py`; `make reporting-compile-dbt` |
| dependency graph + cycle detection | **COMPLETE** | `graph.py` (networkx); cycle fails at COMPILE |
| topological turn engine | **COMPLETE** | turns computed, never authored (ADR-037/038) |
| Airflow coordinator | **CODE COMPLETE, NEVER RUN** | `enable_airflow=false`, OPEN-28 / P0-1 |
| dynamic task mapping | **CODE COMPLETE** | `reporting_common.py:148-152`, `.expand()` per turn |
| dbtRunner wrapper | **COMPLETE, PROVEN** | real `dbt build` on EMR, `PASS=3 WARN=0 ERROR=0` |
| EMR Serverless launcher | **COMPLETE** | `submitter.py` |
| execution runtime state / history | **COMPLETE** | 41 executions; illegal transitions raise |

**Most of §§6-16 already exists.** The genuine work is narrower than the prompt implies:
the non-JOB dependency gates, and running the coordinator for real.

## Already satisfied by existing tests — do not rebuild

- turn != attempt_number: `test_phase15_recovery.py::test_zx_retry_increments_attempt_and_keeps_turn`
- topological wave: `::test_zy_turns_remain_a_topological_wave`
- cycle fails at compile: `::test_18_bad_dependency_cycle_configuration`
- failed branch does not block unrelated: `::test_02_dependency_fails`
- four fixed DAGs, none naming a job_id: `test_phase16_datamart_template.py`
- new mart adds zero DAG files: same file, asserted by modification-time evidence
- new-mart developer experience (§42): **already proven** — `mart_channel_engagement_daily`
  built through the normal workflow, three files, no DAG (`docs/NEW_DATAMART.md`)

## Not yet covered (real work)

1. **Non-JOB dependency gates** — the finding above. Highest value.
2. **Scale tests** (§41): 100/500/1000 synthetic jobs. None exist.
3. **DAG parse-cost test** (§14/§31): no test asserts DAG import does no AWS/business-data I/O.
4. **Spark execution topology ADR** (§21/§45): job-per-model vs per-turn bundling — never evaluated.

## Blockers still open from Phase 18

- **P0-1** Airflow not deployed. Blocks any runtime proof of the coordinator.
- **P1-4** STREAMING_RT: six defects fixed, stream reached `offsets/0`, stopped on missing
  `ops.streaming_batch_ledger`. One CREATE TABLE from done.

## Terraform drift — READ BEFORE ANY APPLY

`bash scripts/tf.sh plan` currently wants to:
- **replace cdc-runtime** (user-data edit + `user_data_replace_on_change`)
- **revert both rotated CDC passwords** (Terraform owns them via `random_password`)

An unguarded `apply` will undo the P1-1 security fix and break the connectors.
Resolve ownership of those parameters first.

## Cost

~$5.50 of $30 left; MSK + 3 EC2 burning ~$1.11/hr. Stop them before a long session.
