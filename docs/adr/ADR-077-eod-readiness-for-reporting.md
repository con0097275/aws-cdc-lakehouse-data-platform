# ADR-077 — Reporting readiness asks the EOD control plane, in one batched query

* **Status**: Accepted (implementation: Phase E)
* **Date**: 2026-09-20
* **Extends**: ADR-037/039 (dependency graph, gates separate from ordering), ADR-076 (EOD
  control plane)
* **Evidence**: `spark/reporting/eod_readiness.py`, `spark/reporting/coordinator.py`,
  `spark/tests/test_reporting_readiness.py`,
  `docs/REPORTING_ORCHESTRATION_REFERENCE_COMPARISON.md`

## Context

The reporting platform already had the dependency engine, the coordinator, four generic flow
DAGs, dbt manifest sync and scale tests to 1,000 jobs (ADR-034..045). Phase E's §1 asks
whether per-pipeline DAG generation is warranted; the reference implementations show the cost
of that choice and no operational requirement here demands it, so the fixed-flow architecture
stands (see the comparison document).

What was missing is narrow and real: **nothing asked the EOD control plane whether a business
date was closed and certified.**

`DatasetReadinessGate` asks `eod_watermark_exists(layer, table, date)` — one query per
dependency, against a signal that predates Phase D, and blind to certification. ADR-076
deliberately leaves a close that BUILT but failed DQ readable so an operator can inspect it.
That gate cannot tell it from a certified close, so a mart could publish a day the platform
refused to vouch for.

## Decision

### 1. `EodTableReadyGate`, generic

Takes table ids, a `cob_date` and a required certification. Returns `READY`, `WAITING`,
`FAILED` or `NOT_APPLICABLE`, carrying the ready/waiting/failed lists. It knows nothing about
marts, dbt or flows.

`WAITING` and `FAILED` are distinct because they demand opposite operator responses: a close
that FAILED will not become ready by waiting, and a gate that says "wait" for it is telling
someone to wait for nothing.

### 2. One batched query, owned by one module

`EodInfoReader` issues a single `table_id IN (...) AND cob_date IN (...)` and caches per
instance — one reader per coordinator run. Twelve jobs over three shared datasets is **one**
query. An absent close is cached too: "we looked and there is nothing" is an answer, and
re-asking it per job would undo the batching exactly when the platform is busiest.

A test greps the DAG files: none may contain `eod_info` SQL of its own.

### 3. `required_datasets` declares LAYER readiness only

```yaml
required_datasets:
  - layer: EOD
    table_id: oracle.coredb.corebank.account
    certification: CERTIFIED
```

The id is the **canonical registry id**, not a physical table: the reference derives one from
the other with `tbl.replace('sat_', 'sat_snp_')`, which is a derived name maintained by hand.

These are **not** `JobDependency` records. A dependency is an edge in the ordering graph; this
is not one. The EOD close runs in a different flow, on a different schedule, possibly on a
different day. Modelling it as an edge would put a CDC table into the mart graph and make
every turn computation depend on a layer it does not own — ADR-039 already separates
readiness from ordering ("Gates do NOT change `turn`").

### 4. dbt edges stay in the manifest

`manifest_sync` registers `ref()` relationships from `target/manifest.json`. A test asserts
that no `required_datasets` entry names a job, so the same graph is never authored twice.

### 5. Certification is `CERTIFIED` unless a job says otherwise

`ANY` exists for a consumer that would rather show provisional numbers than nothing, and it
must be written down per job. `ANY` still refuses a close that produced nothing: it relaxes
the certification bar, not the "did it finish" bar.

### 6. Nothing heavy at parse time

The gate reads the **precompiled** `job_required_datasets` from the plan and issues its query
at run time. A test asserts no DAG module does boto3, Athena, dbt or Glue work at import.

### 7. Opt-in

The coordinator consults the gate only when constructed with one. Turning an unevaluated
condition into a blocking one changes *when* jobs run, and that is a decision rather than an
upgrade side effect — the same reasoning `_dataset_gate_unmet` already documents.

## Options

* **Extend `DatasetReadinessGate` in place.** Rejected: it is keyed on a physical dataset name
  and a watermark probe, and other flows still use it for REALTIME/FULL_CDC. A second,
  narrower gate is clearer than one that means two things.
* **Model `required_datasets` as `JobDependency`.** Rejected — §3.
* **Repeat dbt refs in YAML.** Rejected — §4.
* **Query per dependency.** Rejected — §2.
* **Default to `ANY`.** Rejected: the default must be the safe one.
* **Generate one DAG per mart.** Rejected — the comparison document, with evidence.

## Consequences

* A job that declares `required_datasets` will WAIT until its EOD dependencies certify, once a
  deployment passes an `eod_ready_gate` to the coordinator.
* `mart_account_balance_daily` now declares account, customer and branch — the §5 example.
* The plan carries `job_required_datasets`; `reporting/schema/job.schema.json` accepts the key.
* Two places reconstructed `LoadedConfig` and silently dropped the new field. Both now carry
  it explicitly; that is a standing hazard of rebuilding a config object to change one part.

## Cost

**$0.** One extra Athena query per coordinator run per distinct dataset set — replacing N
watermark probes, so on a wave of twelve jobs it is a reduction.

## Security

No IAM, network or credential change. The gate reads statuses and dates from an OPS table.
Table ids are validated against a fixed layer list and never interpolated from user input.

## Rollback

Construct the coordinator without `eod_ready_gate` and behaviour is exactly as before.
Removing `required_datasets` from a job's YAML removes its conditions; the key is optional.

## Validation

1. `spark/tests/test_reporting_readiness.py` — **29 passed**: all-ready, one-missing (named),
   wrong COB, lag shift, provisional vs certified, explicit `ANY`, `ANY` still refusing an
   unfinished close, FAILED vs WAITING, unreadable control plane, unknown certification,
   batching (3 datasets = 1 query, 12 jobs = 1 query, absent close cached), query shape, no
   `eod_info` SQL in any DAG, auto-discovery from YAML, no table id in any DAG file, dbt refs
   not repeated, canonical ids, no AWS/dbt at import, and the five coordinator wiring cases.
2. Pre-existing and unchanged: cycle detection, same-turn parallelism, failed-branch
   isolation, `turn` ≠ `attempt`, and the 100/500/1000-job compile scale tests (ADR-037/038).
3. `make check-all` exit 0 — **2,580 passed**.
4. **Not claimed**: no coordinator run has used this gate against live `eod_info`, which has
   no rows yet because no close has run against the live catalog.
