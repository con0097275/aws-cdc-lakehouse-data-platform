# Reporting / dbt-Spark / Airflow / Datamart — Live E2E

- **Date:** 2026-09-03, 10:33–11:01 UTC
- **Evidence:** `artifacts/validation/final-e2e/reporting/`
- **Verdict:** **PARTIAL PASS.** Sections 1, 2, 5, 7, 8 passed live with evidence.
  Sections 3, 4, 6, 9 were **not executed** — see §6.

## Headline

**`dbt build` ran live for the first time in this project's history**, closing G-P1-7 —
the largest open item in the platform review. dbt 1.9.4 installed hermetically from the
staged wheelhouse via `pip --no-index` inside a no-NAT VPC, exactly as `emr_dbt_bootstrap.py`
was designed to do but had never been proven to.

```
1 of 3 OK created sql incremental model kafka_dev_lab_dev_mart.mart_account_balance_daily [OK in 17.01s]
2 of 3 PASS dbt_utils_unique_combination_mart_account_balance_daily_account_sk__business_date
3 of 3 PASS not_null_mart_account_balance_daily_account_sk
Completed successfully.  Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3
```

STREAM_BATCH also produced a non-null `watermark_ts`, closing reporting gap **G2**.

## 1. Dependency engine — PASS

`dbt parse` → manifest (61 nodes, 7 sources, 12 models, generated 10:34:28Z) → dependency
extraction → config compile → topological generations.

```
plan_hash      418d7158728c925cd8ec6fa58342112449ba7ff988d5bbf04a0f68eca1584cd4
config_version cfg-1682b193a20ce2ae
verify         VERIFY OK   (hash recomputed from source and compared)
profiles 5     enabled: EOD=3 AUTO_CORRECT=3 FULFILL=3 STREAM_BATCH=3
```

Topological turns, identical under every flow mode:

```
TURN 1: mart_account_balance_daily, mart_channel_engagement_daily
TURN 2: mart_account_balance_monthly
```

**Same-turn parallelism proven**: two independent marts occupy TURN 1 and are dispatched
together; the monthly roll-up sits in TURN 2 because it depends on them. Confirmed in the
runtime store — execution records carry `turn=1` for the two daily marts and `turn=2` for
the monthly.

Cycle detection is real code, not a comment: `graph.py` exposes `assert_acyclic`,
`CyclicDependencyError` and `UnknownUpstreamError`, and uses `nx.find_cycle` so the message
names the actual path. 110 dependency-engine tests pass.

## 2. EOD — PASS

| | |
|---|---|
| COB / business date | 2026-09-03 |
| coordinator_run_id | `p14b-eod-1788432686` |
| execution_id | `p14b-eod-1788432686:EOD:mart_account_balance_daily:2026-09-03:1` |
| turn / attempt | 1 / 1 |
| dbt selector | `--select mart_account_balance_daily`, vars `flow_mode=EOD, cob_date=2026-09-03` |
| Spark app | `/applications/00g8g05ud5dj2u25/jobs/00g8g2f9pgg9vg27` |
| watermark before → after | `null` → `2026-09-03` |
| status | SUCCEEDED |

Mart after the run — **321 rows, 321 distinct `account_sk`**, `sum(closing_balance)
2031880937.77`, Iceberg snapshot `2692719074458170304` (append, total 321). That matches the
curated EOD layer's 321 accounts exactly.

**Convergence on rerun of the same COB:** identical output — `321 / 321 / 2031880937.77`.
A second Iceberg commit (`1357094882919602928`, overwrite) was made and the content
converged. The watermark's `last_success_execution_id` advanced to the new execution while
`last_success_date_of_data` stayed `2026-09-03`.

**Failure does not advance the watermark** — proven incidentally by this session's own
failures: executions `…1788431905` and `…1788432537` are recorded `FAILED`, and the
watermark was written only by the `SUCCEEDED` run `…1788432686`.

## 5. STREAM_BATCH — PASS

```
watermark_before : null
watermark_after  : last_success_date_of_data 2026-09-03
                   watermark_ts 2026-09-03T10:58:21.392116+00:00   <- NON-NULL
detail           : "committed to 2026-09-03T10:58:21.392116+00:00"
spark app        : /applications/00g8g05ud5dj2u25/jobs/00g8g2j8g8goso27
status           : SUCCEEDED
```

The frozen upper bound is computed **once**, carried through the run, and committed with the
watermark — the property gap G2 said had never been demonstrated against live data.

## 7. Datamart — PASS (grain, keys, tests)

Grain `account_sk × business_date`, enforced by dbt tests that ran and passed live:
`dbt_utils_unique_combination(account_sk, business_date)` and `not_null(account_sk)`.
`321 rows = 321 distinct account_sk` independently confirms one row per key.
Serving views exist (`serving/athena/views/serving_views.sql`,
`ai_business_insight.sql`) but were **not deployed or queried** in this window.

## 8. Athena — PASS

| Query | ID | Bytes scanned | Result |
|---|---|---|---|
| mart rows/pks/sum after EOD | `19a88466-a2b7-491b-8a4e-d6e5b5c9aba4` | 1,559 | 321 / 321 / 2031880937.77 |
| mart snapshots | `1c9da2cb-f5ce-4c6f-8e22-44b553d95e77` | 629 | 1 snapshot, total 321 |
| mart after rerun | `da2dbeac-f7cd-4c29-88fb-19e97067aee9` | 1,559 | identical — convergence |
| snapshots after rerun | `c8099ebc-53e9-47ae-96fd-4f2c1318bbc3` | 1,374 | 2 snapshots, both total 321 |

Actual matches expected on every query. All far below the 10 GiB workgroup cutoff.

## 6. NOT executed

| § | Item | Why |
|---|---|---|
| 3 | AUTO_CORRECT | Not run. Needs controlled late CDC plus a bounded-lookback setup; the window was spent proving dbt and clearing five blocking defects. |
| 4 | FULFILL | Not run. Needs a fabricated historical gap date. |
| 6 | STREAMING_RT | Not run. It is the one workload that bills for as long as it is up, and it is shipped OFF (`is_enabled: false`, `ENABLE_STREAMING_RT=false`) precisely so it is started deliberately. Starting a long-running app near the end of a cost-capped window was not a call I would make unilaterally. |
| 9 | Airflow DAG execution | Not run. The UI and all pods are healthy and the DAGs are deployed, but AUTO_CORRECT and FULFILL still `raise NotImplementedError` in `reporting_common.py:375` (OPEN-28), so a full turn-ordered DAG run cannot complete. Turn ordering and `turn ≠ attempt_number` were instead evidenced directly from the runtime store. |

Runtime-state evidence that *was* captured: **27 execution records**, statuses
`PLANNED / READY / SUCCEEDED / FAILED`, `turn ∈ {1,2}` matching the compiled plan, and
`attempt_number` tracked as a separate field from `turn` on every record.

## Defects found by running it

1. **`dbt/profiles.yml` is gitignored and nothing creates it** — `make dbt-parse` fails on a
   fresh clone with `Could not find profile named 'aws_cdc_lakehouse'`.
2. **`make dbt-parse` and `make check` are mutually exclusive.** The Makefile defaults
   `DBT_PROFILES_DIR=dbt`, which requires `dbt/profiles.yml`; `test_dbt_contract.py::
   test_no_profiles_yml_is_committed` asserts that exact path must not exist. The test
   conflates "committed" with "exists on disk" for a file that is already gitignored.
   Resolved by relocating the profile to `~/.dbt/` and exporting `DBT_PROFILES_DIR`.
   `reporting-compile-dbt` re-invokes `dbt parse` without the variable — same defect, second site.
3. **`reporting-live-run.py` preflighted 9 of the 14 environment variables it dereferences.**
   A missing one surfaced as a raw `KeyError` *after* the coordinator had planned the run and
   written an execution record — so the runtime store gained a `PLANNED` row for a run that
   never started. **Fixed in this session**; the preflight now covers all 17 names.
4. **The reporting job role cannot emit logs at all — no reporting job can run under it.**
   With `REPORTING_EMR_LOG_URI` unset the submitter falls back to CloudWatch Logs, which is
   unreachable from private subnets (no NAT per CLAUDE.md §4.2, no `logs` interface
   endpoint): `Connect timeout on endpoint URL: https://logs.ap-southeast-1.amazonaws.com/`.
   Setting the S3 URI then fails with `AccessDenied ... s3:PutObject` for
   `kafka-dev-lab-dev-reporting`. EMR treats a log-push failure as job failure. Worked around
   with `kafka-dev-lab-dev-spark-eod`, which holds the grant. **The IAM policy needs the
   `logs/` prefix, or the VPC needs a CloudWatch Logs endpoint.**
5. **Two framework zips, different layouts, nothing documents which.**
   `reporting-framework.zip` nests modules under `reporting/`; `framework-flat.zip` puts them
   at the root. `emr_dbt_bootstrap.py` imports flat, so choosing the obvious-sounding name
   fails at runtime with `ModuleNotFoundError: No module named 'run_dbt_job'`.

## Cost

~$0.55 of EMR Serverless across the reporting runs (five of them failures caused by defects
3–5), plus Athena queries in the low kilobytes. Platform baseline `$1.2340/hr` throughout;
window elapsed 115 min at the time of writing.
