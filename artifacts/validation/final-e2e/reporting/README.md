# artifacts/validation/final-e2e/reporting

Live reporting / dbt-Spark / datamart E2E, 2026-09-03 10:33-11:01 UTC.
Document: `docs/validation/REPORTING_DATAMART_E2E.md`.

| File | What |
|---|---|
| `10-dbt-parse.txt` | `dbt parse` -> manifest |
| `11-reporting-compile.txt` | config compile, topological turns |
| `12-reporting-verify.txt` | plan hash recomputed and compared |
| `20-eod-evidence.json` | EOD run evidence (first live dbt build) |
| `21-eod-rerun-evidence.json` | EOD rerun, convergence |
| `22-streambatch-evidence.json` | STREAM_BATCH, frozen upper bound |

## Headline

**`dbt build` ran live for the first time in this project.** dbt 1.9.4 installed
hermetically from the staged wheelhouse with `pip --no-index` inside a no-NAT VPC:

```
1 of 3 OK created sql incremental model kafka_dev_lab_dev_mart.mart_account_balance_daily [OK in 17.01s]
2 of 3 PASS dbt_utils_unique_combination_mart_account_balance_daily_account_sk__business_date
3 of 3 PASS not_null_mart_account_balance_daily_account_sk
Completed successfully.  Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3
```

That closes G-P1-7, the single largest open item in the platform review.
STREAM_BATCH also produced a non-null `watermark_ts`, closing reporting gap G2.

## Five defects found by running it

1. **`dbt/profiles.yml` is gitignored and nothing creates it**, so `make dbt-parse` fails on
   a fresh clone with `Could not find profile named 'aws_cdc_lakehouse'`.
2. **`make dbt-parse` and `make check` are mutually exclusive.** The Makefile defaults
   `DBT_PROFILES_DIR=dbt`, requiring `dbt/profiles.yml`; `test_dbt_contract.py::
   test_no_profiles_yml_is_committed` asserts that exact file must NOT exist. Resolved by
   putting the profile at `~/.dbt/profiles.yml` and exporting `DBT_PROFILES_DIR`.
   `reporting-compile-dbt` re-invokes `dbt parse` without the variable -- same defect, 2nd site.
3. **`reporting-live-run.py` preflighted 9 of the 14 environment variables it dereferences**,
   so a missing one surfaced as a raw `KeyError` traceback AFTER the coordinator had already
   planned the run and written an execution record. FIXED in this session.
4. **The reporting job role cannot emit logs at all.** With no `REPORTING_EMR_LOG_URI` the
   submitter falls back to CloudWatch Logs, which is unreachable from the private subnets
   (no NAT per CLAUDE.md 4.2, and no `logs` interface endpoint) -- `Connect timeout on
   endpoint URL: https://logs.ap-southeast-1.amazonaws.com/`. Setting the S3 URI then failed
   with `AccessDenied ... s3:PutObject` for `kafka-dev-lab-dev-reporting`. EMR treats a
   log-push failure as job failure, so NO reporting job can run under its own role.
   Worked around by using `kafka-dev-lab-dev-spark-eod`, which has the grant.
5. **Two framework zips with different layouts and nothing says which to use.**
   `reporting-framework.zip` nests modules under `reporting/`; `framework-flat.zip` has them
   at the root. `emr_dbt_bootstrap.py` imports flat, so the obvious-sounding name fails with
   `ModuleNotFoundError: No module named 'run_dbt_job'`.

## Working configuration (recorded so it is not rediscovered)

```
REPORTING_JOB_ROLE_ARN      = .../kafka-dev-lab-dev-spark-eod     # NOT ...-reporting
REPORTING_EMR_LOG_URI       = s3://<lake>/logs/emr/               # required, not optional
REPORTING_FRAMEWORK_ZIP     = s3://<lake>/artifacts/dbt/framework-flat.zip
REPORTING_DBT_BOOTSTRAP_ARGS= JSON ARRAY, not a shell string
DBT_PROFILES_DIR            = $HOME/.dbt
```

## Added 2026-09-03 16:27–16:32 UTC — the last two flow modes, run live

| Mode | Result |
|---|---|
| **AUTO_CORRECT** | **SUCCEEDED** — `p14b-auto_correct-1788452815`, Spark `00g8g8f679bidg27`, detail `4 date(s), 0 key(s), path=FULL_WINDOW_FALLBACK`, watermark advanced |
| **FULFILL** | **SUCCEEDED** — `p14b-fulfill-1788452968` for gap date `2026-09-01`, watermark advanced to `2026-09-01`. `spark_app_id: null` — resolved in the coordinator without needing a Spark job |

**All five reporting flow modes have now run live**: EOD, STREAM_BATCH, AUTO_CORRECT,
FULFILL, STREAMING_RT. This closes the "4 of 5 modes never executed" gap that Phase 14 left
open (its driver bypassed the flow modules entirely with an inline MERGE).

## Proactive insight (Business AI §9) — pipeline proven, persistence not

```
KPI             total_closing_balance   required_certification RECONCILED
insight_id      insight:c4d1a2365d362676d6d2      (deterministic)
metric_version  metric:0d1b5e7ea203f9de
summary         "Total closing balance was 2,031,880,938 VND. By account_sk:
                 200320 contributed 0.6% of the observed change; ..."
athena          6 queries, 5,363 bytes
RERUN           same insight_id -> True          (idempotent, §9's requirement)
```

Note the language: **"contributed 0.6% of the observed change"** — contribution, not
causation, which is the stated contract for driver narration.

**Still blocked:** persisting to `serving.ai_business_insight_daily`. There is no `serving`
Glue database (the deployment has 7: stream, full_cdc, curated, mart, ops, snapshot,
quarantine) and creating one out-of-band would put a serving object outside Terraform state.
The analytics chain is proven; the publish step needs the database added to the module.
