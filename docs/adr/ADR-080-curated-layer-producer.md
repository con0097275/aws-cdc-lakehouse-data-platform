# ADR-080 — The CURATED layer gets a producer, and one spelling

* **Status:** Accepted
* **Date:** 2026-09-20
* **Supersedes in practice:** the namespace assumptions in `spark/dimensions/build_kimball.py`
  and `spark/common/run_flow.py` (Sessions 09/10)
* **Related:** ADR-033 (layer naming), ADR-034 (Git is the source of truth),
  ADR-044 (dbt execution), ADR-070 (derived-artifact drift)

## Context

`dbt build` could not run. Its models read `source('curated', ...)` —
`fact_transaction`, `fact_account_daily_snapshot`, `fact_digital_engagement_daily` — and
`reporting/layers.yaml` resolves CURATED to `kafka_dev_lab_dev_curated`, a Glue database
holding **zero tables**.

Nothing in the repository wrote that layer. A search for `writeTo`, `saveAsTable` or
`insertInto` against CURATED across `spark/`, `scripts/` and `airflow/dags/` returned
nothing at all.

The code that *would* have produced those facts exists and is good: `dim_builder`, `scd2`,
`periodic_snapshot`, `digital_engagement`, `transform`. What it does not have is a source it
can read. `build_kimball.py` reads `{catalog}.snapshot.banking_account` and writes
`{catalog}.mart.*`. The config-driven platform produces neither:

| what the Kimball leg expects | what the platform actually has |
|---|---|
| `snapshot.banking_account` | `kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account` |
| business columns as columns | business columns inside `payload_after`, a JSON **string** |
| `snapshot_date` | `business_date` |
| writes to `{catalog}.mart.*` | dbt owns MART; CURATED is dbt's input |

Three files disagreed about where one dataset lives:

1. `reporting/layers.yaml` — `CURATED: kafka_dev_lab_dev_curated`
2. `spark/dimensions/build_kimball.py` — writes `{catalog}.mart.fact_account_daily_snapshot`
3. `reporting/jobs/mart_account_balance_daily.yaml` — depends on
   `kafka_dev_lab_dev_snapshot.fact_account_daily_snapshot`

All three return rows for *something*, which is why the disagreement was silent.

## Decision

**1. `reporting/layers.yaml` wins.** CURATED is `kafka_dev_lab_dev_curated` and holds the
conformed entities, the Kimball dimensions AND the Kimball facts. dbt reads it and writes
MART. ADR-033 already says this file is the only place a physical database name appears;
the other two spellings are corrected to match rather than accommodated.

**2. A new job, `spark/jobs/curated/curated_build.py`, produces the layer** — and calls the
Session 09/10 modules **unchanged**. The missing piece was never the Kimball logic. It was
the *conformance* step that turns `payload_after` into the business columns that logic
already expects. Rewriting the dimension builder would have thrown away tested code to
solve a problem it does not have.

**3. The projection is configuration, not code**
(`reporting/curated/entities.yaml`, validated by `cdc/curated.py`). Per column, because
the encoding is a property of the *source column type* and cannot be inferred from the
value: Debezium sends an Oracle `DATE` as epoch **millis** and a `TIMESTAMP(6)` as epoch
**micros**, as bare integers, in the same JSON object —
`{"DOB":23673600000,"UPDATED_AT":1767225600000000}`. Decoding one with the other's factor
does not fail; it reports every customer as born in 1970.

**4. The conformed source is EOD, not FULL_CDC.** `scd2.build_versions` wants one row per
source change, which FULL_CDC has and EOD does not. EOD is still correct, for the reason
`build_kimball._l3` already records: a dimension built from a layer that has not been
deduplicated per PK and had deletes applied lets two runs disagree about which version of a
member is real. The cost — dimension versions dated to the business date rather than the
commit instant — is the granularity a certified daily mart reports at anyway.

**5. An empty source still builds its entity.** Its dimension then holds only the unknown
member and every fact joining it resolves to `UNKNOWN_SK`. Skipping it would leave the
dimension *missing*, which surfaces as an `AnalysisException` at the end of a long job
rather than a visible zero. This is not hypothetical: the five SQL Server topics exist and
their connector is `RUNNING`, but FULL_CDC holds 0 rows for all of them, so the whole
digital domain builds as unknown-only today.

**6. `dim_currency` is declared reference data.** No source system publishes an FX rate
here. The rates live in `entities.yaml`; a currency with no declared rate is **absent**,
which makes `transform.build_fact_transaction` leave `amount_base` NULL and count an
unresolved dimension. The base rate of 1.0 for VND is an observation, not an assumption —
Athena confirms 320/320 accounts and 2050/2050 transactions are VND.

**7. `dim_product` is deliberately not built.** Neither source system has a product table;
`product_code` is a degenerate attribute on `account` and `loan`. Building it would mean
inventing `product_name` and `product_group`, and no fact joins `product_sk`. A test
asserts it is the *only* dimension in `dim_builder.DIMENSIONS` without a configured source,
so a future dimension that loses its source cannot slip through unnoticed.

**8. `fact_transaction.business_date` is the transaction's own date, not the COB.** The two
differ here: the EOD snapshot for one COB carries every transaction current as of that COB,
including 2000 seeded on 2026-01-01 that only reached FULL_CDC on 2026-09-20. Stamping them
all with the COB would move two thousand January facts into September, and no test on that
table could see it.

## Options

**A. Migrate `build_kimball.py` onto the config-driven names.** Rejected: it would edit a
tested entrypoint to do a job it was not shaped for, and its namespaces are only half the
gap — the payload still has to be flattened somewhere.

**B. Register the 2026-09-10 Iceberg metadata already sitting under `warehouse/curated/`.**
Rejected, and this was the tempting one: it makes `dbt build` go green in one command. That
curated fact predates the stack rebuild, while the EOD snapshot it summarises was closed
2026-09-20. A mart across that boundary is complete, plausible and wrong — the failure
`spark/facts/periodic_snapshot.py` was written to prevent.

**C. Have dbt read the EOD layer directly.** Rejected: dbt would then own conformance,
surrogate keys and SCD2, which is PySpark's half of the split (docs/DBT_SPARK.md §2), and
the Session 09/10 modules would be dead code.

**D (chosen). A conformance job that feeds the existing Kimball modules.** Smallest new
surface, no tested code discarded, and the projection becomes reviewable configuration.

## Consequences

* `dbt build` has sources, so MART becomes buildable for the first time.
* One more Spark job to schedule, between the EOD close and the dbt run. It is a cadence
  DAG task, not a per-table one (ADR-071).
* The Session 09/10 modules gain a second caller. `build_kimball.py` still carries the old
  namespaces and is now the *unwired* path; it is left in place rather than deleted because
  its tests are the ones that cover the modules this job depends on.
* The digital half of the star schema is structurally present and empty until the SQL Server
  topics deliver. That is visible in `CURATED_ENTITY ... rows=0` rather than inferred.

## Cost

One additional EMR Serverless job per COB, between the EOD close and the dbt run. Measured:
the 2026-09-20 build took **106 seconds** on 1 driver + 1 executor (2 vCPU / 4 GB each), the
`small` profile in ADR-075. It reads ten EOD tables and writes twenty CURATED ones, so it is
bounded by the snapshot sizes rather than by FULL_CDC history, and it adds no standing cost:
the application auto-stops as before.

## Security

No new credential, endpoint or IAM principal. The job runs under the existing
`kafka-dev-lab-dev-spark-eod` role and reaches Glue and S3 through the default credential
chain (CLAUDE.md §3.2). One thing it DOES move: `dim_customer` carries `full_name` and
`dob`, so the masked `dim_customer_bi` copy is materialised beside it and BI reads that.
Materialised rather than a view, because an Athena view requires the caller to hold read
access to the underlying table, so a view over a table BI can read masks nothing (D14-1).

## Rollback

`DROP TABLE` the twenty tables in `kafka_dev_lab_dev_curated`, or drop the database. Nothing
upstream reads CURATED, and dbt returns to failing on a missing source — the state before
this ADR. The job writes with `createOrReplace`, so re-running is the forward fix and no
manual cleanup sits between the two.

## Validation

Live on 2026-09-20, job run `00g8u02d5bug2027` (SUCCESS):

* `CURATED_ENTITY` for all 8 entities — 320 accounts, 200 customers, 4 branches,
  2050 transactions; the 4 digital entities 0 rows, reported as `EMPTY`.
* dimensions: `dim_account` 321, `dim_customer` 201, `dim_branch` 5, `dim_date` 1095,
  `dim_time` 1440, `dim_channel`/`dim_merchant`/`dim_currency` 1 each. SCD2 validation
  passed for every one, BEFORE any write.
* facts: `fact_transaction` 2050, `fact_account_daily_snapshot` 320,
  `fact_digital_engagement_daily` 0. Grain and referential integrity both passed.
* `dbt build` against it: **PASS=56 WARN=0 ERROR=0 SKIP=0** (run `00g8u05ko9p5ng27`),
  including `assert_mart_reconciles_to_fact` and `assert_mart_status_is_weakest_input`.
* Athena, independently: `mart_account_balance_daily` 320 rows / 320 distinct
  `(account_sk, business_date)`; `fact_transaction` 2050 rows / 2050 distinct
  `transaction_id` across 2 business dates; **0** unknown `account_sk` and **0** null
  `amount_base`.
* `processing_status` is `PROVISIONAL_NRT`, because COB 2026-09-20 had not closed when it
  ran. That is the weakest-input rule working, not a defect.
