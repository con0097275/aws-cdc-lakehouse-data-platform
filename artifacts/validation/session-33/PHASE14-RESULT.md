# Phase 14 — end-to-end reporting validation

Run 2026-08-20. Everything below is from live infrastructure; nothing is simulated except
the one substitution named in §3.

## 1. Pipeline, and what is proven at each hop

| Hop | Evidence | Verdict |
|---|---|---|
| Oracle CDC → Kafka | `cdc.oracle.COREBANK.*` = 2000/320/200/4 vs seed 2000/320/200/4 | **PASS** |
| SQL Server CDC → Kafka | `cdc.sqlserver.digital.dbo.*` = 3000/150/50/4 vs seed, exact | **PASS** |
| Kafka → L1 | EMR job `00g84psb3ndrs827` SUCCESS; `kafka_dev_lab_dev_stream.cdc_events` 8,932 rows | **PASS** |
| L1 → L2 FULL_CDC | EMR job `00g84pvcfbbe1g27` SUCCESS; 8,932 events, idempotent on `event_id` | **PASS** |
| L2 → EOD curated | `curated.fact_account_daily_snapshot` = 320 rows / 320 distinct accounts | **PASS** |
| coordinator | 19 execution records in `kafka-dev-lab-dev-job-execution` | **PASS** |
| dbt-Spark | **SUBSTITUTED** — see §3 | **NOT VALIDATED** |
| Iceberg datamart | `mart.mart_account_balance_daily`, snapshot id changes per scenario | **PASS** |

A double snapshot (the SQL Server connector re-snapshotted on restart) put 6000 rows in L1
for 3000 business keys. EOD collapsed it to the true state — the dedup contract proving
itself on real duplicate data rather than on a fixture.

## 2. The eight scenarios — clean run `00g84qenvh31ig27`

| # | Scenario | execution_id | rows | mart sum | snapshot before → after | status |
|---|---|---|---|---|---|---|
| 1 | normal EOD | `p14-s1-...:EOD:...:1` | 0 → **320** | 2031884176.61 | 6573451454893231677 → 1900945029509105984 | SUCCEEDED |
| 2 | late CDC arrives | — | +999.99 on 1 key | — | — | SOURCE_MUTATED |
| 3 | AUTO_CORRECT | `p14-s3-...:AUTO_CORRECT:...:1` | 320 → 320 | **unchanged** | 1900945029509105984 → 4198656374970512297 | SUCCEEDED |
| 4 | missing historical date | — | 0 rows at 2026-08-17 | — | — | **GAP_CONFIRMED** |
| 5 | FULFILL | `p14-s5-...:FULFILL:2026-08-17:1` | 0 → **320** | 2031885176.60 | 4198656374970512297 → 6929065822369863133 | SUCCEEDED |
| 6 | new realtime change | — | +5.55 on 1 key | — | — | SOURCE_MUTATED |
| 7 | STREAM_BATCH | `p14-s7-...:STREAM_BATCH:...:1` | 320 → 320 | **unchanged** | 6929065822369863133 → 6264587913184777965 | SUCCEEDED |
| 8 | STREAMING_RT | deployment `rt_dep-20260820T111931Z-phase14` | — | — | checkpoint EMPTY | STOPPED |

Watermarks, read back from DynamoDB, each naming the execution that set it:

```
mart_account_balance_daily#EOD           2026-08-20  <- p14-s1-...
mart_account_balance_daily#AUTO_CORRECT  2026-08-20  <- p14-s3-...
mart_account_balance_daily#FULFILL       2026-08-17  <- p14-s5-...
mart_account_balance_daily#STREAM_BATCH  2026-08-20  <- p14-s7-...
```

### The result that matters most

Scenarios 3 and 7 left the mart sum **unchanged**, and that is the framework working, not
failing. The row was CERTIFIED; AUTO_CORRECT writes PROVISIONAL_CORRECTED and STREAM_BATCH
writes PROVISIONAL_NRT, and ADR-042's ladder forbids a lower tier from overwriting a higher
one. The guarded MERGE refused both downgrades against live Iceberg data. A certified date
is escalated to an EOD rerun, never silently corrected.

The corollary is that these two scenarios prove the REFUSAL, not the correction. The path
where a correction actually lands is **not** demonstrated here.

## 3. What is NOT validated — and must not be read as passing

1. **`dbt build` never ran.** dbt-core/dbt-spark are absent from the EMR image and the
   private subnets cannot reach PyPI. The transformation ran as the equivalent Spark SQL
   MERGE with the same MERGE_LADDER semantics. Every field the prompt asks for is real
   except `dbt model/build result`, which is recorded as `SUBSTITUTED_SPARK_SQL_MERGE`.
2. **STREAM_BATCH's frozen-window watermark was not exercised.** Scenario 7 used the generic
   coordinator path, so `watermark_ts` is null and the `[wm - overlap, frozen_upper)` bound
   that Phase 10 is built around was never computed against live data.
3. **A correction landing** — see §2.
4. **Airflow orchestrates none of this.** `enable_airflow=false`; the coordinator was driven
   from a Spark job, so the DAGs, dynamic task mapping and pools are unproven.
5. **STREAMING_RT processed no events.** Scenario 8 is lifecycle only: deployment registered,
   source resolved to FULL_CDC_APPEND, checkpoint reported EMPTY, stopped. The flag is off.

## 4. Defects found and fixed reaching this point

Seventeen, all genuine repository bugs, recorded in `artifacts/validation/session-32/`.
The four that mattered most:

- `create-topics.sh` emits the wrong topic names for **both** sources (missing `dbo`,
  wrong case for Oracle) — eight topics sat at offset 0 while both connectors reported
  RUNNING and healthy.
- The Apicurio **converter** artifact was never staged; `versions.env` stages the *serde*,
  which contains no converter class at all.
- The `connect` role had `msk-produce` but not `msk-consume`; a distributed worker must read
  back its own config/offset/status topics.
- Two Oracle JDBC drivers in one plugin directory made the correct password arrive as NULL
  (`ORA-01005`), which looked like a credential fault for hours.
