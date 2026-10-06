# Phase 14 — end-to-end reporting validation

Run 2026-08-22 on live AWS, account 111122223333, ap-southeast-1.
Architecture as corrected this session: **Kafka → FULL_CDC → {REALTIME, EOD} → coordinator
→ dbt-Spark → Iceberg datamart** (`docs/TARGET_ARCHITECTURE.md` §3).

Rule applied throughout: no PASS without concrete data evidence. Where something is
substituted or unproven it is labelled, not glossed.

## 1. Pipeline — what is proven at each hop

| Hop | Evidence | Verdict |
|---|---|---|
| Oracle CDC → Kafka | `cdc.oracle.COREBANK.*` = 4/200/320/2000 vs seed 4/200/320/2000, exact | **PASS** |
| SQL Server CDC → Kafka | `cdc.sqlserver.digital.dbo.*` = 4/50/150/3000 vs seed, exact | **PASS** |
| Schema contract | Apicurio 0 → **24** artifacts after snapshot | **PASS** |
| Kafka → FULL_CDC | job `00g866mc1ud9h027`, 5,728 ingested, table 20,389 | **PASS** |
| FULL_CDC → EOD | job `00g866oafdkjt027`, 320 rows @ 2026-08-22, tag `EOD_2026-08-22` → snapshot `1917892974346479381` | **PASS** |
| coordinator | 41 execution records in `kafka-dev-lab-dev-job-execution` | **PASS** |
| **dbt-Spark** | **REAL** `dbt=1.9.4` on EMR: `PASS=3 WARN=0 ERROR=0 SKIP=0` | **PASS** |
| Iceberg datamart | `mart.mart_account_balance_daily`, snapshot changes per scenario | **PASS** |

`dbt build` is the headline change from Session 33, which recorded it as
`SUBSTITUTED_SPARK_SQL_MERGE`. It now runs for real from a pinned cp39/aarch64 wheelhouse
staged in S3 and installed offline (`spark/reporting/emr_dbt_bootstrap.py`).

## 2. The eight scenarios

| # | Scenario | flow_mode | execution_id | Spark app | watermark before → after | result | status |
|---|---|---|---|---|---|---|---|
| 1 | normal EOD | EOD | `p14s34-s1-1787378268:EOD:…:2026-08-22:1` | `00g8687gr6bfvg27` | 2026-08-21 → **2026-08-22** | 320 rows CERTIFIED | **SUCCEEDED** |
| 2 | late CDC arrives | — (source) → EOD | `p14s34-s2eod-1787308481:EOD:…:2026-08-21:1` | `00g85j…` | 2026-08-21 held | mart 2031880160.00 → **2031881159.99** | **PASS** |
| 3 | AUTO_CORRECT | AUTO_CORRECT | `p14s34-s3-1787379269:AUTO_CORRECT:…:2026-08-22:1` | *none — refused before submit* | 2026-08-22 | `3 date(s) ESCALATED (already certified)` | **SUCCEEDED (refused)** |
| 4 | missing historical date | — | — | — | — | 2026-08-18: 0 mart, 0 curated, **0 FULL_CDC events** | **GAP_CONFIRMED** |
| 5 | FULFILL | FULFILL | `p14s34-s5-1787378975:FULFILL:…:2026-08-17:1` | per-date | FULFILL → 2026-08-17 | 0 → **320 rows, RECONCILED** | **SUCCEEDED** |
| 6 | new realtime change | — (source) | — | `00g868ahm7p9h827` | — | `app_user` 150 → 151; FULL_CDC `op='u'` LSN `0000002b:00003e30:0003` | **PASS** |
| 7 | STREAM_BATCH | STREAM_BATCH | `p14s34-s7-1787378806:STREAM_BATCH:…:2026-08-22:1` | `00g868ck92kga027` | 2026-08-20 → **2026-08-22**, `watermark_ts = 2026-08-22T12:00:00Z` | `committed to 2026-08-22T12:00:00+00:00` | **SUCCEEDED** |
| 8 | STREAMING_RT | STREAMING_RT | deployment `rt_dep-20260822T061412Z-p14s34` | *not submitted (`is_enabled=false`)* | n/a | checkpoint **EMPTY, read from S3** | **STOPPED** |

Every run carried `turn=1`, `attempt_number=1`, `dependencies=[]`,
`config_version=cfg-20237926f3c07ed2`, `job_id=mart_account_balance_daily`.
STREAMING_RT has no turn/attempt: it is a lifecycle, not a coordinated batch (ADR-041).

## 3. Final state, read back from live infrastructure

```
MART (kafka_dev_lab_dev_mart.mart_account_balance_daily)
  2026-08-17  RECONCILED  320   2,031,885,176.60
  2026-08-20  CERTIFIED   320   2,031,884,176.61
  2026-08-21  CERTIFIED   320   2,031,881,159.99   <- +999.99 late CDC from S2
  2026-08-22  CERTIFIED   320   2,031,880,160.00
  snapshot 2618812421615230733 @ 2026-08-22 06:11:34 UTC

WATERMARKS (kafka-dev-lab-dev-job-watermark-state)
  #EOD           2026-08-22   ts None                       <- p14s34-s1-…
  #AUTO_CORRECT  2026-08-22   ts None                       <- p14s34-s3-…
  #FULFILL       2026-08-17   ts None                       <- p14s34-s5-…
  #STREAM_BATCH  2026-08-22   ts 2026-08-22T12:00:00+00:00  <- p14s34-s7-…

EXECUTIONS  41 total: 27 SUCCEEDED, 9 FAILED, 4 PLANNED, 1 SKIPPED
```

## 4. The results that matter most

**A correction actually landed.** S2 changed one Oracle account by +999.99. That single
`op='u'` event (SCN 3185686) traversed LogMiner → Debezium → Kafka → FULL_CDC → EOD →
curated → mart, and the mart total moved 2,031,880,160.00 → 2,031,881,159.99. Session 33
could only demonstrate the REFUSAL path; the landing path is now proven with data.

**The ladder refuses and promotes.** S3 aimed a correction at a CERTIFIED date and was
refused *before submitting a job* (`spark_app_id: null`) — the flow does not spend money on
work the model would reject. S5 promoted 2026-08-17 from `PROVISIONAL_CORRECTED` to
`RECONCILED`, so the ladder is not merely a floor.

**The frozen window was computed.** `watermark_ts` had been null in every prior run. S7
committed `2026-08-22T12:00:00+00:00`, which is the frozen upper bound of
`[wm − overlap, frozen_upper)`.

**The concurrency guard fired for real.** S7's first attempt returned SKIPPED because a
prior run had died leaving a non-terminal record — exactly the case its message describes
("a worker that died leaving the record non-terminal"). Four such records were finalised
FAILED, not SUCCEEDED: a run whose outcome was never confirmed must not advance a
watermark.

## 5. What is NOT proven — do not read these as passing

1. **Airflow orchestrates none of it.** `enable_airflow=false` (OPEN-28). The DAGs, dynamic
   task mapping and pools are unexercised; `scripts/reporting-live-run.py` replaces the
   SCHEDULER, not the flows.
2. **STREAMING_RT processed zero events.** `is_enabled=false`, so no application was
   submitted and the checkpoint is legitimately EMPTY. What is proven is the lifecycle and
   that the reading came from S3 — not that the stream works.
3. **The REALTIME layer is not materialised.** `REALTIME` binds to
   `kafka_dev_lab_dev_stream`, but nothing yet builds it from FULL_CDC; STREAM_BATCH ran
   against a bounded read and the execution records `source_layer_substituted`.
   See `docs/L2_REALTIME_STREAM.md` §7.
4. **S4's gap cannot be filled.** 2026-08-18 has no FULL_CDC events at all, so no correct
   FULFILL could produce rows for it. S5 used 2026-08-17, where the source genuinely has
   data, with the mart side deleted to create the gap.
5. **Session 33 rows still sit in FULL_CDC.** 8,932 legacy rows carry NULL
   `dv_event_id`/`dv_src_event_id`; they predate those columns and are not in Kafka to
   re-derive. EOD is scoped per business date, so they do not contaminate any result above.
