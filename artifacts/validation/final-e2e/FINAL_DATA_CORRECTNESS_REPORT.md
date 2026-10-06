# FINAL DATA CORRECTNESS REPORT — 2026-09-03

## The correlation chain — account 990002, end to end

One deterministic key traced through every layer. Insert → delete → recreate on the same PK.

| Layer | Evidence |
|---|---|
| **Source** Oracle `corebank.account` | PK **990002**; SCN `3101554` (insert), `3101561` (delete), `3101577` (recreate); final `REOPENED / 777.77` |
| **Debezium** | `oracle-corebank-source` RUNNING, task 0 RUNNING; canonical PK as message key |
| **Kafka** `cdc.oracle.COREBANK.ACCOUNT` | partition **1**, offsets **121, 122, 124** — all three events on ONE partition |
| **Tombstone** | offset **123** is absent from FULL_CDC — the null tombstone, counted (`tombstones=2`) and excluded from decode |
| **FULL_CDC** | 3 rows, `op = c / d / c`, `position_primary` 3101552 < 3101559 < 3101575 |
| `dv_event_id` | `de00c69bd0c3`, `1daf5eda6fe1`, `6084449f4dfa` — distinct per delivered record |
| `dv_src_event_id` | `a548f71ea173`, `6ee24864b5f4`, `b8200851ff0e` — distinct per source change |
| **REALTIME** | 72 h window, cutoff `2026-08-31 10:13:06Z`, snapshot `7340210906418940945` |
| **EOD / snapshot** | COB `2026-09-03`; 990002 present at **777.77**; 990001 (deleted) correctly ABSENT; tag `EOD_2026-09-03` → snapshot `5452782707029562418` |
| **Airflow / reporting** | coordinator `p14b-eod-1788432893`, execution `…:EOD:mart_account_balance_daily:2026-09-03:1`, **turn 1 / attempt 1** |
| **dbt-Spark** | `dbt build --select mart_account_balance_daily`, dbt 1.9.4, `PASS=3 WARN=0 ERROR=0` |
| **Spark app** | `/applications/00g8g05ud5dj2u25/jobs/00g8g2f9pgg9vg27` |
| **Datamart row** | `990002 | 2026-09-03 | 777.77 | CERTIFIED | cfg-1682b193a20ce2ae | EOD` |
| **Athena** | qid `28658a65-…` (FULL_CDC chain), `2ffc94db-…` (mart row) |
| **Business AI** | metric `total_closing_balance`, version `metric:0d1b5e7ea203f9de`, request `f7b3804a-…` |

## Reconciliation — zero drift, source to AI

| Stage | Accounts | Sum |
|---|---|---|
| Oracle `corebank.account` | **321** | **2,031,880,937.77** |
| EOD curated fact | **321** | **2,031,880,937.77** |
| MART | **321** | **2,031,880,937.77** |
| Business AI answer | — | **2,031,880,937.77** |

## Criteria

| Criterion | Status |
|---|---|
| Canonical PK = Kafka key; same PK → same partition | **PASS** — +6 on one partition, others static, both engines |
| All I/U/D preserved in FULL_CDC, no dedup | **PASS** |
| Delete emits `d` envelope AND tombstone; both reach Kafka | **PASS** — offset 67 `d`, offset 68 tombstone (key headers only) |
| Tombstone counted, excluded from decode | **PASS** — `tombstones=1` / `=2` |
| Source ordering (Oracle SCN numeric, SQL Server hex LSN) | **PASS** |
| Replay/rerun idempotent | **PASS** — 360→360, 5,744→5,744, zero-record Iceberg commits |
| EOD one state per PK | **PASS** — 321 rows = 321 distinct PKs |
| Delete removed from active snapshot | **PASS after fix** — defect found live, fixed, re-proven 322→321 |
| Delete + recreate same PK | **PASS** — 990002 present at recreated value |
| Cutoff / COB frozen and declared | **PASS** — watermark row + Iceberg tag |
| Reconciliation source→AI | **PASS** — zero drift |
| Schema evolution through the chain | **NOT_TESTED** — the blocker is FIXED 2026-09-04 (per-record `globalId` selection, finding E2); the scenario itself has not been run |
| Poison record → quarantine | **NOT_TESTED** — the blocker is FIXED 2026-09-04 (PERMISSIVE decode + payload write, G-P1-1); no corrupt record has been injected yet |
| Late-arriving event correction | **PASS** — AUTO_CORRECT run live 2026-09-03: `4 date(s), 0 key(s), FULL_WINDOW_FALLBACK`, watermark advanced |
