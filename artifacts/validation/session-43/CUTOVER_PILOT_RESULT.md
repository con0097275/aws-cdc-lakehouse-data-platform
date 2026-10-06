# Per-table CDC cutover — PILOT RESULT (live, 2026-09-10)

Account 111122223333, profile my-aws-profile, region ap-southeast-1, env dev.
Every number below is from an Athena `QueryExecutionStatistics` or an Iceberg
`$files` metadata read. Nothing is estimated.

## Pilot pair (section B)

| table | engine | legacy rows | identity-bearing | backfilled |
|---|---|---|---|---|
| `oracle.coredb.corebank.account` | Oracle, numeric SCN | 961 | 641 | 641 |
| `sqlserver.digital.dbo.digital_event` | SQL Server, hex LSN | 12,000 | 6,000 | 6,000 |

## Benchmark (section I) — measured, both paths, 9 metrics x 5 scenarios

### oracle/ACCOUNT

| scenario | legacy bytes | per-table bytes | delta | legacy ms | per-table ms |
|---|---|---|---|---|---|
| auto_correct | 86 | 122 | +41.9% | 730 | 2,264 |
| eod_source_scan | 89,193 | 11,231 | -87.4% | 628 | 708 |
| fulfill | 153 | 78 | -49.0% | 1,772 | 580 |
| one_table_one_day | 75 | 0 | -100.0% | 1,662 | 690 |
| one_table_seven_days | 926 | 0 | -100.0% | 668 | 719 |

Files: 14 -> 2 (-85.7%) on both.

### sqlserver/digital_event

| scenario | legacy bytes | per-table bytes | delta | legacy ms | per-table ms |
|---|---|---|---|---|---|
| auto_correct | 87 | 0 | -100.0% | 1,467 | 547 |
| eod_source_scan | 113,261 | 105,163 | -7.1% | 647 | 1,656 |
| fulfill | 427 | 79 | -81.5% | 662 | 1,177 |
| one_table_one_day | 348 | 0 | -100.0% | 534 | 543 |
| one_table_seven_days | 946 | 0 | -100.0% | 671 | 2,347 |

Files: 14 -> 2 (-85.7%) on both.

## Gate (section H) — 9/9 on both tables

`artifacts/cdc/gate-account.json`, `artifacts/cdc/gate-digital-event.json`,
evidence recorded in `artifacts/cdc/cutover-gate.json`.

## Section A — the legacy monolith

`cdc_events` held **20,390 rows before the pilot and 20,390 after**. It was read,
never written; the backfill contains no DROP, DELETE, TRUNCATE or overwrite.

## Final state

2 tables PER_TABLE, 6 LEGACY. Rollback is `cdc-cutover.py set --mode LEGACY`.

## Consumer read path, verified through the resolver

```
PER_TABLE  oracle.coredb.corebank.account         rows=641   scanned=0B
PER_TABLE  sqlserver.digital.dbo.digital_event    rows=6000  scanned=0B
LEGACY     oracle.coredb.corebank.customer        rows=600   scanned=1001B
```

A cut-over table resolves to its per-table target with **no predicate**; a table still on
`LEGACY` resolves to the monolith **with its mandatory predicate**, in one query each. That
is section F working against real AWS, not a unit test.

## EMR job runs

```
register-tables-dryrun   SUCCESS   15 tables discovered
register-tables          FAILED    catalog-qualified CREATE DATABASE
register-tables-v2       FAILED    CREATE NAMESPACE -- still no catalog configured
register-tables-v3       SUCCESS   15 registered, cdc_events rows=20390
provision-pilot          SUCCESS   2 per-table FULL_CDC targets
backfill-pilot-dryrun    exit 1    641/961, 6000/12000 identifiable (exit code then fixed)
backfill-pilot-execute   SUCCESS   identity_match=True on both
```

Three failures are the defects the pilot existed to find. They are listed rather than netted
out: the retry cost is part of what a first live run actually costs.
