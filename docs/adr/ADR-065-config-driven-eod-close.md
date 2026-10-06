# ADR-065 — Config-driven EOD close: cutoff, source-native ordering, certification

* **Status**: Accepted (implementation: Phase 4; no cutover, no live run)
* **Date**: 2026-09-10
* **Extends**: ADR-062/063 (granularity, provisioning, routing), ADR-064 (REALTIME window),
  ADR-024 (UTC and business-day boundaries), ADR-043 (watermark and rerun semantics)
* **Evidence**: `spark/tests/test_eod_cutoff.py`, `spark/tests/test_eod_engine_spark.py`,
  `spark/jobs/l1_stream/ordering.py` (the normative ordering contract)

## Context

`spark/jobs/eod/job.py` closes one table: Oracle `ACCOUNT`, hard-coded down to the column
name `j.ACCOUNT_ID`. Its ranking is

```python
Window.partitionBy("j.ACCOUNT_ID").orderBy(
    F.col("position_primary").cast("decimal(38,0)").desc_nulls_last(), ...)
```

which is **Oracle-only semantics**, and the job's own comment says so:

> a SQL Server position is a HEX LSN triplet; casting that to decimal yields NULL,
> `desc_nulls_last()` sends every row to the back equally, and the ranking silently collapses
> onto `kafka_offset` — which CLAUDE.md 5.4 forbids as a comparator because offsets are only
> monotonic WITHIN one partition. […] The correct SQL Server treatment already exists
> (`spark/jobs/l1_stream/ordering.py`); **wiring it in is the fix if EOD is extended.**

It guards itself with a refusal rather than assuming the filter holds. This ADR is that fix.

Three further gaps blocked per-table EOD: the cutoff was a `business_date` equality rather
than a business-timezone boundary; nothing recorded what a close did or whether it was
certified; and the delete behaviour lived in one job's filter rather than in config.

## Decision

### 1. One generic builder (§A)

`spark/jobs/eod/eod_engine.py` takes `table_id` + `COB_DATE` + the compiled plan. Grain,
ordering, delete policy, cutoff and target are values. No per-table snapshot Python exists.

**It reads FULL_CDC and never REALTIME**, asserted by a test on the plan key it would have to
name. REALTIME is a bounded window that ages rows out, so building on it would make a
historical rebuild depend on a window that no longer contains the days being rebuilt, and
would make a certified balance depend on the *schedule* of a different job. §I is the case
that proves it.

### 2. Cutoff (§B)

```
cutoff_local = start of (COB_DATE + 1) in business_timezone
cutoff_utc   = that instant in UTC
predicate    = source_commit_ts < cutoff_utc
```

Strictly `<`. CLAUDE.md §5.6 permits `event_ts < T00:00` **or** `source_commit_ts <= cutoff`;
those are the same set with different cutoff values. This platform takes the first, because
"the last instant of a day" has no exact representation and every approximation of it
(`23:59:59`, `.999`, `.999999`) silently drops events in the gap.

The date arithmetic is done on the **local date and then converted**, never by adding a
timedelta: across a DST transition those differ by an hour, and an hour of events would be
certified into the wrong business date.

Both cutoffs are recorded. They answer different questions — the local one is what a business
reader checks, the UTC one is what the predicate used — and only keeping both makes a wrong
timezone visible.

A second, **wider** predicate on `event_date` exists purely so Iceberg can prune partitions.
It is one day wider on each side because `event_date` is a UTC date while the business day is
not: a prune written as `event_date == cob_date` would look like an optimisation and would
silently drop real events for every non-UTC table.

`business_date_lag_days` (default 1) decides which day a scheduled run closes, applied to the
**local** date: 02:00 UTC is still the previous day in New York.

### 3. Source-native ordering (§C)

The two engines need **opposite** treatment, and one "just pad it" helper would corrupt one
of them:

| | wire form | treatment |
|---|---|---|
| Oracle SCN | numeric | `lpad(pos, 24, '0')` — `'9' > '10'` as strings but `9 < 10` as numbers |
| SQL Server LSN | `aaaaaaaa:bbbbbbbb:cccc` hex, already fixed-width | `lower(pos)`, compared lexicographically — valid **only** because of that padding, so the width is **validated**, not assumed |

Both produce a sortable **string**, so one ORDER BY serves both engines and the comparison
never depends on a cast that can yield NULL. A position that does not match its engine's
pattern is **refused**, not ranked.

Precedence is the documented contract tuple, all descending because EOD takes its maximum:

```
position_primary, position_secondary, source_commit_ts, kafka_partition, kafka_offset
```

`kafka_partition` **precedes** `kafka_offset`. That is the whole of CLAUDE.md §5.4: an offset
is monotonic only within its own partition, so ordering by partition first means offsets are
compared only between rows that share one. The partition is a *determinism* tie-break, not a
claim that a higher partition happened later.

### 4. Delete policy (§E) — the default after inspecting the contract

| existing evidence | says |
|---|---|
| CLAUDE.md §5.7 | two policies, the exclusion named first |
| `cdc/models.py` | `EXCLUDE_FROM_SNAPSHOT` (default) \| `SOFT_FLAG` |
| deployed `eod/job.py` | ranks, then filters `op != 'd'` — i.e. exclude-latest-delete |

**The default stays `exclude_from_snapshot`.** It is what the platform already does, and a
changed delete default alters certified balances without altering a line of business SQL.
`physical_delete` is added as a third. The brief's names (`exclude_latest_delete`,
`soft_delete`, `physical_delete`) are accepted as **aliases**, normalised on the way in: the
registry, the plan hashes and the provisioned properties already carry the project's
spellings, and renaming them would rewrite every config hash to say the same thing.

### 5. Snapshot mode (§F) — default `rolling_history`, also an inspection result

The provisioned EOD table is `PARTITIONED BY (business_date)` with grain "one row per primary
key per business_date" and `retention_days: 365`. A 365-day retention on a table that holds
one day is meaningless: **the deployed contract has already committed to history.** Defaulting
to `latest_state` would make the first run of every table delete up to 364 certified
partitions. `latest_state` is supported; it is not the default.

### 6. Certification is a gate, not a label (§G)

`ops.eod_run` records the cutoff (both forms), both snapshot ids, the **position evidence**
(`max_position_primary`, `max_source_commit_ts` — how far into the source the close actually
reached), the counts, and the DQ and reconciliation outcomes. `CERTIFIED` requires **both**
gates to pass.

The reconciliation is a genuine identity, not a restatement of the build:
`distinct_keys − deletes == rows` (or `distinct_keys == rows` under `soft_flag`). If a key
vanished between reading and writing, the snapshot still looks like a perfectly ordinary
table — this is the only thing that notices.

Data is written even when validation fails; the **completion marker is withheld**. A snapshot
an operator can inspect is more useful than one thrown away, and the existing contract
(`spark/eod/audit.py::may_publish`) already says a failed validation must not publish.

## Options

* **Keep the Oracle-only decimal cast and widen the source filter.** Rejected — it is the
  defect the deployed job's own comment warns about.
* **Normalise positions at ingest instead of at ranking.** Rejected for now: it rewrites the
  FULL_CDC row contract and every row already written. Ranking-time normalisation is
  equivalent and reversible. Worth revisiting if a third engine appears.
* **`event_date == cob_date` as the cutoff.** Rejected — §2. Correct only for UTC tables, and
  silently wrong for every other one.
* **A `<=` cutoff at end-of-day.** Rejected — no exact representation of "the last instant".
* **Default `latest_state`.** Rejected — §5. It would delete certified history on first run.
* **Fail the run when DQ fails.** Rejected: the built snapshot is diagnostic evidence. The
  gate is the marker, not the write.
* **Compute the EOD grain from `dv_pk_hash` on the source row.** Rejected as the *only*
  source: it is NULL for rows written before Phase 2 and for a writer version that did not
  carry the key, and a NULL grain collapses every such row onto one snapshot key. It is
  recomputed by the identical expression instead, so where the source value exists they agree.

## Consequences

* Every table in the shipped registry keeps `lag=1`, `rolling_history` and
  `exclude_from_snapshot` — a test asserts it. Phase 4 changes nothing any table certifies.
* The compiled plan is **schema version 4**. A version-3 consumer would close a date without
  knowing which business day a run is for or which engine's ordering decides the winner.
* `ops.eod_run` joins the event index and the REALTIME ledger as a provisioned OPS target.
* `spark/jobs/eod/job.py` is untouched and still runs: it is Oracle-`ACCOUNT`-only and every
  recorded recipe calls it. It is superseded, not deleted.
* **A discrepancy is recorded rather than papered over:** `ordering.py::normalize_position`
  expects `event_serial_no` in `position_secondary` for SQL Server, while
  `full_cdc/job.py` writes `change_lsn` there. The engine follows the row contract that
  actually exists (`change_lsn`, a fixed-width hex triplet, which orders correctly). Open
  issue, not a blocker: `event_serial_no` would only matter to break a tie between two events
  sharing both LSNs, and the contract's later tie-breakers already handle that.

## Cost

| item | impact |
|---|---|
| the engine | **$0** — same read and same write as the job it generalises |
| `ops.eod_run` | one small row per table per COB; coordinates and outcomes, no payload |
| the `event_date` prune predicate | **reduces** cost: partition pruning on a table that is otherwise scanned in full |
| `latest_state` | a DELETE per run; cheaper storage, at the cost of history that cannot be recovered without a rebuild |
| `--fulfill` | one extra FULL_CDC read per date rebuilt. **No Kafka, no REALTIME window** |
| ordering | neutral — `lpad`/`lower` are per-row string ops, not a shuffle |

Immaterial against the $100/month budget of record. Nothing here is always-on.

## Security

* **No new resource, no IAM change, no data movement.** The engine reads a table the Spark
  role already reads and writes one it already writes.
* The ledger carries **coordinates, counts and outcomes — never payload**, so it does not
  widen PII exposure, the same rule the event index and the REALTIME ledger follow.
* The engine will not create a table (ADR-063 §F), so it cannot bring an unowned,
  unclassified, unretained data product into existence.
* `business_timezone` and the delete policy are config, not input: a caller cannot shift
  another table's day boundary or change what a delete means to it.

## Rollback

* The engine is **additive**. `spark/jobs/eod/job.py` still runs with its original arguments.
* No table's close changes: lag, snapshot mode and delete policy all default to what the
  deployed contract already does, asserted by test.
* A COB partition is replaced, not appended, so re-running a date with a reverted config
  restores the previous state from FULL_CDC. EOD is **derived** — it can always be rebuilt.
* `latest_state` is the one setting whose rollback is not free: it deletes prior partitions,
  and recovering them means re-closing each date. That is why it is not the default.

## Validation

1. `pytest spark/tests/test_eod_cutoff.py` — boundary derivation under three business
   timezones (UTC; the lab's own UTC+7; and a US zone across a DST change), the lag applied
   to the local date, per-engine ordering, the partition-before-offset rule, delete-policy
   aliases, and the certification gate. **41 tests.**

   Note that the *predicate* is always `cutoff_utc`: the boundary is derived in the business
   timezone and converted, so no comparison is ever made in local time (D8 / ADR-024).
2. `pytest spark/tests/test_eod_engine_spark.py` — against a real Iceberg catalog: insert,
   update, multiple updates, delete, delete-then-recreate, late event, event past the cutoff,
   Oracle numeric ordering, SQL Server lexicographic ordering, a hex LSN in an Oracle table
   refused, a wrong-width LSN refused, ties on position and across partitions, composite key,
   non-UTC cutoff, rerun convergence, historical rebuild with **no REALTIME table in
   existence**, rolling vs latest state, and the ledger contents. **28 tests.**
3. `python3 -m cdc.compile --check` — the shipped registry compiles and is unchanged.

**Not validated, and not claimed:** nothing has run on EMR. No EOD snapshot has been built in
AWS by this engine and no `ops.eod_run` row exists in Glue. The live gate is one close whose
ledger `row_count` matches an **independent Athena count** of distinct keys in FULL_CDC below
the same `cutoff_utc` — computed in Athena, not by the job that wrote it.
