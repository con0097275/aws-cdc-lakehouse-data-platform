# L1 FULL_CDC — canonical CDC truth, written directly from Kafka

> **ARCHITECTURE CORRECTION — 2026-08-21.** This document was written against a three-step
> chain (`L1 STREAM -> L2 FULL_CDC -> L3 SNAPSHOT`) in which STREAM was the raw Kafka
> landing. The layer model is a **fan-out**, and STREAM is a DERIVED layer, not a landing:
>
> ```
> Kafka --(decode + validate, append-only)--> L1 FULL_CDC   <- CANONICAL
>                                                  |
>                                     +------------+------------+
>                                     v                         v
>                            L2 REALTIME (STREAM)        L3 EOD (SNAPSHOT)
>                            rolling window T-N -> T     1 state per PK as-of T-1
>                            db: ..._stream              db: ..._snapshot
> ```
>
> The column contracts and dedup rules below are unchanged and still normative; what moved
> is which layer each belongs to:
>
> | Old reading | Now |
> |---|---|
> | L1 STREAM = raw Kafka landing | **L2 REALTIME** — derived from FULL_CDC, rolling window |
> | L2 FULL CDC | **L1 FULL_CDC** — canonical, written directly from Kafka |
> | L3 SNAPSHOT | **L3 EOD** — derived from FULL_CDC by cutoff + dedup |
>
> There is no raw landing layer: Kafka is decoded and validated straight into FULL_CDC.
> See `docs/TARGET_ARCHITECTURE.md` §3.


- Session: 07
- Date: 2026-08-14
- Status: **transformation and MERGE semantics PROVEN locally; the pipeline is `NOT_TESTED`**
- Contract: [`docs/DATA_CONTRACTS.md`](DATA_CONTRACTS.md) §6

---

## 1. The one line that defines L2

```sql
MERGE INTO full_cdc.<table> AS t
USING (...) AS s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT *
```

It delivers both required properties at once:

- **Rerunning a business date does not duplicate** — the `event_id` already matches, so
  nothing is inserted.
- **Every distinct I/U/D for a PK survives** — distinct business events have distinct
  `event_id`s, so they are all `NOT MATCHED` and all inserted.

There is deliberately **no `WHEN MATCHED THEN UPDATE`**. L2 rows are immutable history
(`CLAUDE.md` §5.3). Adding one would convert *operational* idempotency into *business*
dedup and silently destroy exactly the history L2 exists to hold.

Both properties are proven against a real Iceberg table:
`test_rerun_does_not_duplicate_event_id` and `test_all_IUD_for_the_same_pk_survive`.

## 2. The cutoff window

`[T 00:00:00Z, T+1 00:00:00Z)` — **half-open, UTC**.

`<`, not `<=` (S01-18). `CLAUDE.md` §5.6's prose uses `<=`, which would place a
midnight-boundary event in **two** business dates and double-count it.
`DATA_CONTRACTS` §6.1 is normative. `test_midnight_boundary_belongs_to_exactly_one_day`
asserts the count is exactly one — never zero, never two.

The cutoff is on `source_commit_ts` (**business time**), never `ingest_ts` (pipeline
time). Using pipeline time makes the same event land on different dates across reruns.

## 3. Late-arriving events

An event that committed during day T but only reached L1 after T's EOD run would be
missed forever by the base window. §6.2's sweep:

```sql
WHERE (source_commit_ts >= :start AND source_commit_ts < :end)
   OR (l1_write_ts     >= :watermark AND source_commit_ts < :end)
```

The second clause picks up anything L1 wrote since the previous L2 run, regardless of
commit time. **The overlap is harmless precisely because the MERGE is idempotent on
`event_id`** — that is what the key buys: a wider, safer sweep at no correctness cost.

Two guards on the watermark:

- A **safety overlap** rewinds it by 15 minutes to absorb clock skew between Connect and
  Spark. Without it, an event written microseconds before the recorded watermark falls
  through the gap between runs and is never swept.
- A watermark **after** the window end is **rejected**. Re-running an old date after a
  newer one would otherwise sweep future rows backwards into it.

## 4. Atomic publish

The completion marker is written **only** when the write succeeded **and** every
validation passed. A marker written optimistically is worse than none — downstream reads
it as "day T is complete" and certifies numbers from a partial load.

Validations, all of which block publication:

| Check | Why |
|---|---|
| `input == output + duplicates` | A gap means rows vanished between read and write |
| Non-negative counts | The audit itself is wrong |
| **Empty window** | Legitimate on a quiet day, but must be deliberate, not a wrong cutoff |
| `watermark_high >= watermark_low` | Inverted watermarks mean the read was wrong |
| **Zero quarantined rows** | Investigate before marking the day complete |

`validate_run` returns **all** failures, not the first, so one run surfaces every problem
instead of one per retry.

On failure the job raises, and **neither the watermark nor the completion marker is
written**. The next run re-reads the same window; `event_id` idempotency makes that safe.
Proven by `test_failed_validation_raises_and_does_not_advance_watermark`.

The failed run **is** still recorded in `ops.reconciliation_run` — the ledger is
append-only, because diagnosing "why was day T wrong" needs the failures, not just the
retry that eventually worked.

## 5. Two real bugs the tests caught

Both would have produced **silently wrong business dates** in production.

### 5.1 Spark session timezone

The first local run reported `input_count = 5` instead of `4`, with watermarks shifted to
06:00–11:00. The workstation is **UTC+7**, and Spark defaults to the JVM's local zone —
so a `TIMESTAMP` literal in the cutoff predicate was interpreted locally while the data
was written as UTC. An event at 23:00Z on 13 Aug landed in the **14 Aug** window.

Fix: `spark.sql.session.timeZone = UTC` in **both** jobs, not just the tests. This is
exactly the failure ADR-024 exists to prevent, and it is invisible without a test that
asserts the boundary.

### 5.2 Naive watermarks from Spark

Spark returns **naive** `datetime`s even with `session.timeZone = UTC`. Comparing one to
a timezone-aware window bound raises `TypeError` — and worse, comparing two naive values
from different zones would be *wrong without erroring*. `read_watermark` now attaches UTC
explicitly.

## 6. Test results

```
spark/tests/test_l2_window.py         26 passed   window, sweep, audit, publish gate
spark/tests/test_l2_local_spark.py     9 passed   REAL Iceberg MERGE
-----------------------------------------------
Session 07 subtotal                   35 passed
Full suite (Sessions 06 + 07)         84 passed in 26.14s
```

| Acceptance criterion | Status |
|---|---|
| Rerun does not duplicate `event_id` | **PASS** — real MERGE |
| Multiple operations on the same PK all survive | **PASS** — real MERGE |
| Audit ledger complete | **PASS** |
| Failed validation does not publish a marker | **PASS** |
| Rebuild/replay instructions exist | **PASS** — §8 |
| **EOD against live L1 data on EMR Serverless** | **`NOT_TESTED`** |
| **Partial-failure/retry against a real cluster** | **`NOT_TESTED`** |
| **Maintenance procedures executed** | **`NOT_TESTED`** |

The MERGE semantics are proven. **The pipeline is not** — no EOD run has ever read real
L1 data, and the maintenance procedures have never executed.

## 7. Maintenance

`spark/eod/maintenance.sql`, run **after** the completion marker, never before —
rewriting files under a job that is still committing loses data.

Order: compact → rewrite manifests → expire snapshots → **orphan files last**.

The orphan-file retention floor is **72 hours**, derived (S01-19) from job timeout
(120 min) × retries (3) plus margin. A shorter retention deletes files an in-flight commit
is about to reference, and the table is corrupt in a way that only surfaces on the next
read (risk R14).

## 8. Rebuild and replay

**Replay one business date** — safe at any time, because the MERGE is idempotent:

```bash
spark-submit stream_to_full_cdc.py --business-date 2026-08-14 \
  --l1-table glue_catalog.stream.<t> --l2-table glue_catalog.full_cdc.<t>
```

**Rebuild a date range** — replay each date in ascending order. Never descending: the
watermark guard rejects a watermark after the window end, which is the guard doing its
job rather than an obstacle.

**Full rebuild** — truncate L2, reset `ops.layer_watermark` for that table, replay every
date from the earliest `source_commit_ts` in L1. **L1 must not be deleted first**
(scope item 8): L1 is the only source from which L2 can be rebuilt, so its retention must
exceed any window in which a rebuild might be needed.
