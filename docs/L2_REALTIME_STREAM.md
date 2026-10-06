# L2 REALTIME (STREAM) — rolling window T-N → T, derived from L1 FULL_CDC

> **The config-driven REALTIME engine is `docs/REALTIME_LAYER.md`**
> (ADR-064): window boundaries, the run ledger, and §3 on why the REALTIME window
> and the STREAM_BATCH watermark are different mechanisms. This document describes
> the layer's contract and history.

> Created 2026-08-21. This document did not exist, and its absence was itself the bug:
> `docs/L2_FULL_CDC.md` and `docs/L3_SNAPSHOT.md` were both present, so the STREAM layer
> looked like the *raw Kafka landing* that FULL_CDC replaced rather than the *derived
> rolling window* it actually is. `reporting/layers.yaml` had `REALTIME` SUBSTITUTED onto
> the `full_cdc` database with a note reading "this layer does not exist yet", which
> reinforced the wrong reading.

## 1. Where it sits

```
Kafka --(decode + validate, append-only)--> L1 FULL_CDC   <- CANONICAL
                                                 |
                                    +------------+------------+
                                    v                         v
                          L2 REALTIME (STREAM)          L3 EOD (SNAPSHOT)
                          rolling window T-N -> T       1 state per PK as-of T-1
                          db: kafka_dev_lab_dev_stream  db: kafka_dev_lab_dev_snapshot
```

REALTIME and EOD are **siblings**. Both are deterministic functions of `FULL_CDC`; they
differ only in their bound:

| | Bound | Dedup | Tier it feeds |
|---|---|---|---|
| **REALTIME** | `[T-N, T)` — rolling, N configurable | same `event_order` ranking | `PROVISIONAL_NRT` via STREAM_BATCH |
| **EOD** | `<= T-1 23:59:59` — frozen COB cutoff | same `event_order` ranking | `CERTIFIED` via EOD Reporting |

That symmetry is the point. If REALTIME used a different ranking, a variance between a
provisional number and a certified one would be ambiguous between *late data* and
*divergent logic* — and only one of those is a bug worth chasing.

## 2. Physical binding

| Logical | Physical | Status |
|---|---|---|
| `REALTIME` | `kafka_dev_lab_dev_stream` | ACTIVE |

The database is named `_stream` for historical reasons — it predates this layer model and
is not renamed, because it is deployed and holds data. `reporting/layers.yaml` is the only
place the mapping appears; job configs and business SQL name the **logical** layer.

## 3. The bounded predicate is mandatory

```sql
source_commit_ts >= :window_start AND source_commit_ts < :window_end
```

A REALTIME read without a bound is a full-history scan wearing a rolling-window label. It
returns rows, costs the whole table, and reports a number that is not what the caller asked
for. `source_resolver` carries the predicate with the binding so the bound travels with the
layer rather than depending on each caller remembering it.

Half-open `[start, end)` — never `<= end`. A closed upper bound double-counts any event
landing exactly on the boundary when the next window opens there.

## 4. Retention, and what it costs to get wrong

N is a **retention** decision, not a performance one. An event older than `T-N` is not
merely slow to reach — it is **invisible** to this layer. That is precisely why
AUTO_CORRECT reads `FULL_CDC` rather than `REALTIME`: a correction restricted to the recent
window can only fix what was never really broken (`docs/TARGET_ARCHITECTURE.md` §3).

Iceberg maintenance applies here as to every table — compaction, manifest rewrites,
snapshot expiry, orphan cleanup — with retention exceeding the longest window any job might
still commit into (CLAUDE.md §6). A rolling window that expires snapshots faster than
STREAM_BATCH's `safety_overlap_minutes` will drop rows a running batch is about to read.

## 5. Who reads it

| Consumer | Why |
|---|---|
| **STREAM_BATCH** | its only source; a finite Airflow micro-batch over `[wm − overlap, frozen_upper)` |
| *not* AUTO_CORRECT | needs events older than N — see §4 |
| *not* EOD / FULFILL | both need the frozen COB cutoff, which is `EOD`'s bound, not this one |

## 6. Contract

Column contracts are identical to `L1 FULL_CDC` (`docs/L1_FULL_CDC.md` and
`docs/DATA_CONTRACTS.md` §6), including `dv_event_id` and `dv_src_event_id`. REALTIME is a
**bounded projection** of FULL_CDC, not a reshaping of it: identical columns, identical
semantics, narrower time range. Anything that reads one can read the other.

## 7. Status

**NOT YET BUILT.** The binding is ACTIVE and the contract is fixed, but no job currently
materialises `kafka_dev_lab_dev_stream` from `FULL_CDC`. Until one does, STREAM_BATCH runs
against a bounded read of `FULL_CDC` with the same predicate and records
`source_layer_substituted=true` on the execution — so no run quietly reads a stand-in and
reports it as the real thing.

Building it is upstream layer work, tracked as an open item, and it is genuinely optional
for correctness: the bounded projection returns the same rows. What it buys is cost — a
window scan over a small table instead of over the full history.
