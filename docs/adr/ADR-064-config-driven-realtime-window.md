# ADR-064 — Config-driven REALTIME window: calendar boundaries, frozen bounds, run ledger

* **Status**: Accepted (implementation: Phase 3; no cutover, no live run)
* **Date**: 2026-09-10
* **Extends**: ADR-062 (CDC granularity), ADR-063 (per-table provisioning and routing),
  ADR-033 (layer naming), ADR-024 (UTC and business-day boundaries)
* **Relates to**: ADR-041 (STREAM_BATCH vs STREAMING_RT), ADR-043 (watermark semantics)
* **Evidence**: `docs/REALTIME_LAYER.md`, `spark/tests/test_realtime_window.py`,
  `spark/tests/test_realtime_engine_spark.py`

## Context

`spark/jobs/realtime/job.py` materialised REALTIME with three fixed arguments —
`--full-cdc`, `--realtime`, `--window-hours` — and computed its bound as
`current_timestamp() - INTERVAL N HOURS` **inside the query**. That was adequate while
REALTIME was one table built by one recipe. Per-table provisioning (ADR-063) makes it
inadequate in four specific ways:

1. **One job per table does not scale and is not the point.** The registry already describes
   every table's window; a second place to state it is a second place to be wrong.
2. **The window moved during the run.** A bound re-derived per step differs from the one
   reported by however long the run took, and every later comparison is off by that amount —
   in a way that is indistinguishable from late-arriving data.
3. **There was no upper bound at all.** The filter was `>= cutoff`, so what the window
   contained depended on when each partition happened to be read.
4. **`72` was the only expressible window.** A policy that means *three business days* and a
   policy that means *72 hours* are different products, and the platform had no way to say
   which one a table wanted.

Nothing recorded what a run did, either: no ledger, so "which FULL_CDC snapshot did
yesterday's 09:15 window read?" was unanswerable.

## Decision

### 1. One generic engine (§A)

`spark/jobs/realtime/realtime_engine.py` takes `table_id` + `as_of` + the compiled plan. Everything
that differs between tables is a value. Adding a table is a registry entry and a provision,
never a new job. `--table ALL` runs every enabled table.

It reads **FULL_CDC and never Kafka** — asserted against the source by a test. Two
independent readers of the topics would decode the same records twice, quarantine
independently, and drift apart in exactly the ways FULL_CDC exists to prevent; and a rebuild
would need a topic replay that retention may no longer permit.

### 2. Two boundary modes, and the platform does not guess (§B)

`calendar_day` takes the lower bound at **midnight in `business_timezone`**; `rolling_hours`
measures back from the run instant. Three calendar days and 72 hours are different windows
and both are defensible, so the mode is config. A table declaring any `_days` field becomes
`calendar_day`; a table configured in hours stays `rolling_hours` — which is what makes this
phase a no-op for every table currently in the registry.

Calendar arithmetic is done on the **local date and then converted**, never by subtracting a
timedelta: across a DST transition those differ by an hour, and the hour lands in a
neighbouring day's partition where nothing reports it.

### 3. Four bounds, frozen once (§D)

`upper` (frozen), `logical_lower` (promised), `grace_lower` (materialised), `retention_bound`
(pruned below). `[grace_lower, upper)` is half-open at both ends. `resolve_window` takes the
instant from its **caller** and never reads the clock; a naive instant is refused, because
`naive.astimezone(utc)` silently assumes the machine's local zone and the same `--as-of`
would then mean different windows on a laptop and on an EMR worker.

`ops.realtime_run` records the run: both snapshot ids, all four bounds, row and prune counts,
status. `source_snapshot_id` is captured **before** the read, so it names the snapshot the
run actually saw rather than a later one a concurrent ingest wrote.

### 4. Retention is enforced in the unit the policy is written in (§F)

`physical_retention_days >= lookback_days + late_arrival_grace_days`, at compile time.
Checking only the hours fields would leave a day-configured table unvalidated *while
appearing checked*, because the hours defaults are always present.

Pruning is a row-level `DELETE`. Never a file delete: removing objects under a table's
location leaves Iceberg metadata pointing at files that are gone, every reader fails, and the
table cannot be time-travelled back because the snapshots reference the same missing files.

### 5. Full refresh uses `overwrite`, not `createOrReplace`

Both are atomic; only one leaves the table *definition* alone. `createOrReplace` resets the
partition spec, write properties and governance metadata that `cdc/provision.py` set —
silently, to whatever the DataFrame implies. Harmless while nothing provisioned the table;
not harmless now.

### 6. REALTIME window ≠ STREAM_BATCH watermark (§E)

Documented in `docs/REALTIME_LAYER.md` §3. REALTIME is a physical layer that remembers
nothing between runs; STREAM_BATCH is a reporting flow that advances its own watermark only
on success. The single coupling is a **sizing** constraint: the window must exceed the
largest `safety_overlap` of any batch that reads it, or the batch asks for rows that aged out
and silently reads fewer than it should.

## Options

* **Keep the fixed-argument job and add flags.** Rejected — it is the registry retyped, and
  it cannot express a calendar boundary without a second notion of "day".
* **Default `calendar_day` for everything.** Rejected: it would change the window every
  deployed table serves, silently, on deploy. Compatibility is why hours stays the default
  for hours-configured tables.
* **Infer the boundary from which fields are non-null, at runtime.** Rejected: adding a
  `lookback_days` beside existing hours would silently change a live table's window. The
  mode is resolved once, at compile time, and carried in the plan.
* **Let the engine call `now()` where it needs a bound.** Rejected — §3. This is the defect
  the old job had.
* **`createOrReplace` for full refresh.** Rejected — §5.
* **Expire snapshots as part of the prune.** Deferred. Expiry is destructive and its
  retention guard is a separate decision (CLAUDE.md §6); a materialisation run is the wrong
  place to make it. The prune bounds the *rows*; snapshot expiry belongs to maintenance.

## Consequences

* Every table in the shipped registry keeps `rolling_hours` and the exact window it had. A
  test asserts this.
* The compiled plan is now **schema version 3**. A version-2 consumer would read
  `window_hours` off a table whose policy is written in calendar days and materialise a
  different window than the config names, so the router refuses it rather than proceeding.
* Two more provisioned OPS tables' worth of targets: the run ledger joins the event index.
* `spark/jobs/realtime/job.py` stays, because every recorded submission recipe calls it, but
  it now delegates its window arithmetic to `cdc.realtime` — two implementations of a window
  is exactly how a rolling run and a calendar run come to serve different rows while both
  reporting success. It also gained the upper bound it never had.

## Cost

| item | impact |
|---|---|
| the engine | **$0** — same read and same write as the job it generalises |
| `ops.realtime_run` | one small row per table per run; coordinates only, no payload |
| `calendar_day` vs `rolling_hours` | neutral. A calendar window is usually *smaller* (whole days from midnight rather than N×24h back from mid-morning) |
| `incremental_merge` | cheaper per run on a large window, at the cost of a prune that must run; `full_refresh` remains the default |
| `--rebuild` | one extra FULL_CDC read. No Kafka, no connector, no replay |
| retention prune | a row-level DELETE per run; a no-op under `full_refresh`, and still run because the config *promises* the retention |

Immaterial against the $100/month budget of record (ADR-030 as amended 2026-09-06). Nothing
here is always-on; the layer costs EMR seconds per scheduled run.

## Security

* **No new resource, no IAM change, no data movement.** The engine reads a table the Spark
  role already reads and writes one it already writes.
* The run ledger carries **coordinates and outcomes, never payload**, so it does not widen
  PII exposure — the same rule the event index follows (ADR-062 §G).
* The engine will not create a table, so it cannot bring an unowned, unclassified,
  unretained data product into existence (ADR-063 §F).
* `business_timezone` is config, not input: a caller cannot shift another table's day
  boundary.

## Rollback

* The engine is **additive**. `spark/jobs/realtime/job.py` still runs with its original
  arguments, so every existing recipe works unchanged.
* No table's window changes: hours-configured tables keep `rolling_hours`, asserted by test.
* A table switched to `calendar_day` is reverted by removing the `_days` fields, or by
  stating `boundary: rolling_hours`; the next run re-materialises the old window from
  FULL_CDC. Nothing is lost, because REALTIME is **derived** — it can always be rebuilt.
* The run ledger is append-only and read by nothing; dropping it costs history, not data.

## Validation

Accepted on evidence that needs no platform:

1. `pytest spark/tests/test_realtime_window.py` — boundary arithmetic, both modes, DST,
   business timezones, the frozen bound, section-F enforcement in both units. **32 tests.**
2. `pytest spark/tests/test_realtime_engine_spark.py` — against a real Iceberg catalog: 1/3/7
   day windows, a late event inside grace, an event outside it, exclusive upper bound,
   delete events carried, two tables with different windows, ageing-out, the table
   definition surviving the overwrite, row-level prune, rerun, rebuild, past-window rebuild,
   schema evolution, disabled table, unprovisioned target refused, and the ledger contents.
   **25 tests.**
3. `python3 -m cdc.compile --check` — the shipped registry still compiles and is unchanged.

**Not validated, and not claimed:** nothing has run on EMR. No REALTIME table has been
materialised in AWS by this engine, and no ledger row exists in Glue. The live gate is one
`--rebuild` of a known window whose row count matches an independent Athena count of
FULL_CDC over the same bounds.
