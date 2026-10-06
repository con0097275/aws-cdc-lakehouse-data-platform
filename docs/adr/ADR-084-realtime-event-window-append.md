# ADR-084 — The event window appends on the cursor, and its extent equals its window

* **Status:** Accepted
* **Date:** 2026-09-28
* **Phase:** R2-E
* **Related:** ADR-064, ADR-082, ADR-083

## Context

Every `event_window` table wrote with `overwrite_window`: each run re-read
`[grace_lower, upper)` from FULL_CDC and replaced the target's whole contents. Correct,
self-healing, and 144 full 96-hour reads per table per day.

R2-C made an incremental read possible. Switching these tables to it requires changing the
write strategy too — `overwrite_window` and an incremental delta are the one combination
that destroys data, because an overwrite replaces everything the delta does not contain
(ADR-083, refused at compile).

## Decision

`transaction`, `digital_event` and `payment_method` become
`shape: event_window`, `write_strategy: append`, `source_progress: iceberg_snapshot`.

Three consequences had to be handled explicitly, and two of them were found by tests rather
than by design.

### `retention_hours` becomes the table's real extent, so it is set to the window

Under `overwrite_window` the table WAS the window: each run rewrote exactly
`[grace_lower, upper)`, and anything older vanished as a side effect. Under `append` nothing
removes an aged row except the retention prune — so `retention_hours: 168` would have
started keeping three days the layer never promised, on the day this landed, with no code
saying so.

Set to `96` (= `window_hours` 72 + `late_grace_hours` 24), the physical extent equals the
materialised window and the observable contract is unchanged. The compiler already enforces
`retention >= window + grace`, so 96 is the tightest legal value.

### A NOOP still prunes

**Found by a test.** R2-C's `SUCCEEDED_NOOP` short-circuit returned before the prune, which
was harmless while every table overwrote. Under `append` it is not: retention follows the
**clock**, not the arrival of new data. A table whose source went quiet kept serving rows
outside its promised window indefinitely — and reported SUCCESS every ten minutes while it
did. The prune now runs on the NOOP path; the cursor still does not move, because pruning is
not reading.

### `--rebuild` is a flag, not a change of shape

`main()` used to rewrite the entry to `event_window` / `overwrite_window` before rebuilding.
That was right while every table was an event window and is destructive now: rebuilding a
`latest_state` table would have replaced its contents with the raw event window — many rows
per business key in a table whose entire contract is one — and reported SUCCESS. `run_table`
now takes `force_rebuild`, and each shape rebuilds in its own way.

An event window still rebuilds by **overwriting** even when its normal strategy is `append`:
appending during a rebuild merges the re-read window onto what is already there, which is the
opposite of rebuilding, and cannot remove a row that should no longer be present.

## Consequences

* **The three event-window tables now read a delta instead of 96 hours**, most runs. The run
  after each weekly compaction still reads the whole window (ADR-083).
* **The observable table is unchanged.** Same rows, same extent — which is why
  `retention_hours` moved to 96 in the same commit rather than later.
* **Small files accumulate.** Ten-minute appends plus a per-run prune produce more, smaller
  files than one overwrite did. That is work moved into `cdc_maintenance` (R2-I), not
  removed.
* **`--rebuild` is now safe on a `latest_state` table**, which it was not before this phase.

## Options

**Leave them on `overwrite_window`.** Correct, self-healing, and the cost R2-C exists to
remove. It stays available per table and via `--force-window-scan`.

**Append and leave retention at 168h.** Fewer config lines, and the table silently changes
extent on the day it ships.

**Prune only when rows were read.** Cheaper, and it is the bug above.

## Cost

Structurally: a delta read plus a prune instead of a 96-hour scan plus a full rewrite, for
the three highest-volume tables. Measured in R2-I; no number is claimed here.

The append path writes small files every ten minutes where the overwrite wrote one compacted
set, which moves work into `cdc_maintenance` rather than removing it.

## Security

No change.

## Rollback

Per table, in the registry: `write_strategy: overwrite_window`,
`processing: {source_progress: window_scan}`, `retention_hours: 168`. Recompile. The next run
overwrites the target from FULL_CDC, so the table is correct after one run with no migration.

## Validation

`spark/tests/test_realtime_cursor.py`, R2-E section — against real Iceberg:

* three events for one key stay three rows, over two runs, with three distinct keys — the
  one assertion that separates this shape from `latest_state`
* an aged row is pruned under `append`, and **a NOOP prunes too** while leaving the cursor
* rebuilding an event window overwrites: a row inserted into the target that FULL_CDC does
  not have is gone afterwards
* a rebuild reads the whole window and records `fallback_reason=forced_rebuild`

Plus the R2-C tests, which already covered append idempotency on `dv_event_id` under a
replayed range and a compaction fallback.
