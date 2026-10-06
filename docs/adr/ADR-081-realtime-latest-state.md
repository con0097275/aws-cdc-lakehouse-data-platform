# ADR-081 — A latest-state mode for the REALTIME layer

* **Status:** Accepted
* **Date:** 2026-09-23
* **Related:** ADR-064 (config-driven REALTIME), ADR-065 (EOD close), ADR-042 (accuracy ladder)

## Context

REALTIME is a bounded **event window**: `window_rows` filters FULL_CDC on
`source_commit_ts` and `materialise` overwrites the target. There is no ranking and no
collapse per key, so a row updated three times inside the window appears three times. The
collapse happens downstream, in `transform.collapse_to_grain`.

That is correct for a consumer that ranks anyway, and wrong for a hot path. A dashboard
asking "what is this account's balance now" wants one row per key, and paying to re-read and
re-rank a 96-hour window on every `*/10` tick to get it is the dominant cost of the layer.

## Decision

**A third `refresh_mode`, `latest_state`. Not a changed default.**

```yaml
realtime:
  refresh_mode: latest_state
eod:
  delete_policy: soft_flag        # REQUIRED with latest_state; see below
```

An event window and a state table are different products, and different tables want
different ones: `transaction` is append-only at 3,000/day and wants the window;
`account` is 320 mutable rows and wants the state. Changing the default would alter what
every existing consumer reads.

### Upsert within the day, REBUILD at the boundary

Two failure modes, and they are not the same one.

**Out-of-order UPDATES.** Debezium delivers by commit time, not event order: a connector
catching up after a stall publishes logically older changes now. Overwriting on arrival lets
an older event replace a newer state, undetectably — the row is complete, merely stale, and
the next run does not fix it because that key has no new event. So the MERGE is **guarded**
on an order key built from `cdc.eod.ordering_for`, the same ranking the certified close uses.
If REALTIME ranked differently from EOD, a provisional-vs-certified variance would be
ambiguous between "late data" and "divergent logic", and only one of those is worth chasing.

**Out-of-order DELETES.** This defeats the guard, and physically deleting the row is what
causes it: with the row gone there is nothing left to compare against, so a late older insert
falls to `WHEN NOT MATCHED` and **resurrects** a key the source deleted. So a delete is
written as a **tombstone** — the row stays, `is_deleted` true, carrying the delete's order
key — and the late event correctly loses.

Tombstones then accumulate, keys whose events age out never leave, and drift from any single
bad run persists. An upsert cannot express *"this row should no longer exist at all"*. So the
run **rebuilds at the day boundary**: the whole window is re-derived from FULL_CDC in one
pass, which clears all three at once. The boundary is the right cadence because it is where
the EOD close draws its own line — rebuilding there keeps the hot path and the cold path
describing the same day.

The decision reads the last **SUCCEEDED** run's upper bound, not the latest run's: a failed
run must not count as "we already rebuilt today", or the rebuild is skipped exactly when the
state is least trustworthy.

### `delete_policy: soft_flag` is REQUIRED, and refused at compile time

`latest_state` retains tombstones between rebuilds. `exclude_from_snapshot` says a deleted
key is excluded. The pair is contradictory, and permitting it would put deleted rows into a
layer whose readers do not filter `is_deleted` — **nothing in `stream_batch_flow`,
`flow_runner` or the dbt models filters it today**, so a deleted row would reach a mart.

`config_loader` refuses the pair, naming both the fix and the reason. A silent integration
bug becomes a red terminal at `make check`.

## Options

**A. Change `full_refresh` to collapse per key.** Rejected: it changes what every existing
consumer reads, and `transaction` genuinely wants the event window.

**B. Upsert only, no rebuild.** Rejected: cannot express ageing-out or a final delete, and
drift is permanent. This is the shape the user proposed and then corrected themselves.

**C. Physically delete on a delete event.** Rejected: the resurrection bug above. It was
implemented first and found in review before it shipped.

**D (chosen). Guarded upsert + tombstones + day-boundary rebuild**, gated on a compatible
delete policy.

## Consequences

* A table on `latest_state` carries flagged deletes between rebuilds. Its readers must filter
  `is_deleted` — which `soft_flag` already tells them.
* One extra ledger value (`refresh_mode`) and one extra read of `ops.realtime_run` per run.
* The mode is **opt-in and currently enabled on no table.** Turning it on for a table whose
  mart is live requires checking that mart's SQL first.

## Cost

Lower per run, once enabled: the upsert reads only the incremental slice instead of
re-reading and rewriting the whole window every 10 minutes. The boundary rebuild costs one
full-window pass per day. Against `full_refresh` at `*/10` — 144 full-window rewrites a day —
that is the point of the mode.

## Security

None. Same tables, same role, same encryption. A tombstone carries no data the live row did
not already carry.

## Rollback

Set `refresh_mode: full_refresh` and recompile. The next run overwrites the table with the
event window, which discards tombstones as a side effect. No migration.

## Validation

22 tests in `spark/tests/test_realtime_latest_state.py`:

* the order key is built from `ordering_for`, and its precedence matches DATA_CONTRACTS 4.1
  (position, then commit time, then partition, then offset — partition before offset because
  an offset is monotonic only within one)
* every component is coalesced, so a NULL never sorts high and lets an incomplete event win
* the two engines do not produce the same expression, and an unknown engine is refused
* the MERGE keys on `dv_pk_hash`, not `dv_event_id`, and is guarded by `>`
* the batch is collapsed to one row per key before the merge
* **no physical delete survives in the upsert path** — asserted against comment-stripped
  source, because the function's own comment explains the bug it avoids
* the rebuild fires on a new day, not within one, and only a SUCCEEDED run counts
* the `latest_state` + `exclude_from_snapshot` pairing is refused at compile time

Not yet run against live AWS: no table is configured for it.
