# REALTIME failure recovery

ADR-083/084/085. Every failure mode here has one property in common: **none of them is a
crash.** A wrong cursor, a resurrected key, an unpruned window — each produces a table that
reads fine and holds the wrong rows.

---

## 1. First question: what did the run actually do?

```sql
SELECT run_id, operation, status, read_mode, fallback_reason,
       source_snapshot_before, source_snapshot_id, input_rows, row_count,
       pruned_count, attempt, failure_reason, started_at, finished_at
FROM   kafka_dev_lab_dev_ops.realtime_run
WHERE  table_id = '<id>'
ORDER BY started_at DESC LIMIT 20;
```

And the current state:

```sql
SELECT * FROM kafka_dev_lab_dev_ops.realtime_info WHERE table_id = '<id>';
```

`realtime_run` is every attempt, forever. `realtime_info` is one row: where the cursor is
now. If they disagree about the last snapshot, the info write failed — see §4.

## 2. "The run is suddenly slow / expensive"

Look at `read_mode` and `fallback_reason`.

| `fallback_reason` | what happened | what to do |
|---|---|---|
| `unsafe_snapshot` | a compaction (`rewrite_data_files`) landed in the range. An append scan cannot see rows that moved between files | **nothing.** One expensive run after each maintenance window is the design |
| `cursor_expired` | `expire_snapshots` removed the cursor's snapshot | nothing, once. If it repeats, `maintenance.expire_snapshots_days` is shorter than the gap between runs |
| `cursor_ahead_of_source` | FULL_CDC was rolled back | investigate the rollback. The layer self-heals; the source may not have |
| `no_cursor` | first run after enabling, or after `realtime_info` was cleared | expected once |
| `forced_rebuild` | day-boundary rebuild, `--rebuild`, or `--force-window-scan` | expected |
| `write_strategy_rebuilds` | the table writes with `overwrite_window` | expected — that strategy always reads the whole window |
| `not_configured` | `source_progress: window_scan` | expected |

Repeated `unsafe_snapshot` on every run means maintenance is running too often relative to
the cadence, not that the cursor is broken.

## 3. "The table stopped changing"

```sql
SELECT status, read_mode, input_rows, updated_at
FROM   kafka_dev_lab_dev_ops.realtime_info WHERE table_id = '<id>';
```

* `status = 'SUCCEEDED_NOOP'`, repeatedly → **the SOURCE has not moved.** The cursor equals
  FULL_CDC's current snapshot. This is an ingestion problem, not a REALTIME one: check the
  streaming app (`docs/FULL_CDC_STREAMING_RUNBOOK.md`).
* `status = 'FAILED'` → `failure_reason` in `realtime_run`. The cursor was **not** advanced,
  so a retry re-reads the same range.
* rows present but stale, `status = 'SUCCEEDED'` → the window moved past them and the prune
  removed them. Check `pruned_count`.

A NOOP still prunes, deliberately: retention follows the clock, not the arrival of data.

## 4. `REALTIME_INFO_WRITE_FAILED` in the driver log

The target committed and the cursor did not advance. The next run re-reads a range it
already incorporated, which is why both write paths are idempotent:

* `guarded_merge` — re-applying is a no-op; the order-key guard rejects what it already has
* `append` — the MERGE keys on `dv_event_id`, so a replayed event does not duplicate

**Do nothing.** It converges on the next run. If it does not, the ledger's
`source_snapshot_before` for two consecutive runs will show the same value twice.

## 5. `REALTIME_COMMIT_UNVERIFIED`

The write returned cleanly and the target's snapshot did not move. The cursor is **not**
advanced. Check that the target is not read-only and that the job role can write to it; the
next run re-reads the same range.

## 6. A key came back after being deleted

This is the failure `latest_state` exists for, and if it happens the guard was bypassed.
Check, in order:

1. Is the target's row a tombstone (`op = 'd'`)? It must be present, not absent.
2. Did something run `DELETE FROM <target> WHERE op = 'd'`? That is the resurrection bug:
   removing the tombstone leaves nothing for the next batch's guard to compare against.
3. Was `spark/jobs/realtime/job.py` pointed at this table? It writes the raw event window
   and refuses a `latest_state` target — but only when the compiled plan is readable.

Recovery: `--rebuild`. It re-ranks the whole window from FULL_CDC in one pass, which fixes
tombstones, aged-out keys and any drift at once.

```bash
spark-submit spark/jobs/realtime/realtime_engine.py \
  --plan s3://<lake>/artifacts/cdc/table-plan.json \
  --warehouse s3://<lake>/warehouse --table <id> --rebuild
```

## 7. The cursor is suspect and you want the safe path

```bash
... realtime_engine.py --table <id> --force-window-scan
```

Ignores the cursor, re-reads the whole window, advances the cursor to the frozen snapshot.
It costs what the layer cost before R2-C, and it is always correct.

## 8. A rebase removed too much / too little

`--rebase-cob` refuses far more often than it acts, so "nothing happened" is the normal
outcome. `REALTIME_REBASE_SKIPPED` names the reason:

| reason | meaning |
|---|---|
| `eod_not_certified` | the close for that COB is not CERTIFIED. Fix the close first |
| `eod_has_no_cutoff` | `eod_info.cutoff_ts_utc` is null, so what the baseline contains is unknown |
| `already_at_this_baseline` | idempotent no-op |
| `baseline_would_go_backwards` | an older COB was requested; its earlier cutoff would remove rows the current baseline does not hold |
| `not_latest_state` | an event window has no baseline |
| `not_enabled` | `rebase.on_eod_certified` is false |

If a rebase removed a post-cutoff change, that is a genuine defect — the delete matches on
`dv_pk_hash` **and** `dv_event_id` against a frozen read precisely so it cannot. Capture
`source_snapshot_before` from the `operation = 'rebase'` ledger row and the target's snapshot
history before rebuilding.

## 9. Everything is wrong and you want to start over

REALTIME is always rebuildable from FULL_CDC — that is the property the whole design
preserves.

```bash
# 1. clear the cursor so the next run is a full scan
DELETE FROM kafka_dev_lab_dev_ops.realtime_info WHERE table_id = '<id>';
# 2. rebuild
... realtime_engine.py --table <id> --rebuild
```

Nothing in FULL_CDC is touched, and no Kafka offset moves.
