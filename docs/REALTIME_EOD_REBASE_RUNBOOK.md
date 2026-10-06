# Rebasing the REALTIME overlay onto a certified EOD close

ADR-085. Runs automatically; this is what to do when it does not.

---

## 1. What it is

```
current state  =  certified EOD baseline  +  REALTIME changes after its cutoff
```

When the close for business date D certifies, the changes between D-1 and D are now in the
baseline. A rebase removes them from the overlay and retains everything after D's cutoff.
Without it the two are composed and the same change is counted twice.

Only `shape: latest_state` tables rebase. An event window keeps events; a rebase there would
delete events the layer promised to serve, and the compiler refuses the pairing.

## 2. How it runs

`cdc_realtime_rebase`, one DAG, `30 3 * * *` — one hour after the shipped EOD cadences. It
maps only over tables whose plan carries `rebase_on_eod_certified`.

It runs on a **cadence, not a sensor**, deliberately: the rebase refuses on a close that is
not CERTIFIED, so an early run is a recorded skip rather than a wrong delete. A sensor per
table would add a poking task for an operation whose own precondition check is already the
authority.

By hand:

```bash
spark-submit spark/jobs/realtime/realtime_engine.py \
  --plan s3://<lake>/artifacts/cdc/table-plan.json \
  --warehouse s3://<lake>/warehouse \
  --table ALL --rebase-cob AUTO
```

`AUTO` derives each table's COB from **its own** business timezone and lag. A date computed
by the scheduler would apply the scheduler's timezone to every table (ADR-065 §2). Pass an
explicit `YYYY-MM-DD` only when rebasing a specific historical close.

## 3. Verifying it happened

```sql
SELECT table_id, baseline_cob_date, last_success_run_id, updated_at
FROM   kafka_dev_lab_dev_ops.realtime_info;

SELECT run_id, table_id, prev_baseline_cob_date, baseline_cob_date,
       source_snapshot_before AS frozen_target_snapshot,
       input_rows AS candidates, pruned_count AS removed, row_count AS retained
FROM   kafka_dev_lab_dev_ops.realtime_run
WHERE  operation = 'rebase' ORDER BY started_at DESC LIMIT 10;
```

That second query is the evidence the R2 brief §16 asks for, in one row: both baselines, the
frozen upper snapshot, rows removed and retained, and the run.

## 4. When it skips

`REALTIME_REBASE_SKIPPED <table> cob=<date> reason=<reason>`. Every reason is a refusal to
delete something the baseline may not hold:

| reason | fix |
|---|---|
| `eod_not_certified` | the close is missing or not CERTIFIED. `docs/EOD_CONTROL_PLANE.md` §5 |
| `eod_has_no_cutoff` | `eod_info.cutoff_ts_utc` is null — the close recorded nothing about what it contains |
| `already_at_this_baseline` | nothing to do |
| `baseline_would_go_backwards` | an older COB was requested. Its cutoff is earlier, so it would remove rows the current baseline does not hold |
| `not_latest_state` / `not_enabled` | by design |

**A skip is the safe outcome.** The overlay un-rebased is merely larger; composing it with
the baseline stays correct, because the overlay's rows are all newer than the baseline's
cutoff either way.

## 5. The race, and why it is safe

The overlay keeps accepting events throughout the rebase. So:

1. the keys to remove are read from a **frozen** snapshot of the target (`VERSION AS OF`)
2. the delete matches on `dv_pk_hash` **and** `dv_event_id`

A key that receives a new event between the read and the delete no longer matches and
survives. The delete removes only what it actually looked at.

## 6. If a rebase removed a post-cutoff change

That is a defect, not an operational state. Before rebuilding, capture the evidence:

```sql
SELECT * FROM kafka_dev_lab_dev_ops.realtime_run
WHERE operation = 'rebase' AND table_id = '<id>' ORDER BY started_at DESC LIMIT 1;
SELECT * FROM <realtime_target>.snapshots ORDER BY committed_at DESC LIMIT 10;
```

Then recover — the layer is always rebuildable from FULL_CDC:

```bash
... realtime_engine.py --table <id> --rebuild
```
