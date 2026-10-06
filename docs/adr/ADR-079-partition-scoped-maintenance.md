# ADR-079 — Maintenance is partition-scoped, per layer, and orphan removal outlives writers

* **Status**: Accepted (implementation: Phase G)
* **Date**: 2026-09-20
* **Extends**: ADR-068 (CDC table operations), ADR-062 (partitioning)
* **Evidence**: `cdc/maintenance.py`, `spark/ops/cdc_maintenance_job.py`,
  `spark/tests/test_maintenance_phase_g.py`

## Context

The metric-driven planner already existed and Phase A assessed it as KEEP: four actions,
thresholds anchored to the Phase 0 measurement (mean file 137 KiB, 13 manifests for 14 data
files), a cadence floor per temperature, `remove_orphan_files` opt-in, and `min_small_files:
8` so a two-file table never triggers a rewrite that costs more than it saves.

Three gaps remained.

1. **It planned per whole table.** `rewrite_data_files` ran with no `where`, so compacting
   FULL_CDC rewrote *every event ever captured* to fix today's small files — and each rewrite
   is a new snapshot holding the old files until expiry.
2. **The orphan threshold was not tied to writer duration.** `remove_orphan_files` deletes
   files that are unreferenced and older than a threshold. A file an in-flight job has
   written but not yet committed is exactly that.
3. **MART was not a maintained layer**, though a mart is an Iceberg table with the same
   small-file problem.

## Decision

### 1. Compaction is scoped by LAYER, because the layers differ in what they are

| layer | scope | why |
|---|---|---|
| FULL_CDC | recent `event_date` (3d) | Append-only canonical history. Only recent days receive writes, so only they accumulate small files; history is never rewritten (CLAUDE.md 5.2/5.3). |
| REALTIME | recent `event_date` (2d) | A bounded rolling window. Beyond the retention bound the prune deletes the rows anyway, so compacting there is work about to be discarded. |
| EOD `rolling_history` | recent `business_date` (7d) | A partition per business date; only recent ones move. |
| EOD `latest_state` | **whole table** | One COB *is* the table. Scoping would exclude the only data there is. |
| MART | recent `date_of_data` (7d), column configurable | No single shape; a mart that knows its hotspot names it. |

`compact_recent_days: 0` means the whole table **deliberately** — unscoped has to be a
decision someone wrote down, not an omission.

### 2. Orphan removal must outlive the longest writer, with margin

`orphan_min_age_hours = max_writer_hours × 2`. A threshold equal to the longest writer fails
the first time a writer is slower than its longest observed run. `validate_orphan_age`
refuses a configuration below the floor and says what the delete would destroy.

### 3. The predicate is passed as a double-quoted argument

`where => "event_date >= DATE '2026-09-17'"`. Single-quoting it and doubling the inner quotes
produces a string Spark unescapes into `event_date >= DATE 2026-09-17`, which Iceberg rejects
with `Cannot parse predicates in where option` — on EMR, after the job has acquired capacity.

## Options

* **Compact the whole table always.** Rejected — §1; it is the current behaviour and it pays
  the entire table's bytes to fix one day's.
* **Scope by a fixed number of days for every layer.** Rejected: `latest_state` EOD would
  compact nothing, and REALTIME would compact partitions the prune is about to delete.
* **Tie orphan age only to `expire_snapshots_days`.** Rejected: snapshot metadata retention
  and writer duration are unrelated quantities that happened to be ordered correctly.
* **Escape the predicate's quotes by doubling.** Rejected — §3, it does not work.

## Consequences

* A FULL_CDC compaction now rewrites ~3 days instead of the table. On the lab's data that is
  a small absolute saving and the right shape; on a year of history it is the difference
  between a maintenance window and an outage.
* `compact_recent_days`, `compact_partition_column` and `max_writer_hours` join the
  maintenance policy. All are optional and the defaults preserve today's behaviour except
  the scoping itself.
* MART is expressible; wiring reporting-config marts into the maintenance DAG is **not** done
  (open issue G3), because marts live in the reporting plan and the DAG maps the CDC plan.

## Cost

**Reduces cost.** Nothing new runs; the same actions run over fewer bytes. Maintenance stays
off by default (`ENABLE_CDC_TABLE_PLATFORM`).

## Security

No IAM, network or credential change. The scope predicate is built from a config column name
and a computed date, never from user input. Raw S3 objects are never deleted directly —
every deletion goes through an Iceberg procedure that knows what is referenced.

## Rollback

Set `compact_recent_days: 0` per table to restore whole-table compaction. The orphan
validation is a new refusal; raising `max_writer_hours` or `remove_orphan_files_days`
satisfies it.

## Validation

1. `spark/tests/test_maintenance_phase_g.py` — **24 passed**: layer policy (five layers
   incl. `latest_state` unscoped, configurable window, explicit zero, unknown layer refused,
   every scope explains itself), orphan safety (floor, refusal with the consequence, shipped
   default clears it, a longer writer raises it, the whole registry validated), the Iceberg
   call (predicate present, quotes intact, no empty `where`, target size carried), and
   **compaction on real Iceberg**: 8 controlled small files created and detected, compaction
   reduces file count and raises average size with every row preserved, a scoped compaction
   leaves an out-of-scope partition untouched, manifests rewritten, snapshots expired with
   the current one and all data retained.
2. **Two defects this phase found in its own new code**: `policy.get(...) or DEFAULT`
   swallowed an explicit `compact_recent_days: 0` (the same falsy-zero shape as ADR-070's
   `maintenance.actions: []`), and the doubled-quote escaping produced a predicate Iceberg
   refuses.
3. **Not claimed**: no maintenance run has executed against the live catalog, and
   `cdc_maintenance` has never run on the deployed Airflow.
