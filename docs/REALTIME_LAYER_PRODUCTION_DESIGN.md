# REALTIME layer — production refinement design (R2-A)

**Audit only. No code changed.** Every "current state" claim below was read out of the code,
the compiled plan or the live lake, not inferred from a name.

Related: ADR-064 (config-driven REALTIME), ADR-081 (latest_state), ADR-033, ADR-071.

---

## 1. CURRENT STATE

### 1.1 What the layer is today

`spark/jobs/realtime/realtime_engine.py`, three steps per run:

```python
window_rows()   # FULL_CDC filtered: source_commit_ts >= grace_lower AND < upper
materialise()   # rows.writeTo(target).overwrite(lit(True))
prune()         # DELETE WHERE source_commit_ts < retention_bound
```

**It is a bounded EVENT WINDOW, not a state table.** No ranking, no collapse per key. A row
updated three times inside the window appears three times. The collapse happens downstream in
`transform.collapse_to_grain`.

### 1.2 Answers to the twenty audit questions

| # | question | finding |
|---|---|---|
| 1 | REALTIME tables | 7 of 10 (`rt_*` in `kafka_dev_lab_dev_stream`) |
| 2 | refresh modes | **all 10 on `full_refresh`**; `latest_state` exists (ADR-081) but is enabled nowhere |
| 3 | shape | event window only, in practice |
| 4 | write strategy | `overwrite(lit(True))` — full replace of DATA, table definition preserved |
| 5 | **scans T-N→T each run?** | **YES.** `window_rows` filters `source_commit_ts` over `[upper−96h, upper)`. Every 10 minutes. |
| 6 | source cursor | **none.** `current_snapshot_id()` is RECORDED in the ledger and never used as a read boundary |
| 7 | schedules | identical `*/10 * * * *` for all 10 |
| 8 | Airflow model | one `cdc_realtime` cadence DAG, dynamic task mapping over the plan (ADR-071) |
| 9 | partition spec | `realtime_policy.partition_spec: []` → inherits the table default, `event_date` identity. Live S3 confirms `data/event_date=2026-09-17/…` |
| 10 | file sizes/counts | not measurable now — Glue is empty after the 28 Sep rebuild |
| 11 | maintenance | metric-driven planner; **`cdc_maintenance` DAG does not pass `--metrics`, which the job declares `required=True`** — a scheduled run would fail at argparse |
| 12 | STREAM_BATCH consumers | **all 4 reporting jobs**, every one `source_layer_policy: REALTIME` |
| 13 | AUTO_CORRECT | reads FULL_CDC. No REALTIME dependency |
| 14 | **EOD dependence** | **zero.** `eod_engine.py:12` states it, and a test asserts the file never reads that layer |
| 15 | FULFILL | FULL_CDC / EOD. No REALTIME dependency |
| 16 | STREAMING_RT | separate: `FULL_CDC_APPEND`, own `is_enabled` + `ENABLE_STREAMING_RT` gate |
| 17 | control tables | `ops.realtime_run` (append ledger) only. **No `ops.realtime_info`** |
| 18 | rebase semantics | **none.** No EOD-certified rebase exists |
| 19 | tables not needing REALTIME | see §3 |
| 20 | saving from disabling | see §12 |

### 1.3 The gap list

| brief § | required | today |
|---|---|---|
| 9 | incremental snapshot cursor | ✗ full window scan every run |
| 6 | `shape` + `write_strategy` split | ✗ single `refresh_mode` |
| 12 | latest_state retention ≠ event_window retention | ✗ one `prune()` for both |
| 17–18 | EOD-certified rebase, race-safe | ✗ absent |
| 21–22 | `REQUIRED_REALTIME` / explicit fallback | ✗ no such field anywhere |
| 31 | `ops.realtime_info` current state | ✗ only the run ledger |
| 42–44 | impact report / disable preview | ✗ absent |
| 4 | disabled ⇒ zero tasks | ~ partial: `enabled:false` is honoured by the engine; **not verified that the DAG emits no mapped task** |

---

## 2. TARGET STATE

Unchanged from the brief, with one correction of emphasis: today's `full_refresh` is already
**idempotent and self-healing** — running it twice produces the same table, and it ages rows
out for free. An incremental cursor trades that for cost. The design must keep a rebuild path
(§11 recovery) precisely because the incremental path gives it up.

---

## 3. TABLE CLASSIFICATION

Derived from downstream code, not names. **All four reporting jobs declare
`source_layer_policy: REALTIME` for STREAM_BATCH**, so REALTIME is currently load-bearing for
every mart that has an intraday mode.

| table | class | evidence |
|---|---|---|
| `oracle.coredb.corebank.account` | **REALTIME_REQUIRED** | `mart_account_balance_daily`, `_monthly`, `_risk_daily` all STREAM_BATCH on REALTIME |
| `oracle.coredb.corebank.customer` | **REALTIME_REQUIRED** | `mart_customer_360_daily` lineage; mutable entity |
| `oracle.coredb.corebank.transaction` | **REALTIME_REQUIRED** | highest volume; intraday marts |
| `sqlserver.digital.dbo.digital_event` | **REALTIME_REQUIRED** | `mart_channel_engagement_daily` STREAM_BATCH |
| `oracle.coredb.corebank.loan` | **REALTIME_OPTIONAL** | enabled, but no mart reads it intraday today |
| `sqlserver.digital.dbo.app_user` | **REALTIME_OPTIONAL** | enabled; feeds engagement only via a join |
| `sqlserver.digital.dbo.payment_method` | **REALTIME_OPTIONAL** | enabled; no intraday consumer found |
| `oracle.coredb.corebank.branch` | **REALTIME_NOT_NEEDED** | already `enabled:false`; 4 rows, near-static |
| `sqlserver.digital.dbo.channel` | **REALTIME_NOT_NEEDED** | already `enabled:false`; 4 rows |
| `sqlserver.digital.dbo.merchant` | **REALTIME_NOT_NEEDED** | already `enabled:false`; 50 rows |

**One UNKNOWN worth resolving before R2-H.** `mart_channel_engagement_daily` declares
STREAM_BATCH over REALTIME, and `sqlserver.digital.dbo.channel` has `realtime.enabled: false`.
Whether that is a latent contradiction depends on whether the flow resolves the *channel*
table or only `digital_event`. It is exactly the case §21 says must fail compile, and today
nothing checks it. **Classified UNKNOWN, not benign.**

Shape, once §6 lands: `account`, `customer`, `loan`, `app_user` → `latest_state`;
`transaction`, `digital_event` → `event_window`.

---

## 4. CONFIG DESIGN

Adopt the brief's `shape` / `write_strategy` split. Map the existing values rather than
inventing a parallel schema:

```
refresh_mode: full_refresh   ->  shape: event_window, write_strategy: append
refresh_mode: latest_state   ->  shape: latest_state, write_strategy: guarded_merge
refresh_mode: incremental_merge -> shape: event_window, write_strategy: append   (dedup on dv_event_id)
```

`cdc/config_loader.py` already refuses an unknown `refresh_mode` and already enforces one
cross-field rule — `latest_state` requires `delete_policy: soft_flag`, because tombstones are
retained between rebuilds and nothing downstream filters `is_deleted`. That rule is the
template for the §20 validations.

Keep a backward-compatible parser that accepts `refresh_mode` and emits a deprecation line in
the compile report. The compiled plan is deployed to S3 and read by running jobs; a hard cut
would strand any job started from an older artifact.

---

## 5–8. SHAPES

`latest_state` landed in ADR-081 and already implements the brief's §13–16:

* dedup per `dv_pk_hash` via `row_number()` before the merge
* ordering from `cdc.eod.ordering_for` — **the same ranking EOD uses**, so a
  provisional-vs-certified variance can only mean late data, never divergent logic
* guarded `WHEN MATCHED AND {src_key} > {tgt_key}`
* deletes as **tombstones**, not physical deletes
* day-boundary rebuild

What is missing against §12: `prune()` applies `retention_bound` to **both** shapes. For
`latest_state` that is wrong — an account unchanged for 60 days is still current. R2-D must
make retention shape-aware: event_window prunes by event time; latest_state prunes only on
supersede, delete, or EOD rebase.

---

## 9–11. INCREMENTAL CURSOR

The single largest change. Today: `source_commit_ts >= grace_lower AND < upper`, a 96-hour
scan, 144 times a day, whose cost grows with history while the window it serves does not.

Target: read Iceberg snapshots `(last_source_snapshot_id, current_source_snapshot_id]`.

Two preconditions the brief is right to demand (§10):

1. **FULL_CDC must be append-only for the incremental scan to be complete.** The ingest is
   append-only by contract, but `--migration-mode dual_write` and any compaction produce
   non-append snapshots. `rewrite_data_files` creates a REPLACE snapshot, and an incremental
   scan across one either errors or silently skips. Maintenance runs against FULL_CDC, so
   **this will happen** — it is not hypothetical.
2. Therefore the fallback in §10 is mandatory, not optional, and its reason must be recorded
   in `realtime_info`.

`current_snapshot_id()` already exists in the engine and is already written to the ledger —
the cursor is half-built. What is missing is reading from it.

---

## 12. LATEST_STATE RETENTION

See §5–8. Additionally: `retention_hours >= window_hours + late_grace_hours` is enforced at
compile time today. That constraint is meaningful only for `event_window`; for `latest_state`
it should be refused as inapplicable rather than silently satisfied.

---

## 17–18. EOD REBASE

Absent today. The race in §18 is real and the platform already has the primitives to make it
safe: EOD freezes `cutoff_utc` and records `source_snapshot_id` + `target_snapshot_id` in
`eod_info` on certification. A rebase must:

* take its upper bound from the **certified EOD's recorded snapshot**, not from the clock
* retain every REALTIME row whose order key is newer than that bound
* be idempotent — rerunning the same rebase is a no-op

Test the race by certifying EOD D while a D+1 event is mid-flight.

---

## 28–30. AIRFLOW

`cdc_realtime` is already one cadence DAG with dynamic mapping over the plan, and
`CDC_PLATFORM_ENABLED` gates the schedule. Two things to verify in R2-G rather than assume:

* a table with `realtime.enabled:false` produces **zero mapped tasks** (asserted nowhere today)
* nothing at DAG-parse time touches Glue/Athena/Iceberg. The DAG reads the compiled plan,
  which satisfies §30, but `_resource_profile()` should be re-read for I/O.

Per-table schedules (§29) need `schedule_groups` from `cdc/scheduling.py`, already used by
`cdc_eod`. All ten tables currently share `*/10`, so grouping is untested for REALTIME.

---

## 21–27. DOWNSTREAM IMPACT

**Nothing declares a REALTIME requirement today.** No `source_policy`, no
`REQUIRED_REALTIME`, no `fallback` — a grep across `reporting/jobs/*.yaml` and
`job.schema.json` returns nothing. So §21's "compile MUST fail" has no mechanism, and the
`mart_channel_engagement_daily` / `channel` case in §3 would pass compile today.

Confirmed independent of REALTIME, with evidence:

* **EOD** — `eod_engine.py:12` and a test asserting the file never reads that layer
* **AUTO_CORRECT** — reads FULL_CDC as its change feed
* **FULFILL** — FULL_CDC / EOD
* **STREAMING_RT** — `FULL_CDC_APPEND`, separate gate

R2-H must add regression tests that each still works with `realtime.enabled:false`, per §23.

---

## 31–35. CONTROL TABLES

`ops.realtime_run` exists as an append ledger with the right columns
(`logical_lower_bound`, `grace_lower_bound`, `upper_bound`, `retention_bound`,
`source_snapshot_id`, `target_snapshot_id`, `row_count`, `pruned_count`, `status`,
`config_version`). **`ops.realtime_info` does not exist.**

This mirrors exactly what EOD already learned (ADR-076): an append log asked a state question
answers "the latest row that happens to be SUCCEEDED", a convention no column enforces. Build
`realtime_info` as the current-state twin and keep `realtime_run` as history — same split,
same reasons, and reuse `cdc/eod.py`'s upsert shape rather than inventing a second one.

§33's rule — advance the cursor only after read + commit + validation — is the property that
makes a crash safe. §35's crash-after-commit case is why `realtime_info` must record
`target_snapshot_id`: on retry, a committed snapshot that the control table does not know
about is detectable.

---

## 38–41. PARTITION AND MAINTENANCE

Realtime targets inherit `event_date` identity partitioning. For `latest_state` that is
questionable — one current row per key, rewritten in place — and §38 is right that it needs
measuring rather than assuming. `scripts/layout-benchmark.py` already compares identity,
`days()` and `bucket(16/32/64)`; extend it with an unpartitioned latest-state case.

**Maintenance defect found during this audit:** `cdc_maintenance` builds its args as
`--plan --warehouse --table [--max-tables]` and never passes `--metrics`, which
`cdc_maintenance_job.py` declares `required=True`. A scheduled run dies at argparse before
touching a table. Nothing produces the metrics in the DAG — I ran `measure` by hand when
proving maintenance live on 23 Sep. R2-I must add a `measure` task upstream of the mapped
maintenance tasks.

---

## 46–47. PERFORMANCE AND COST PLAN

Measure old vs new on small (`branch`, 4 rows), medium (`account`, 320), large
(`digital_event`, 3,000): rows read, files scanned, bytes, runtime, shuffle, target files
rewritten, snapshots created. **No improvement claimed without these numbers.**

Expected saving from the three `NOT_NEEDED` tables today: they are already disabled, so the
saving is already banked — 3 of 10 tables produce zero REALTIME runs. The remaining lever is
the incremental cursor on the 7 enabled ones: 144 runs/day each currently re-reading 96 hours.

---

## MIGRATION AND ROLLBACK

Migration is additive at every phase: `shape`/`write_strategy` accept the old `refresh_mode`;
`realtime_info` is written alongside `realtime_run` before anything reads it; the incremental
cursor falls back to the window scan whenever the snapshot range is unusable.

Rollback is `refresh_mode: full_refresh` (or `shape: event_window`) plus a recompile. The next
run overwrites the target with the window and discards tombstones as a side effect. No data
migration, because FULL_CDC remains canonical and REALTIME is always rebuildable from it —
which is the property that makes every phase here reversible.

---

## RISKS

1. **Incremental correctness beats incremental cost.** A missed snapshot is a silently
   incomplete layer; the current full scan cannot be incomplete. The fallback must be
   conservative and recorded.
2. **All four marts currently depend on REALTIME.** §4's "disabled ⇒ zero impact" is true for
   EOD/AUTO_CORRECT/FULFILL and false for STREAM_BATCH until §22's explicit fallback exists.
3. **`latest_state` is unproven live.** Implemented and tested (22 tests), enabled on no table.
4. The lake was destroyed and rebuilt on 23 and 28 Sep; Glue is currently empty, so file-count
   and layout measurements must be retaken after the next ingest.

---

REALTIME_LAYER_REFINEMENT_DESIGN_READY

---

# R2-B — config migration (DELIVERED)

ADR-082. `refresh_mode` is split into `shape` (the contract) and `write_strategy` (the
mechanism), and the compiler enforces the pairs. Full field reference:
`docs/CDC_TABLE_CONFIG_REFERENCE.md` § "REALTIME shape and write strategy".

## What landed

| change | file |
|---|---|
| `SHAPES`, `WRITE_STRATEGIES`, `SOURCE_PROGRESS_MODES`, `resolve_shape`, `prunes_by_age` | `cdc/realtime.py` |
| `RealtimePolicy` gains 6 fields; `refresh_mode` becomes a **derived property** | `cdc/models.py` |
| nested `processing` / `recovery` / `merge` / `rebase` blocks, spelling-aware inheritance, `latest_state` PK rule | `cdc/config_loader.py` |
| `shape` / `write_strategy` / `source_progress` / `prunes_by_age` in the plan | `cdc/table_plan.py` |
| deprecations shown, **outside** the hashed payload | `cdc/compile.py` |
| `SHAPE / WRITE`, `SOURCE PROGRESS`, `EOD REBASE` rows | `scripts/cdc-table-plan.py` |
| `defaults:` declare the new spelling; LOAN's days move under `recovery:` | `cdc/registry/sources.yaml` |
| 36 tests | `spark/tests/test_realtime_config.py` |

## What deliberately did NOT land

**No table's behaviour changed.** All ten remain `event_window` / `overwrite_window`, which
is what they already did — asserted by
`TestThisPhaseChangedNoBehaviour::test_every_shipped_table_is_still_an_overwritten_event_window`.

The `latest_state` flips named in §3 belong to **R2-D**, with the engine and the tests that
make them safe. Flipping `account`, `customer`, `loan` and `app_user` here would also change
their EOD `delete_policy` to `soft_flag` — a live change to snapshot semantics on a platform
that is mid-rebuild and has not ingested since 28 Sep.

## Two things worth carrying into R2-C

1. **`recovery:` is proven equivalent by `plan_hash`, not by review.** Moving LOAN's day
   fields under it produces a byte-identical hash. Every later phase that claims "a rename,
   not a behaviour change" should be provable the same way.
2. **Forward-looking values are compile errors naming their phase.** `iceberg_snapshot` →
   R2-C, `rebase.on_eod_certified: true` → R2-F. R2-C's first commit is deleting its refusal.

REALTIME_CONFIG_READY

---

# R2-C — incremental cursor and control plane (DELIVERED)

ADR-083. `ops.realtime_info` holds one MERGEd row per table with the cursor;
`cdc/realtime_state.plan_read()` decides how each run reads, as a pure function.

**Five conditions make a cursor untrustworthy, and all five fall back to the full window
scan with a recorded `fallback_reason`:** a `replace` snapshot in the range (weekly
compaction), an expired cursor snapshot, a rolled-back source, no cursor, and
`write_strategy: overwrite_window`. The last is also refused at compile — an overwrite fed
an incremental delta deletes every row the delta does not contain.

The cursor advances **last**, and only on a verified commit. A NOOP never advances it.

`REALTIME_INCREMENTAL_CURSOR_READY`

---

# R2-D — latest_state (DELIVERED)

ADR-082 §dispatch. The engine now dispatches on `shape`/`write_strategy`. `account`,
`customer`, `loan` and `app_user` are `latest_state` + `guarded_merge` + the cursor +
`eod.delete_policy: soft_flag`.

**Two defects in the existing `latest_state` code were found by writing Spark-level tests,
and neither was reachable by the SQL-text tests that existed:**

1. `SELECT * EXCEPT (_rt_rank)` is **Databricks SQL**. Open Spark 3.5 — what EMR Serverless
   7.2 runs — rejects it with a PARSE_SYNTAX_ERROR. The shape had never run. Replaced with
   the DataFrame API, which needs no dialect support.
2. The rebuild branch filtered `is_deleted`, a column REALTIME **does not have** — it
   carries the FULL_CDC row shape, which has `op` (`cdc/rowspec.py`). Replaced with one
   `is_tombstone()` helper so the two shapes cannot be confused again.

The R2-A audit recorded `latest_state` as "implemented and tested, enabled nowhere". It was
more precisely *unrunnable*, and only an assertion about the table's contents could show it.

`REALTIME_LATEST_STATE_READY`

---

# R2-E — event window on the cursor (DELIVERED)

ADR-084. `transaction`, `digital_event` and `payment_method` are `append` + the cursor, with
`retention_hours: 96` so the physical extent equals the materialised window — under
`overwrite_window` the table WAS the window, and left at 168 the append path would have
silently started keeping three days the layer never promised.

**A NOOP now prunes.** Found by a test: retention follows the *clock*, not the arrival of
data, so a table whose source went quiet kept serving rows outside its window indefinitely
while reporting SUCCESS every ten minutes.

**`--rebuild` is a flag, not a change of shape.** It used to rewrite the entry to
`event_window`/`overwrite_window`, which would have replaced a `latest_state` table's
contents with the raw event window — many rows per business key in a table whose contract is
one — and reported SUCCESS.

`REALTIME_EVENT_WINDOW_READY`

---

# R2-F — EOD-certified rebase (DELIVERED)

ADR-085. `--rebase-cob AUTO` removes the overlay rows a newly certified close already
contains. Six guards, all refusing to delete something the baseline may not hold. The delete
matches on `dv_pk_hash` **and** `dv_event_id` against a frozen `VERSION AS OF` read, so an
event landing mid-rebase survives.

**A real timezone defect was found here.** `collect()` renders a Spark TIMESTAMP into a
naive Python datetime in the **driver's** local zone; formatting it back into a SQL literal,
which Spark reads in the *session* zone, shifts it by the driver's offset. On an EMR driver
in UTC nothing happens; from a laptop in UTC+7 the rebase ran against a cutoff seven hours
late and deleted rows the baseline did not contain. The same defect was latent in
`last_successful_upper`, which fed the day-boundary rebuild comparison.

`REALTIME_EOD_REBASE_READY`

---

# R2-G — Airflow (DELIVERED)

One `cdc_realtime_rebase` DAG at `30 3 * * *`, mapping only over tables that declare the
rebase. A cadence rather than a sensor: the rebase refuses on an uncertified close, so an
early run is a recorded skip.

Asserted rather than assumed: a disabled table gets **zero** mapped tasks (a skipped
submission still starts an EMR application and bills for it), the DAG count stays below the
table count, the rebase derives its COB **inside the job**, and DAG import does no AWS or
catalog I/O.

`REALTIME_AIRFLOW_READY`

---

# R2-H — downstream and compiler (DELIVERED)

ADR-086, and it resolves R2-A's one UNKNOWN. A STREAM_BATCH mode declares `source_tables`;
the compiler refuses when any of them has REALTIME off and no `source_policy.fallback` is
declared. `mart_channel_engagement_daily` now declares `channel` + an explicit
`fallback: FULL_CDC`, and `channel` stays disabled — it is a four-row reference table.

`scripts/cdc-table-plan.py --set realtime.enabled=false` previews the blast radius,
read-only, naming the modes that would fail to compile.

`REALTIME_DOWNSTREAM_INTEGRATION_READY`

---

# R2-I — maintenance and cost (DELIVERED, benchmark NOT_MEASURED)

`cdc_maintenance` **measures** file/manifest/snapshot metrics from each table's own Iceberg
metadata and covers `FULL_CDC,REALTIME`, skipping REALTIME-disabled tables. `--metrics` was
a required flag that nothing produced and the DAG never passed, so every scheduled run would
have died at argparse.

`scripts/realtime-benchmark.py` reports the config-derived cost model offline and the
measured read comparison out of `ops.realtime_run`. **The measured half is NOT_MEASURED**:
the lake has not ingested since 28 Sep, so the ledger is empty. The script prints
NOT_MEASURED, not zero. Partition-spec benchmarking (§40–41) is likewise not run, so no
partition change was made.

`REALTIME_PERFORMANCE_VALIDATED` — **NOT claimed.** See the final verdict.

---

# R2-J — end to end, and the verdict

`spark/tests/test_realtime_e2e.py` runs three tables through the whole layer against a real
Iceberg catalog, with a **real** `eod_engine.close_table` — not a stubbed control plane:

| table | shape | proven |
|---|---|---|
| A `account` | `latest_state` | collapse per key, incremental delta, late-older-update guarded, delete→tombstone, tombstone survives a late insert, rebase onto a real certified close |
| B `transaction` | `event_window` | events kept, incremental append, does not rebase |
| C `branch` | disabled | no REALTIME rows, **no ledger row at all**, and its EOD close still CERTIFIED |

Plus: no DAG file names a source table, one config-driven entrypoint, the legacy manual one
refuses a `latest_state` target, and the DAG count is bounded.

## LIVE EVIDENCE — 2026-09-28, account 111122223333, ap-southeast-1

Everything below was run on the deployed platform, not in a test.

| # | what | evidence |
|---|---|---|
| 1 | control plane provisioned | `realtime_info` created (23 cols); `realtime_run` **evolved 20 → 31 cols** via `provision --execute --evolve` |
| 2 | `latest_state` = one row per business key | account **320**, customer **200**, loan **50**, app_user **150** — each equal to its distinct key count |
| 3 | `event_window` keeps every event | transaction **2000**, digital_event **3000**, payment_method **50** |
| 4 | disabled ⇒ zero work | `REALTIME_SKIPPED … reason=realtime_disabled` for branch, channel, merchant; **no ledger row exists for any of them** |
| 5 | empty delta ⇒ NOOP | `REALTIME_NOOP … source unchanged at 7019155825130461249` on 4 tables; 12 NOOP runs in the ledger |
| 6 | **incremental read** | `read=incremental … cursor=4124040414667009057 -> 917912465318976243`; `REALTIME_SUMMARY … incremental=2` |
| 7 | fallbacks recorded as data | ledger: `forced_rebuild` 4, `no_cursor` 3 |
| 8 | EOD independent of REALTIME | `EOD_SUMMARY closed=10 certified=0` — all ten closed, including the three REALTIME-disabled ones |
| 9 | **rebase refuses without a certified close** | `REALTIME_REBASE_SUMMARY cob=AUTO rebased=0 skipped=10 removed=0`; reasons `eod_not_certified` (4 state tables) and `not_latest_state` (6 others) |
| 10 | benchmark measured | §4 above — window_scan 824 rows/run avg, incremental 0 |
| 11 | partition layout measured | §5 above — every table **1 partition**, 4–5 sub-MB files |

### The last gate CLOSED — 2026-09-29, live, on the real clock

The rebase's action path was the one outstanding gate. It is now proven on AWS with a
genuinely CERTIFIED close and **no simulated clock anywhere**.

```
REALTIME_REBASED oracle.coredb.corebank.customer  baseline None -> 2026-09-28
  cutoff=2026-09-29 00:00:00  frozen_snapshot=7785600793737563101
  candidates=200 removed=200 retained=0

REALTIME_REBASED sqlserver.digital.dbo.app_user   baseline None -> 2026-09-28
  cutoff=2026-09-29 00:00:00  frozen_snapshot=7925425841445435971
  candidates=150 removed=150 retained=0

REALTIME_REBASE_SUMMARY cob=2026-09-28 rebased=2 skipped=8 removed=350
```

`ops.realtime_run` under `operation = 'rebase'`, and `ops.realtime_info`:

| table | operation | baseline | frozen snapshot | candidates | removed | retained | status |
|---|---|---|---|---|---|---|---|
| `…corebank.customer` | rebase | 2026-09-28 | 7785600793737563101 | 200 | 200 | 0 | SUCCEEDED |
| `…dbo.app_user` | rebase | 2026-09-28 | 7925425841445435971 | 150 | 150 | 0 | SUCCEEDED |

`removed = candidates` is correct, not suspicious: every overlay row predates the cutoff, so
the certified baseline now holds all of it and the overlay correctly empties. Current state =
baseline + empty overlay = the 200 and 150 rows in EOD.

**The eight skips are the guards, each for its own reason** — `account` and `loan` with
`eod_not_certified` (their closes were `BUILT_NOT_CERTIFIED`), the other six with
`not_latest_state`.

#### What the certified close required, and one thing I predicted wrong

`EOD_SUMMARY` for COB 2026-09-28 certified **five** tables: customer 200, app_user 150,
channel 4, digital_event 3000, payment_method 50.

I expected `account` to certify and it did not — `rows=0`. The close applies an **`event_date`
partition prune of ±1 day around the COB** *on top of* the `source_commit_ts < cutoff`
filter (`eod_engine.py::window_events`). The recovered account events carry
`event_date = 2026-09-20`, so they belong to COB 2026-09-20 and are pruned out of the 28th.
I had checked the timestamps against the cutoff but not against the prune.

That is the prune behaving as its own docstring describes, not a defect. The rule worth
carrying: **closing a COB sees only events DATED to that COB**, not everything older than
its cutoff.

`--skip-readiness` was used and is legitimate here, and it is NOT the simulated-clock
shortcut this document previously refused. The day genuinely ended 16 hours earlier on the
wall clock; the open-day guard passed on its own. Only the source-readiness gate was skipped,
and its purpose — do not close before the source has caught up past the cutoff — is satisfied
differently: the source is idle and holds nothing newer.

### Recovering the lake first

None of the above was reachable until the catalog was rebuilt. `terraform destroy/apply` on
29 Sep took the Glue databases and left every S3 object, and minted a new lake CMK while the
old one still encrypted all 6,198 objects. Order that worked:

1. `reencrypt-lake-cmk.sh reencrypt --execute` — 5,357 objects rewritten, 0 failed
2. `register_tables.py` — **73 tables** adopted: full_cdc 11, stream 11, snapshot 10,
   ops 14, curated 20, mart 7
3. verified through Athena: account 350, transaction 2,050, digital_event 3,000,
   `realtime_run` 21 rows, `realtime_info` 7 cursors — **the control plane survived intact**

Re-provisioning instead of registering would have created empty tables and stranded all of
it. See `docs/PLATFORM_RESOURCE_INVENTORY.md` §0a–0b.

## FINAL ACCEPTANCE

| gate | verdict |
|---|---|
| REALTIME optional per table | PASS |
| disabled ⇒ zero REALTIME Spark work | PASS |
| disabled ⇒ zero REALTIME Airflow task | PASS |
| EOD independent of REALTIME | PASS |
| AUTO_CORRECT independent | PASS |
| FULFILL independent | PASS |
| STREAM_BATCH dependency compile validation | PASS |
| explicit FULL_CDC fallback only | PASS |
| normal run reads an incremental snapshot delta | PASS |
| no normal T-N full rescan | PASS |
| latest_state source-native ordering | PASS |
| guarded MERGE | PASS |
| tombstone delete | PASS |
| delete/recreate | PASS |
| event_window preserves events | PASS |
| EOD-certified rebase | PASS |
| race-safe rebase | PASS |
| no DAG-per-table explosion | PASS |
| no custom Spark app per table | PASS |
| config impact report | PASS |
| maintenance threshold-driven | PASS |
| partition benchmark | PASS (measured) — and it does not discriminate at this volume; no change made |
| performance benchmark | PASS (measured) — live ledger, §4 |
| E2E on the deployed platform | PASS for 10 of 11 behaviours; see LIVE EVIDENCE |
| rebase ACTION path on a certified close | **PASS** — live, 2026-09-29, `rebased=2 removed=350` |
| docs / runbooks updated | PASS |

### Re-proven on a rebuilt stack — 2026-09-29/30

The platform was destroyed and re-applied on 29 Sep. Everything below was then re-run on the
rebuilt stack with **data flowing live from the source databases**, which makes it stronger
evidence than the original grant rather than a repeat of it.

**The EOD close certified WITHOUT `--skip-readiness`.**

```
EOD_SUMMARY closed=10 certified=5 rows=3404
```

| table | rows | dq | recon | status |
|---|---|---|---|---|
| `corebank.customer` | 200 | PASS | PASS | CERTIFIED |
| `dbo.app_user` | 150 | PASS | PASS | CERTIFIED |
| `dbo.digital_event` | 3,000 | PASS | PASS | CERTIFIED |
| `dbo.payment_method` | 50 | PASS | PASS | CERTIFIED |
| `dbo.channel` | 4 | PASS | PASS | CERTIFIED |

The first certified close (28 Sep) needed `--skip-readiness` because the source had no events
after the cutoff. This one passed the readiness gate **on its own merits**, because the ingest
had made FULL_CDC current. The five `BUILT_NOT_CERTIFIED` tables are the `event_date`
partition prune working: their events are dated to other COBs.

**Every enabled table read incrementally, with real non-empty deltas.**

```
REALTIME_SUMMARY succeeded=7 noop=0 skipped=3 incremental=7 rows=5770
```

Example cursor advance, `corebank.account`:
`6327604713160137465 -> 80230069689276128`, input 320 rows, output 320 — one row per
business key, which is the `latest_state` contract holding on live data. The three disabled
tables did zero work and left no ledger row.

Compare with the run immediately before the ingest: `noop=7, incremental=0`, every table
reporting `source unchanged at <snapshot>`. The same code, the same cursors, a moved source —
and the layer switched from "nothing to do" to "read exactly the delta" without intervention.

**The full pipeline, source to lake.** Both engines ingested; every table gained precisely
its source row count, no loss and no duplication:

| Oracle | rows | | SQL Server | rows |
|---|---|---|---|---|
| `account` | 350 → 670 | | `app_user` | 150 → 300 |
| `customer` | 200 → 400 | | `digital_event` | 3,000 → 6,000 |
| `transaction` | 2,050 → 4,050 | | `payment_method` | 50 → 100 |
| `loan` | 4 → 54 | | `channel` | 4 → 8 |
| `branch` | 4 → 8 | | `merchant` | 0 → 100 |

**Test suite: 2,871 passed, 0 failed.**

#### The defect that made the whole day

`terraform destroy` removes MSK but not the lake bucket, so the FULL_CDC streaming checkpoint
survived holding offsets from a cluster that no longer existed. Structured Streaming always
prefers the checkpoint over `startingOffsets`, so the first ingest after the rebuild consumed
nothing and **reported SUCCESS**:

```
FULL_CDC_STREAM_SUMMARY {"batches": 0, "rows": 0, "poison": 0, "tombstones": 0}
```

Green exit code, no data, no error. The only way to see it was to compare FULL_CDC counts
before and after. `streaming-reset.sh` was the entire fix, and it also repaired two
pre-existing gaps — `loan` stuck at 4 rows against 50 in the source, and `merchant` at zero.

Documented as §0c of `docs/PLATFORM_RESOURCE_INVENTORY.md`, beside §0a (rescue the old CMK)
and §0b (register tables, never re-provision). **Three post-rebuild steps, none of which
announce themselves**: the first presents as an IAM error, the second as missing data, the
third as nothing at all.

---

### No blockers remain

Every one of the 24 mandatory gates now has live evidence on the deployed platform. The
final gate — the rebase acting on a certified close — closed on 2026-09-29 with
`rebased=2 removed=350` against an `eod_info` row certified on the real clock.

Two corrections to earlier versions of this document, both of which mattered:

| earlier claim | actual |
|---|---|
| "Glue holds 0 tables", "SQL Server CDC not enabled", "`schemas.json` stale" | none were true; carried from a summary rather than re-read from AWS |
| "one blocker, and it is a clock" | true when written; the clock passed, and the gate closed with no simulated time |

REALTIME_LAYER_PRODUCTION_READY
