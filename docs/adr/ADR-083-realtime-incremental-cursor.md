# ADR-083 — The REALTIME incremental cursor and `ops.realtime_info`

* **Status:** Accepted
* **Date:** 2026-09-28
* **Phase:** R2-C
* **Related:** ADR-064, ADR-076 (the same split, for EOD), ADR-082, ADR-079

## Context

Every REALTIME run re-filtered FULL_CDC over the whole window — `source_commit_ts` in
`[grace_lower, upper)`, 96 hours wide, 144 times a day per table. The R2-A audit recorded
`current_snapshot_id()` being written to the ledger and never used as a read boundary.

The obvious framing is "the scan is wasteful, make it incremental". That framing is
incomplete, and getting it wrong is how this phase would have introduced a silent defect.
**The full scan is also idempotent and self-healing.** It cannot be incomplete, because it
does not depend on remembering anything: whatever went wrong last run, this run re-derives
the answer. An incremental read trades that away. It is correct only while the cursor is
correct, and a wrong cursor produces no error at all — it produces a REALTIME table missing
events nobody counted.

So this is a **trade**, not an improvement, and the design is shaped by which direction the
failure falls:

> a run that reads too much is slow.
> a run that reads too little is wrong, and nothing reports it.

## Decision

### The cursor lives in `ops.realtime_info`, one row per table, MERGEd

`ops.realtime_run` stays the append log of every attempt. A second table answers "where is
this table's cursor" **directly**, rather than by the convention "the latest row, by some
ordering, that happens to be SUCCEEDED" — a convention no column enforces and every reader
re-implements. This is the same fix ADR-076 made for the EOD layer, for the same reason.

`realtime_run` keeps its name and is the run history the R2 brief §19 asks for. A third
table called `realtime_run_hist` would be a rename of something that already works.

### The decision is a pure function

`cdc/realtime_state.plan_read()` takes the configured progress mode, the cursor, the source's
current snapshot and its snapshot lineage, and returns a `ReadPlan`. No Spark. The branch
that decides what a run reads is the branch that must be exhaustively testable, and it is
tested against every failure mode rather than only the happy path.

### Five ways a cursor is untrustworthy, and all five fall back to the full scan

| condition | why an incremental read would be wrong |
|---|---|
| a `replace` snapshot in the range | `rewrite_data_files` (weekly `cdc_maintenance`) rewrites the same rows into different files. An append scan does not see them |
| the cursor snapshot expired | `expire_snapshots` removed the range's starting point |
| the cursor is ahead of the source | a rollback. "Nothing new" would freeze the layer at a state the source no longer has |
| no cursor | first run |
| `write_strategy: overwrite_window` | an overwrite REPLACES the target; a partial read would **delete every row the delta does not contain** |

None of them raise. An unsafe cursor is an expected operational state — a compaction ran, a
snapshot expired — and refusing to run would mean the layer stops serving because it could
not take the cheap path. Each records a `fallback_reason` **as a column**, because "why did
this run cost ten times the last one" must be answerable a week later.

The last row is also refused at **compile** (`cdc/config_loader.py`). The planner's check is
the second line of defence, for a plan compiled before that rule existed.

### The window filter is applied to the incremental delta too

Not redundant. The window is the **contract** — what the layer promises to serve. The cursor
is an optimisation of how candidates are *found*. A connector catching up after a long stall
appends genuinely old changes now; admitting one because it arrived in this snapshot range
would put a row outside the promised window into the table, and the next rebuild would remove
it again. A row that appears and disappears across runs is the hardest discrepancy to chase.

### The cursor advances LAST, and only on a verified commit

    read -> write target -> VERIFY the commit -> advance the cursor

`advance_cursor()` is the one place this is decided, and it returns the previous value unless
the commit **and** the validation both succeeded. Advancing on commit alone is the subtle
version of the bug: the rows are in the target so it *looks* done, but a validation that
failed means we do not know they are right, and moving the cursor makes that range
unreachable without a full rebuild.

A NOOP never advances it either — nothing was read, so nothing was incorporated.

### An unmoved source is `SUCCEEDED_NOOP`, a distinct status

"The cursor did not advance and nothing was written" and "a normal run happened to produce
the same count" look identical in a ledger that conflates them, and only one of them means
the upstream has stopped.

## Consequences

* **The cheap path is opt-in per table.** `processing.source_progress` defaults to
  `window_scan`, so nothing changes for a table that does not ask for the cursor. The
  shipped registry still asks for none: R2-E flips the `event_window` tables once the append
  path is the default write strategy.
* **Cost becomes bursty rather than flat.** Most runs read a delta; the run after each
  weekly compaction reads the whole window. That is visible in `read_mode` and
  `fallback_reason`, which is why they are columns.
* **`ops.realtime_run` gained nine columns** — `shape`, `write_strategy`, `attempt`,
  `read_mode`, `fallback_reason`, `source_snapshot_before`, `input_rows`, `spark_app_id`,
  `baseline_cob_date`. `ledger_row()` is positional against that contract and now asserts
  its own length, because inserting a column in the middle and forgetting the writer shifts
  every value one place and Spark only complains when two adjacent types differ.
* **`write_info` failing does not fail the run**, so the append path must be idempotent on
  `dv_event_id` rather than merely usually correct. A test rewinds the cursor and replays.

## Options

**Keep the full scan.** Correct, self-healing, and 144 full window reads per table per day.
Rejected on cost, but it remains the fallback and `--force-window-scan` exposes it.

**Store the cursor in the run ledger.** No new table, and "where is the cursor" becomes a
convention. Rejected for the reason ADR-076 gives.

**Use `source_commit_ts` as the cursor instead of a snapshot id.** Simpler, and wrong: an
event can be *appended* long after it *committed*, so a timestamp cursor skips exactly the
late events the grace band exists to catch.

**Fail the run when the cursor is unsafe.** Safe, and it stops the layer serving because a
weekly compaction ran. Rejected.

## Cost

The intended effect is the R2-I measurement, not a claim here. Structurally: a table on
`iceberg_snapshot` reads the snapshots appended since its last run instead of 96 hours of
FULL_CDC, and falls back to the full read after a compaction — so the cost profile is "cheap
most runs, one expensive run per maintenance window", not "cheap always".

No new AWS resource. `ops.realtime_info` is one unpartitioned Iceberg table holding one row
per source table — ten rows here.

## Security

No change. No new credential, IAM statement or network path. `realtime_info` holds
coordinates and counts, never payload, and is provisioned into the same OPS database under
the same policies as `eod_info` and `realtime_run`.

## Rollback

Set `processing.source_progress: window_scan` in the registry (or delete the key — it is the
default) and recompile. Every table returns to the pre-R2-C read path with no code change and
no data migration; the cursor column is simply not consulted. `--force-window-scan` does the
same for a single run without touching config.

`ops.realtime_run` gained nine columns, so an existing ledger needs the new columns added or
the table recreated. The lake currently holds no such table, which is why this landed now.

## Validation

`spark/tests/test_realtime_cursor.py` — 37 tests.

Pure (26): every fallback branch, the half-open range, cursor advance under failed commit /
failed validation / NOOP / window scan, the control table's key and MERGE, and that the
compiler refuses `iceberg_snapshot` + `overwrite_window` before the planner ever sees it.

Against real Iceberg (11): the second run reads **4 rows, not 14** — the single number this
phase is about — while the target still holds all 14; the cursor is stored and advanced in
one MERGEd row; an unmoved source is a NOOP that does not move the cursor; a real
`rewrite_data_files` between two runs forces the full scan and loses nothing; a rewound
cursor replays without duplicating; and a 200-hour-old event appended into the delta is still
excluded by the window.

Full suite: `spark/tests/` + `airflow/tests/`.
