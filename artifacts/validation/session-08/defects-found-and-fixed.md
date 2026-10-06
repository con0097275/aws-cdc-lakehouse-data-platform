# Session 08 — defects found under review, with reproduction evidence

Session 08 had already produced `build_snapshot.py`, `entity_config.py`, `entities.json`,
`docs/L3_SNAPSHOT.md` and 17 passing tests. Re-executing it under the architect +
data-engineer passes found **four defects and one missing deliverable**. Each defect below
was reproduced as a live failure BEFORE the fix, and each fix is pinned by a test that
fails when the fix is reverted (mutation-checked, not assumed).

---

## D08-1 — a late delete that empties a partition leaves stale ACTIVE rows (CRITICAL)

`build_snapshot.py` wrote with `.overwritePartitions()`. Dynamic partition overwrite only
replaces partitions **present in the written dataframe**. When a late delete removes the
last surviving PK for a date the result is empty, so **no partition is touched**.

Reproduced live:

```
BUILD1 rows in table: 1 status: CERTIFIED
BUILD2 rows in table: 1 reported row_count: 1 status: CERTIFIED
EXPECTED 0 rows (PK was deleted). RESULT: FAIL - STALE ROW SURVIVES
  stale row: Row(customer_id=1, status='ACTIVE', ..., source_event_id='e1', l3_run_id='r1')
```

The deleted record survived in the **certified active snapshot**, and `validate()`
re-read the table and reported the *previous* run's row as its own output.

Violates acceptance criterion "Latest delete excluded", and `DATA_CONTRACTS.md` §7, which
already specified "full overwrite of the snapshot_date partition".

**Fix:** `.overwrite(F.col("snapshot_date") == F.lit(snapshot_date))` — overwrite by
filter deletes the partition first, so an empty result correctly empties it.
**Plus** `validate()` now fails if any surviving row carries a different `l3_run_id`;
a write path that silently no-ops can no longer be certified.

Mutation check — restoring `overwritePartitions()`:
```
FAILED test_l3_snapshot.py::TestLateDeleteEmptiesPartition::test_late_delete_emptying_partition_clears_it
```

## D08-2 — `canonical_pk_expr()` produced malformed JSON

`concat_ws(',', '"COL":', value)` places the separator **between a key and its own value**
and leaves the value unquoted. Verified against the L1 writer:

```
SINGLE  L1 writer : {"CUSTOMER_ID":"1"}
SINGLE  expr      : {"CUSTOMER_ID":,1}          MATCH: False
COMPOS  L1 writer : {"ACCOUNT_ID":"7","PRODUCT_CODE":"SAV"}
COMPOS  expr      : {"ACCOUNT_ID":,7,"PRODUCT_CODE":,SAV}   MATCH: False
```

The existing test asserted only that `"A_COL"` preceded `"Z_COL"` in the generated SQL
**text** — a substring check on generated code cannot detect this.

Latent, because `build_snapshot.py` reads `primary_key` straight from L2 rather than
recomputing it. But it is shipped as the documented way to derive the canonical PK, so
anything re-keying L2 would make every PK look new.

**Fix:** `to_json(named_struct(...))` — quotes values, escapes embedded quotes and
backslashes. `TestCanonicalPkMatchesL1Writer` asserts byte-equality against
`envelope.canonical_primary_key`.

## D08-3 — no L2 → L3 reconciliation, though scope item 8 requires it

`validate()` checked PK uniqueness only. Scope item 8: "Publish certification only after
uniqueness/**reconciliation** checks."

**Fix:** `reconcile_l2_l3()` derives the expected active-row count from L2 with a
`GROUP BY` / `max_by` aggregation — deliberately **not** the build's `row_number()`
window, because a reconciliation that reuses the build's own query agrees with the build
even when the build is wrong.

It names the five `event_order` fields explicitly rather than comparing the struct:
struct comparison follows *declared* field order (so the ranking would follow physical
layout, not `DATA_CONTRACTS.md` §4.1) and a map is not orderable at all.

Mutation check — comparing the struct directly: all 3 reconciliation tests fail.

## D08-4 — the reproducibility checksum ignored the business columns

Hashed only `(primary_key, source_event_id)` — answers "did the same events win?" but not
"did they produce the same values?". A changed projection or type cast would rebuild to an
identical checksum while the content differed, so scope item 6's "compare count/hash with
previous build" would pass on a real regression.

**Fix:** hash all columns except the volatile `l3_run_id` / `l3_write_ts`.

Mutation check — restoring the PK-only checksum:
```
FAILED test_l3_snapshot.py::TestChecksumDetectsContentChange::test_checksum_changes_when_projected_values_change
```

---

## Gaps closed

**G08-1 — zero SQL Server LSN coverage in L3.** Every L3 test used `oracle_scn`. Oracle
SCN and SQL Server LSN normalise in *opposite* directions (pad vs validate), to different
widths and alphabets, and `entities.json` ships a SQL Server entity — so the whole Oracle
suite would pass with the SQL Server path broken. Added 4 tests: hex ordering, the
lower-offset/other-partition case in hex, `event_serial_no` tie-breaking within one LSN,
and delete/recreate.

**G08-2 — test fixtures did not match the contract schema.** Spark inferred the nested
`event_order` dict as `MAP<STRING,STRING>`. The contract says **struct**, and string-typing
coerced `source_ts_ms`, `kafka_partition` and `kafka_offset` so tie-breakers compared
**lexicographically, not numerically** — offset 999 would sort below offset 1000. The
ordering tests were only meaningful once an explicit schema was declared.

**G08-3 — the "snapshot validation/rebuild scripts" deliverable did not exist.** Added
`scripts/snapshot-rebuild.sh`: identity guard, date validation, entity check, dry-run by
default, records each certification and diffs rebuild vs previous (scope item 6). Exits 2
when a snapshot rebuilds but does not certify.

---

## Result

`101 passed` -> `114 passed`. 17 L3 tests -> 30.
Still `NOT_TESTED`: L3 against live L2, and comparison against a previous *production*
build. Nothing was applied; incremental cost $0.00.
