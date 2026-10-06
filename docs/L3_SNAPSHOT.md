# L3 EOD (SNAPSHOT) — as-of T-1, derived from L1 FULL_CDC

> **The config-driven EOD builder is `docs/EOD_LAYER.md`**
> (ADR-065): the cutoff, per-engine source-native ordering, the delete and
> snapshot-mode policies, and the certification gate. This document describes
> the layer's contract and history.

> **ARCHITECTURE CORRECTION — 2026-08-21.** This document was written against a three-step
> chain (`L1 STREAM -> L2 FULL_CDC -> L3 SNAPSHOT`) in which STREAM was the raw Kafka
> landing. The layer model is a **fan-out**, and STREAM is a DERIVED layer, not a landing:
>
> ```
> Kafka --(decode + validate, append-only)--> L1 FULL_CDC   <- CANONICAL
>                                                  |
>                                     +------------+------------+
>                                     v                         v
>                            L2 REALTIME (STREAM)        L3 EOD (SNAPSHOT)
>                            rolling window T-N -> T     1 state per PK as-of T-1
>                            db: ..._stream              db: ..._snapshot
> ```
>
> The column contracts and dedup rules below are unchanged and still normative; what moved
> is which layer each belongs to:
>
> | Old reading | Now |
> |---|---|
> | L1 STREAM = raw Kafka landing | **L2 REALTIME** — derived from FULL_CDC, rolling window |
> | L2 FULL CDC | **L1 FULL_CDC** — canonical, written directly from Kafka |
> | L3 SNAPSHOT | **L3 EOD** — derived from FULL_CDC by cutoff + dedup |
>
> There is no raw landing layer: Kafka is decoded and validated straight into FULL_CDC.
> See `docs/TARGET_ARCHITECTURE.md` §3.


- Session: 08
- Date: 2026-08-14
- Status: **snapshot semantics PROVEN locally; the pipeline is `NOT_TESTED`**
- Revised 2026-08-14: three defects found and fixed under review — see §4.1, §5 and §7
- Contract: [`docs/DATA_CONTRACTS.md`](DATA_CONTRACTS.md) §7

---

## 1. Three ways to get this wrong

L3 is a `row_number()` and a filter. Each of the three properties below is a separate way
to produce a plausible-looking snapshot that is quietly wrong.

**1. AS-OF, not "latest".** `CLAUDE.md` §5.6 — rebuilding an old `snapshot_date` must
reproduce the original result. The cutoff filters on `source_commit_ts`, and the window
function ranks only rows inside it. A "latest state" query returns today's answer for
every historical date.

**2. Ordered by source position, never Kafka offset alone.** `CLAUDE.md` §5.4 — offset
only increases *within one partition*, so a global offset sort silently reorders events
across partitions. `test_source_position_beats_kafka_offset` builds exactly that case: a
higher SCN with a *lower* offset in a *different* partition, and asserts it wins.

**3. Deletes are explicit.** §7.2 — no ambiguity, two tables:

| Table | Rule |
|---|---|
| `snapshot.<domain>_<entity>` | `WHERE rn = 1 AND operation <> 'd'`. A PK whose latest event as-of cutoff is a delete is **absent** |
| `snapshot.<domain>_<entity>_history` | Same ranking, no filter, `is_deleted = (operation = 'd')`. **Opt-in per table** |

The `_history` variant is opt-in because **retaining deleted PII conflicts with erasure
obligations** and must be a deliberate, recorded choice. The job raises if history is
requested for an entity that has not enabled it.

## 1a. Both engines, not one

The ordering contract is a single rule applied to **two source position formats that
normalise in opposite directions** (`DATA_CONTRACTS.md` §4.2):

| | Oracle | SQL Server |
|---|---|---|
| Raw form | SCN, **numeric** | LSN, **hex triplet** `aaaaaaaa:bbbbbbbb:cccc` |
| Problem | `'9' > '10'` as a string, but `9 < 10` numerically | none — already fixed-width |
| Normalisation | **zero-pad** to 24 chars | **validate** the widths; reject anything else |
| `position_secondary` | change SCN | `event_serial_no`, zero-padded to 10 |

A single "just pad it" helper would silently corrupt one of them, so the two have separate
functions. The important consequence for L3: **the entire Oracle suite would still pass if
the SQL Server path were broken.** Different width, different alphabet, different rules —
and `entities.json` ships a SQL Server entity (`digital.app_user`), so L3 is proven against
both:

| Test | Asserts |
|---|---|
| `test_higher_lsn_wins_despite_lower_offset_other_partition` | the §5.4 case, in hex |
| `test_lsn_hex_ordering_is_not_decimal_ordering` | `0x0a` (10) beats `0x09` (9) — padded hex really does order as a string |
| `test_event_serial_breaks_ties_within_one_lsn` | a burst commits many changes under **one** LSN; only `event_serial_no` separates them |
| `test_sqlserver_delete_excluded_and_recreate_restores` | delete/recreate lifecycle on the SQL Server path |

## 2. Why rebuilds are deterministic

`MASTER_PLAN.md` Gate B requires that rebuilding L3 for the same cutoff yields an
equivalent checksum. That holds **only if the ranking is total** — no ties.

`event_order`'s five components make ties impossible: two distinct events cannot share
all of `(position_primary, position_secondary, source_ts_ms, kafka_partition,
kafka_offset)`, because the **last two alone are unique per Kafka record**. This is the
structural reason the tie-breaker chain is not optional.

The checksum XORs a per-row `sha256` over the **business columns**, excluding the
volatile `l3_run_id` and `l3_write_ts`. XOR is order-independent, so two rebuilds that
produce the same rows in a different physical layout still match — summing would also be
order-independent but collides far more readily.

Hashing only `(primary_key, source_event_id)` — as an earlier version did — answers *"did
the same events win?"* but not *"did they produce the same values?"*. A changed projection
or type cast would then rebuild to an identical checksum while the table content differed,
and the reproducibility gate would pass on a real regression.
`test_checksum_changes_when_projected_values_change` pins this: it rewrites the payload of
the winning event, leaves the event id alone, and asserts the checksum moves.

`test_rebuild_same_cutoff_yields_identical_checksum` asserts the positive case.

## 3. Delete lifecycle

Every case in the session file, proven against a real Iceberg table:

| Scenario | Result | Test |
|---|---|---|
| create → update | latest update wins; lineage points at the winning event | `test_latest_update_wins` |
| create → delete | PK **absent** from the active table | `test_latest_delete_excludes_the_pk` |
| create → delete → **recreate** | PK **active again** | `test_delete_then_recreate_is_active_again` |
| create → delete, history enabled | PK present with `is_deleted = true` | `test_history_table_keeps_the_deleted_pk_flagged` |

**Delete-then-recreate works by construction, not by special-casing.** The re-insert has
a higher `event_order`, so it wins `rn = 1`, and its `operation = 'c'` passes the delete
filter. No branch in the code handles it.

## 4. Late events

A late event committed *inside* the window but written to L1 *after* the snapshot was
built changes the answer — which is precisely why rebuild-by-date exists.

`test_late_event_changes_a_rebuilt_snapshot` builds a snapshot, appends a late event with
a higher position, rebuilds, and asserts (a) the late event wins and (b) the partition was
**overwritten, not appended** — one row, not two.

### 4.1 The empty-partition case (fixed in this session)

The job originally wrote with `overwritePartitions()`. Dynamic partition overwrite only
replaces partitions **present in the written dataframe**, so when a late *delete* removed
the last surviving PK for a date, the result set was empty, **no partition was touched**,
and the previous ACTIVE row survived in the certified table — while the build still
reported `CERTIFIED`, echoing the stale row's count back as its own.

This was reproduced as a live failure before the fix. Two changes close it:

1. The write is now `overwrite(snapshot_date = <date>)` — overwrite **by filter**, which
   deletes the partition first, so an empty result correctly empties it. This is also what
   `DATA_CONTRACTS.md` §7 already specified: *"full overwrite of the snapshot_date
   partition"*.
2. `validate()` now fails certification if any surviving row carries an `l3_run_id` other
   than the current run. A write path that silently no-ops can no longer be certified —
   **certifying stale data is worse than failing.**

Guarded by `test_late_delete_emptying_partition_clears_it` and
`test_validate_rejects_a_partition_that_was_not_replaced`; both fail if the old write mode
is restored.

## 5. Entity configuration

PK definitions live in `spark/snapshot/config/entities.json`, not in the job: they differ
per table, and a composite PK written inline is the kind of thing that gets copy-pasted
wrong.

The canonical PK expression **sorts keys**, matching `envelope.canonical_primary_key`
exactly. If the two ever diverged, the join key would differ and every PK would look new —
producing a snapshot with the right row count and entirely wrong lineage.

`canonical_pk_expr()` was **broken** and this session fixed it. It built the JSON with
`concat_ws(',', '"COL":', value)`, which places the separator *between a key and its own
value* and leaves the value unquoted:

```text
emitted by the expression : {"CUSTOMER_ID":,1}
emitted by the L1 writer  : {"CUSTOMER_ID":"1"}
```

The old test asserted only that `"A_COL"` appeared before `"Z_COL"` in the generated SQL
**text**, which passes regardless of whether the SQL is correct — a substring check on
generated code cannot detect this, only evaluating it can. The expression is now
`to_json(named_struct(...))`, which quotes values and escapes embedded quotes and
backslashes, and `TestCanonicalPkMatchesL1Writer` asserts byte-equality against
`envelope.canonical_primary_key` for the single-PK, composite-PK and quote-in-value cases.

The defect was latent — `build_snapshot.py` reads `primary_key` straight from L2 rather
than recomputing it — but it is shipped as the documented way to derive the canonical PK,
so anything that re-keys L2 would have made every PK look new.

`target_namespace` is configurable rather than a hardcoded `snapshot.` prefix. Baking the
catalog into the config class made it untestable and tied it to one deployment — that was
found by the tests failing, and fixed in the config rather than worked around in the test.

## 6. Typed projection

L1 and L2 keep `before`/`after` as JSON strings (S01-16), so a source DDL change does not
force an Iceberg migration on the streaming table. **Typing happens here**, where a
migration is a reviewed change rather than a stream outage.

Nulls are handled by construction: `get_json_object` returns null for a missing field, and
a delete's null `after` never reaches the projection because the active table filters
deletes out before it.

## 7. Certification

A snapshot is `CERTIFIED` only when all four hold (scope item 8):

- **row count == distinct PK count** — one active row per PK, no duplicates;
- the `source_event_id` lineage column is present;
- **every row carries this run's `l3_run_id`** — proving the partition was actually
  replaced (§4.1);
- **L2 → L3 reconciliation matches** — the active-row count derived independently from L2
  equals the L3 row count.

The reconciliation is deliberately a `GROUP BY` / `max_by` aggregation rather than the
build's own `row_number()` window. A reconciliation that reuses the build's query proves
only that the query is deterministic — **it agrees with the build even when the build is
wrong.** Two different derivations of the same number is the entire point.

It also never compares the `event_order` struct directly. Comparison semantics depend on
how the column is typed: a struct compares field-by-field in *declared* order, so the
ranking would follow the table's physical layout rather than `DATA_CONTRACTS.md` §4.1, and
a map is not orderable at all. The five fields are named explicitly in precedence order.
`test_reconciliation_uses_source_position_not_offset` builds the case where offset ordering
would reconcile to the wrong answer.

`main()` exits non-zero when the status is not `CERTIFIED`, so an uncertified snapshot
cannot be silently consumed downstream.

## 8. Test results

```
spark/tests/test_l3_snapshot.py    30 passed
Full suite (Sessions 06–08)       114 passed in 47.40s
```

The L2 test fixtures now declare an **explicit schema** instead of letting Spark infer one.
Inference turned the nested `event_order` dict into `MAP<STRING,STRING>`, which is wrong
twice: the contract says `event_order` is a **struct**, and string-typing coerced
`source_ts_ms`, `kafka_partition` and `kafka_offset` so the tie-breakers compared
**lexicographically instead of numerically** — offset `999` would sort below offset `1000`.
The ordering tests are only meaningful against the real types.

| Acceptance criterion | Status |
|---|---|
| One active row per PK | **PASS** |
| Latest delete excluded | **PASS** |
| Rebuild same cutoff equivalent | **PASS** — checksum |
| Offset never used as sole global order | **PASS** — asserted with a contrary case |
| Snapshot date / cutoff / lineage columns present | **PASS** |
| Composite PK, initial `r`, out-of-order arrival | **PASS** |
| **SQL Server LSN** ordering through the full build | **PASS** — hex triplet, serial tie-break, delete/recreate |
| Late delete emptying a partition clears it | **PASS** — was a live defect |
| L2 → L3 reconciliation | **PASS** — independent derivation |
| Canonical PK matches the L1 writer byte-for-byte | **PASS** — was a live defect |
| **L3 against live L2 on EMR Serverless** | **`NOT_TESTED`** |
| **Sample/hash comparison against a previous production build** | **`NOT_TESTED`** |

## 9. Rebuild

Use the operator script rather than calling `spark-submit` by hand — it guards the
identity, validates the date, records each certification and **diffs the rebuild against
the previous one** (scope item 6):

```bash
scripts/snapshot-rebuild.sh --entity banking.customer --date 2026-08-14            # dry-run
scripts/snapshot-rebuild.sh --entity banking.customer --date 2026-08-14 --execute
scripts/snapshot-rebuild.sh --entity banking.account  --date 2026-08-14 --history --execute
```

It is dry-run by default and exits `2` when a snapshot rebuilds but does not certify, so a
failed reconciliation cannot be mistaken for success. A checksum that **changes** for the
same cutoff is reported as a warning, not an error: late events arriving in L2 after the
previous build (§6.2) and a non-total ordering (a defect) look identical from there, and
the script says so rather than picking one.

The underlying job, if invoked directly:

```bash
spark-submit build_snapshot.py --entity banking.customer --snapshot-date 2026-08-14
spark-submit build_snapshot.py --entity banking.account  --snapshot-date 2026-08-14 --history
```

Rebuilding is safe at any time and **overwrites only that `snapshot_date` partition**.
Rebuilding a range in any order is safe — unlike L2, there is no watermark, because L3 is
a pure function of L2 plus a cutoff.

That property is the point: **L3 is reproducible from L2, and L2 is reproducible from L1.**
Which is why `CLAUDE.md` scope item 8 forbids deleting L1 before L2 validation and an
adequate retention window.
