# ADR-078 — Carry-forward state: day D is day D-1 plus day D's changes

* **Status**: Accepted (implementation: Phase F)
* **Date**: 2026-09-20
* **Extends**: ADR-042 (datamart merge semantics), ADR-043 (watermark and rerun),
  ADR-077 (EOD readiness)
* **Evidence**: `spark/reporting/carry_forward.py`,
  `spark/tests/test_carry_forward_spark.py`

## Context

Phase F asked for the five flow applications. Four of the five requirements were already
satisfied and proven live in the 2026-09-03 window: EOD (§1), FULFILL with every flag
(`from_date`, `to_date`, `specific_dates`, `force`, `dry_run` — §3), STREAM_BATCH's frozen
upper bound and watermark discipline (§4), and STREAMING_RT's lifecycle with a durable
checkpoint (§5). AUTO_CORRECT already had a bounded lookback, affected-date discovery
(`AffectedDatePolicy`), affected-key narrowing (`AffectedKeyStrategy`) and dependent-mart
propagation. 178 tests cover those five flows.

One thing from §2 was missing: **"adapt useful carry-forward/state patterns from reference
auto_correct code."**

`auto_correct.py` bounds a window and **re-derives** it from FULL_CDC. That is correct, and
for a daily STATE mart it is the expensive way to be correct: repairing one late event that
touched one key on one day rescans the entire lookback window.

The reference mart does the opposite (`f_acct_depo_by_day_level_acct_auto_correct_generate.py`
lines 433-538): it seeds day D from day D-1's `latest_of_day = 1` rows and applies only the
day's changes on top. In 1,871 lines that is the one load-bearing idea, and it belongs in
framework core.

## Decision

`spark/reporting/carry_forward.py` implements the pattern generically.

### 1. Nothing here names a business column

The reference's 1,800 lines of `bal_dau_ki`, `ma_cn`, `kyhan`, `custtyp`, `int_rate` are a
MART's schema. The grain, the ordering, the identity columns and the flags are all
configuration (`CarryForwardSpec`); the payload is whatever the caller's DataFrame carries. A
framework that knew about `bal_dau_ki` would need editing for the second mart.

### 2. Three materialised markers, adapted not copied

| reference | here | why |
|---|---|---|
| `latest_of_day = 1` | `latest_of_day` | Written once, read a thousand times. Two readers that each recompute `row_number()` can disagree. |
| `is_full_filled` | `is_carried_forward` | A row knows whether it came from a real event or from yesterday's state. Same idea, a name that says it. |
| `super_key = concat(hk,'|',system_time,'|',etl_date)` | `super_key`, columns from config | A point-in-time identity distinct from the business key. No two marts spell those three the same way. |

### 3. A real event outranks a carried row, whatever the clocks say

The carried row is yesterday's fact restamped with today's date. Ranking it against today's
events by `system_time` compares two different days' clocks — and yesterday's can easily be
the later one (a 23:00 change carried into a day whose first event is at 01:00). The rank is
therefore `is_carried_forward ASC` first, then the configured ordering.

### 4. Only yesterday's CURRENT rows carry

Carrying every historical row forward multiplies the mart by its own history, one day at a
time. History is retained in place (`latest_of_day = 0`), because a point-in-time mart that
dropped it could not answer "what did we believe at 10:00".

### 5. Frames are aligned by NAME

A positional union between a state table and a change feed is a silent column transposition,
and every value it produces is of the right type. Columns absent from either side are filled
as typed NULLs.

### 6. `super_key` is session-zone dependent, and that is why the UTC pin matters

It embeds a timestamp cast to string. Every job here pins
`spark.sql.session.timeZone=UTC` (ADR-024); the same row rendered in a UTC+7 session produces
a different `super_key` for the same fact, and nothing downstream would report a mismatch —
it would look like a new row.

## Options

* **Keep re-deriving the window.** Rejected for state marts: the cost is the whole point.
  It remains the right choice where a mart aggregates across keys, which is what
  `AffectedKeyStrategy.ALL_KEYS_IN_DATE` already expresses.
* **Copy the reference's mart SQL.** Rejected — §1.
* **Rank carried rows by timestamp alongside real ones.** Rejected — §3.
* **Derive `latest_of_day` in each reader.** Rejected — §2.
* **Positional union.** Rejected — §5.

## Consequences

* Carry-forward is a **library**, not yet a wired flow mode: `auto_correct_flow` still
  re-derives. Adopting it per mart is a config decision (grain, ordering, identity) that has
  to be made mart by mart, and no shipped mart declares one yet.
* A mart that adopts it gains two columns (`latest_of_day`, `is_carried_forward`) and
  optionally `super_key`.

## Cost

**$0 added**; the point is to remove cost later. A 90-day window rescan for a one-key repair
becomes one partition read plus the day's changes.

## Security

No IAM, network or credential change. No business data is named, read or written by the
module itself.

## Rollback

Nothing calls it yet, so removing the module is inert. A mart that adopts it reverts by
dropping the spec and falling back to the existing re-derivation.

## Validation

1. `spark/tests/test_carry_forward_spark.py` — **19 passed** on real Spark: unchanged keys
   carried, restamping, a real event outranking a carried row, last-event-wins, exactly one
   current row per key, history retained, first-day base case, only yesterday's current rows
   carried, the carried marker, `super_key` stability, three-day composition, rerun
   convergence, a late event repairing only its own key, and five refusals (no grain, no
   ordering, date in the grain, missing column, name-aligned union).
2. **Not claimed**: no mart uses it, and it has not run on EMR. Phase F's other four flows
   were proven live on 2026-09-03 and are unchanged by this ADR.
