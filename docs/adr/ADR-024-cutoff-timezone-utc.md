# ADR-024 — Cutoff timezone: UTC

- Status: **ACCEPTED** (Session 01)
- Closes: defect D8
- Related: `docs/DATA_CONTRACTS.md` §1

## Context

L2 and L3 correctness depends on the window `[T 00:00, T+1 00:00)`.
`reference/ICEBERG_LAYER_SPEC.md:56` says "theo timezone đã khóa" — *in the locked
timezone* — but **no document ever names it.** Every cutoff, every `snapshot_date`,
every `business_date` and every Airflow schedule depends on this, and it must be
fixed before Session 06 writes the first cutoff predicate.

## Options

| Option | Verdict |
|---|---|
| **UTC everywhere** | **CHOSEN** |
| `Asia/Ho_Chi_Minh` (UTC+7) everywhere | Rejected |
| Store UTC, present local in marts | Rejected for now, revisitable |

## Decision

**UTC**, with no exceptions: cutoffs, `business_date`, `snapshot_date`, Airflow
schedules, and every timestamp column.

Three properties decide it:

1. **`source_ts_ms` is already epoch-UTC.** Debezium emits it that way, so UTC is the
   only choice requiring **zero** conversions on the correctness-critical path. Every
   conversion is a place a boundary bug can live.
2. **Iceberg `timestamptz` semantics stay unambiguous.** Spark and Athena disagree
   subtly about `timestamp` without timezone; normalizing everything to UTC removes
   the disagreement rather than managing it.
3. **No DST, no offset arithmetic, anywhere.** `Asia/Ho_Chi_Minh` is a fixed +7 with
   no DST, so it would have been *workable* — but it still requires a conversion at
   every cutoff, and "workable with care" is a poor foundation for a hard invariant.

The cost is real and should be stated plainly: **"end of day" no longer matches a
Vietnam business day.** A Vietnamese business day ends at 17:00 ICT = 10:00 UTC, so a
UTC-midnight cutoff splits a local afternoon. For a portfolio lab with synthetic data
this is immaterial. For a real bank it would not be, which is why option 3 is
documented as the revisit path rather than dismissed.

## Consequences

- `CLAUDE.md` §5.6 offers both `event_ts < T00:00` and `source_commit_ts <= cutoff`.
  The `<` form is correct; `<=` would place a boundary event in two windows.
  `docs/DATA_CONTRACTS.md` §1.2 records this as a defect in the prose, and the
  half-open interval `[start, end)` is normative.
- Airflow DAGs run on UTC schedules. A DAG that "runs at midnight" runs at 07:00 ICT.
- Power BI displays UTC unless a mart explicitly converts. If a report shows local
  time, the conversion must be in the mart layer with a DQ check on the boundary
  (option 3's partial form).

## Cost

Zero.

## Security

None.

## Rollback

Changing the timezone later means **rebuilding every L2 `business_date` assignment
and every L3 snapshot partition**, because rows would have been assigned to different
windows. In practice this is one-way once real history accumulates — which is exactly
why it is decided in Session 01 rather than discovered in Session 08.

## Validation

- Boundary test: an event at exactly `T 00:00:00.000 UTC` lands in window `T`, not
  `T-1`.
- An event at `T 23:59:59.999 UTC` lands in window `T`.
- Reprocessing the same cutoff produces byte-identical assignments.
- No document uses a local-time cutoff (enforced by `scripts/validate-docs.py`).
