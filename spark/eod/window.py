"""EOD cutoff window and late-arrival sweep (DATA_CONTRACTS §6.1, §6.2).

Three things here are easy to get wrong and expensive to discover later:

  1. The window is HALF-OPEN: >= start AND < end. A closed upper bound assigns a
     midnight-boundary event to TWO business dates, double-counting it in L2.
  2. Everything is UTC (ADR-024). A local-time cutoff silently shifts the boundary and
     moves events between days depending on where the job ran.
  3. The cutoff is on source_commit_ts (BUSINESS time), never ingest_ts (pipeline time).
     Using ingest_ts makes the same event land on different dates across reruns.

Pure functions, unit-tested in spark/tests/test_l2_window.py.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


class WindowError(ValueError):
    """Raised when a window is unusable. Fatal by design — a wrong window silently
    produces wrong business numbers rather than an error."""


def cutoff_window(business_date: date) -> tuple[datetime, datetime]:
    """Half-open [T 00:00:00Z, T+1 00:00:00Z) for a business date.

    S01-18: `<`, not `<=`. CLAUDE.md §5.6's prose uses `<=`, which would place a
    boundary event in two windows; DATA_CONTRACTS §6.1 is normative and uses `<`.
    """
    if not isinstance(business_date, date) or isinstance(business_date, datetime):
        raise WindowError(f"business_date must be a date, got {type(business_date).__name__}")
    start = datetime.combine(business_date, datetime.min.time(), tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def is_in_window(commit_ts: datetime, start: datetime, end: datetime) -> bool:
    """Half-open membership. Exists so the boundary rule is testable in isolation."""
    if commit_ts.tzinfo is None:
        raise WindowError("commit_ts must be timezone-aware; naive timestamps hide UTC bugs")
    return start <= commit_ts < end


def sweep_predicate(start: datetime, end: datetime, last_watermark: datetime | None) -> str:
    """SQL predicate implementing §6.1 plus the §6.2 late-arrival sweep.

    The second clause picks up anything L1 wrote since the previous L2 run regardless of
    its commit time — an event that committed on T-1 but only reached L1 on T would
    otherwise be missed forever by the first clause.

    The overlap is harmless BECAUSE the MERGE is idempotent on event_id. That is exactly
    what the key buys: a wider, safer sweep at no correctness cost.
    """
    if end <= start:
        raise WindowError(f"end {end} must be after start {start}")
    base = (f"(source_commit_ts >= TIMESTAMP '{_fmt(start)}' "
            f"AND source_commit_ts < TIMESTAMP '{_fmt(end)}')")
    if last_watermark is None:
        # First ever run for this table: no watermark, so the base window is all there is.
        return base
    if last_watermark > end:
        raise WindowError(
            f"watermark {last_watermark} is after the window end {end}; "
            "re-running an OLD date after a NEWER one would sweep future rows into it"
        )
    late = (f"(l1_write_ts >= TIMESTAMP '{_fmt(last_watermark)}' "
            f"AND source_commit_ts < TIMESTAMP '{_fmt(end)}')")
    return f"({base} OR {late})"


def safety_overlap(last_watermark: datetime | None, minutes: int) -> datetime | None:
    """Rewind the watermark slightly to absorb clock skew between Connect and Spark.

    Without it, an event written to L1 microseconds before the recorded watermark can
    fall through the gap between two runs and never be swept.
    """
    if last_watermark is None:
        return None
    if minutes < 0:
        raise WindowError("safety overlap must not be negative")
    return last_watermark - timedelta(minutes=minutes)


def _fmt(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
