"""Shared contract for the realtime serving layer. STREAMING_RT.

Everything here is PURE and unit-testable without Spark, Kafka or AWS. The four apps
(`rt_eod_base`, `rt_stream_app`, `rt_autocorrect`, `rt_datamart_app`) hold the I/O; the
decisions live here, so the decisions can be tested.

THE ONE RULE THIS LAYER IS BUILT AROUND
---------------------------------------
The stream does not fix its own mistakes. It publishes what it enriched and RECORDS what it
could not, into pointer tables. A slower pass repairs from a fresher source.

The alternative -- have the stream retry until the dimension shows up -- is what makes
streaming pipelines fall over: the retry holds the micro-batch open, backpressure builds,
and the lag that started as one missing dimension becomes an outage. Publishing an
incomplete row and flagging it keeps the fast path's latency bounded by construction.

WHY `dim_complete` IS ONE FLAG AND NOT A PER-COLUMN NULL CHECK
--------------------------------------------------------------
The reference implementation decides completeness from ONE probe column
(`_is_dim_complete = miaccttypcd IS NOT NULL`) rather than from "are all dim columns
non-null". That looks sloppy and is deliberate: a dimension row can legitimately carry NULL
in an optional attribute, so "any NULL means incomplete" would flag healthy rows forever and
the pointer table would fill with work that can never be resolved. The probe column is the
one that proves THE JOIN MATCHED. Here it is `segment_code`, from the CUSTOMER dimension.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

#: Which dimension join decides completeness. See the module docstring.
DIM_PROBE_COLUMN = "segment_code"

#: How long the stream waits for a late dimension before giving up and flagging it. The
#: reference uses 20s. Kept identical: it is short enough that the micro-batch is never held
#: hostage, long enough to absorb the common case of a CUSTOMER event arriving microseconds
#: after the ACCOUNT event that references it.
PENDING_MAX_WAIT_SECONDS = 20

#: Cap on how many unresolved rows the driver holds. Beyond this the queue REFUSES rather
#: than growing: an unbounded in-driver buffer is a driver OOM with extra steps, and the
#: reference implementation hit exactly that (622K-row broadcast, driver 4g, ENRICH_FAIL).
PENDING_QUEUE_MAX = 10_000


@dataclass(frozen=True)
class AccountEvent:
    """One decoded ACCOUNT change from the CDC stream -- the fact."""
    account_id: int
    customer_id: int | None
    balance: float
    currency: str | None
    product_code: str | None
    account_status: str | None
    source_ts: datetime


@dataclass(frozen=True)
class CustomerDim:
    """One row of the CUSTOMER dimension -- what the fact is enriched with."""
    customer_id: int
    segment_code: str | None
    branch_id: int | None


@dataclass(frozen=True)
class EnrichedRow:
    account_id: int
    customer_id: int | None
    balance: float
    currency: str | None
    product_code: str | None
    account_status: str | None
    segment_code: str | None
    branch_id: int | None
    source_ts: datetime
    dim_complete: bool


@dataclass
class PendingEntry:
    """A row the stream could not finish. Held in the driver, then flagged on timeout."""
    event: AccountEvent
    missing_dims: str
    enqueued_at: datetime
    retry_count: int = 0

    def age_seconds(self, now: datetime) -> float:
        return (now - self.enqueued_at).total_seconds()

    def expired(self, now: datetime, max_wait: int = PENDING_MAX_WAIT_SECONDS) -> bool:
        return self.age_seconds(now) >= max_wait


def enrich(event: AccountEvent, dims: dict[int, CustomerDim]) -> EnrichedRow:
    """Join one fact to the dimension cache. Never raises, never drops.

    A missing dimension produces a row with NULL dims and `dim_complete=False` -- it is
    still a real balance and a report that silently omitted the account would be wrong in a
    way nobody could see. The flag is what makes the incompleteness visible instead.
    """
    dim = dims.get(event.customer_id) if event.customer_id is not None else None
    return EnrichedRow(
        account_id=event.account_id,
        customer_id=event.customer_id,
        balance=event.balance,
        currency=event.currency,
        product_code=event.product_code,
        account_status=event.account_status,
        segment_code=dim.segment_code if dim else None,
        branch_id=dim.branch_id if dim else None,
        source_ts=event.source_ts,
        # Completeness is "the join matched", probed on ONE column. See module docstring.
        dim_complete=bool(dim and getattr(dim, DIM_PROBE_COLUMN) is not None),
    )


def split_by_completeness(rows: list[EnrichedRow]) -> tuple[list[EnrichedRow],
                                                            list[EnrichedRow]]:
    """(publishable now, needs the pending path). Both halves are published eventually."""
    full = [r for r in rows if r.dim_complete]
    partial = [r for r in rows if not r.dim_complete]
    return full, partial


def dedup_latest_per_account(events: list[AccountEvent]) -> list[AccountEvent]:
    """One row per account: the LATEST BY EVENT TIME, not by arrival order.

    This ordering is the single most consequential line in the fast path, and the reference
    implementation documents why (`enrichment.py:300`): the source is a history table, so a
    retroactive correction for an older effective date can arrive AFTER the current one.
    Ordering by arrival (Kafka offset) would then keep the retroactive row and publish a
    STALE balance -- silently, and with a perfectly healthy-looking pipeline.

    Event time first; arrival order only breaks ties within the same event time.
    """
    best: dict[int, tuple[datetime, int, AccountEvent]] = {}
    for arrival, ev in enumerate(events):
        key = ev.account_id
        rank = (ev.source_ts, arrival)
        if key not in best or rank > (best[key][0], best[key][1]):
            best[key] = (ev.source_ts, arrival, ev)
    return [v[2] for v in sorted(best.values(), key=lambda t: t[2].account_id)]


def merge_base_and_stream(base: list[dict], stream: list[dict],
                          watermark: datetime | None) -> list[dict]:
    """The view, in Python, so its semantics are testable without a warehouse.

    STREAM wins for any account whose row is NEWER THAN THE WATERMARK; BASE supplies every
    account the stream has not touched since. This is a LEFT ANTI JOIN, not a row-level
    "prefer the newer timestamp" -- once the stream has spoken for an account after the
    watermark, that account's BASE row is dropped entirely.

    Getting this wrong in the obvious direction (always prefer STREAM) resurrects yesterday's
    fast-path rows after EOD has settled the day exactly, which is the one case where BASE is
    unambiguously more correct than STREAM.

    ONE EXCEPTION, AND IT IS THE FIX FOR RT-1.
    A correction pass repairs BASE for a single account without moving the watermark (the
    watermark is a statement about the WHOLE table, so a per-account repair cannot advance
    it). Under the plain rule above, the repaired BASE row stays masked by the stale STREAM
    row until the next EOD -- observed live 2026-09-03: account 990500 was repaired to
    `segment_code=PRIORITY` in BASE and the view still served `dim_complete=false` from
    STREAM.

    So a BASE row also wins when it was BUILT AFTER the stream row was WRITTEN. That is a
    strictly newer statement about the same account, from a slower and better-informed
    source. A later stream row (written after the repair) takes the account back, which is
    what keeps the layer realtime rather than freezing it at the last correction.

    The reference implementation has this same gap and names it; this closes it.
    """
    if watermark is None:
        fresh = {r["account_id"]: r for r in stream}
    else:
        fresh = {r["account_id"]: r for r in stream if r["source_ts"] > watermark}

    by_base = {r["account_id"]: r for r in base}
    winners: list[dict] = []
    for acct, srow in fresh.items():
        brow = by_base.get(acct)
        if brow is not None and _base_is_newer(brow, srow):
            winners.append(brow)
        else:
            winners.append(srow)
    winners.extend(r for acct, r in by_base.items() if acct not in fresh)
    return sorted(winners, key=lambda r: r["account_id"])


def _base_is_newer(base_row: dict, stream_row: dict) -> bool:
    """Was BASE rebuilt/repaired after this stream row was written?

    Both timestamps are optional so the comparison degrades safely: if either is absent we
    cannot show BASE is newer, and the plain STREAM-wins rule applies. Defaulting the other
    way would let a BASE row with no provenance silently outrank live data.
    """
    built = base_row.get("built_at")
    written = stream_row.get("written_at")
    return bool(built and written and built > written)


def detect_dim_changes(previous: dict[int, CustomerDim],
                       current: dict[int, CustomerDim]) -> list[dict]:
    """Dimension values that CHANGED -- pointer table 2.

    Only changes to an already-known customer count. A customer appearing for the first time
    is not a change; it is the arrival that pointer table 1 was waiting for, and reporting it
    here would make AUTOCORRECT re-process every new customer on every cycle.
    """
    out: list[dict] = []
    for cid, cur in current.items():
        prev = previous.get(cid)
        if prev is None:
            continue
        for fld in ("segment_code", "branch_id"):
            old, new = getattr(prev, fld), getattr(cur, fld)
            if old != new:
                out.append({"customer_id": cid, "changed_field": fld,
                            "old_value": None if old is None else str(old),
                            "new_value": None if new is None else str(new)})
    return out


class PendingQueue:
    """The driver-side wait for a late dimension. Bounded, refuses when full, THREAD-SAFE.

    `add` returning False is not an error path to smooth over -- it is backpressure. The
    caller flags the row immediately instead of buffering it, which is strictly better than
    growing the queue until the driver dies and the whole batch is lost.

    THE LOCK IS NOT DEFENSIVE PROGRAMMING -- it is required.
    The resolver runs on its OWN THREAD (see `rt_stream_app.start_resolver`), because
    draining only inside a micro-batch means a pending row STARVES whenever the source goes
    quiet: no new events, no batch, no timeout, and the row is never flagged. Observed on the
    first live run -- one account sat in the queue for the whole window with `timed_out=0`
    while the pointer table stayed empty. Two threads now touch `_items`, so it is locked.
    """

    def __init__(self, max_size: int = PENDING_QUEUE_MAX) -> None:
        import threading
        self._items: list[PendingEntry] = []
        self._max = max_size
        self._lock = threading.Lock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def add(self, entry: PendingEntry) -> bool:
        with self._lock:
            if len(self._items) >= self._max:
                return False
            self._items.append(entry)
            return True

    def drain_expired(self, now: datetime,
                      max_wait: int = PENDING_MAX_WAIT_SECONDS) -> list[PendingEntry]:
        with self._lock:
            expired = [e for e in self._items if e.expired(now, max_wait)]
            self._items = [e for e in self._items if not e.expired(now, max_wait)]
        return expired

    def retry_resolvable(self, dims: dict[int, CustomerDim],
                         now: datetime) -> tuple[list[EnrichedRow], list[PendingEntry]]:
        """Re-check the live dim cache for everything still within its wait window.

        Returns (rows that resolved, entries still waiting). Resolved rows leave the queue;
        the rest stay and age toward the timeout.
        """
        resolved: list[EnrichedRow] = []
        still: list[PendingEntry] = []
        with self._lock:
            for e in self._items:
                row = enrich(e.event, dims)
                if row.dim_complete:
                    resolved.append(row)
                else:
                    e.retry_count += 1
                    still.append(e)
            self._items = still
        return resolved, still
