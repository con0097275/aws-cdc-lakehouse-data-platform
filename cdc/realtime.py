"""REALTIME window resolution. Pure date arithmetic -- no Spark, no AWS, no I/O.

    run_upper_bound (FROZEN)  ->  logical window  ->  grace window  ->  retention bound

Separated from the Spark engine on purpose: the window is where this layer is easy to get
wrong and impossible to check by reading. A boundary that is one day out, or an hour out
across a DST change, produces a table that is the right SHAPE and the wrong CONTENTS -- it
reconciles, it partitions, it serves queries, and the numbers are simply not the ones the
policy promised. Pure functions make every boundary a value a test can assert on.

WHY DAYS AND HOURS ARE DIFFERENT THINGS (section B)
----------------------------------------------------
"3 days" is not 72 hours. A rolling 72h window from a run at 09:15 starts at 09:15 three
days earlier, so it holds three PARTIAL days and its contents shift every run -- two runs an
hour apart disagree about what "the last 3 days" contains, and neither is wrong. A calendar
window starts at MIDNIGHT in the table's business timezone and holds three WHOLE days, so
every run inside a day agrees.

Which one is right depends on what the table is for, so the platform refuses to guess: the
boundary is part of the config. `CALENDAR_DAY` is the default for a day-configured table,
because a policy written in days almost always means days, and `ROLLING_HOURS` is what an
hours-configured table keeps -- which is what makes this change a no-op for every table
already in the registry.

DST IS WHY THIS IS DATE ARITHMETIC AND NOT SUBTRACTION
-------------------------------------------------------
`instant - timedelta(days=3)` is subtraction of 72 hours, and across a DST transition that
lands an hour off the local midnight it was supposed to hit. The calendar bounds below do
the arithmetic on the LOCAL DATE and then convert, so "three days ago at midnight" is the
midnight that actually occurred. The business timezone is per table and configurable
(ADR-024 pins UTC as the default, not as the only possibility), so this is reachable.
"""
from __future__ import annotations

import zoneinfo
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from .models import ConfigError

#: The window a run promises to serve, and how its lower bound is found.
BOUNDARY_CALENDAR_DAY = "calendar_day"
BOUNDARY_ROLLING_HOURS = "rolling_hours"
BOUNDARIES = (BOUNDARY_CALENDAR_DAY, BOUNDARY_ROLLING_HOURS)

#: How a run writes the window it computed.
REFRESH_FULL = "full_refresh"
REFRESH_INCREMENTAL = "incremental_merge"
#: One row per BUSINESS KEY, upserted. The hot-path shape: a consumer reads current state
#: without collapsing anything itself.
#:
#: The other two modes keep EVENTS. This one keeps STATE, and that is a different product:
#:   full_refresh      every event in [grace_lower, upper), rebuilt each run
#:   incremental_merge every event, appended by dv_event_id
#:   latest_state      one row per dv_pk_hash, the winner by the EOD ranking
REFRESH_LATEST_STATE = "latest_state"
REFRESH_MODES = (REFRESH_FULL, REFRESH_INCREMENTAL, REFRESH_LATEST_STATE)

# --------------------------------------------------------------------------- #
# shape and write strategy -- the two questions `refresh_mode` conflated (R2-B)
# --------------------------------------------------------------------------- #
#
# `refresh_mode` answered two independent questions with one word:
#
#   WHAT IS IN THE TABLE      one row per business key, or the events themselves?
#   HOW IS IT WRITTEN         replace the window, append to it, or merge under a guard?
#
# They are independent. An event window can be rebuilt or appended to and it is the same
# PRODUCT either way -- the difference is cost and idempotency, not meaning. A state table
# written by anything other than a guarded merge is not a state table at all. Keeping them
# in one field meant a consumer asking "may I read this as current state?" had to know which
# of three strings implied it, and a new write strategy could not be added without inventing
# a fourth product name.
#
# SHAPE is the CONTRACT. It is what a downstream reader may assume.

#: One row per business key: the current state of each entity, as of the run's upper bound.
#: A reader may use it as current state directly (subject to the tombstone rule, ADR-081).
SHAPE_LATEST_STATE = "latest_state"
#: The events themselves, bounded by the window. A row updated three times appears three
#: times. A reader wanting current state must collapse it itself -- see §29 of the R2 brief
#: and `docs/REALTIME_LAYER_PRODUCTION_DESIGN.md`.
SHAPE_EVENT_WINDOW = "event_window"
SHAPES = (SHAPE_EVENT_WINDOW, SHAPE_LATEST_STATE)

# WRITE_STRATEGY is the MECHANISM. It changes cost and failure behaviour, never meaning.

#: Rebuild the whole window atomically each run. Idempotent and self-healing, and it ages
#: rows out for free -- but it re-reads the entire window every run, which is what R2-C's
#: incremental cursor exists to stop.
WRITE_OVERWRITE_WINDOW = "overwrite_window"
#: Append new events, identified by `dv_event_id`. Cheap; needs the retention prune to be a
#: separate step because nothing is removed by the write itself.
WRITE_APPEND = "append"
#: MERGE under an order-key guard: a row is only updated by an event that is NEWER in
#: source order. The only strategy that can produce `latest_state` correctly.
WRITE_GUARDED_MERGE = "guarded_merge"
WRITE_STRATEGIES = (WRITE_OVERWRITE_WINDOW, WRITE_APPEND, WRITE_GUARDED_MERGE)

#: Which write strategies each shape can be produced by. `latest_state` has exactly one:
#: an append or an overwrite would either duplicate a key or lose the guard that stops a
#: late out-of-order event from regressing the row.
SHAPE_WRITE_STRATEGIES = {
    SHAPE_EVENT_WINDOW: (WRITE_OVERWRITE_WINDOW, WRITE_APPEND),
    SHAPE_LATEST_STATE: (WRITE_GUARDED_MERGE,),
}

#: The legacy vocabulary, and exactly what each value meant. This is a RENAME, not a
#: behaviour change: every mapping below preserves what the engine does today.
LEGACY_REFRESH_MODES = {
    REFRESH_FULL: (SHAPE_EVENT_WINDOW, WRITE_OVERWRITE_WINDOW),
    REFRESH_INCREMENTAL: (SHAPE_EVENT_WINDOW, WRITE_APPEND),
    REFRESH_LATEST_STATE: (SHAPE_LATEST_STATE, WRITE_GUARDED_MERGE),
}
#: And back again, for the Spark engine until R2-E reads `shape` directly. Deriving it
#: rather than storing both is the point: two fields that must agree are two fields that
#: eventually will not.
REFRESH_MODE_FOR = {v: k for k, v in LEGACY_REFRESH_MODES.items()}

DEFAULT_SHAPE = SHAPE_EVENT_WINDOW
DEFAULT_WRITE_STRATEGY = {
    SHAPE_EVENT_WINDOW: WRITE_OVERWRITE_WINDOW,
    SHAPE_LATEST_STATE: WRITE_GUARDED_MERGE,
}

# --------------------------------------------------------------------------- #
# how a run finds the rows it has not processed yet
# --------------------------------------------------------------------------- #
#: Filter FULL_CDC by `source_commit_ts` over the whole window, every run. What runs today.
PROGRESS_WINDOW_SCAN = "window_scan"
#: Read only the FULL_CDC snapshots appended since the last successful run. R2-C.
PROGRESS_ICEBERG_SNAPSHOT = "iceberg_snapshot"
SOURCE_PROGRESS_MODES = (PROGRESS_WINDOW_SCAN, PROGRESS_ICEBERG_SNAPSHOT)
DEFAULT_SOURCE_PROGRESS = PROGRESS_WINDOW_SCAN

#: How the winner of two events for the same key is chosen. One value today, named rather
#: than implied, because `order_key_expr` below is engine-specific and a second strategy
#: (say, a source-supplied version column) is a real possibility.
ORDERING_SOURCE_NATIVE = "source_native"
ORDERING_STRATEGIES = (ORDERING_SOURCE_NATIVE,)

#: A deleted key is KEPT, flagged. Required by `latest_state`: physically removing the row
#: leaves nothing for the next batch's guard to compare against, and a late out-of-order
#: event then resurrects the key (ADR-081).
MERGE_DELETE_SOFT_TOMBSTONE = "soft_tombstone"
MERGE_DELETE_POLICIES = (MERGE_DELETE_SOFT_TOMBSTONE,)


def resolve_shape(raw: dict, *, what: str) -> tuple[str, str, str]:
    """`(shape, write_strategy, deprecation)` from either vocabulary.

    Accepts the new `shape` / `write_strategy` pair and the legacy `refresh_mode`, and
    REFUSES a config that declares both in disagreement. A config that says one thing in
    two places is the defect this split exists to remove; silently preferring one of them
    would leave the other looking live.

    `deprecation` is a warning string, or empty. It is returned rather than printed so the
    caller decides where it goes -- a compile prints it, a test asserts on it.
    """
    raw = raw or {}
    legacy = raw.get("refresh_mode")
    shape = raw.get("shape")
    strategy = raw.get("write_strategy")
    deprecation = ""

    if legacy is not None:
        if legacy not in LEGACY_REFRESH_MODES:
            raise ConfigError(f"{what}.refresh_mode: {legacy!r} is not one of "
                              f"{', '.join(REFRESH_MODES)}")
        mapped_shape, mapped_strategy = LEGACY_REFRESH_MODES[legacy]
        if shape is None and strategy is None:
            deprecation = (f"{what}: `refresh_mode: {legacy}` is deprecated; it compiles to "
                           f"`shape: {mapped_shape}, write_strategy: {mapped_strategy}`. "
                           f"Declare those instead.")
            return mapped_shape, mapped_strategy, deprecation
        # Both vocabularies present. They must AGREE -- see the docstring.
        eff_shape = shape or mapped_shape
        eff_strategy = strategy or mapped_strategy
        if (eff_shape, eff_strategy) != (mapped_shape, mapped_strategy):
            raise ConfigError(
                f"{what}: `refresh_mode: {legacy}` means "
                f"`shape: {mapped_shape}, write_strategy: {mapped_strategy}`, but this "
                f"table also declares shape={eff_shape!r} write_strategy={eff_strategy!r}. "
                f"Remove `refresh_mode`; it is the deprecated spelling of the same thing.")
        deprecation = (f"{what}: `refresh_mode` is deprecated and redundant here -- "
                       f"`shape`/`write_strategy` already say the same thing. Remove it.")
        return mapped_shape, mapped_strategy, deprecation

    shape = DEFAULT_SHAPE if shape is None else str(shape).strip().lower()
    if shape not in SHAPES:
        raise ConfigError(f"{what}.shape: {shape!r} is not one of {', '.join(SHAPES)}")
    if strategy is None:
        strategy = DEFAULT_WRITE_STRATEGY[shape]
    else:
        strategy = str(strategy).strip().lower()
        if strategy not in WRITE_STRATEGIES:
            raise ConfigError(f"{what}.write_strategy: {strategy!r} is not one of "
                              f"{', '.join(WRITE_STRATEGIES)}")
    allowed = SHAPE_WRITE_STRATEGIES[shape]
    if strategy not in allowed:
        raise ConfigError(
            f"{what}: shape {shape!r} cannot be written with write_strategy "
            f"{strategy!r}. Allowed: {', '.join(allowed)}. "
            + (f"A {SHAPE_LATEST_STATE} table needs {WRITE_GUARDED_MERGE}: an append would "
               f"duplicate the key and an overwrite would drop the guard that stops a late "
               f"out-of-order event regressing the row."
               if shape == SHAPE_LATEST_STATE else
               f"An {SHAPE_EVENT_WINDOW} table keeps events; a guarded merge would collapse "
               f"them to one row per key, which is a different product."))
    return shape, strategy, deprecation


def prunes_by_age(shape: str) -> bool:
    """Whether the retention bound is a PRUNE or merely a recovery horizon.

    `event_window` prunes: an event older than the retention bound is outside what the
    layer serves, so removing it is correct.

    `latest_state` does NOT. A valid account may not change for months, and deleting its
    row because its last event is old would silently empty the table of exactly the
    entities that are most stable (R2 brief §15). For a state table the same numbers mean
    "how far back a rebuild reads", not "what gets deleted".
    """
    return shape != SHAPE_LATEST_STATE

#: Terminal states of a run, recorded in the ledger (section D).
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"
STATUS_RUNNING = "RUNNING"
STATUS_SKIPPED_DISABLED = "SKIPPED_DISABLED"


def _zone(tz: str):
    try:
        return zoneinfo.ZoneInfo(tz)
    except Exception:
        raise ConfigError(
            f"business_timezone {tz!r} is not a valid IANA zone. A wrong zone moves the "
            f"day boundary and materialises a different set of events than the policy "
            f"names (ADR-024)") from None


def start_of_local_day(instant: datetime, tz: str, *, days_back: int = 0) -> datetime:
    """Midnight, `days_back` local days before the local day containing `instant`, as UTC.

    The arithmetic is done on the local DATE and then converted, never by subtracting a
    timedelta from the instant: across a DST transition those differ by an hour, and the
    hour lands in a neighbouring day's partition where nothing reports it.
    """
    if instant.tzinfo is None:
        raise ConfigError("window bounds need an aware datetime; got a naive one")
    zone = _zone(tz)
    local_day: date = instant.astimezone(zone).date() - timedelta(days=days_back)
    return datetime.combine(local_day, time.min, tzinfo=zone).astimezone(timezone.utc)


@dataclass(frozen=True)
class RealtimeWindow:
    """Every boundary one run uses, resolved once and then FROZEN (section D).

    Four bounds, and they are genuinely four different things:

    `logical_lower` .. `upper`   what the layer PROMISES to serve. This is the window a
                                 consumer may rely on, and the one the SLA is about.
    `grace_lower`   .. `upper`   what is actually MATERIALISED. Wider, so an event that
                                 committed before the logical bound but arrived late is
                                 still present rather than silently absent.
    `retention_bound`            rows older than this are pruned. At or before
                                 `grace_lower`, enforced at compile time (section F).
    """
    table_id: str
    run_id: str
    upper: datetime
    logical_lower: datetime
    grace_lower: datetime
    retention_bound: datetime
    boundary: str
    business_timezone: str
    lookback_days: int | None = None
    grace_days: int | None = None
    retention_days: int | None = None
    lookback_hours: int | None = None
    grace_hours: int | None = None
    retention_hours: int | None = None

    def __post_init__(self) -> None:
        # Ordering is an INVARIANT, not an expectation. A window whose lower bound is at or
        # after its upper bound materialises nothing, and "the job succeeded and the table
        # is empty" is the single hardest REALTIME failure to notice.
        if not self.grace_lower <= self.logical_lower:
            raise ConfigError(
                f"{self.table_id}: grace lower bound {self.grace_lower.isoformat()} is "
                f"after the logical lower bound {self.logical_lower.isoformat()}")
        if not self.logical_lower < self.upper:
            raise ConfigError(
                f"{self.table_id}: window is empty or inverted -- logical lower "
                f"{self.logical_lower.isoformat()} is not before upper "
                f"{self.upper.isoformat()}")
        if not self.retention_bound <= self.grace_lower:
            raise ConfigError(
                f"{self.table_id}: retention bound {self.retention_bound.isoformat()} is "
                f"after the materialised lower bound {self.grace_lower.isoformat()}, so "
                f"the prune would delete rows this run just wrote (section F)")

    @property
    def duration_hours(self) -> float:
        return (self.upper - self.grace_lower).total_seconds() / 3600.0

    def describe(self) -> str:
        return (f"{self.table_id} [{self.grace_lower.isoformat()} -> "
                f"{self.upper.isoformat()}) boundary={self.boundary} "
                f"logical_lower={self.logical_lower.isoformat()} "
                f"retention_bound={self.retention_bound.isoformat()}")

    def payload(self) -> dict:
        """The ledger row's window fields (section D)."""
        return {
            "table_id": self.table_id,
            "run_id": self.run_id,
            "boundary": self.boundary,
            "business_timezone": self.business_timezone,
            "upper_bound": self.upper,
            "logical_lower_bound": self.logical_lower,
            "grace_lower_bound": self.grace_lower,
            "retention_bound": self.retention_bound,
        }


def resolve_window(policy: dict, *, table_id: str, run_id: str,
                   run_upper_bound: datetime,
                   business_timezone: str = "UTC") -> RealtimeWindow:
    """Freeze every boundary for one run from the resolved realtime policy.

    `run_upper_bound` is supplied by the CALLER and never read from the clock in here. That
    is what section D asks for and it is not a formality: a job that consulted `now()` at
    each step would filter with one bound, prune with another and record a third, and the
    three would differ by however long the run took. Every downstream comparison against the
    ledger would then be off by that amount, in a way that looks like late data.
    """
    boundary = policy.get("boundary", BOUNDARY_ROLLING_HOURS)
    if boundary not in BOUNDARIES:
        raise ConfigError(f"{table_id}: realtime.boundary {boundary!r} is not one of "
                          f"{', '.join(BOUNDARIES)}")
    # REFUSED BEFORE THE CONVERSION, not after. `naive.astimezone(utc)` does not raise -- it
    # silently assumes the MACHINE's local zone, so the same `--as-of` string would resolve
    # to a different window on a laptop in UTC+7 than on an EMR worker in UTC. Converting
    # first would put that reinterpretation upstream of every check below.
    if run_upper_bound.tzinfo is None:
        raise ConfigError(
            f"{table_id}: run_upper_bound must be timezone-aware. A naive instant is "
            f"assumed to be the machine's local zone, so the window would depend on where "
            f"the job happened to run")
    upper = run_upper_bound.astimezone(timezone.utc)

    if boundary == BOUNDARY_CALENDAR_DAY:
        lookback = int(policy["lookback_days"])
        grace = int(policy["late_arrival_grace_days"])
        retention = int(policy["physical_retention_days"])
        # N days INCLUDING the day the run falls in, so lookback_days=1 means "today".
        # Off-by-one here is the difference between serving today and serving today plus
        # yesterday, and both look completely normal.
        logical_lower = start_of_local_day(upper, business_timezone,
                                           days_back=lookback - 1)
        grace_lower = start_of_local_day(upper, business_timezone,
                                         days_back=lookback - 1 + grace)
        retention_bound = start_of_local_day(upper, business_timezone,
                                             days_back=retention - 1)
        return RealtimeWindow(
            table_id=table_id, run_id=run_id, upper=upper,
            logical_lower=logical_lower, grace_lower=grace_lower,
            retention_bound=retention_bound, boundary=boundary,
            business_timezone=business_timezone,
            lookback_days=lookback, grace_days=grace, retention_days=retention)

    lookback_h = int(policy["window_hours"])
    grace_h = int(policy["late_grace_hours"])
    retention_h = int(policy["retention_hours"])
    logical_lower = upper - timedelta(hours=lookback_h)
    grace_lower = upper - timedelta(hours=lookback_h + grace_h)
    retention_bound = upper - timedelta(hours=retention_h)
    return RealtimeWindow(
        table_id=table_id, run_id=run_id, upper=upper,
        logical_lower=logical_lower, grace_lower=grace_lower,
        retention_bound=retention_bound, boundary=boundary,
        business_timezone=business_timezone,
        lookback_hours=lookback_h, grace_hours=grace_h, retention_hours=retention_h)


# --------------------------------------------------------------------------- #
# section 3/5 -- cadence and sizing live in `cdc/scheduling.py`
# --------------------------------------------------------------------------- #
#
# They moved there in Phase D, when EOD needed the identical two decisions. The names below
# are re-exported so every existing caller and test keeps working, and so there is exactly
# one implementation of "what is a valid cron" and "how big is a medium job".
from .scheduling import (RESOURCE_PROFILES, DEFAULT_RESOURCE_PROFILE,   # noqa: E402,F401
                         resolve_resource_profile)
from . import scheduling as _sched                                       # noqa: E402

#: The candidate cadence the Phase C brief names. A DEFAULT, not a constant: a table whose
#: window is written in calendar days gains nothing from a 10-minute refresh. Until Phase C
#: this value lived in an Airflow env var, so `schedule:` in the registry compiled into the
#: plan and changed nothing -- config that looks live and is not.
DEFAULT_SCHEDULE = "*/10 * * * *"


def normalise_schedule(raw, *, what: str, default: str = DEFAULT_SCHEDULE) -> str:
    """REALTIME's cadence, defaulting to `*/10 * * * *`. See `cdc/scheduling.py`."""
    return _sched.normalise_schedule(raw, what=what, default=default)


def schedule_groups(plan: dict) -> dict:
    """`{schedule: [table_id, ...]}` for every REALTIME-enabled table."""
    return _sched.schedule_groups(plan, policy_key="realtime_policy",
                                  default=DEFAULT_SCHEDULE)


#: The ledger that records what each run actually did (section D). One table for the whole
#: platform, in OPS -- coordinates and outcomes, never payload.
RUN_LEDGER_TABLE = "realtime_run"

RUN_LEDGER_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("run_id", "STRING", "the run this row describes"),
    ("table_id", "STRING", "canonical source identity"),
    ("target_table", "STRING", "the REALTIME table materialised"),
    ("source_table", "STRING", "the FULL_CDC table read"),
    ("boundary", "STRING", "calendar_day | rolling_hours"),
    ("business_timezone", "STRING", "the zone the calendar boundary was taken in"),
    ("refresh_mode", "STRING", "full_refresh | incremental_merge | latest_state"),
    ("logical_lower_bound", "TIMESTAMP", "start of the window the layer PROMISES"),
    ("grace_lower_bound", "TIMESTAMP", "start of what was actually materialised"),
    ("upper_bound", "TIMESTAMP", "the FROZEN upper bound; never moves during a run"),
    ("retention_bound", "TIMESTAMP", "rows older than this were pruned"),
    ("operation", "STRING",
     "materialise | rebase. Two genuinely different things happen to this table and both "
     "leave a row here: one ADDS what is new, the other REMOVES what the certified "
     "baseline now holds. A history that conflated them could not answer 'what removed "
     "these rows'"),
    ("prev_baseline_cob_date", "DATE", "the baseline a rebase moved this table OFF"),
    ("shape", "STRING", "event_window | latest_state -- the CONTRACT this run produced"),
    ("write_strategy", "STRING", "overwrite_window | append | guarded_merge"),
    ("attempt", "INT", "1-based, counted from history rather than from the process"),
    ("read_mode", "STRING",
     "incremental | window_scan | noop -- how this run actually read FULL_CDC. Recorded "
     "because a plan that SAYS incremental and a run that FELL BACK cost ten times as "
     "much, and a week later nothing else can tell them apart"),
    ("fallback_reason", "STRING",
     "why an incremental run fell back to the window scan; empty when it did not"),
    ("source_snapshot_before", "BIGINT",
     "THE CURSOR as this run found it -- the last snapshot fully incorporated before it "
     "started. With source_snapshot_id this gives the exact range the run consumed"),
    ("source_snapshot_id", "BIGINT",
     "the FULL_CDC snapshot read. Makes a run reproducible: re-reading THAT snapshot with "
     "these bounds must give the same rows"),
    ("input_rows", "BIGINT", "rows READ from FULL_CDC, before any collapse"),
    ("spark_app_id", "STRING", "the Spark application, for finding the driver log"),
    ("baseline_cob_date", "DATE",
     "the certified EOD close this run's state is an overlay ON. NULL until R2-F"),
    ("target_snapshot_id", "BIGINT", "the REALTIME snapshot written"),
    ("row_count", "BIGINT", "rows in the target after the run"),
    ("pruned_count", "BIGINT", "rows removed by the retention prune"),
    ("status", "STRING", "RUNNING | SUCCEEDED | FAILED | SKIPPED_DISABLED"),
    ("failure_reason", "STRING", "why, when status is FAILED"),
    ("started_at", "TIMESTAMP", "when the run began"),
    ("finished_at", "TIMESTAMP", "when it ended"),
    ("config_version", "STRING", "the compiled plan this run was driven by"),
)


# --------------------------------------------------------------------------- #
# latest_state: the ordering that decides which event wins
# --------------------------------------------------------------------------- #


def order_key_expr(engine: str, alias: str = "") -> str:
    """ONE string-comparable expression encoding the full event-order tuple.

    Built from `cdc.eod.ordering_for`, which is the SAME source of truth the EOD close
    ranks with. That symmetry is the whole reason this is not hand-written here: if
    REALTIME ranked differently from EOD, a variance between a provisional number and a
    certified one would be ambiguous between "late data" and "divergent logic", and only
    one of those is a bug worth chasing.

    Precedence, from DATA_CONTRACTS 4.1:

        position_primary, position_secondary, source_commit_ts, kafka_partition, kafka_offset

    `kafka_partition` PRECEDES `kafka_offset` because an offset is monotonic only within
    its own partition (CLAUDE.md 5.4).

    A single concatenated key rather than a chained comparison: a MERGE guard written as
    `(a>a') OR (a=a' AND b>b') OR ...` has one NULL-handling mistake per term, and every
    one of them is silent -- it does not error, it picks the wrong winner. Fixed-width
    zero-padded parts make one `>` correct for every component at once.

    Every part is coalesced to a floor, so a NULL never compares HIGH and lets an
    incomplete event beat a complete one.
    """
    from cdc.eod import ordering_for

    spec = ordering_for(engine)
    p = f"{alias}." if alias else ""
    parts = [
        spec.sort_key(f"coalesce({p}position_primary, '')"),
        spec.sort_key(f"coalesce({p}position_secondary, '')"),
        f"date_format(coalesce({p}source_commit_ts, TIMESTAMP '1970-01-01 00:00:00'), "
        f"'yyyyMMddHHmmssSSS')",
        f"lpad(cast(coalesce({p}kafka_partition, 0) as string), 10, '0')",
        f"lpad(cast(coalesce({p}kafka_offset, 0) as string), 20, '0')",
    ]
    return "concat(" + ", '|', ".join(parts) + ")"
