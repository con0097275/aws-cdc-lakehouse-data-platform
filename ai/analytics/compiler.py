"""Deterministic metric -> SQL compiler.

The LLM never writes this SQL. It chooses a metric, a window and a dimension by NAME; the
shape of the statement is fixed here and only parameters vary. That is what makes the query
reviewable once instead of on every request, and it is why the Athena guards have a bounded
surface to defend.

Four refusals happen BEFORE any SQL exists, because each is cheaper and clearer as a compile
error than as an Athena failure or, worse, a plausible wrong number:

  * a dimension not allowed for the metric
  * a dimension that is allowed but whose table is NOT MATERIALISED
  * a time grain the metric cannot legally roll up to (semi-additive protection)
  * a certification tier below the metric's minimum
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .semantic import (DimensionNotAllowed, DimensionUnavailable, Metric, MetricError,
                       Registry, load_registry)
from .timespec import TimeWindow

#: The certification ladder, weakest to strongest. Defined HERE, in the lower-level module,
#: so `plan.py` and the SQL it generates can never disagree about which tier is weaker.
STATUS_ORDER = ("REALTIME", "PROVISIONAL_NRT", "PROVISIONAL_CORRECTED", "RECONCILED",
                "CERTIFIED")

#: Identifiers we will emit. Anything else is rejected rather than quoted, because a value
#: that needs escaping to be safe here is a value that should not have reached the compiler.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATION = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

MAX_ROWS = 1000
MAX_WINDOW_DAYS = 400


def _ident(v: str, what: str) -> str:
    if not _IDENT.match(v or ""):
        raise MetricError(f"unsafe {what}: {v!r}")
    return v


@dataclass
class CompiledQuery:
    metric_id: str
    metric_version: str
    sql: str
    relation: str
    window: dict
    dimension: str | None
    row_limit: int
    minimum_certification: str
    notes: list[str] = field(default_factory=list)

    def sql_hash(self) -> str:
        import hashlib
        return "sql:" + hashlib.sha256(self.sql.encode()).hexdigest()[:16]

    def to_dict(self) -> dict:
        return {"metric_id": self.metric_id, "metric_version": self.metric_version,
                "sql": self.sql, "sql_hash": self.sql_hash(), "relation": self.relation,
                "window": self.window, "dimension": self.dimension,
                "row_limit": self.row_limit,
                "minimum_certification": self.minimum_certification, "notes": self.notes}


def _check_dimension(reg: Registry, metric: Metric, dimension: str) -> str:
    if dimension not in metric.allowed_dimensions:
        raise DimensionNotAllowed(
            f"{dimension!r} is not an allowed dimension for {metric.metric_id}. "
            f"Allowed: {list(metric.allowed_dimensions)}. Explaining a metric with an "
            "undeclared column is how a spurious correlation becomes a reported cause.")
    d = reg.dimension(dimension)
    if not d.available:
        raise DimensionUnavailable(
            f"{dimension!r} is declared for {metric.metric_id} but is NOT QUERYABLE: "
            f"{d.blocked_reason}. It needs {d.requires_join}. Refusing rather than "
            "silently answering without the breakdown that was asked for.")
    return _ident(d.column, "dimension column")


def compile_metric_query(metric_id: str, window: TimeWindow, *, dimension: str | None = None,
                         grain: str = "day", registry: Registry | None = None,
                         row_limit: int = MAX_ROWS,
                         enforce_certification: bool = True,
                         order_by_value: str | None = None,
                         collapse_time: bool = False) -> CompiledQuery:
    """`order_by_value` is "desc" (TOP_N) or "asc" (BOTTOM_N).

    `collapse_time` drops business_date from the projection so TOP_N ranks segments over the
    WHOLE window instead of returning one row per date per segment -- "top 5 accounts last
    week" means five rows, not five per day.
    """
    reg = registry or load_registry()
    m = reg.get(metric_id)
    notes: list[str] = []

    if m.status != "active":
        raise MetricError(f"{metric_id} has status {m.status!r}")
    if grain not in m.supported_time_grains:
        raise MetricError(
            f"{metric_id} supports {list(m.supported_time_grains)} but {grain!r} was asked. "
            f"time_additivity={m.time_additivity}: rolling it up would produce a number "
            "that looks plausible and is wrong.")
    for d in (window.start, window.end):
        if not _DATE.match(d):
            raise MetricError(f"date {d!r} must be YYYY-MM-DD")
    if window.days() > MAX_WINDOW_DAYS:
        raise MetricError(f"window of {window.days()} days exceeds {MAX_WINDOW_DAYS}")
    if not _RELATION.match(m.source_relation):
        raise MetricError(f"unsafe relation {m.source_relation!r}")
    if not 1 <= row_limit <= MAX_ROWS:
        raise MetricError(f"row_limit must be 1..{MAX_ROWS}")

    time_col = _ident(m.time_column, "time column")
    dim_col = _check_dimension(reg, m, dimension) if dimension else None

    if order_by_value not in (None, "desc", "asc"):
        raise MetricError(f"order_by_value must be desc|asc, got {order_by_value!r}")
    if order_by_value and not dim_col:
        raise MetricError("ranking requires a dimension: TOP_N over no segment is one row")
    if collapse_time and m.time_additivity == "semi_additive_last" and window.days() > 1:
        raise MetricError(
            f"{metric_id} is SEMI-ADDITIVE across time: collapsing a {window.days()}-day "
            "window would SUM balances across dates and overstate the total. Rank a single "
            "date, or use an additive metric.")

    select, group = [], []
    if not collapse_time:
        select.append(f"{time_col} AS business_date")
        group.append(time_col)
    if dim_col:
        select.append(f"{dim_col} AS segment")
        group.append(dim_col)
    select.append(f"{m.measure_sql} AS metric_value")
    select.append("COUNT(*) AS row_count")
    # The weakest tier present in the window. Reporting the strongest, or the mode, would
    # let one provisional partition hide inside an otherwise certified answer.
    #
    # This was `MIN(processing_status)`, and MIN on a varchar is ALPHABETICAL: 'CERTIFIED'
    # sorts before 'PROVISIONAL_NRT' and 'RECONCILED', so the strongest tier won whenever it
    # was present -- exactly the hiding the comment above forbids, and exactly backwards.
    # It never fired only because every date's rows are written by one run and share a tier.
    # `min_by(x, y)` returns the x at the minimum y, so the rank decides and the NAME is
    # still what comes back.
    # Rank-prefixed so plain MIN orders by TIER, and so the expression is portable: the
    # test harness executes this same SQL against SQLite, which has no `min_by`. The prefix
    # is one digit, so lexicographic order and numeric order agree. `plan.py` strips it.
    _rank = " ".join(f"WHEN '{s_}' THEN '{i}:{s_}'" for i, s_ in enumerate(STATUS_ORDER))
    select.append(f"MIN(CASE processing_status {_rank} ELSE '9:UNKNOWN' END) "
                  "AS weakest_status")

    where = [f"{time_col} BETWEEN DATE '{window.start}' AND DATE '{window.end}'"]
    for f in m.default_filters:
        if not re.match(r"^[A-Za-z0-9_ ='<>.\-]+$", f):
            raise MetricError(f"unsafe default filter {f!r}")
        where.append(f)

    if enforce_certification:
        notes.append(f"caller must verify weakest_status >= {m.minimum_certification}")
    if m.time_additivity == "semi_additive_last" and window.days() > 1:
        notes.append(
            f"{metric_id} is SEMI-ADDITIVE across time: this returns one value PER DATE. "
            "Summing the rows would be wrong; take the last date, or compare per date.")

    order = (f"metric_value {order_by_value.upper()}" if order_by_value
             else ", ".join(group))
    sql = ("SELECT " + ", ".join(select)
           + f" FROM {m.source_relation}"
           + " WHERE " + " AND ".join(where)
           + " GROUP BY " + ", ".join(group)
           + " ORDER BY " + order
           + f" LIMIT {row_limit}")

    return CompiledQuery(metric_id=m.metric_id, metric_version=m.version_hash(), sql=sql,
                         relation=m.source_relation, window={"start": window.start,
                                                             "end": window.end,
                                                             "label": window.label},
                         dimension=dimension, row_limit=row_limit,
                         minimum_certification=m.minimum_certification, notes=notes)
