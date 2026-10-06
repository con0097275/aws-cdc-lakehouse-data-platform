"""Where the numbers come from.

Two implementations behind one interface so every engine is testable with no AWS at all:

  AthenaSource   the governed path. Read-only SQL, workgroup-enforced scan cap.
  FrameSource    in-memory rows for tests and for local work while the platform is destroyed.

The engines never build a full SQL string themselves from user text. They ask for a
(metric, date-range, optional dimension) series and the source composes it from the
DECLARED MetricSpec. That is the difference between a metric layer and NL-to-SQL: the shape
of the query is fixed and reviewable, and only the parameters vary.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol

from .registry import MetricSpec

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _safe_date(d: str) -> str:
    if not _DATE.match(d):
        raise ValueError(f"date {d!r} must be YYYY-MM-DD")
    return d


@dataclass(frozen=True)
class Point:
    date: str
    value: float
    segment: str | None = None


class DataSource(Protocol):
    def series(self, spec: MetricSpec, start: str, end: str,
               dimension: str | None = None) -> list[Point]: ...

    def row_counts(self, spec: MetricSpec, start: str, end: str) -> dict[str, int]:
        """RAW rows per date, not aggregated points.

        The completeness check needs the underlying row count. Deriving it from `series`
        counts one aggregated point per date, which is always 1 -- the check then silently
        never fires, which is the worst possible outcome for a guard whose entire job is to
        catch a short partition.
        """
        ...


class AthenaSource:
    """Governed reads through the existing read-only Athena tool."""

    name = "athena"

    def __init__(self, runner=None):
        self._runner = runner

    def _run(self, sql: str) -> list[dict]:
        if self._runner is not None:
            return self._runner(sql)
        from agent_tools.athena_tool import run_query      # imported late: no AWS at import
        return run_query(sql, limit=1000)["rows"]

    def series(self, spec: MetricSpec, start: str, end: str,
               dimension: str | None = None) -> list[Point]:
        _safe_date(start), _safe_date(end)
        dim = spec.validate_dimension(dimension) if dimension else None
        cols = f"{spec.date_column} AS d" + (f", {dim} AS seg" if dim else "")
        group = f"{spec.date_column}" + (f", {dim}" if dim else "")
        sql = (f"SELECT {cols}, {spec.measure} AS v FROM {spec.qualified()} "
               f"WHERE {spec.date_column} BETWEEN DATE '{start}' AND DATE '{end}' "
               f"GROUP BY {group} ORDER BY 1")
        out = []
        for r in self._run(sql):
            out.append(Point(date=str(r["d"]), value=float(r["v"] or 0),
                             segment=str(r["seg"]) if dim else None))
        return out

    def row_counts(self, spec: MetricSpec, start: str, end: str) -> dict[str, int]:
        _safe_date(start), _safe_date(end)
        sql = (f"SELECT {spec.date_column} AS d, COUNT(*) AS n FROM {spec.qualified()} "
               f"WHERE {spec.date_column} BETWEEN DATE '{start}' AND DATE '{end}' "
               f"GROUP BY {spec.date_column} ORDER BY 1")
        return {str(r["d"]): int(r["n"]) for r in self._run(sql)}


class FrameSource:
    """Rows held in memory. `rows` is a list of dicts with date, value and dimension keys."""

    name = "frame"

    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def series(self, spec: MetricSpec, start: str, end: str,
               dimension: str | None = None) -> list[Point]:
        _safe_date(start), _safe_date(end)
        dim = spec.validate_dimension(dimension) if dimension else None
        agg: dict[tuple[str, str | None], list[float]] = {}
        for r in self._rows:
            d = str(r[spec.date_column])
            if not (start <= d <= end):
                continue
            key = (d, str(r[dim]) if dim else None)
            agg.setdefault(key, []).append(float(r["value"]))
        out = []
        for (d, seg), vals in sorted(agg.items()):
            m = spec.measure.upper()
            if m.startswith("SUM"):
                v = sum(vals)
            elif m.startswith("AVG"):
                v = sum(vals) / len(vals)
            elif m.startswith("COUNT"):
                v = float(len(vals))
            else:
                raise ValueError(f"FrameSource cannot evaluate {spec.measure!r}")
            out.append(Point(date=d, value=v, segment=seg))
        return out

    def row_counts(self, spec: MetricSpec, start: str, end: str) -> dict[str, int]:
        _safe_date(start), _safe_date(end)
        out: dict[str, int] = {}
        for r in self._rows:
            d = str(r[spec.date_column])
            if start <= d <= end:
                out[d] = out.get(d, 0) + 1
        return out
