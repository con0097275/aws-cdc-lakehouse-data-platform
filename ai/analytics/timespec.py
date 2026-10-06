"""Canonical business-time semantics.

One place decides what "yesterday", "last week" and "month to date" mean. Without this the
model interprets a business date differently on each request and two identical questions
return different windows — which looks like a data problem and is not.

Every window is CLOSED-CLOSED [start, end] on the metric's business_date, because the mart
is partitioned by business_date and a half-open window silently drops the last partition.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta

GRAIN_DAYS = {"day": 1, "week": 7, "month": 30}


@dataclass(frozen=True)
class TimeWindow:
    start: str
    end: str
    label: str

    def days(self) -> int:
        return (date.fromisoformat(self.end) - date.fromisoformat(self.start)).days + 1


@dataclass(frozen=True)
class TimeSpec:
    primary: TimeWindow
    comparison: TimeWindow | None = None
    grain: str = "day"

    def to_dict(self) -> dict:
        return {"primary": {"start": self.primary.start, "end": self.primary.end,
                            "label": self.primary.label},
                "comparison": (None if not self.comparison else
                               {"start": self.comparison.start, "end": self.comparison.end,
                                "label": self.comparison.label}),
                "grain": self.grain}


def _d(s: str) -> date:
    return date.fromisoformat(s)


def _w(a: date, b: date, label: str) -> TimeWindow:
    return TimeWindow(a.isoformat(), b.isoformat(), label)


#: Phrase -> resolver. Ordered longest-first at match time so "same day last week" is not
#: shadowed by "last week".
PHRASES = {
    "today":                 lambda t: (_w(t, t, "today"), None),
    "yesterday":             lambda t: (_w(t - timedelta(1), t - timedelta(1), "yesterday"),
                                        _w(t - timedelta(2), t - timedelta(2), "day before")),
    "previous day":          lambda t: (_w(t - timedelta(1), t - timedelta(1), "previous day"),
                                        _w(t - timedelta(2), t - timedelta(2), "day before")),
    "same day last week":    lambda t: (_w(t, t, "today"),
                                        _w(t - timedelta(7), t - timedelta(7),
                                           "same day last week")),
    "last 7 days":           lambda t: (_w(t - timedelta(6), t, "last 7 days"),
                                        _w(t - timedelta(13), t - timedelta(7),
                                           "prior 7 days")),
    "rolling 7d":            lambda t: (_w(t - timedelta(6), t, "rolling 7d"),
                                        _w(t - timedelta(13), t - timedelta(7),
                                           "prior 7d")),
    "last 30 days":          lambda t: (_w(t - timedelta(29), t, "last 30 days"),
                                        _w(t - timedelta(59), t - timedelta(30),
                                           "prior 30 days")),
    "rolling 30d":           lambda t: (_w(t - timedelta(29), t, "rolling 30d"),
                                        _w(t - timedelta(59), t - timedelta(30),
                                           "prior 30d")),
    "last week":             lambda t: (_w(t - timedelta(6), t, "last week"),
                                        _w(t - timedelta(13), t - timedelta(7),
                                           "week before")),
    "month to date":         lambda t: (_w(t.replace(day=1), t, "month to date"), None),
    "mtd":                   lambda t: (_w(t.replace(day=1), t, "month to date"), None),
}

_EXPLICIT = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")


def resolve(phrase: str, *, as_of: str, grain: str = "day") -> TimeSpec:
    """Resolve a business-time phrase against an explicit as-of date.

    `as_of` is REQUIRED and never defaulted to the wall clock: a report run at 02:00 for
    business date T-1 must not silently resolve "today" to T.
    """
    if grain not in GRAIN_DAYS:
        raise ValueError(f"unsupported time grain {grain!r}")
    t = _d(as_of)
    text = " ".join((phrase or "").lower().split())

    dates = _EXPLICIT.findall(text)
    if len(dates) >= 2:
        return TimeSpec(_w(_d(dates[0]), _d(dates[1]), f"{dates[0]}..{dates[1]}"),
                        None, grain)
    if len(dates) == 1:
        d0 = _d(dates[0])
        return TimeSpec(_w(d0, d0, dates[0]),
                        _w(d0 - timedelta(1), d0 - timedelta(1), "previous day"), grain)

    for key in sorted(PHRASES, key=len, reverse=True):
        if key in text:
            primary, comparison = PHRASES[key](t)
            return TimeSpec(primary, comparison, grain)

    # Default: the as-of date itself, compared with the day before. Stated explicitly rather
    # than inferred, so an unparsed phrase produces a predictable window instead of silence.
    return TimeSpec(_w(t, t, f"as of {as_of}"),
                    _w(t - timedelta(1), t - timedelta(1), "previous day"), grain)
