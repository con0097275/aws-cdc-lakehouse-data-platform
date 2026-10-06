"""The validated analytical request, and the six intents BAI-P2 supports.

A parser (deterministic today, an LLM later) may PROPOSE this structure. Nothing executes
until `validate()` has checked it against the metric registry. That order is the whole
control: the proposer is untrusted, the validator is not, and the validator refuses rather
than repairing — a silently repaired request answers a question the user did not ask.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

from .semantic import MetricError, Registry, load_registry
from .timespec import TimeSpec, resolve as resolve_time


class Intent(str, Enum):
    VALUE = "VALUE"
    TREND = "TREND"
    PERIOD_COMPARISON = "PERIOD_COMPARISON"
    BREAKDOWN = "BREAKDOWN"
    TOP_N = "TOP_N"
    BOTTOM_N = "BOTTOM_N"


MAX_TOP_N = 100
_FILTER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\s*=\s*'[A-Za-z0-9_. -]{1,64}'$")


@dataclass
class AnalyticalRequest:
    intent: Intent
    metric_id: str
    time_phrase: str = "yesterday"
    as_of: str = ""
    grain: str = "day"
    dimension: str | None = None
    filters: tuple[str, ...] = field(default_factory=tuple)
    top_n: int = 5
    sort_direction: str = "desc"
    time: TimeSpec | None = None

    def validate(self, registry: Registry | None = None) -> "AnalyticalRequest":
        reg = registry or load_registry()
        m = reg.get(self.metric_id)                       # UnknownMetric if absent

        if not self.as_of:
            raise MetricError("as_of is required; business time is never taken from the "
                              "wall clock (a 02:00 run for T-1 must not resolve to T)")
        self.time = resolve_time(self.time_phrase, as_of=self.as_of, grain=self.grain)

        if self.intent in (Intent.BREAKDOWN, Intent.TOP_N, Intent.BOTTOM_N):
            if not self.dimension:
                raise MetricError(f"{self.intent.value} requires a dimension")
        if self.intent in (Intent.TOP_N, Intent.BOTTOM_N):
            if not 1 <= self.top_n <= MAX_TOP_N:
                raise MetricError(f"top_n must be 1..{MAX_TOP_N}")
            self.sort_direction = "desc" if self.intent is Intent.TOP_N else "asc"
        if self.intent is Intent.PERIOD_COMPARISON and self.time.comparison is None:
            raise MetricError(
                f"{self.time_phrase!r} resolves to no comparison window; say which period "
                "to compare against rather than assuming one")
        if self.intent is Intent.TREND and self.time.primary.days() < 2:
            raise MetricError("TREND needs at least 2 dates")

        # Dimension legality is the registry's call, not this model's; ask it now so the
        # request fails here rather than at SQL time.
        if self.dimension:
            if self.dimension not in m.allowed_dimensions:
                raise MetricError(
                    f"{self.dimension!r} is not an allowed dimension for {m.metric_id}. "
                    f"Allowed: {list(m.allowed_dimensions)}")
            d = reg.dimension(self.dimension)
            if not d.available:
                from .semantic import DimensionUnavailable
                raise DimensionUnavailable(
                    f"{self.dimension!r} is declared for {m.metric_id} but NOT QUERYABLE: "
                    f"{d.blocked_reason} (needs {d.requires_join})")

        for f in self.filters:
            if not _FILTER.match(f):
                raise MetricError(
                    f"filter {f!r} is not of the form column = 'value'. Free-form predicates "
                    "are refused, not escaped.")
            col = f.split("=")[0].strip()
            if col not in {reg.dimension(x).column for x in m.allowed_dimensions
                           if x in reg.dimensions}:
                raise MetricError(f"filter column {col!r} is not an allowed dimension")
        return self

    def to_dict(self) -> dict:
        return {"intent": self.intent.value, "metric_id": self.metric_id,
                "time_phrase": self.time_phrase, "as_of": self.as_of, "grain": self.grain,
                "dimension": self.dimension, "filters": list(self.filters),
                "top_n": self.top_n, "sort_direction": self.sort_direction,
                "time": self.time.to_dict() if self.time else None}


#: Deterministic phrase -> intent. Ordered: the more specific pattern must win.
_INTENT_RULES = [
    (re.compile(r"\b(top|highest|largest|biggest|best)\s+(\d+)?", re.I), Intent.TOP_N),
    (re.compile(r"\b(bottom|lowest|smallest|worst)\s+(\d+)?", re.I), Intent.BOTTOM_N),
    (re.compile(r"\b(break\s*down|breakdown|by\s+\w+|split\s+by|per\s+\w+)\b", re.I),
     Intent.BREAKDOWN),
    (re.compile(r"\b(compare|versus|vs\.?|against|previous day|same day last week|"
                r"change (from|since)|difference)\b", re.I), Intent.PERIOD_COMPARISON),
    (re.compile(r"\b(trend|over (the )?last|history|daily series|for the last \d+ days?|"
                r"last \d+ days?)\b", re.I), Intent.TREND),
]


def parse_intent(question: str) -> Intent:
    for rx, intent in _INTENT_RULES:
        if rx.search(question or ""):
            return intent
    return Intent.VALUE


_N = re.compile(r"\b(?:top|bottom|highest|lowest|largest|smallest)\s+(\d{1,3})\b", re.I)
_BY = re.compile(r"\bby\s+([a-z_][a-z0-9_]*)\b", re.I)
#: "top 5 <DIMENSION> by <metric>" -- the dimension sits BEFORE `by`, the opposite of
#: "break <metric> down by <DIMENSION>". Using the `by` capture for both put the METRIC name
#: in the dimension slot and the request was refused for the wrong reason.
_RANK_DIM = re.compile(
    r"\b(?:top|bottom|highest|lowest|largest|smallest)\s+\d{1,3}\s+([a-z_][a-z0-9_]*)"
    r"(?:\s+by\b|\s*$)", re.I)


def parse_request(question: str, *, as_of: str,
                  registry: Registry | None = None) -> AnalyticalRequest:
    """Deterministic end-to-end parse. No model call: Bedrock is unavailable, and a
    deterministic parser is testable in a way a prompt is not."""
    from .resolver import resolve_metric
    reg = registry or load_registry()
    intent = parse_intent(question)
    metric = resolve_metric(question, reg).metric_id      # raises AMBIGUOUS / UNKNOWN

    dim = None
    ranking = intent in (Intent.TOP_N, Intent.BOTTOM_N)
    mb = _RANK_DIM.search(question or "") if ranking else _BY.search(question or "")
    if mb is None and ranking:
        mb = _BY.search(question or "")          # "top 5 by account_sk" also occurs
    if mb:
        cand = mb.group(1).lower()
        # Accept a dimension NAME or its column, so "by product_code" and "by product" both
        # reach the registry's own legality check rather than being silently dropped.
        for name, d in reg.dimensions.items():
            if cand in (name, d.column) or name.startswith(cand):
                dim = name
                break
        else:
            dim = cand                                    # let validate() refuse it by name

    n = int(mn.group(1)) if (mn := _N.search(question or "")) else 5
    return AnalyticalRequest(intent=intent, metric_id=metric, time_phrase=question,
                             as_of=as_of, dimension=dim, top_n=n).validate(reg)
