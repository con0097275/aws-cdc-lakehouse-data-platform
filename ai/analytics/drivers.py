"""Driver analysis: which segments moved, and by how much of the total movement.

THE WORDING IS PART OF THE CONTRACT (BAI-P3 §3). Every field and every rendered phrase says
"contributed to the observed change". None says "caused" it. A segment can carry 80% of a
decline because a pipeline dropped its rows, because a holiday closed it, or because
something real happened; contribution analysis cannot distinguish those and must not imply
that it can.

Dimensions are analysed INDEPENDENTLY (§4). Two-dimensional drill-down is a separate,
explicitly budgeted call, because branch x segment x product is a combinatorial explosion
that burns the query budget and produces cells too small to mean anything.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .core import ANALYTICS_VERSION, delta_pct

DRIVER_VERSION = "analytics:drivers-v1"


@dataclass
class DriverContribution:
    segment: str
    baseline: float
    actual: float
    delta: float
    share_of_delta: float | None      # signed; None when the total delta is zero
    status: str                       # MOVED | NEW | DISAPPEARED

    def phrase(self) -> str:
        if self.share_of_delta is None:
            return f"{self.segment} moved by {self.delta:,.2f}"
        return (f"{self.segment} contributed {self.share_of_delta:.1%} "
                f"of the observed change")

    def to_dict(self) -> dict:
        return asdict(self) | {"phrase": self.phrase()}


@dataclass
class DriverAnalysis:
    metric_id: str
    dimension: str
    period_a: dict
    period_b: dict
    total_actual: float | None
    total_baseline: float | None
    total_delta: float | None
    total_delta_pct: float | None
    contributions: list[DriverContribution]
    positive: list[str] = field(default_factory=list)
    negative: list[str] = field(default_factory=list)
    coverage: float | None = None      # |sum of ranked deltas| / |total delta|
    unexplained: float | None = None
    method: str = "per-segment delta / total delta"
    version: str = DRIVER_VERSION
    notes: list[str] = field(default_factory=list)
    interpretation: str = (
        "Contributions describe how much of the OBSERVED change each segment accounts "
        "for. They do not establish cause.")

    def to_dict(self) -> dict:
        d = asdict(self)
        d["contributions"] = [c.to_dict() for c in self.contributions]
        return d


def analyse_drivers(metric_id: str, dimension: str,
                    actual_by_segment: dict[str, float],
                    baseline_by_segment: dict[str, float], *,
                    period_a: dict | None = None, period_b: dict | None = None,
                    top_k: int = 10) -> DriverAnalysis:
    segs = sorted(set(actual_by_segment) | set(baseline_by_segment))
    contribs: list[DriverContribution] = []
    for s in segs:
        a = actual_by_segment.get(s)
        b = baseline_by_segment.get(s)
        status = ("NEW" if b is None else "DISAPPEARED" if a is None else "MOVED")
        a_v, b_v = float(a or 0.0), float(b or 0.0)
        contribs.append(DriverContribution(s, b_v, a_v, a_v - b_v, None, status))

    total_a = sum(c.actual for c in contribs)
    total_b = sum(c.baseline for c in contribs)
    total_d = total_a - total_b

    notes: list[str] = []
    for c in contribs:
        c.share_of_delta = (c.delta / total_d) if total_d else None
    if not total_d:
        notes.append("the total did not move, so shares of the movement are undefined; "
                     "segment-level deltas may still be non-zero and offsetting")

    contribs.sort(key=lambda c: abs(c.delta), reverse=True)
    ranked = contribs[:top_k]
    coverage = None
    unexplained = None
    if total_d:
        coverage = abs(sum(c.delta for c in ranked)) / abs(total_d)
        unexplained = total_d - sum(c.delta for c in ranked)
        if len(contribs) > top_k:
            notes.append(f"{len(contribs) - top_k} smaller segments are not listed; they "
                         f"account for {unexplained:,.2f} of the movement")

    return DriverAnalysis(
        metric_id=metric_id, dimension=dimension,
        period_a=period_a or {}, period_b=period_b or {},
        total_actual=total_a, total_baseline=total_b, total_delta=total_d,
        total_delta_pct=delta_pct(total_a, total_b),
        contributions=ranked,
        positive=[c.segment for c in ranked if c.delta > 0][:5],
        negative=[c.segment for c in ranked if c.delta < 0][:5],
        coverage=coverage, unexplained=unexplained, notes=notes)
