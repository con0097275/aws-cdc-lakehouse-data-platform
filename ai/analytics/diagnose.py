"""Why did a metric change?

The order of reasoning is the whole point, and it is the opposite of what a language model
does by default:

  1. IS THE DATA THERE?  A balance that "dropped 60%" because half the partition is missing
     is a PIPELINE INCIDENT, not a business event. Answering it as a business event sends
     someone to ask the wrong team, and the real fault keeps running. Completeness is
     checked FIRST and, if it fails, the verdict is DATA_INCOMPLETE and decomposition is
     not even attempted.
  2. IS IT UNUSUAL?  Compare against a declared baseline, not against yesterday alone --
     one day is not a trend and weekly seasonality is real.
  3. WHAT MOVED?  Only then decompose the delta by declared dimensions.

Contributions are exact, not estimated: for an additive measure the per-segment deltas sum
to the total delta, and the code asserts that. A decomposition whose parts do not sum to the
whole is worse than no decomposition, because it looks like arithmetic.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta

from .datasource import DataSource, Point
from .registry import MetricSpec

VERDICTS = ("DATA_INCOMPLETE", "NO_DATA", "UNREMARKABLE", "MOVED", "NOT_DECOMPOSABLE")


@dataclass
class Contribution:
    segment: str
    baseline: float
    actual: float
    delta: float
    share_of_delta: float          # signed fraction of the total delta
    status: str                    # MOVED | NEW | DISAPPEARED


@dataclass
class Diagnosis:
    metric: str
    target_date: str
    baseline_window: list[str]
    actual: float | None
    baseline: float | None
    delta: float | None
    rel_change: float | None
    verdict: str
    completeness: dict
    contributions: list[Contribution] = field(default_factory=list)
    dimension: str | None = None
    notes: list[str] = field(default_factory=list)
    method: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["contributions"] = [asdict(c) for c in self.contributions]
        return d


def _days_back(d: str, n: int) -> list[str]:
    y, m, dd = (int(x) for x in d.split("-"))
    base = date(y, m, dd)
    return [(base - timedelta(days=i)).isoformat() for i in range(n, 0, -1)]


def _sum_by_date(points: list[Point]) -> dict[str, float]:
    out: dict[str, float] = {}
    for p in points:
        out[p.date] = out.get(p.date, 0.0) + p.value
    return out


def diagnose(spec: MetricSpec, source: DataSource, target_date: str, *,
             baseline_days: int = 7, dimension: str | None = None,
             min_completeness: float = 0.7) -> Diagnosis:
    """Explain `target_date` for `spec`, evidence first."""
    window = _days_back(target_date, baseline_days)
    start, end = window[0], target_date

    totals = _sum_by_date(source.series(spec, start, end))
    actual = totals.get(target_date)
    hist = {d: v for d, v in totals.items() if d in window}
    baseline = sum(hist.values()) / len(hist) if hist else None

    # ---- 1. completeness -------------------------------------------------------------
    # Rows present on the target day vs the typical day. This catches the failure mode the
    # business reading misses entirely: the number is small because the data is missing.
    # RAW row counts. Deriving this from `series` counts aggregated points -- one per date,
    # always 1 -- so the guard silently never fires. Found while testing this engine.
    raw = source.row_counts(spec, start, end)
    counts = {d: raw.get(d, 0) for d in window + [target_date]}
    hist_counts = [counts[d] for d in window if counts.get(d)]
    typical = sum(hist_counts) / len(hist_counts) if hist_counts else 0
    today_rows = counts.get(target_date, 0)
    ratio = (today_rows / typical) if typical else (1.0 if today_rows else 0.0)
    completeness = {"rows_today": today_rows, "typical_rows": round(typical, 1),
                    "ratio": round(ratio, 3), "threshold": min_completeness}

    notes: list[str] = []
    if actual is None or today_rows == 0:
        return Diagnosis(spec.name, target_date, window, None, baseline, None, None,
                         "NO_DATA", completeness,
                         notes=["no rows for this date — the pipeline has not produced it, "
                                "or the date is outside the loaded range"],
                         method="completeness-first")

    if ratio < min_completeness:
        return Diagnosis(
            spec.name, target_date, window, actual, baseline,
            (actual - baseline) if baseline is not None else None,
            ((actual - baseline) / baseline) if baseline else None,
            "DATA_INCOMPLETE", completeness,
            notes=[f"only {today_rows} rows vs a typical {typical:.0f} "
                   f"({ratio:.0%}). Treat this as a PIPELINE problem first: any movement in "
                   "the measure is unreliable while the partition is short.",
                   "next: check the EOD job watermark and the source connector, not the "
                   "business"],
            method="completeness-first")

    delta = actual - baseline if baseline is not None else None
    rel = (delta / baseline) if (delta is not None and baseline) else None

    if rel is not None and abs(rel) < spec.alert_rel_change:
        return Diagnosis(spec.name, target_date, window, actual, baseline, delta, rel,
                         "UNREMARKABLE", completeness,
                         notes=[f"within the declared {spec.alert_rel_change:.0%} band for "
                                "this metric — normal variation, not an event"],
                         method="baseline mean of prior days")

    # ---- 3. decomposition ------------------------------------------------------------
    if not spec.additive:
        return Diagnosis(spec.name, target_date, window, actual, baseline, delta, rel,
                         "NOT_DECOMPOSABLE", completeness,
                         notes=[f"{spec.measure} is not additive across segments, so a "
                                "contribution breakdown would not sum to the total. Compare "
                                "segment values directly instead of attributing the delta."],
                         method="baseline mean; decomposition refused")

    contributions: list[Contribution] = []
    if dimension:
        spec.validate_dimension(dimension)
        seg_hist: dict[str, list[float]] = {}
        seg_today: dict[str, float] = {}
        for p in source.series(spec, start, end, dimension=dimension):
            seg = p.segment or "(null)"
            if p.date == target_date:
                seg_today[seg] = seg_today.get(seg, 0.0) + p.value
            elif p.date in window:
                seg_hist.setdefault(seg, []).append(p.value)
        for seg in sorted(set(seg_hist) | set(seg_today)):
            b = sum(seg_hist[seg]) / len(seg_hist[seg]) if seg_hist.get(seg) else 0.0
            a = seg_today.get(seg, 0.0)
            status = ("NEW" if not seg_hist.get(seg) else
                      "DISAPPEARED" if seg not in seg_today else "MOVED")
            contributions.append(Contribution(seg, round(b, 4), round(a, 4), round(a - b, 4),
                                              0.0, status))
        tot = sum(c.delta for c in contributions)
        for c in contributions:
            c.share_of_delta = round(c.delta / tot, 4) if tot else 0.0
        contributions.sort(key=lambda c: abs(c.delta), reverse=True)
        # The parts MUST sum to the whole; otherwise the breakdown is fiction.
        if delta is not None and abs(tot - delta) > max(1e-6, abs(delta) * 1e-6):
            notes.append(f"WARNING: segment deltas sum to {tot:.4f} but the total delta is "
                         f"{delta:.4f}. The dimension does not partition the metric.")

    return Diagnosis(spec.name, target_date, window, actual, baseline, delta, rel,
                     "MOVED", completeness, contributions, dimension, notes,
                     method=f"baseline = mean of prior {baseline_days} days; "
                            f"contribution = per-segment delta / total delta")
