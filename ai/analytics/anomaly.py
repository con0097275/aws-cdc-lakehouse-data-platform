"""Anomaly detection with transparent baselines.

Four methods, all explainable to a business user in one sentence. Nothing here is a learned
model: with the history this platform has, a learned detector would be fitting noise, and
its output could not be argued with.

TWO RULES THIS MODULE ENFORCES

1. An anomaly is a STATISTICAL OBSERVATION, never a diagnosis. `severity` describes how far
   the value sits from its baseline. It is not "fraud", not "an incident", not "an error" --
   those are conclusions a human draws with context this module does not have.

2. Insufficient history REFUSES. A z-score over three points is arithmetic, not evidence:
   the standard deviation of a tiny sample is unstable, so almost anything scores as normal
   or as extreme depending on which points happen to be there.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .core import ewma, rolling_mean, rolling_median, rolling_std

ANOMALY_VERSION = "analytics:anomaly-v1"

#: Below this the baseline is not a baseline. Chosen so a weekday effect has been seen at
#: least twice before it is treated as normal.
MIN_HISTORY = {"zscore": 7, "iqr": 8, "ewma": 7, "weekday": 14}

SEVERITY_BANDS = ((4.0, "EXTREME"), (3.0, "HIGH"), (2.0, "MODERATE"), (0.0, "NORMAL"))


def _severity(score: float | None) -> str:
    if score is None:
        return "UNKNOWN"
    for threshold, label in SEVERITY_BANDS:
        if abs(score) >= threshold:
            return label
    return "NORMAL"


@dataclass
class AnomalyResult:
    method: str
    version: str
    actual: float | None
    baseline: float | None
    deviation: float | None
    score: float | None
    severity: str
    history_points: int
    window: list[str] = field(default_factory=list)
    refused: str | None = None
    notes: list[str] = field(default_factory=list)
    interpretation: str = (
        "A statistical observation about distance from a baseline. It is not a diagnosis "
        "and does not identify a cause.")

    def to_dict(self) -> dict:
        return asdict(self)


def _zero_variance(method: str, actual: float, mean: float | None, n: int,
                   window: list[str] | None) -> AnomalyResult:
    """Every historical point is identical, so the z-score has no denominator.

    That is NOT automatically "unknown". If the value equals the baseline it is exactly
    normal and saying UNKNOWN understates what is known; if it differs, the deviation is
    real but cannot be expressed in standard deviations. The two cases are reported
    differently rather than collapsed into one uninformative verdict.
    """
    dev = (actual - mean) if mean is not None else None
    if dev == 0:
        return AnomalyResult(method, ANOMALY_VERSION, actual, mean, 0.0, 0.0, "NORMAL", n,
                             window or [],
                             notes=["baseline has zero variance and the value matches it "
                                    "exactly"])
    return AnomalyResult(method, ANOMALY_VERSION, actual, mean, dev, None, "UNKNOWN", n,
                         window or [], refused="baseline has zero variance",
                         notes=["Every historical point is identical, so a z-score is "
                                "undefined. The difference is real but cannot be scaled; "
                                "compare the values directly."])


def _refuse(method: str, n: int) -> AnomalyResult:
    need = MIN_HISTORY[method]
    return AnomalyResult(
        method=method, version=ANOMALY_VERSION, actual=None, baseline=None, deviation=None,
        score=None, severity="UNKNOWN", history_points=n,
        refused=f"{n} historical points; {method} needs at least {need}",
        notes=["A score computed from too few points is unstable: it would call the same "
               "value normal or extreme depending on which points happen to be present. "
               "No score is produced."])


def zscore(actual: float, history: list[float], *, window: list[str] | None = None,
           k: int = 30) -> AnomalyResult:
    """Distance from the rolling mean, in sample standard deviations."""
    h = [v for v in history if v is not None]
    if len(h) < MIN_HISTORY["zscore"]:
        return _refuse("zscore", len(h))
    mean = rolling_mean(h, k)
    sd = rolling_std(h, k)
    if not sd:
        return _zero_variance("zscore", actual, mean, len(h), window)
    score = (actual - mean) / sd
    return AnomalyResult("zscore", ANOMALY_VERSION, actual, mean, actual - mean, score,
                         _severity(score), len(h), window or [])


def iqr(actual: float, history: list[float], *, window: list[str] | None = None,
        multiplier: float = 1.5) -> AnomalyResult:
    """Tukey fences. Robust to the outliers a mean would absorb."""
    h = sorted(v for v in history if v is not None)
    if len(h) < MIN_HISTORY["iqr"]:
        return _refuse("iqr", len(h))
    q1 = rolling_median(h[:len(h) // 2], len(h))
    q3 = rolling_median(h[(len(h) + 1) // 2:], len(h))
    med = rolling_median(h, len(h))
    spread = (q3 - q1) if (q1 is not None and q3 is not None) else None
    if not spread:
        return AnomalyResult("iqr", ANOMALY_VERSION, actual, med,
                             (actual - med) if med is not None else None, None, "UNKNOWN",
                             len(h), window or [], refused="interquartile range is zero")
    lo, hi = q1 - multiplier * spread, q3 + multiplier * spread
    outside = (actual < lo) or (actual > hi)
    score = ((actual - hi) / spread if actual > hi else
             (actual - lo) / spread if actual < lo else 0.0)
    return AnomalyResult("iqr", ANOMALY_VERSION, actual, med, actual - med, score,
                         _severity(score) if outside else "NORMAL", len(h), window or [],
                         notes=[f"fences [{lo:,.2f}, {hi:,.2f}] at {multiplier}x IQR"])


def ewma_anomaly(actual: float, history: list[float], *, alpha: float = 0.3,
                 window: list[str] | None = None) -> AnomalyResult:
    """Deviation from an exponentially weighted level. Reacts faster to a real level shift."""
    h = [v for v in history if v is not None]
    if len(h) < MIN_HISTORY["ewma"]:
        return _refuse("ewma", len(h))
    level = ewma(h, alpha)
    resid = [v - ewma(h[:i + 1], alpha) for i, v in enumerate(h) if i]
    sd = rolling_std(resid, len(resid)) if len(resid) > 1 else None
    score = ((actual - level) / sd) if sd else None
    return AnomalyResult("ewma", ANOMALY_VERSION, actual, level, actual - level, score,
                         _severity(score), len(h), window or [],
                         notes=[f"alpha={alpha}"])


def weekday_baseline(actual: float, history: list[tuple[str, float]], *,
                     target_date: str) -> AnomalyResult:
    """Compare a date against the SAME WEEKDAY only.

    A Monday compared to a Sunday baseline reads as an anomaly every single week. Needs two
    full cycles before a weekday pattern is evidence rather than coincidence.
    """
    from datetime import date
    pts = [(d, v) for d, v in history if v is not None]
    if len(pts) < MIN_HISTORY["weekday"]:
        return _refuse("weekday", len(pts))
    dow = date.fromisoformat(target_date).weekday()
    same = [v for d, v in pts if date.fromisoformat(d).weekday() == dow]
    if len(same) < 2:
        return AnomalyResult("weekday", ANOMALY_VERSION, actual, None, None, None,
                             "UNKNOWN", len(pts), [d for d, _ in pts],
                             refused=f"only {len(same)} prior observations of this weekday")
    mean = sum(same) / len(same)
    sd = rolling_std(same, len(same))
    if not sd:
        return _zero_variance("weekday", actual, mean, len(pts), [d for d, _ in pts])
    score = (actual - mean) / sd
    return AnomalyResult("weekday", ANOMALY_VERSION, actual, mean, actual - mean, score,
                         _severity(score), len(pts), [d for d, _ in pts],
                         notes=[f"compared against {len(same)} prior "
                                f"{date.fromisoformat(target_date):%A}s"])


METHODS = {"zscore": zscore, "iqr": iqr, "ewma": ewma_anomaly}


def detect(actual: float, history: list[float], *, methods: tuple[str, ...] = ("zscore",
                                                                               "iqr", "ewma"),
           window: list[str] | None = None) -> dict[str, AnomalyResult]:
    """Run several methods and report all of them.

    Disagreement is informative and is preserved rather than resolved: z-score flagging what
    IQR does not usually means one extreme historical point is inflating the mean.
    """
    return {m: METHODS[m](actual, history, window=window) for m in methods}
