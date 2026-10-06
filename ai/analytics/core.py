"""Deterministic analytical primitives.

Every function here is total: it returns `None` rather than raising or inventing a value
when the input cannot support an answer. A zero baseline has no percentage change — not an
infinite one, and not zero. Returning 0.0 there is the single most common way a dashboard
reports "no change" for a metric that went from nothing to something.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

ANALYTICS_VERSION = "analytics:core-v1"


def delta(actual: float | None, baseline: float | None) -> float | None:
    if actual is None or baseline is None:
        return None
    return actual - baseline


def delta_pct(actual: float | None, baseline: float | None) -> float | None:
    """None when the baseline is zero or absent.

    A ratio against zero is undefined. Callers must render "n/a", not "0%" and not "inf%":
    a metric going 0 -> 500 is a real event and both alternatives hide it.
    """
    if actual is None or baseline in (None, 0):
        return None
    return (actual - baseline) / baseline


def growth_rate(series: list[float]) -> float | None:
    """Compound per-step growth. None if the first value is <= 0 or the series is short."""
    vals = [v for v in series if v is not None]
    if len(vals) < 2 or vals[0] <= 0:
        return None
    n = len(vals) - 1
    if vals[-1] <= 0:
        return None
    return (vals[-1] / vals[0]) ** (1 / n) - 1


def rolling_mean(series: list[float], k: int) -> float | None:
    vals = [v for v in series if v is not None]
    return sum(vals[-k:]) / min(k, len(vals)) if vals else None


def rolling_median(series: list[float], k: int) -> float | None:
    vals = sorted(v for v in series[-k:] if v is not None)
    if not vals:
        return None
    m = len(vals) // 2
    return vals[m] if len(vals) % 2 else (vals[m - 1] + vals[m]) / 2


def rolling_std(series: list[float], k: int, *, sample: bool = True) -> float | None:
    """Sample standard deviation by default (n-1).

    The population form understates spread on the short windows this platform actually has,
    which inflates every z-score computed from it.
    """
    vals = [v for v in series[-k:] if v is not None]
    n = len(vals)
    if n < (2 if sample else 1):
        return None
    mean = sum(vals) / n
    denom = (n - 1) if sample else n
    return math.sqrt(sum((v - mean) ** 2 for v in vals) / denom)


def moving_average(series: list[float], k: int) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(series)):
        window = [v for v in series[max(0, i - k + 1):i + 1] if v is not None]
        out.append(sum(window) / len(window) if len(window) == k else None)
    return out


def ewma(series: list[float], alpha: float = 0.3) -> float | None:
    vals = [v for v in series if v is not None]
    if not vals:
        return None
    if not 0 < alpha <= 1:
        raise ValueError("alpha must be in (0, 1]")
    s = vals[0]
    for v in vals[1:]:
        s = alpha * v + (1 - alpha) * s
    return s


def period_over_period(current: list[float], prior: list[float]) -> dict:
    a = sum(v for v in current if v is not None) if current else None
    b = sum(v for v in prior if v is not None) if prior else None
    return {"current": a, "prior": b, "delta": delta(a, b), "delta_pct": delta_pct(a, b)}


@dataclass(frozen=True)
class Share:
    segment: str
    value: float
    share: float | None


def contribution_shares(values: dict[str, float]) -> list[Share]:
    """Share of a TOTAL. None share when the total is zero.

    Shares of a total that nets to zero are meaningless — +100 and -100 would both report
    an infinite share — so the total is reported and the shares are withheld.
    """
    total = sum(values.values())
    return sorted(
        (Share(k, v, (v / total) if total else None) for k, v in values.items()),
        key=lambda s: abs(s.value), reverse=True)
