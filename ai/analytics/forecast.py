"""Forecast the next value of a declared metric.

A forecast without a measured error is an opinion. Every result here carries the backtested
error of the SAME method on the SAME series, plus an interval derived from that error -- so
"tomorrow is about 1.2M" arrives with "and this method has been off by ~8% historically",
which is the part that makes it usable for a decision.

Three deliberately simple methods. There is no deep model here because there is no data to
justify one: a few weeks of daily points cannot fit anything complex without overfitting,
and a complicated forecast on a short series is a confident wrong answer.

  seasonal_naive   value from `season` days ago. Strong when weekday effects dominate.
  moving_average   mean of the last k. Strong when the series is flat and noisy.
  drift            last value plus average per-step change. Weak, but catches a trend.

The method is CHOSEN BY BACKTEST, not by preference, and the losing methods' scores are
returned too so the choice can be second-guessed.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta

from .datasource import DataSource
from .registry import MetricSpec

MIN_POINTS = 14          # two weekly cycles; below this a weekly pattern is not evidence


@dataclass
class Forecast:
    metric: str
    target_date: str
    point: float | None
    lower: float | None
    upper: float | None
    method: str
    backtest_mape: float | None
    backtest_n: int
    history_points: int
    candidates: dict = field(default_factory=dict)
    refused: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _next_day(d: str) -> str:
    y, m, dd = (int(x) for x in d.split("-"))
    return (date(y, m, dd) + timedelta(days=1)).isoformat()


def _seasonal_naive(v: list[float], season: int = 7) -> float | None:
    return v[-season] if len(v) >= season else None


def _moving_average(v: list[float], k: int = 7) -> float | None:
    if len(v) < k:
        return None
    return sum(v[-k:]) / k


def _drift(v: list[float]) -> float | None:
    if len(v) < 2:
        return None
    return v[-1] + (v[-1] - v[0]) / (len(v) - 1)


def _naive(v: list[float]) -> float | None:
    """Last value. The baseline every other method must beat to justify itself."""
    return v[-1] if v else None


def _ses(v: list[float], alpha: float = 0.3) -> float | None:
    """Simple exponential smoothing: a level with no trend or season."""
    if len(v) < 3:
        return None
    level = v[0]
    for x in v[1:]:
        level = alpha * x + (1 - alpha) * level
    return level


def _holt(v: list[float], alpha: float = 0.3, beta: float = 0.1) -> float | None:
    """Holt's linear trend (ETS additive-trend, no season).

    Included because a metric with a genuine trend is systematically under-forecast by SES
    and by a moving average, and the backtest is what decides whether that trend is real
    here rather than an assumption.
    """
    if len(v) < 4:
        return None
    level, trend = v[0], v[1] - v[0]
    for x in v[1:]:
        prev = level
        level = alpha * x + (1 - alpha) * (level + trend)
        trend = beta * (level - prev) + (1 - beta) * trend
    return level + trend


METHODS = {"naive": _naive, "seasonal_naive": _seasonal_naive,
           "moving_average": _moving_average, "drift": _drift,
           "ses": _ses, "holt": _holt}


def _mape(actual: list[float], pred: list[float]) -> float | None:
    pairs = [(a, p) for a, p in zip(actual, pred) if a not in (0, None) and p is not None]
    if not pairs:
        return None
    return sum(abs((a - p) / a) for a, p in pairs) / len(pairs)


def _backtest(values: list[float], fn, min_train: int = 7) -> tuple[float | None, int]:
    """Walk-forward: predict each point from only the points BEFORE it.

    Fitting on the whole series and scoring on the same series reports an error the method
    will never achieve in use. Walk-forward is the only honest version of this number.
    """
    actual, pred = [], []
    for i in range(min_train, len(values)):
        p = fn(values[:i])
        if p is not None:
            actual.append(values[i])
            pred.append(p)
    return _mape(actual, pred), len(pred)


def forecast(spec: MetricSpec, source: DataSource, *, as_of: str,
             history_days: int = 28, target_date: str | None = None) -> Forecast:
    """Forecast the day after `as_of` (or an explicit `target_date`)."""
    y, m, dd = (int(x) for x in as_of.split("-"))
    start = (date(y, m, dd) - timedelta(days=history_days)).isoformat()
    tgt = target_date or _next_day(as_of)

    pts = source.series(spec, start, as_of)
    by_date: dict[str, float] = {}
    for p in pts:
        by_date[p.date] = by_date.get(p.date, 0.0) + p.value
    series = [by_date[d] for d in sorted(by_date)]

    if len(series) < MIN_POINTS:
        return Forecast(
            spec.name, tgt, None, None, None, "none", None, 0, len(series),
            refused=f"only {len(series)} daily points; {MIN_POINTS} are required",
            notes=["Two weekly cycles are the minimum before a weekday pattern is evidence "
                   "rather than coincidence. A number produced from less would carry an "
                   "interval so wide it could not inform a decision — so none is produced."])

    scores: dict[str, dict] = {}
    for name, fn in METHODS.items():
        mape, n = _backtest(series, fn)
        if fn(series) is not None and mape is not None:
            scores[name] = {"mape": round(mape, 4), "n": n}

    if not scores:
        return Forecast(spec.name, tgt, None, None, None, "none", None, 0, len(series),
                        refused="no method could be backtested on this series")

    best = min(scores, key=lambda k: scores[k]["mape"])
    point = METHODS[best](series)
    mape = scores[best]["mape"]
    # Interval from the method's OWN measured error, not an assumed distribution.
    band = abs(point) * mape if point is not None else None
    notes = [f"method chosen by walk-forward backtest over {scores[best]['n']} points",
             f"interval is +/- the backtested MAPE ({mape:.1%}), not a confidence interval "
             "from a fitted distribution — it says how wrong this method usually was here"]
    if mape > 0.25:
        notes.append(f"WARNING: {mape:.0%} historical error. Directionally useful at best; "
                     "do not use this for a threshold decision.")
    return Forecast(spec.name, tgt, round(point, 4), round(point - band, 4),
                    round(point + band, 4), best, mape, scores[best]["n"], len(series),
                    candidates=scores, notes=notes)
