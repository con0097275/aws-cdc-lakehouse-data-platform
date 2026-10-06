"""Orchestrates the deterministic analytics into one EvidencePack v2.

Everything is CONDITIONAL and says so. Driver analysis needs a materialised dimension;
anomaly needs a baseline; forecast needs two seasonal cycles. When a precondition fails the
field stays `None` and the reason goes into `notes` — a null here always means "not
produced", never "nothing found". Those two are opposite conclusions and a business reader
cannot tell them apart from an empty field.
"""
from __future__ import annotations

from .anomaly import ANOMALY_VERSION, detect
from .budget import QueryBudget
from .compiler import compile_metric_query
from .core import ANALYTICS_VERSION, delta_pct
from .drivers import DRIVER_VERSION, analyse_drivers
from .plan import EvidencePack, build_plan, execute_plan
from .request import AnalyticalRequest, Intent
from .semantic import MetricError, Registry, load_registry
from .timespec import TimeWindow, resolve as resolve_time

FORECAST_VERSION = "analytics:forecast-v1"


def _series(runner, budget: QueryBudget, metric_id, window: TimeWindow, reg):
    q = compile_metric_query(metric_id, window, registry=reg)
    out = budget.run(q.sql, runner)
    return {r["business_date"]: float(r["metric_value"]) for r in out.get("rows", [])
            if r.get("metric_value") not in (None, "")}


def _by_segment(runner, budget: QueryBudget, metric_id, window: TimeWindow, dim, reg):
    q = compile_metric_query(metric_id, window, dimension=dim, registry=reg)
    out = budget.run(q.sql, runner)
    agg: dict[str, float] = {}
    for r in out.get("rows", []):
        if r.get("metric_value") in (None, ""):
            continue
        agg[str(r.get("segment"))] = agg.get(str(r.get("segment")), 0.0) \
            + float(r["metric_value"])
    return agg


def analyse(req: AnalyticalRequest, *, runner, registry: Registry | None = None,
            budget: QueryBudget | None = None, history_days: int = 30,
            driver_dimensions: tuple[str, ...] = ()) -> EvidencePack:
    """Run the base plan, then add whatever the data actually supports."""
    reg = registry or load_registry()
    b = budget or QueryBudget()
    req.validate(reg)
    m = reg.get(req.metric_id)

    pack = execute_plan(build_plan(req, reg), runner=lambda s: b.run(s, runner),
                        registry=reg)
    pack.analytics_versions = {"core": ANALYTICS_VERSION, "drivers": DRIVER_VERSION,
                               "anomaly": ANOMALY_VERSION, "forecast": FORECAST_VERSION}

    tgt = req.time.primary.end
    hist_win = TimeWindow(
        (resolve_time(f"last {history_days} days", as_of=tgt).primary.start), tgt,
        f"last {history_days} days")

    # ---- trend + anomaly ------------------------------------------------------------
    try:
        b.check_history(hist_win.days())
        series = _series(runner, b, m.metric_id, hist_win, reg)
    except Exception as e:                                        # noqa: BLE001
        series = {}
        pack.notes.append(f"history unavailable: {type(e).__name__}: {e}")

    ordered = [series[d] for d in sorted(series)]
    if len(ordered) >= 2:
        pack.trend = {"dates": sorted(series), "values": ordered,
                      "first": ordered[0], "last": ordered[-1],
                      "change_pct": delta_pct(ordered[-1], ordered[0]),
                      "points": len(ordered)}
    else:
        pack.notes.append(f"trend not computed: {len(ordered)} dates in the history window")

    actual = series.get(tgt)
    hist_before = [series[d] for d in sorted(series) if d < tgt]
    if actual is not None and hist_before:
        results = detect(actual, hist_before, window=sorted(d for d in series if d < tgt))
        pack.anomaly = {k: v.to_dict() for k, v in results.items()}
        refused = [v.refused for v in results.values() if v.refused]
        if refused:
            pack.notes.append(f"anomaly inconclusive: {refused[0]}")
    else:
        pack.notes.append("anomaly not computed: no value for the target date, or no "
                          "prior history in the window")

    # ---- drivers --------------------------------------------------------------------
    dims = driver_dimensions or ((req.dimension,) if req.dimension else ())
    driver_out: dict = {}
    for dim in dims:
        if dim is None:
            continue
        d = reg.dimensions.get(dim)
        if d is None or not d.available:
            pack.notes.append(
                f"drivers not computed for {dim!r}: "
                f"{(d.blocked_reason if d else 'not a declared dimension')}")
            continue
        if req.time.comparison is None:
            pack.notes.append(f"drivers not computed for {dim!r}: no comparison period")
            continue
        try:
            b.check_dimension()
            a = _by_segment(runner, b, m.metric_id, req.time.primary, dim, reg)
            c = _by_segment(runner, b, m.metric_id, req.time.comparison, dim, reg)
        except Exception as e:                                    # noqa: BLE001
            pack.notes.append(f"drivers not computed for {dim!r}: {type(e).__name__}: {e}")
            continue
        driver_out[dim] = analyse_drivers(
            m.metric_id, dim, a, c, period_a=req.time.primary.__dict__,
            period_b=req.time.comparison.__dict__).to_dict()
    pack.drivers = driver_out or None

    # ---- forecast -------------------------------------------------------------------
    from .forecast import MIN_POINTS, forecast as run_forecast
    from .registry import MetricSpec as _EngineSpec

    def _as_engine_spec(sm):
        """analytics.registry.MetricSpec, not analytics.semantic.Metric.

        The forecast and diagnosis engines predate the BAI-P1 registry and take the older
        spec. Passing the semantic Metric failed with a bare
        `AttributeError: date_column` -- the same mismatch already fixed for
        get_data_quality, missed here because no live path exercised it until now.
        """
        return _EngineSpec(
            name=sm.metric_id, dataset=sm.source_relation, measure=sm.measure_sql,
            date_column=sm.time_column,
            dimensions=tuple(sm.allowed_dimensions), owner=sm.owner,
            grain=sm.business_grain, additive=sm.additive, unit=sm.unit,
            description=sm.description)

    if len(ordered) >= MIN_POINTS:
        from .datasource import FrameSource
        rows = [{"business_date": d, "value": series[d], "product_code": "_"}
                for d in sorted(series)]
        f = run_forecast(_as_engine_spec(m), FrameSource(rows), as_of=tgt,
                         history_days=history_days)
        pack.forecast = f.to_dict()
    else:
        pack.forecast = None
        pack.notes.append(
            f"forecast NOT produced: {len(ordered)} daily points, {MIN_POINTS} required. "
            "Two seasonal cycles are the minimum before a weekday pattern is evidence; a "
            "number from less would carry an interval too wide to inform a decision.")

    pack.freshness = {"as_of": req.as_of, "target_date": tgt,
                      "latest_date_present": (max(series) if series else None),
                      "sla_hours": m.freshness_sla_hours}
    pack.budget = b.to_dict()
    return pack
