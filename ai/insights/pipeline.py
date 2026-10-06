"""Generate one BusinessInsight per registered KPI, for one business date.

ORDER MATTERS AND IS THE POINT:

    readiness gate -> certification gate -> analytics -> summary -> record

Nothing is computed before the gates pass. Generating an insight from provisional data and
labelling it provisional afterwards still puts a number on an executive's screen, and the
label is the part people skip.

A failing KPI never fails the run. Each insight is independent and records its own
status, so one broken metric does not suppress the digest for the other three.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ai"))

from analytics.budget import QueryBudget                      # noqa: E402
from analytics.engine import analyse                          # noqa: E402
from analytics.request import AnalyticalRequest, Intent       # noqa: E402
from analytics.semantic import load_registry                  # noqa: E402

from .config import InsightSpec, load_insight_config          # noqa: E402
from .record import BusinessInsight, TopDriver, make_insight_id, now_iso  # noqa: E402
from .summarise import summarise                              # noqa: E402


@dataclass
class Readiness:
    ready: bool
    reason: str = ""
    certification: str | None = None


def default_readiness(metric_id: str, business_date: str, *, runner, registry) -> Readiness:
    """Is the upstream mart actually populated for this date?

    A watermark is a CLAIM; this asks the table. A job can report SUCCEEDED and produce
    nothing, and every downstream gate that trusts the watermark then believes it.
    """
    from analytics.compiler import compile_metric_query
    from analytics.timespec import TimeWindow
    try:
        q = compile_metric_query(metric_id, TimeWindow(business_date, business_date,
                                                       business_date), registry=registry)
        out = runner(q.sql)
    except Exception as e:                                     # noqa: BLE001
        return Readiness(False, f"readiness probe failed: {type(e).__name__}: {e}")
    rows = out.get("rows") or []
    if not rows:
        return Readiness(False, f"no rows in the mart for {business_date}")
    # `min(statuses)` here was the same alphabetical-min defect as the compiler's: on bare
    # tier names 'CERTIFIED' sorts first, so the STRONGEST tier was reported as the weakest.
    # The compiler now emits a rank-prefixed value, which makes this min correct by
    # construction -- `_weakest` applies the ladder and strips the prefix.
    from analytics.plan import _weakest
    statuses = [r.get("weakest_status") for r in rows if r.get("weakest_status")]
    return Readiness(True, "", _weakest(statuses) if statuses else None)


def _severity(spec: InsightSpec, delta_pct: float | None, anomaly: dict | None) -> tuple:
    """Severity from BOTH movement size and anomaly score; the stronger wins."""
    sev, score, method = "NONE", None, None
    if delta_pct is not None:
        a = abs(delta_pct)
        sev = ("HIGH" if a >= spec.high_delta_pct else
               "MODERATE" if a >= spec.moderate_delta_pct else "LOW")
    if anomaly:
        usable = {k: v for k, v in anomaly.items() if not v.get("refused")}
        if usable:
            k = max(usable, key=lambda x: abs(usable[x].get("score") or 0))
            score, method = usable[k].get("score"), k
            rank = ["NONE", "LOW", "MODERATE", "HIGH", "CRITICAL"]
            asev = {"NORMAL": "LOW", "MODERATE": "MODERATE", "HIGH": "HIGH",
                    "EXTREME": "CRITICAL"}.get(usable[k].get("severity"), "LOW")
            if rank.index(asev) > rank.index(sev if sev in rank else "NONE"):
                sev = asev
        else:
            sev = sev if delta_pct is not None else "UNKNOWN"
    return sev, score, method


def generate_insight(spec: InsightSpec, business_date: str, *, runner, registry=None,
                     insight_version: str = "insight:v1", model=None,
                     readiness_fn=default_readiness) -> BusinessInsight:
    reg = registry or load_registry()
    iid = make_insight_id(spec.metric_id, business_date, insight_version)

    def skeleton(status, reason, **kw):
        base = dict(
            insight_id=iid, business_date=business_date, metric_id=spec.metric_id,
            metric_version=reg.get(spec.metric_id).version_hash(),
            insight_version=insight_version, schema_version="insight_record:v1",
            actual_value=None, baseline_value=None, delta=None, delta_pct=None,
            severity="UNKNOWN", anomaly_score=None, anomaly_method=None,
            top_drivers=[], driver_dimension=None, driver_coverage=None,
            forecast_point=None, forecast_lower=None, forecast_upper=None,
            forecast_method=None, forecast_note=None,
            certification_status="UNKNOWN", certification_ok=False,
            dq_status="NOT_AVAILABLE", query_ids=[], sql_hashes=[], bytes_scanned=None,
            summary="", summary_source="deterministic", model_version=None,
            prompt_version=None, tokens_in=None, tokens_out=None, generation_ms=None,
            status=status, skip_reason=reason, created_at=now_iso(), notes=[])
        base.update(kw)
        return BusinessInsight(**base)

    if not spec.enabled:
        return skeleton("SKIPPED", spec.disabled_reason or "disabled in insight_config")

    ready = readiness_fn(spec.metric_id, business_date, runner=runner, registry=reg)
    if not ready.ready:
        return skeleton("SKIPPED", f"upstream not ready: {ready.reason}")

    if ready.certification:
        try:
            reg.assert_certification(spec.metric_id, ready.certification)
        except Exception as e:                                 # noqa: BLE001
            return skeleton("SKIPPED", f"certification: {e}",
                            certification_status=ready.certification)

    try:
        # as_of is the day AFTER the business date, so "yesterday" resolves to the business
        # date itself and the comparison window falls on the day before it. That is also
        # operationally true: an insight for date D is generated after D's EOD closes, on
        # D+1.
        #
        # Do NOT set req.time directly. analyse() calls validate() again, which re-resolves
        # from time_phrase and silently discards any override -- an earlier version did
        # exactly that and produced every insight for the WRONG DATE while reporting the
        # right one.
        from datetime import date, timedelta
        gen_as_of = (date.fromisoformat(business_date) + timedelta(days=1)).isoformat()
        req = AnalyticalRequest(
            intent=Intent.PERIOD_COMPARISON, metric_id=spec.metric_id,
            time_phrase=spec.time_phrase, as_of=gen_as_of).validate(reg)
        if req.time.primary.end != business_date:
            return skeleton("FAILED",
                            f"time resolution error: {spec.comparison!r} resolved to "
                            f"{req.time.primary.end}, expected {business_date}")
        pack = analyse(req, runner=runner, registry=reg,
                       budget=QueryBudget(max_queries=spec.max_queries),
                       history_days=spec.history_days,
                       driver_dimensions=spec.driver_dimensions)
    except Exception as e:                                     # noqa: BLE001
        return skeleton("FAILED", f"analysis failed: {type(e).__name__}: {e}")

    c = pack.comparison or {}
    anomaly = pack.anomaly if spec.anomaly.enabled else None
    sev, score, method = _severity(spec, c.get("delta_pct"), anomaly)

    dim = next(iter(pack.drivers), None) if pack.drivers else None
    drivers, coverage = [], None
    if dim:
        d = pack.drivers[dim]
        coverage = d.get("coverage")
        drivers = [TopDriver(x["segment"], x["delta"], x["share_of_delta"], x["status"])
                   for x in d["contributions"][:3]]

    f = pack.forecast if spec.forecast_enabled else None
    fnote = None if spec.forecast_enabled else "forecast disabled in insight_config"
    if spec.forecast_enabled and pack.forecast is None:
        fnote = next((n for n in pack.notes if "forecast" in n.lower()), "not produced")

    s = summarise(pack, spec, sev, model=model)
    return BusinessInsight(
        insight_id=iid, business_date=business_date, metric_id=spec.metric_id,
        metric_version=pack.metric_version, insight_version=insight_version,
        schema_version="insight_record:v1",
        actual_value=c.get("current"), baseline_value=c.get("baseline"),
        delta=c.get("delta"), delta_pct=c.get("delta_pct"),
        severity=sev, anomaly_score=score, anomaly_method=method,
        top_drivers=drivers, driver_dimension=dim, driver_coverage=coverage,
        forecast_point=(f or {}).get("point"), forecast_lower=(f or {}).get("lower"),
        forecast_upper=(f or {}).get("upper"), forecast_method=(f or {}).get("method"),
        forecast_note=fnote,
        certification_status=pack.data_status, certification_ok=pack.certification_ok,
        dq_status=pack.dq_status,
        query_ids=[r.query_id for r in pack.results if r.query_id],
        sql_hashes=[r.sql_hash for r in pack.results],
        bytes_scanned=(pack.budget or {}).get("bytes_scanned"),
        summary=s.text, summary_source=s.source, model_version=s.model_version,
        prompt_version=s.prompt_version, tokens_in=s.tokens_in, tokens_out=s.tokens_out,
        generation_ms=s.duration_ms, status="OK", skip_reason=None,
        created_at=now_iso(), notes=list(pack.notes) + list(s.notes))


def run_all(business_date: str, *, runner, config=None, registry=None,
            model=None) -> list[BusinessInsight]:
    """One KPI failing must not suppress the others."""
    cfg = config or load_insight_config()
    reg = registry or load_registry()
    return [generate_insight(s, business_date, runner=runner, registry=reg,
                             insight_version=cfg.insight_version, model=model)
            for s in cfg.specs]
