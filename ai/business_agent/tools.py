"""The business tool catalog.

ONLY tools with a real implementation behind them are exposed. A tool that returns a
plausible shape from nothing is worse than an absent one: the graph routes to it, the
evidence pack records a successful call, and the answer is confident and empty.

Every tool is READ-ONLY by construction — none writes, and `WRITE_TOOLS` is asserted empty
by test, exactly as ADR-057 requires of the V1 agent.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
for p in (str(ROOT), str(ROOT / "ai"), str(ROOT / "spark")):
    if p not in sys.path:
        sys.path.insert(0, p)


@dataclass(frozen=True)
class BusinessTool:
    name: str
    description: str
    fn: Callable[..., Any]
    confidence: str          # DATA_FACT | STATISTICAL_OBSERVATION | MODEL_PREDICTION
    reads: str               # what it touches, for the evidence trail
    read_only: bool = True


def _registry():
    from analytics.semantic import load_registry
    return load_registry()


# ------------------------------------------------------------------ metric layer
def resolve_metric(phrase: str) -> dict:
    from analytics.resolver import resolve_metric as _r
    return _r(phrase, _registry()).to_dict()


def get_metric_definition(metric_id: str) -> dict:
    m = _registry().get(metric_id)
    return {"metric_id": m.metric_id, "business_name": m.business_name,
            "description": m.description, "measure": m.measure_sql,
            "grain": m.business_grain, "owner": m.owner, "unit": m.unit,
            "currency": m.currency, "additive": m.additive,
            "time_additivity": m.time_additivity,
            "allowed_dimensions": list(m.allowed_dimensions),
            "minimum_certification": m.minimum_certification,
            "version": m.version_hash()}


def query_metric(metric_id: str, as_of: str, *, runner, time_phrase: str = "yesterday",
                 dimension: str | None = None) -> dict:
    from analytics.plan import build_plan, execute_plan
    from analytics.request import AnalyticalRequest, Intent
    req = AnalyticalRequest(Intent.VALUE, metric_id, time_phrase=time_phrase,
                            as_of=as_of, dimension=dimension).validate(_registry())
    return execute_plan(build_plan(req), runner=runner).to_dict()


def compare_metric_periods(metric_id: str, as_of: str, *, runner,
                           time_phrase: str = "yesterday") -> dict:
    from analytics.plan import build_plan, execute_plan
    from analytics.request import AnalyticalRequest, Intent
    req = AnalyticalRequest(Intent.PERIOD_COMPARISON, metric_id, time_phrase=time_phrase,
                            as_of=as_of).validate(_registry())
    return execute_plan(build_plan(req), runner=runner).to_dict()


def breakdown_metric(metric_id: str, as_of: str, dimension: str, *, runner,
                     time_phrase: str = "yesterday") -> dict:
    from analytics.plan import build_plan, execute_plan
    from analytics.request import AnalyticalRequest, Intent
    req = AnalyticalRequest(Intent.BREAKDOWN, metric_id, time_phrase=time_phrase,
                            as_of=as_of, dimension=dimension).validate(_registry())
    return execute_plan(build_plan(req), runner=runner).to_dict()


def analyze_metric_drivers(metric_id: str, as_of: str, dimension: str, *, runner) -> dict:
    from analytics.engine import analyse
    from analytics.request import AnalyticalRequest, Intent
    req = AnalyticalRequest(Intent.PERIOD_COMPARISON, metric_id, time_phrase="yesterday",
                            as_of=as_of).validate(_registry())
    pack = analyse(req, runner=runner, driver_dimensions=(dimension,))
    return {"drivers": pack.drivers, "comparison": pack.comparison, "notes": pack.notes}


def detect_metric_anomaly(metric_id: str, as_of: str, *, runner) -> dict:
    from analytics.engine import analyse
    from analytics.request import AnalyticalRequest, Intent
    req = AnalyticalRequest(Intent.PERIOD_COMPARISON, metric_id, time_phrase="yesterday",
                            as_of=as_of).validate(_registry())
    pack = analyse(req, runner=runner)
    return {"anomaly": pack.anomaly, "trend": pack.trend, "notes": pack.notes}


def forecast_metric(metric_id: str, as_of: str, *, runner) -> dict:
    from analytics.engine import analyse
    from analytics.request import AnalyticalRequest, Intent
    req = AnalyticalRequest(Intent.PERIOD_COMPARISON, metric_id, time_phrase="yesterday",
                            as_of=as_of).validate(_registry())
    pack = analyse(req, runner=runner)
    return {"forecast": pack.forecast,
            "notes": [n for n in pack.notes if "forecast" in n.lower()]}


# ------------------------------------------------------------------ knowledge / governance
def retrieve_business_knowledge(question: str, metric_id: str | None = None,
                                top_k: int = 3) -> dict:
    """Definitions and glossary ONLY. Never a value — quantities are stripped."""
    from analytics.rag_boundary import knowledge_for_metric
    from agent_tools.catalog import retrieve_knowledge
    out: dict = {"definition": None, "chunks": []}
    if metric_id:
        out["definition"] = knowledge_for_metric(_registry().get(metric_id)).to_dict()
    try:
        r = retrieve_knowledge(question, top_k=top_k)
        from analytics.rag_boundary import strip_numeric_claims
        for c in r.get("chunks", []):
            text, n = strip_numeric_claims(c.get("text", ""))
            out["chunks"].append({"citation": c.get("citation"), "text": text[:600],
                                  "stripped_numeric_claims": n})
        out["corpus_version"] = r.get("corpus_version")
    except Exception as e:                                       # noqa: BLE001
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def get_lineage(metric_id: str) -> dict:
    from analytics.lineage import lineage_for
    return lineage_for(_registry().get(metric_id)).to_dict()


def get_certification_status(metric_id: str, as_of: str, *, runner) -> dict:
    pack = query_metric(metric_id, as_of, runner=runner)
    m = _registry().get(metric_id)
    return {"metric_id": metric_id, "data_status": pack["data_status"],
            "certification_ok": pack["certification_ok"],
            "minimum_certification": m.minimum_certification}


def get_data_quality(metric_id: str, as_of: str, *, runner) -> dict:
    """DQ from the governance advisor. Reports UNKNOWN rather than PASS when a check
    could not run — a green board built from checks that did not execute is worse than none."""
    from analytics.governance import review
    from analytics.datasource import AthenaSource
    from analytics.registry import MetricSpec
    m = _registry().get(metric_id)
    # TWO metric types exist: analytics.registry.MetricSpec (what the governance and
    # diagnosis engines were built against in AI-v2) and analytics.semantic.Metric (the
    # BAI-P1 governed registry). Passing the wrong one fails with a bare AttributeError on
    # `date_column`. Adapt explicitly rather than widening either contract.
    spec = MetricSpec(
        name=m.metric_id, dataset=m.source_relation, measure=m.measure_sql,
        date_column=m.time_column,
        dimensions=tuple(d for d in m.allowed_dimensions),
        owner=m.owner, grain=m.business_grain, additive=m.additive, unit=m.unit,
        description=m.description)
    src = AthenaSource(runner=lambda sql: runner(sql).get("rows", []))
    # `upstream` enables cross-layer reconciliation: the curated fact the mart projects.
    upstream = {"kafka_dev_lab_dev_mart.mart_account_balance_daily":
                "kafka_dev_lab_dev_curated.fact_account_daily_snapshot"}.get(
                    m.source_relation)
    return review(spec, src, as_of=as_of,
                  sql_runner=lambda sql: runner(sql).get("rows", []),
                  upstream=upstream).to_dict()


# ------------------------------------------------------------------ features / ML
def get_feature(account_sk: str, as_of: str, *, mart_rows: list[dict]) -> dict:
    from business_ml.features import load_group, materialize
    rows = materialize(mart_rows, load_group(), as_of=as_of)
    for r in rows:
        if str(r.account_sk) == str(account_sk):
            return r.to_dict()
    return {"error": f"no features for account {account_sk} at {as_of}"}


def predict_business_risk(as_of: str, *, mart_rows: list[dict], top_k: int = 5) -> dict:
    """Unsupervised anomaly score per account. NOT a risk rating, and labelled as such."""
    from business_ml.features import load_group, materialize
    from business_ml.inference import run_anomaly_batch
    rows = materialize(mart_rows, load_group(), as_of=as_of)
    preds = [p.to_dict() for p in run_anomaly_batch(rows)][:top_k]
    return {"predictions": preds,
            "interpretation": "Unsupervised deviation from the account population. It is "
                              "not a risk rating and not evidence of wrongdoing."}


CATALOG: dict[str, BusinessTool] = {
    t.name: t for t in (
        BusinessTool("resolve_metric", "Business phrase -> one governed metric",
                     resolve_metric, "DATA_FACT", "metric registry"),
        BusinessTool("get_metric_definition", "Governed definition of a metric",
                     get_metric_definition, "DATA_FACT", "metric registry"),
        BusinessTool("query_metric", "Actual value for a period", query_metric,
                     "DATA_FACT", "mart via read-only Athena"),
        BusinessTool("compare_metric_periods", "Value vs a comparison period",
                     compare_metric_periods, "DATA_FACT", "mart"),
        BusinessTool("breakdown_metric", "Value split by an allowed dimension",
                     breakdown_metric, "DATA_FACT", "mart"),
        BusinessTool("analyze_metric_drivers", "Contribution of each segment to a change",
                     analyze_metric_drivers, "STATISTICAL_OBSERVATION", "mart"),
        BusinessTool("detect_metric_anomaly", "Deviation from a historical baseline",
                     detect_metric_anomaly, "STATISTICAL_OBSERVATION", "mart"),
        BusinessTool("forecast_metric", "Forecast with a backtested error",
                     forecast_metric, "STATISTICAL_OBSERVATION", "mart"),
        BusinessTool("retrieve_business_knowledge", "Definitions and glossary, never values",
                     retrieve_business_knowledge, "DATA_FACT", "knowledge corpus"),
        BusinessTool("get_lineage", "Metric -> mart -> dbt model -> source", get_lineage,
                     "DATA_FACT", "dbt manifest"),
        BusinessTool("get_data_quality", "Freshness/completeness/watermark triage",
                     get_data_quality, "STATISTICAL_OBSERVATION", "mart"),
        BusinessTool("get_certification_status", "Certification tier of the data",
                     get_certification_status, "DATA_FACT", "mart"),
        BusinessTool("get_feature", "Point-in-time features for one account", get_feature,
                     "DATA_FACT", "offline feature store"),
        BusinessTool("predict_business_risk", "Unsupervised account anomaly score",
                     predict_business_risk, "MODEL_PREDICTION", "feature store + model"),
    )
}

#: Asserted empty by test. ADR-057: the V1 agent has no mutation authority at all.
WRITE_TOOLS: dict = {}

#: Deliberately ABSENT, with the reason recorded rather than stubbed.
NOT_IMPLEMENTED = {
    "get_reconciliation_status": "ops.metric_variance / data_certification hold no rows",
    "close_account": "out of scope: V1 is advisory, never an actor (BAI-P6 section 7)",
    "block_customer": "out of scope: V1 is advisory, never an actor (BAI-P6 section 7)",
}
