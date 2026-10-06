"""Query plan, execution and EvidencePack.

A plan is a SMALL, BOUNDED list of compiled queries — never an open loop. VALUE is one
query; PERIOD_COMPARISON is two. Nothing here can decide to run a third, which is what keeps
"cost per question" a property of the plan rather than of the model's mood.

The EvidencePack is the only thing a language model is ever handed. It carries the numbers,
the SQL hash, the Athena query id, the bytes scanned and the certification tier — so a
generated sentence can be checked against the query that produced it.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from .compiler import STATUS_ORDER, CompiledQuery, compile_metric_query
from .request import AnalyticalRequest, Intent
from .semantic import CertificationTooWeak, Registry, load_registry

#: Hard ceiling. A question that needs more than this is a design error, not a big question.
MAX_QUERIES_PER_REQUEST = 2


@dataclass
class QueryPlan:
    request: dict
    queries: list[CompiledQuery]
    labels: list[str]

    def to_dict(self) -> dict:
        return {"request": self.request, "labels": self.labels,
                "queries": [q.to_dict() for q in self.queries]}


def build_plan(req: AnalyticalRequest, registry: Registry | None = None) -> QueryPlan:
    reg = registry or load_registry()
    req.validate(reg)
    t = req.time
    common = dict(registry=reg, dimension=req.dimension, grain=req.grain)

    if req.intent is Intent.VALUE:
        qs = [compile_metric_query(req.metric_id, t.primary, **common)]
        labels = [t.primary.label]
    elif req.intent is Intent.TREND:
        qs = [compile_metric_query(req.metric_id, t.primary, **common)]
        labels = [t.primary.label]
    elif req.intent is Intent.PERIOD_COMPARISON:
        qs = [compile_metric_query(req.metric_id, t.primary, **common),
              compile_metric_query(req.metric_id, t.comparison, **common)]
        labels = [t.primary.label, t.comparison.label]
    elif req.intent is Intent.BREAKDOWN:
        qs = [compile_metric_query(req.metric_id, t.primary, **common)]
        labels = [f"{t.primary.label} by {req.dimension}"]
    else:  # TOP_N / BOTTOM_N
        qs = [compile_metric_query(req.metric_id, t.primary, row_limit=req.top_n,
                                   order_by_value=req.sort_direction, collapse_time=True,
                                   **common)]
        labels = [f"{req.intent.value.lower()} {req.top_n} {req.dimension}"]

    if len(qs) > MAX_QUERIES_PER_REQUEST:
        raise ValueError(f"plan of {len(qs)} queries exceeds {MAX_QUERIES_PER_REQUEST}")
    return QueryPlan(req.to_dict(), qs, labels)


@dataclass
class QueryResult:
    label: str
    metric_id: str
    metric_version: str
    sql_hash: str
    relation: str
    window: dict
    rows: list[dict]
    row_count: int
    total_value: float | None
    weakest_status: str | None
    query_id: str | None
    bytes_scanned: int | None
    duration_ms: float
    error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EvidencePack:
    request_id: str
    intent: str
    metric_id: str
    metric_version: str
    metric_definition: dict
    plan: dict
    results: list[QueryResult]
    comparison: dict | None
    data_status: str
    certification_ok: bool
    unit: str
    currency: str
    lineage: dict
    generated_at: str
    notes: list[str] = field(default_factory=list)
    dq_status: str = "NOT_AVAILABLE"
    # ---- EvidencePack v2 (BAI-P3 §8). Each is None when NOT COMPUTED, and the reason is
    # in `notes`. A null here always means "not produced", never "nothing found".
    trend: dict | None = None
    drivers: dict | None = None
    anomaly: dict | None = None
    forecast: dict | None = None
    freshness: dict | None = None
    budget: dict | None = None
    analytics_versions: dict = field(default_factory=dict)
    pack_version: str = "evidence:v2"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["results"] = [r.to_dict() for r in self.results]
        return d


#: Imported, not redeclared: the SQL in `compiler.py` orders tiers from this same tuple.
_STATUS_ORDER = STATUS_ORDER


def _status_name(s: str | None) -> str | None:
    """Strip the rank prefix the compiler adds (`"1:PROVISIONAL_NRT"` -> the tier name).

    The SQL orders tiers by a numeric prefix because MIN on the bare name is ALPHABETICAL,
    and 'CERTIFIED' sorts before 'PROVISIONAL_NRT' -- the strongest tier winning the
    "weakest" column, which is exactly backwards. The prefix never leaves this module.
    """
    if not s:
        return s
    head, sep, tail = s.partition(":")
    return tail if (sep and head.isdigit() and tail) else s


def _weakest(statuses: list[str | None]) -> str | None:
    present = [_status_name(s) for s in statuses if s]
    if not present:
        return None
    return min(present, key=lambda s: _STATUS_ORDER.index(s)
               if s in _STATUS_ORDER else -1)


def execute_plan(plan: QueryPlan, *, runner=None, registry: Registry | None = None,
                 enforce_certification: bool = True) -> EvidencePack:
    """Run the plan. `runner(sql) -> dict` is injected so tests need no AWS."""
    reg = registry or load_registry()
    m = reg.get(plan.request["metric_id"])
    results: list[QueryResult] = []

    if runner is None:
        from agent_tools.athena_tool import run_query
        def runner(sql):                                   # noqa: E306
            return run_query(sql, limit=1000)

    for q, label in zip(plan.queries, plan.labels):
        t0 = time.perf_counter()
        try:
            out = runner(q.sql)
            rows = out.get("rows", [])
            vals = [float(r["metric_value"]) for r in rows
                    if r.get("metric_value") not in (None, "")]
            results.append(QueryResult(
                label=label, metric_id=q.metric_id, metric_version=q.metric_version,
                sql_hash=q.sql_hash(), relation=q.relation, window=q.window, rows=rows,
                row_count=len(rows),
                total_value=(sum(vals) if vals else None),
                weakest_status=_weakest([r.get("weakest_status") for r in rows]),
                query_id=(out.get("resource_ids") or [None])[0],
                bytes_scanned=out.get("bytes_scanned"),
                duration_ms=round((time.perf_counter() - t0) * 1000, 1)))
        except Exception as e:                             # noqa: BLE001
            results.append(QueryResult(
                label=label, metric_id=q.metric_id, metric_version=q.metric_version,
                sql_hash=q.sql_hash(), relation=q.relation, window=q.window, rows=[],
                row_count=0, total_value=None, weakest_status=None, query_id=None,
                bytes_scanned=None,
                duration_ms=round((time.perf_counter() - t0) * 1000, 1),
                error=f"{type(e).__name__}: {e}"))

    status = _weakest([r.weakest_status for r in results]) or "UNKNOWN"
    notes: list[str] = []
    cert_ok = True
    if status == "UNKNOWN":
        cert_ok = False
        notes.append("no certification status returned; treat the number as UNVERIFIED")
    else:
        try:
            reg.assert_certification(m.metric_id, status)
        except CertificationTooWeak as e:
            cert_ok = False
            notes.append(str(e))

    comparison = None
    if len(results) == 2 and all(r.total_value is not None for r in results):
        a, b = results[0].total_value, results[1].total_value
        comparison = {"current_label": results[0].label, "current": a,
                      "baseline_label": results[1].label, "baseline": b,
                      "delta": round(a - b, 6),
                      "delta_pct": (round((a - b) / b, 6) if b else None)}

    from .lineage import lineage_for
    for r in results:
        if r.error:
            notes.append(f"{r.label}: {r.error}")
        elif r.row_count == 0:
            notes.append(f"{r.label}: no rows — the date may not be loaded")

    if m.time_additivity == "semi_additive_last" and plan.queries[0].window["start"] != \
            plan.queries[0].window["end"]:
        notes.append(f"{m.metric_id} is SEMI-ADDITIVE across time: values are PER DATE and "
                     "must not be summed.")

    return EvidencePack(
        request_id=str(uuid.uuid4()), intent=plan.request["intent"], metric_id=m.metric_id,
        metric_version=m.version_hash(),
        metric_definition={"business_name": m.business_name, "measure": m.measure_sql,
                           "grain": m.business_grain, "owner": m.owner,
                           "additive": m.additive, "time_additivity": m.time_additivity,
                           "minimum_certification": m.minimum_certification},
        plan=plan.to_dict(), results=results, comparison=comparison, data_status=status,
        certification_ok=cert_ok, unit=m.unit, currency=m.currency,
        lineage=lineage_for(m).to_dict(),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"), notes=notes)
