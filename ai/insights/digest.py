"""Daily executive digest, built from BusinessInsight records only.

Nothing is recomputed here. If a value is not in a record it does not appear — which is why
the digest cannot drift from the insights it summarises, and why a KPI that was SKIPPED is
shown as skipped rather than quietly omitted. An executive reading five KPIs when six are
registered has no way to know one is missing.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .record import BusinessInsight

_RANK = {"CRITICAL": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3, "NONE": 4, "UNKNOWN": 5}


@dataclass
class Digest:
    business_date: str
    top_movements: list[dict] = field(default_factory=list)
    anomalies: list[dict] = field(default_factory=list)
    largest_drivers: list[dict] = field(default_factory=list)
    forecast_warnings: list[dict] = field(default_factory=list)
    data_warnings: list[dict] = field(default_factory=list)
    skipped: list[dict] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)
    generated_from: int = 0

    def to_dict(self) -> dict:
        return self.__dict__


def build_digest(insights: list[BusinessInsight], *, top: int = 5) -> Digest:
    d = Digest(business_date=insights[0].business_date if insights else "")
    d.generated_from = len(insights)
    ok = [i for i in insights if i.status == "OK"]

    d.skipped = [{"metric_id": i.metric_id, "reason": i.skip_reason}
                 for i in insights if i.status == "SKIPPED"]
    d.failed = [{"metric_id": i.metric_id, "reason": i.skip_reason}
                for i in insights if i.status == "FAILED"]

    movers = sorted((i for i in ok if i.delta_pct is not None),
                    key=lambda i: abs(i.delta_pct), reverse=True)
    d.top_movements = [
        {"metric_id": i.metric_id, "actual": i.actual_value, "baseline": i.baseline_value,
         "delta": i.delta, "delta_pct": i.delta_pct, "severity": i.severity,
         "summary": i.summary} for i in movers[:top]]

    d.anomalies = sorted(
        [{"metric_id": i.metric_id, "severity": i.severity, "score": i.anomaly_score,
          "method": i.anomaly_method}
         for i in ok if i.severity in ("MODERATE", "HIGH", "CRITICAL")],
        key=lambda x: _RANK.get(x["severity"], 9))

    for i in ok:
        for t in i.top_drivers[:1]:
            if t.share_of_delta is not None:
                d.largest_drivers.append(
                    {"metric_id": i.metric_id, "dimension": i.driver_dimension,
                     "segment": t.segment, "share_of_delta": t.share_of_delta,
                     # Wording is fixed here too: the digest is the most-read surface and
                     # the easiest place for "contributed" to become "caused".
                     "phrase": f"{t.segment} contributed "
                               f"{t.share_of_delta:.1%} of the observed change"})
    d.largest_drivers.sort(key=lambda x: abs(x["share_of_delta"]), reverse=True)
    d.largest_drivers = d.largest_drivers[:top]

    d.forecast_warnings = [{"metric_id": i.metric_id, "note": i.forecast_note}
                           for i in ok if i.forecast_note]
    d.data_warnings = [
        {"metric_id": i.metric_id, "certification": i.certification_status,
         "certification_ok": i.certification_ok, "dq": i.dq_status}
        for i in ok if not i.certification_ok]
    return d


def render_digest(d: Digest) -> str:
    out = [f"BUSINESS DATE: {d.business_date}",
           f"(from {d.generated_from} registered KPIs)", ""]
    out.append("TOP MOVEMENTS")
    if not d.top_movements:
        out.append("  none with a measurable change")
    for i, m in enumerate(d.top_movements, 1):
        pct = f"{m['delta_pct']:+.2%}" if m["delta_pct"] is not None else "n/a"
        out.append(f"  {i}. {m['metric_id']}  {m['actual']:,.2f}  {pct}  [{m['severity']}]")

    out += ["", "ANOMALIES"]
    out += [f"  {a['metric_id']}: {a['severity']} (score "
            f"{a['score']:.2f} via {a['method']})" for a in d.anomalies] or ["  none"]

    out += ["", "LARGEST DRIVERS"]
    out += [f"  {x['metric_id']} by {x['dimension']}: {x['phrase']}"
            for x in d.largest_drivers] or ["  none"]

    out += ["", "FORECAST"]
    out += [f"  {f['metric_id']}: {f['note']}" for f in d.forecast_warnings] or ["  none"]

    out += ["", "DATA QUALITY / CERTIFICATION"]
    out += [f"  {w['metric_id']}: {w['certification']} "
            f"(below required tier)" for w in d.data_warnings] or ["  all KPIs met their required tier"]

    if d.skipped:
        out += ["", "SKIPPED"]
        out += [f"  {s['metric_id']}: {s['reason']}" for s in d.skipped]
    if d.failed:
        out += ["", "FAILED"]
        out += [f"  {f['metric_id']}: {f['reason']}" for f in d.failed]
    return "\n".join(out)
