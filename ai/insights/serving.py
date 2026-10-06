"""Persist BusinessInsight records to serving.ai_business_insight_daily.

IDEMPOTENT BY CONSTRUCTION. `insight_id` is a deterministic hash of
(metric_id, business_date, insight_version), so a rerun MERGEs onto the same row.

MERGE, not DELETE-then-INSERT. The delete form opens a window in which the dashboard shows
nothing for that date, and if the insert then fails the window never closes.
"""
from __future__ import annotations

from .record import BusinessInsight

TABLE = "serving.ai_business_insight_daily"


def _lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def merge_statement(insight: BusinessInsight, table: str = TABLE) -> str:
    row = insight.to_serving_row()
    cols = list(row)
    src = ", ".join(f"{_lit(row[c])} AS {c}" for c in cols)
    updates = ", ".join(f"t.{c} = s.{c}" for c in cols if c != "insight_id")
    return (f"MERGE INTO {table} t USING (SELECT {src}) s "
            f"ON t.insight_id = s.insight_id "
            f"WHEN MATCHED THEN UPDATE SET {updates} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
            f"VALUES ({', '.join('s.' + c for c in cols)})")


def write_insights(insights: list[BusinessInsight], *, runner, table: str = TABLE) -> int:
    """Returns the number of rows merged. A failure on one KPI does not stop the rest."""
    written = 0
    for i in insights:
        try:
            runner(merge_statement(i, table))
            written += 1
        except Exception as e:                                   # noqa: BLE001
            print(f"insight {i.metric_id} could not be persisted: {type(e).__name__}: {e}")
    return written


def read_insights(business_date: str, *, runner, table: str = TABLE) -> list:
    from .record import BusinessInsight, TopDriver
    rows = runner(f"SELECT * FROM {table} WHERE business_date = DATE "
                  f"'{business_date}'").get("rows", [])
    out = []
    for r in rows:
        drivers = []
        for i in (1, 2, 3):
            seg = r.get(f"driver_{i}_segment")
            if seg:
                drivers.append(TopDriver(seg, float(r.get(f"driver_{i}_delta") or 0),
                                         (float(r[f"driver_{i}_share"])
                                          if r.get(f"driver_{i}_share") else None),
                                         "MOVED"))
        out.append(BusinessInsight(
            insight_id=r.get("insight_id"), business_date=str(r.get("business_date")),
            metric_id=r.get("metric_id"), metric_version=r.get("metric_version"),
            insight_version=r.get("insight_version"), schema_version="insight_record:v1",
            actual_value=_f(r.get("actual_value")), baseline_value=_f(r.get("baseline_value")),
            delta=_f(r.get("delta")), delta_pct=_f(r.get("delta_pct")),
            severity=r.get("severity") or "UNKNOWN", anomaly_score=_f(r.get("anomaly_score")),
            anomaly_method=r.get("anomaly_method"), top_drivers=drivers,
            driver_dimension=r.get("driver_dimension"),
            driver_coverage=_f(r.get("driver_coverage")),
            forecast_point=_f(r.get("forecast_point")), forecast_lower=_f(r.get("forecast_lower")),
            forecast_upper=_f(r.get("forecast_upper")), forecast_method=r.get("forecast_method"),
            forecast_note=None, certification_status=r.get("certification_status") or "UNKNOWN",
            certification_ok=str(r.get("certification_ok")).lower() in ("true", "1"),
            dq_status=r.get("dq_status") or "NOT_AVAILABLE", query_ids=[], sql_hashes=[],
            bytes_scanned=None, summary=r.get("summary") or "",
            summary_source=r.get("summary_source") or "deterministic",
            model_version=r.get("model_version"), prompt_version=r.get("prompt_version"),
            tokens_in=None, tokens_out=None, generation_ms=None,
            status=r.get("status") or "OK", skip_reason=r.get("skip_reason"),
            created_at=r.get("created_at") or ""))
    return out


def _f(v):
    try:
        return float(v) if v not in (None, "", "NULL") else None
    except (TypeError, ValueError):
        return None
