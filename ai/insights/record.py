"""The BusinessInsight record — the stable contract Power BI reads.

Stable means: a consumer written against this today keeps working when the generator
changes. So every field is explicit, `insight_id` is DETERMINISTIC, and nothing is a free
dict the BI layer has to parse.

`insight_id` is a content hash of (metric, business_date, insight_version). Rerunning the
same day produces the SAME id, which is what makes the serving write idempotent without a
sequence, a lock or a delete-then-insert window in which the dashboard shows nothing.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

INSIGHT_SCHEMA_VERSION = "insight_record:v1"

SEVERITY = ("NONE", "LOW", "MODERATE", "HIGH", "CRITICAL", "UNKNOWN")


@dataclass
class TopDriver:
    segment: str
    delta: float
    share_of_delta: float | None
    status: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BusinessInsight:
    insight_id: str
    business_date: str
    metric_id: str
    metric_version: str
    insight_version: str
    schema_version: str

    actual_value: float | None
    baseline_value: float | None
    delta: float | None
    delta_pct: float | None

    severity: str
    anomaly_score: float | None
    anomaly_method: str | None

    top_drivers: list[TopDriver]
    driver_dimension: str | None
    driver_coverage: float | None

    forecast_point: float | None
    forecast_lower: float | None
    forecast_upper: float | None
    forecast_method: str | None
    forecast_note: str | None

    certification_status: str
    certification_ok: bool
    dq_status: str

    query_ids: list[str]
    sql_hashes: list[str]
    bytes_scanned: int | None

    summary: str
    summary_source: str            # deterministic | llm
    model_version: str | None
    prompt_version: str | None
    tokens_in: int | None
    tokens_out: int | None
    generation_ms: float | None

    status: str                    # OK | SKIPPED | FAILED
    skip_reason: str | None
    created_at: str
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["top_drivers"] = [t.to_dict() for t in self.top_drivers]
        return d

    def to_serving_row(self) -> dict:
        """Flattened for serving.ai_business_insight_daily.

        Drivers are flattened to the top three as scalar columns rather than a nested
        array: Power BI handles a struct array badly, and a dashboard that has to unnest
        JSON is a dashboard nobody builds.
        """
        row = {
            "insight_id": self.insight_id, "business_date": self.business_date,
            "metric_id": self.metric_id, "metric_version": self.metric_version,
            "insight_version": self.insight_version,
            "actual_value": self.actual_value, "baseline_value": self.baseline_value,
            "delta": self.delta, "delta_pct": self.delta_pct,
            "severity": self.severity, "anomaly_score": self.anomaly_score,
            "anomaly_method": self.anomaly_method,
            "driver_dimension": self.driver_dimension,
            "driver_coverage": self.driver_coverage,
            "forecast_point": self.forecast_point,
            "forecast_lower": self.forecast_lower, "forecast_upper": self.forecast_upper,
            "forecast_method": self.forecast_method,
            "certification_status": self.certification_status,
            "certification_ok": self.certification_ok, "dq_status": self.dq_status,
            "summary": self.summary, "summary_source": self.summary_source,
            "model_version": self.model_version, "prompt_version": self.prompt_version,
            "query_ids": ",".join(self.query_ids), "status": self.status,
            "skip_reason": self.skip_reason, "created_at": self.created_at,
        }
        for i in range(3):
            d = self.top_drivers[i] if i < len(self.top_drivers) else None
            row[f"driver_{i + 1}_segment"] = d.segment if d else None
            row[f"driver_{i + 1}_delta"] = d.delta if d else None
            row[f"driver_{i + 1}_share"] = d.share_of_delta if d else None
        return row


def make_insight_id(metric_id: str, business_date: str, insight_version: str) -> str:
    """Deterministic. Same inputs -> same id -> idempotent MERGE."""
    payload = json.dumps({"m": metric_id, "d": business_date, "v": insight_version},
                         sort_keys=True)
    return "insight:" + hashlib.sha256(payload.encode()).hexdigest()[:20]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
