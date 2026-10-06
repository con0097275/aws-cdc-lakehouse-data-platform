"""Insight registration loader.

Mirrors the reporting framework's contract: the YAML IS the registration, and adding a KPI
adds no DAG file (ADR-039). Validation is strict at load — an unknown metric_id is refused
rather than skipped, because a KPI silently missing from the digest looks exactly like a KPI
with nothing to report.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "aiplatform" / "insights" / "insight_config.yaml"
COMPARISONS = ("previous_day", "same_day_last_week", "prior_7_days")


class InsightConfigError(ValueError):
    pass


@dataclass(frozen=True)
class AnomalyPolicy:
    enabled: bool = True
    methods: tuple[str, ...] = ("zscore", "iqr", "ewma")
    min_severity_to_report: str = "MODERATE"


@dataclass(frozen=True)
class InsightSpec:
    metric_id: str
    enabled: bool
    required_certification: str
    comparison: str
    driver_dimensions: tuple[str, ...]
    anomaly: AnomalyPolicy
    forecast_enabled: bool
    moderate_delta_pct: float
    high_delta_pct: float
    history_days: int
    max_queries: int
    owner: str
    disabled_reason: str = ""

    @property
    def time_phrase(self) -> str:
        return {"previous_day": "yesterday",
                "same_day_last_week": "same day last week",
                "prior_7_days": "last 7 days"}[self.comparison]


@dataclass(frozen=True)
class InsightConfig:
    version: int
    insight_version: str
    specs: tuple[InsightSpec, ...]

    def enabled(self) -> list[InsightSpec]:
        return [s for s in self.specs if s.enabled]

    def get(self, metric_id: str) -> InsightSpec:
        for s in self.specs:
            if s.metric_id == metric_id:
                return s
        raise InsightConfigError(f"no insight registered for {metric_id!r}")


def _merge(defaults: dict, override: dict) -> dict:
    out = dict(defaults)
    for k, v in override.items():
        out[k] = {**out.get(k, {}), **v} if isinstance(v, dict) and isinstance(
            out.get(k), dict) else v
    return out


def load_insight_config(path: Path | str | None = None, *, registry=None) -> InsightConfig:
    raw = yaml.safe_load(Path(path or CONFIG_PATH).read_text())
    if registry is None:
        import sys
        sys.path.insert(0, str(ROOT / "ai"))
        from analytics.semantic import load_registry
        registry = load_registry()

    defaults = raw.get("defaults", {})
    specs: list[InsightSpec] = []
    seen: set[str] = set()
    for item in raw["insights"]:
        mid = item["metric_id"]
        if mid in seen:
            raise InsightConfigError(f"duplicate insight registration for {mid!r}")
        seen.add(mid)
        if mid not in registry.metrics:
            raise InsightConfigError(
                f"{mid!r} is registered for insights but is not a governed metric. "
                "Add it to aiplatform/metrics/business_metrics.yaml first — a KPI missing "
                "from the digest is indistinguishable from one with nothing to report.")
        m = _merge(defaults, item)
        if m["comparison"] not in COMPARISONS:
            raise InsightConfigError(f"{mid}: comparison must be one of {COMPARISONS}")
        if m["required_certification"] not in registry.certification_ranks:
            raise InsightConfigError(f"{mid}: unknown required_certification")
        metric = registry.get(mid)
        for d in m.get("driver_dimensions", []) or []:
            if d not in metric.allowed_dimensions:
                raise InsightConfigError(
                    f"{mid}: driver dimension {d!r} is not allowed for this metric")
        a = m.get("anomaly", {})
        th = m.get("severity_thresholds", {})
        specs.append(InsightSpec(
            metric_id=mid, enabled=bool(m["enabled"]),
            required_certification=m["required_certification"],
            comparison=m["comparison"],
            driver_dimensions=tuple(m.get("driver_dimensions") or ()),
            anomaly=AnomalyPolicy(bool(a.get("enabled", True)),
                                  tuple(a.get("methods", ("zscore", "iqr", "ewma"))),
                                  a.get("min_severity_to_report", "MODERATE")),
            forecast_enabled=bool((m.get("forecast") or {}).get("enabled", False)),
            moderate_delta_pct=float(th.get("moderate_delta_pct", 0.05)),
            high_delta_pct=float(th.get("high_delta_pct", 0.15)),
            history_days=int(m.get("history_days", 30)),
            max_queries=int(m.get("max_queries", 6)),
            owner=m.get("owner", raw.get("owner", "unknown")),
            disabled_reason=m.get("disabled_reason", "")))
    return InsightConfig(raw["version"], raw["insight_version"], tuple(specs))
