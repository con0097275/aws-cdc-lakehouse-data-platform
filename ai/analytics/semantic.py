"""Business metric registry loader and contract.

The registry is the single governed definition of each KPI. Its job is to make "total
deposits", "deposit balance" and "deposit amount" resolve to ONE definition, and to make an
undefined metric a REFUSAL rather than an improvisation.

Two fields carry most of the safety, and conflating them is a classic reporting bug:

  additive         may the metric be decomposed ACROSS SEGMENTS (branch, product)?
  time_additivity  may it be rolled up ACROSS DATES?

`closing_balance` is `additive: true` but `semi_additive_last`. Summing it across accounts on
one date is right; summing it across 30 dates gives a number ~30x too large that looks
plausible and no downstream check would catch. The dbt model already says so in prose
("across time use LAST, never SUM"); this makes it machine-enforced.
"""
from __future__ import annotations

import os

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "aiplatform" / "metrics" / "business_metrics.yaml"

AGGREGATIONS = {"sum": "SUM", "avg": "AVG", "count": "COUNT",
                "count_distinct": "COUNT(DISTINCT {})", "min": "MIN", "max": "MAX"}
TIME_ADDITIVITY = ("additive", "semi_additive_last", "non_additive")
TIME_GRAINS = ("day", "week", "month")


class MetricError(ValueError):
    """A metric contract was violated. Never downgraded to a warning."""


class AmbiguousMetric(MetricError):
    def __init__(self, phrase: str, candidates: list[str]):
        self.phrase, self.candidates = phrase, candidates
        super().__init__(
            f"AMBIGUOUS_METRIC: {phrase!r} matches {len(candidates)} governed metrics: "
            f"{candidates}. Ask which one is meant; guessing between two business "
            f"definitions is how two reports disagree.")


class UnknownMetric(MetricError):
    pass


class DimensionNotAllowed(MetricError):
    pass


class DimensionUnavailable(MetricError):
    """Declared for the metric, but its dimension table is not materialised."""


class CertificationTooWeak(MetricError):
    pass


#: Demo/seed data lives in its OWN relation, never in the certified mart. Set
#: `AI_MART_RELATION` to read a seeded table instead of the deployed one:
#:
#:   AI_MART_RELATION=kafka_dev_lab_dev_mart.mart_account_balance_daily_demo
#:
#: Seeded history cannot go into `mart_account_balance_daily`, because every tier on the
#: certification ladder is a claim about a process that ran -- CERTIFIED asserts an EOD
#: close and a certification gate, RECONCILED asserts a reconciliation. Synthetic rows can
#: honestly claim none of them, and there is no tier meaning "made up for a demo". So the
#: rows move, not the label: a separate relation, and `metric_version` changes with it
#: because a metric read from a different table IS a different metric.
def _relation(declared: str) -> str:
    override = os.environ.get("AI_MART_RELATION", "").strip()
    if not override:
        return declared
    # Only the account-balance mart is redirected. A blanket override would silently point
    # an unrelated metric at a table that does not carry its measure.
    return override if declared.endswith(".mart_account_balance_daily") else declared


@dataclass(frozen=True)
class Dimension:
    name: str
    column: str
    available: bool
    description: str = ""
    requires_join: str | None = None
    blocked_reason: str | None = None


@dataclass(frozen=True)
class Metric:
    metric_id: str
    business_name: str
    aliases: tuple[str, ...]
    description: str
    source_model: str
    source_relation: str
    dbt_node: str
    aggregation: str
    business_grain: str
    time_column: str
    supported_time_grains: tuple[str, ...]
    additive: bool
    time_additivity: str
    allowed_dimensions: tuple[str, ...]
    minimum_certification: str
    owner: str
    domain: str
    status: str
    measure_column: str | None = None
    measure_expression: str | None = None
    unit: str = ""
    currency: str = ""
    format: str = ""
    freshness_sla_hours: int | None = None
    default_filters: tuple[str, ...] = field(default_factory=tuple)

    @property
    def measure_sql(self) -> str:
        inner = self.measure_expression or self.measure_column
        if self.aggregation == "count_distinct":
            return f"COUNT(DISTINCT {inner})"
        return f"{AGGREGATIONS[self.aggregation]}({inner})"

    def version_hash(self) -> str:
        """Content hash of the DEFINITION. Changing a measure or grain changes the hash, so
        a stored result can be checked against the definition that produced it."""
        import hashlib
        import json
        payload = json.dumps({
            "metric_id": self.metric_id, "relation": self.source_relation,
            "measure": self.measure_expression or self.measure_column,
            "aggregation": self.aggregation, "grain": self.business_grain,
            "time_column": self.time_column, "additive": self.additive,
            "time_additivity": self.time_additivity,
            "min_cert": self.minimum_certification}, sort_keys=True)
        return "metric:" + hashlib.sha256(payload.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class Registry:
    version: int
    registry_id: str
    metrics: dict[str, Metric]
    dimensions: dict[str, Dimension]
    certification_ranks: dict[str, int]
    alias_index: dict[str, str]

    def get(self, metric_id: str) -> Metric:
        if metric_id not in self.metrics:
            raise UnknownMetric(
                f"unknown metric {metric_id!r}. Declared: {sorted(self.metrics)}")
        return self.metrics[metric_id]

    def dimension(self, name: str) -> Dimension:
        if name not in self.dimensions:
            raise DimensionNotAllowed(f"{name!r} is not a declared dimension")
        return self.dimensions[name]

    def accepts(self, metric_id: str, status: str) -> bool:
        m = self.get(metric_id)
        have = self.certification_ranks.get(status)
        need = self.certification_ranks[m.minimum_certification]
        if have is None:
            raise CertificationTooWeak(f"unknown certification status {status!r}")
        return have >= need

    def assert_certification(self, metric_id: str, status: str) -> None:
        if not self.accepts(metric_id, status):
            m = self.get(metric_id)
            raise CertificationTooWeak(
                f"{metric_id} requires at least {m.minimum_certification} but the data is "
                f"{status}. Returning it as a business answer would present provisional "
                "data as final.")


def _norm(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace.

    PUNCTUATION MATTERS HERE. Without stripping it, "credit?" is a different token from
    "credit", so "What is the movement debit credit?" matched only ONE alias instead of two
    and resolved confidently to the wrong metric — a question mark silently converting an
    ambiguous request into a definite answer. Found by the BAI-P7 golden set.
    """
    import re as _re
    t = _re.sub(r"[^a-z0-9_ ]+", " ", str(s).lower())
    return " ".join(t.replace("_", " ").split())


def load_registry(path: Path | str | None = None) -> Registry:
    raw: dict[str, Any] = yaml.safe_load(Path(path or REGISTRY_PATH).read_text())
    dims = {k: Dimension(name=k, column=v["column"], available=bool(v["available"]),
                         description=v.get("description", ""),
                         requires_join=v.get("requires_join"),
                         blocked_reason=v.get("blocked_reason"))
            for k, v in raw["dimensions"].items()}
    ranks = raw["certification_ranks"]

    metrics: dict[str, Metric] = {}
    alias_index: dict[str, str] = {}
    for m in raw["metrics"]:
        mid = m["metric_id"]
        if mid in metrics:
            raise MetricError(f"duplicate metric_id {mid!r}")
        if m["aggregation"] not in AGGREGATIONS:
            raise MetricError(f"{mid}: unknown aggregation {m['aggregation']!r}")
        if m["time_additivity"] not in TIME_ADDITIVITY:
            raise MetricError(f"{mid}: time_additivity must be one of {TIME_ADDITIVITY}")
        if bool(m.get("measure_column")) == bool(m.get("measure_expression")):
            raise MetricError(f"{mid}: give exactly one of measure_column/measure_expression")
        if m["minimum_certification"] not in ranks:
            raise MetricError(f"{mid}: unknown minimum_certification")
        for g in m["supported_time_grains"]:
            if g not in TIME_GRAINS:
                raise MetricError(f"{mid}: unsupported time grain {g!r}")
        for d in m["allowed_dimensions"]:
            if d not in dims:
                raise MetricError(f"{mid}: allowed dimension {d!r} is not declared")
        # A semi-additive or non-additive metric must not claim it can roll up over time.
        if m["time_additivity"] != "additive" and set(m["supported_time_grains"]) - {"day"}:
            raise MetricError(
                f"{mid}: time_additivity={m['time_additivity']} but supports grains "
                f"{m['supported_time_grains']}. A metric that cannot be summed across dates "
                "must not advertise week/month rollups.")

        metric = Metric(
            metric_id=mid, business_name=m["business_name"],
            aliases=tuple(m.get("aliases", [])), description=m.get("description", ""),
            source_model=m["source_model"],
            source_relation=_relation(m["source_relation"]),
            dbt_node=m["dbt_node"], aggregation=m["aggregation"],
            business_grain=m["business_grain"], time_column=m["time_column"],
            supported_time_grains=tuple(m["supported_time_grains"]),
            additive=bool(m["additive"]), time_additivity=m["time_additivity"],
            allowed_dimensions=tuple(m["allowed_dimensions"]),
            minimum_certification=m["minimum_certification"], owner=m["owner"],
            domain=m["domain"], status=m["status"],
            measure_column=m.get("measure_column"),
            measure_expression=m.get("measure_expression"),
            unit=m.get("unit", ""), currency=m.get("currency", ""),
            format=m.get("format", ""),
            freshness_sla_hours=m.get("freshness_sla_hours"),
            default_filters=tuple(m.get("default_filters", [])))
        metrics[mid] = metric

        for a in (mid, metric.business_name, *metric.aliases):
            key = _norm(a)
            prior = alias_index.get(key)
            if prior and prior != mid:
                # Two metrics claiming one phrase is a REGISTRY defect, not a runtime
                # ambiguity to resolve later. Fail at load so it cannot ship.
                raise MetricError(
                    f"alias {a!r} is claimed by both {prior!r} and {mid!r}. One phrase "
                    "cannot mean two governed metrics.")
            alias_index[key] = mid

    return Registry(version=raw["version"], registry_id=raw["registry_id"],
                    metrics=metrics, dimensions=dims, certification_ranks=ranks,
                    alias_index=alias_index)
