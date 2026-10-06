"""Reliability metrics, SLOs and alert rules — declared, with a target that is honest.

DRP10. Three rules shape this module.

**1. A metadata SLO is not a data SLO.** "The catalogue is 30 minutes stale" and "the mart is
30 minutes stale" are different promises with different consequences, and merging them means
a lineage backlog pages the same person as a late close.

**2. An unmeasured target is not a target.** Every SLO here carries a `basis`:
`measured` (from a real run), `inherited` (from an existing, enforced config value) or
`proposed` (nobody has measured it). A `proposed` target is a hypothesis, and the module
refuses to let one masquerade as measured.

**3. Alerts are deduplicated, not merely rate-limited.** An alert storm is not "too many
alerts"; it is *one* condition arriving as fifty. Every rule declares a `group_by`, so ten
tables failing the same check on the same COB is one page.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .models import ConfigError


class Basis(str, Enum):
    MEASURED = "measured"
    INHERITED = "inherited"
    PROPOSED = "proposed"


class Plane(str, Enum):
    DATA = "data"
    METADATA = "metadata"


@dataclass(frozen=True)
class Metric:
    name: str
    plane: Plane
    unit: str
    description: str
    source: str

    def payload(self) -> dict:
        return {"name": self.name, "plane": self.plane.value, "unit": self.unit,
                "description": self.description, "source": self.source}


@dataclass(frozen=True)
class Slo:
    metric: str
    plane: Plane
    target: str
    basis: Basis
    evidence: str

    def __post_init__(self) -> None:
        if self.basis is not Basis.PROPOSED and not self.evidence:
            raise ConfigError(
                f"{self.metric}: a {self.basis.value} target must cite its evidence. A number "
                f"with no source is a number nobody can defend when it is missed.")

    @property
    def enforceable(self) -> bool:
        """A `proposed` target may be reported against; it may not page anyone."""
        return self.basis is not Basis.PROPOSED

    def payload(self) -> dict:
        return {"metric": self.metric, "plane": self.plane.value, "target": self.target,
                "basis": self.basis.value, "evidence": self.evidence,
                "enforceable": self.enforceable}


@dataclass(frozen=True)
class Alert:
    name: str
    condition: str
    severity: str                # PAGE | TICKET | LOG
    group_by: tuple              # what collapses fifty events into one page
    runbook: str

    def __post_init__(self) -> None:
        if not self.group_by:
            raise ConfigError(
                f"{self.name}: no group_by. An alert storm is one condition arriving as "
                f"fifty; without a grouping key, ten tables failing one check on one COB is "
                f"ten pages and the eleventh gets muted.")
        if self.severity == "PAGE" and not self.runbook:
            raise ConfigError(f"{self.name}: a PAGE with no runbook wakes someone with no "
                              f"instructions")

    def payload(self) -> dict:
        return {"name": self.name, "condition": self.condition, "severity": self.severity,
                "group_by": list(self.group_by), "runbook": self.runbook}


METRICS = (
    # -- metadata plane ----------------------------------------------------
    Metric("openlineage_emit_total", Plane.METADATA, "count",
           "OpenLineage events emitted, by job and result", "DataHubClient.metrics"),
    Metric("openlineage_emit_failed_total", Plane.METADATA, "count",
           "emissions that exhausted their bounded retry", "DataHubClient.metrics"),
    Metric("metadata_publish_degraded_total", Plane.METADATA, "count",
           "BEST_EFFORT publishes that did not land — the job still succeeded",
           "DataHubClient.metrics"),
    Metric("metadata_ingestion_age_seconds", Plane.METADATA, "seconds",
           "time since the last successful catalogue ingestion", "metadata_ingestion DAG"),
    Metric("datahub_available", Plane.METADATA, "bool",
           "GMS /health; None when NOT CHECKED, never False", "DataHubClient.health"),
    Metric("orphan_critical_assets", Plane.METADATA, "count",
           "critical assets with no upstream in the graph", "lineage_graph.audit"),
    Metric("lineage_audit_findings", Plane.METADATA, "count",
           "all six lineage-quality rules", "lineage_graph.audit"),
    Metric("impact_plan_size", Plane.METADATA, "count",
           "blast radius of the most recent plan", "LineageImpactService.plan"),
    Metric("impact_plan_latency_seconds", Plane.METADATA, "seconds",
           "time to produce a recovery plan", "LineageImpactService.plan"),
    # -- data plane --------------------------------------------------------
    Metric("dq_failures_by_layer", Plane.DATA, "count",
           "blocking DQ results, by layer and severity", "ops.dq_result_v2"),
    Metric("freshness_violations", Plane.DATA, "count",
           "assets past their freshness SLA", "ops.dq_result_v2"),
    Metric("reconciliation_failures", Plane.DATA, "count",
           "recon results outside tolerance", "ops.reconciliation_run"),
    Metric("open_incidents", Plane.DATA, "count",
           "incidents not in a terminal status", "ops.data_incident"),
    Metric("auto_recovery_attempts", Plane.DATA, "count",
           "attempts per incident, against the policy cap", "ops.recovery_execution"),
    Metric("auto_recovery_success_rate", Plane.DATA, "ratio",
           "recoveries that closed their incident", "ops.recovery_execution"),
    Metric("mttd_seconds", Plane.DATA, "seconds",
           "commit -> first blocking verdict", "ops.dq_result_v2 + ops.eod_run"),
    Metric("mttr_seconds", Plane.DATA, "seconds",
           "incident opened -> re-certified", "ops.data_incident"),
    Metric("certification_age_seconds", Plane.DATA, "seconds",
           "time since the newest certified COB per mart", "ops.eod_run"),
)

SLOS = (
    Slo("certification_age_seconds", Plane.DATA, "a COB certifies by 06:00 UTC on D+1",
        Basis.MEASURED,
        "2026-09-30 close: closed=10 certified=5 rows=3404, with the readiness gate enforced"),
    Slo("freshness_violations", Plane.DATA,
        "per-table, the SLA in cdc/registry/sources.yaml (60m default, 15m for transaction "
        "and digital_event)", Basis.INHERITED, "registry dq.freshness_sla_minutes"),
    Slo("metadata_publish_degraded_total", Plane.METADATA,
        "a degraded publish never fails a data flow; worst case 31.5 s per call",
        Basis.MEASURED, "ClientSettings.worst_case_seconds, asserted by a test"),
    Slo("metadata_ingestion_age_seconds", Plane.METADATA, "catalogue no more than 24 h stale",
        Basis.PROPOSED, ""),
    Slo("impact_plan_latency_seconds", Plane.METADATA, "a plan in under 60 s",
        Basis.PROPOSED, ""),
    Slo("mttd_seconds", Plane.DATA, "a blocking defect detected within one close cycle",
        Basis.PROPOSED, ""),
    Slo("mttr_seconds", Plane.DATA, "a bounded recovery re-certifies within one COB",
        Basis.PROPOSED, ""),
)

ALERTS = (
    Alert("blocker_dq", "any BLOCKER-severity DQ result", "PAGE",
          ("cob_date", "layer", "check_id"),
          "docs/LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md §2"),
    Alert("reconciliation_failure", "any blocking reconciliation break", "PAGE",
          ("cob_date", "source_dataset"),
          "docs/LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md §1b"),
    Alert("late_certification", "certification_age_seconds past the COB SLO", "PAGE",
          ("cob_date",), "docs/EOD_SNAPSHOT_RUNBOOK.md"),
    Alert("critical_lineage_stale", "a tier_1 asset has no lineage edge for N days", "TICKET",
          ("asset",), "docs/LINEAGE_TROUBLESHOOTING.md §1"),
    Alert("recovery_failed", "a recovery execution ended FAILED or hit the attempt cap",
          "PAGE", ("incident_id",), "docs/LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md §6"),
    Alert("metadata_plane_outage",
          "datahub_available false for longer than the configured threshold", "TICKET",
          ("environment",), "docs/DATAHUB_OPERATIONS_RUNBOOK.md §9"),
)


def enforceable_slos() -> tuple:
    return tuple(s for s in SLOS if s.enforceable)


def alertable(alert: Alert, slos=SLOS) -> bool:
    """An alert may only PAGE on a target somebody has measured or inherited.

    Paging on a `proposed` number wakes people for a guess, and the second time it happens
    the alert is muted -- which is how a real one gets missed later.
    """
    if alert.severity != "PAGE":
        return True
    related = [s for s in slos if s.metric in alert.condition or s.metric == alert.name]
    return any(s.enforceable for s in related) if related else True


def payload() -> dict:
    return {"metrics": [m.payload() for m in METRICS],
            "slos": [s.payload() for s in SLOS],
            "alerts": [a.payload() for a in ALERTS],
            "enforceable_slos": len(enforceable_slos()),
            "proposed_slos": len(SLOS) - len(enforceable_slos())}
