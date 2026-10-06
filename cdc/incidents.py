"""Incidents, recovery plans and their execution — the contracts, not the engine.

DRP1 defines the SHAPE. DRP8 builds the planner and DRP9 executes. Writing the contract
first is deliberate: the dangerous part of lineage-driven recovery is not computing a
descendant set, it is deciding when the platform may act on one without asking. That
decision is a data structure with named conditions, so it can be read, tested and refused.

THE ONE RULE
------------
    A plan is a PROPOSAL. Execution is a separate, recorded act.

`RecoveryPlan` is frozen and identified by a hash of its own content, so "the plan we
approved" and "the plan we ran" are comparable rather than assumed equal. A plan that is
edited becomes a different plan with a different id, and an execution that references the
old id is visibly stale.

WHY A PLAN MAY NAME SOMETHING IT CANNOT RUN
--------------------------------------------
A Power BI report downstream of a broken mart is genuinely impacted and genuinely not
executable -- there is no job. Dropping it from the plan would understate the blast radius;
putting it in `executable_descendants` would make the planner try to run it. So the plan
carries both lists, and `excluded_assets` says WHY each exclusion happened. An impact
analysis that silently omits what it cannot fix is the one that gets someone a surprise on
Monday.

THE FIVE REFUSALS THAT ARE NOT NEGOTIABLE
------------------------------------------
Two of them are not theoretical here. The 2026-09-29 rebuild produced a Structured
Streaming checkpoint that outlived its MSK cluster, and the ingest reported SUCCESS with
`batches: 0, rows: 0` (docs/PLATFORM_RESOURCE_INVENTORY.md §0c). An automatic recovery
permitted to reset checkpoints would have hidden that rather than fixed it. So:
a source-system write, an offset or checkpoint reset, an unknown root, an unknown affected
interval, or one `declared` edge on the path -- any of these forces approval, whatever the
blast radius says.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from .assets import EXECUTABLE_KINDS, AssetId
from .models import ConfigError


class EvidenceClass(str, Enum):
    """How a lineage edge is known. `docs/LINEAGE_SOURCE_MATRIX.md` §1.

    DRP1 only needs the vocabulary; DRP6 populates it from the real graph.
    """

    OBSERVED = "observed"     # a runtime emitted an event for this specific run
    DERIVED = "derived"       # computed from the producer's own artefact (OPS, dbt manifest, Iceberg)
    DECLARED = "declared"     # asserted from config; no telemetry, no producer artefact
    ABSENT = "absent"         # no edge at all


#: The only classes an AUTOMATIC recovery may traverse. One `declared` edge anywhere on the
#: path downgrades the whole plan -- acting on a declared edge means rewriting data on the
#: strength of a YAML file nobody verified.
AUTO_TRAVERSABLE = (EvidenceClass.OBSERVED, EvidenceClass.DERIVED)


class IncidentStatus(str, Enum):
    OPEN = "OPEN"
    PLANNED = "PLANNED"
    RECOVERING = "RECOVERING"
    RECOVERED = "RECOVERED"
    RESOLVED = "RESOLVED"              # recovered AND re-certified
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"
    EXHAUSTED = "EXHAUSTED"            # attempts spent; no further automatic action
    CLOSED_NO_ACTION = "CLOSED_NO_ACTION"


#: Statuses from which no further AUTOMATIC action may be taken. `EXHAUSTED` is what stops
#: the DQ -> rerun -> DQ loop the brief forbids: a reliability platform that can loop is an
#: outage generator with good intentions.
TERMINAL_STATUSES = (IncidentStatus.RESOLVED, IncidentStatus.EXHAUSTED,
                     IncidentStatus.CLOSED_NO_ACTION)


class FailureClass(str, Enum):
    """WHAT went wrong, which decides whether a rerun could even help.

    `SOURCE_DEFECT` and `INFRA` are separated from the rest because rerunning a pipeline
    over bad source data reproduces the same bad answer, more expensively.
    """

    DQ_VIOLATION = "dq_violation"
    RECONCILIATION_BREAK = "reconciliation_break"
    FRESHNESS_BREACH = "freshness_breach"
    SCHEMA_CHANGE = "schema_change"
    LATE_ARRIVING_DATA = "late_arriving_data"
    JOB_FAILURE = "job_failure"
    SOURCE_DEFECT = "source_defect"
    INFRA = "infra"
    UNKNOWN = "unknown"


#: Failure classes a rerun CAN fix. Anything else produces a plan that requires approval,
#: because "run it again" is not a remedy for data that was wrong at the source.
REPAIRABLE_BY_RERUN = (FailureClass.LATE_ARRIVING_DATA, FailureClass.JOB_FAILURE,
                       FailureClass.DQ_VIOLATION, FailureClass.RECONCILIATION_BREAK,
                       FailureClass.FRESHNESS_BREACH)


class RootCauseStatus(str, Enum):
    UNKNOWN = "unknown"
    SUSPECTED = "suspected"
    CONFIRMED = "confirmed"


class CostClass(str, Enum):
    """Coarse on purpose. A recovery planner that produced dollar estimates would be
    inventing precision it does not have; what an approver actually needs is whether this
    is a partition rewrite or a full history rebuild."""

    SMALL = "small"       # one partition / one COB on a handful of assets
    MEDIUM = "medium"     # several COBs, or a full table rebuild
    LARGE = "large"       # multi-table rebuild, or a history replay
    UNBOUNDED = "unbounded"


COST_ORDER = {CostClass.SMALL: 1, CostClass.MEDIUM: 2, CostClass.LARGE: 3,
              CostClass.UNBOUNDED: 4}


class ApprovalRequirement(str, Enum):
    AUTOMATIC = "AUTOMATIC"
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"


@dataclass(frozen=True)
class RecoveryPolicy:
    """The guard rails. Both `max_attempts` and `max_blast_radius` are REQUIRED -- a policy
    missing either is refused at construction rather than defaulted, because a silently
    defaulted limit is the one nobody reviews."""

    max_attempts: int
    max_blast_radius: int
    max_cost_class: CostClass = CostClass.MEDIUM
    allow_source_writes: bool = False
    allow_offset_reset: bool = False
    allowed_evidence: tuple[EvidenceClass, ...] = AUTO_TRAVERSABLE
    allowed_failure_classes: tuple[FailureClass, ...] = REPAIRABLE_BY_RERUN

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ConfigError(
                "RecoveryPolicy.max_attempts must be a positive integer. Without a cap, a "
                "DQ failure that triggers a rerun that fails DQ again never terminates.")
        if not isinstance(self.max_blast_radius, int) or self.max_blast_radius < 1:
            raise ConfigError(
                "RecoveryPolicy.max_blast_radius must be a positive integer. An unbounded "
                "radius means one bad check can rewrite the whole warehouse automatically.")
        if self.allow_source_writes:
            raise ConfigError(
                "RecoveryPolicy.allow_source_writes cannot be true. Recovery repairs the "
                "lakehouse; writing back to Oracle or SQL Server is not a pipeline action "
                "and no automatic decision may take it.")

    def payload(self) -> dict:
        return {"max_attempts": self.max_attempts,
                "max_blast_radius": self.max_blast_radius,
                "max_cost_class": self.max_cost_class.value,
                "allow_source_writes": self.allow_source_writes,
                "allow_offset_reset": self.allow_offset_reset,
                "allowed_evidence": sorted(e.value for e in self.allowed_evidence),
                "allowed_failure_classes": sorted(f.value for f in self.allowed_failure_classes)}


@dataclass(frozen=True)
class AffectedScope:
    """WHAT has to be repaired. An empty scope is refused: "rebuild everything" is not a
    scope, it is the absence of one, and it is how a small defect becomes a large outage."""

    cob_dates: tuple[date, ...] = ()
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    business_keys: tuple[str, ...] = ()

    @property
    def bounded(self) -> bool:
        return bool(self.cob_dates or (self.interval_start and self.interval_end))

    def payload(self) -> dict:
        return {"cob_dates": [d.isoformat() for d in sorted(self.cob_dates)],
                "interval_start": self.interval_start.isoformat() if self.interval_start else None,
                "interval_end": self.interval_end.isoformat() if self.interval_end else None,
                "business_keys": sorted(self.business_keys),
                "bounded": self.bounded}


@dataclass(frozen=True)
class ExcludedAsset:
    asset: str
    reason: str

    def payload(self) -> dict:
        return {"asset": self.asset, "reason": self.reason}


@dataclass(frozen=True)
class Incident:
    incident_id: str
    dataset_id: str
    failure_class: FailureClass
    severity: str                       # ERROR | WARN, matching the DQ severity vocabulary
    status: IncidentStatus = IncidentStatus.OPEN
    scope: AffectedScope = field(default_factory=AffectedScope)
    source_run_id: str = ""
    dq_check_id: str = ""
    recon_run_id: str = ""
    root_cause_status: RootCauseStatus = RootCauseStatus.UNKNOWN
    blast_radius_count: int = 0
    automatic_recovery_allowed: bool = False
    recovery_plan_id: str = ""
    attempts: int = 0
    opened_at: datetime | None = None
    updated_at: datetime | None = None
    resolved_at: datetime | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        for name in ("incident_id", "dataset_id"):
            if not str(getattr(self, name) or "").strip():
                raise ConfigError(f"Incident.{name} is required")
        AssetId.parse(self.dataset_id)
        if self.status is IncidentStatus.RESOLVED and not self.resolved_at:
            raise ConfigError(
                f"Incident {self.incident_id}: RESOLVED without resolved_at. An incident "
                f"with no resolution time cannot be aged, and unaged incidents are how a "
                f"backlog stops being visible.")

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def payload(self) -> dict:
        return {"incident_id": self.incident_id, "dataset_id": self.dataset_id,
                "failure_class": self.failure_class.value, "severity": self.severity,
                "status": self.status.value, "source_run_id": self.source_run_id,
                "dq_check_id": self.dq_check_id, "recon_run_id": self.recon_run_id,
                "root_cause_status": self.root_cause_status.value,
                "blast_radius_count": self.blast_radius_count,
                "automatic_recovery_allowed": self.automatic_recovery_allowed,
                "recovery_plan_id": self.recovery_plan_id, "attempts": self.attempts,
                "opened_at": self.opened_at.isoformat() if self.opened_at else None,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
                "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
                "detail": self.detail, "scope": self.scope.payload()}


@dataclass(frozen=True)
class RecoveryPlan:
    """Immutable. `plan_id` is a hash of the content, so an edited plan is a DIFFERENT plan
    and an execution pointing at the old id is visibly stale rather than quietly wrong."""

    incident_id: str
    root_asset: str
    scope: AffectedScope
    #: The lineage graph this plan was computed from. A plan is only as good as the graph
    #: it read, and graphs change -- recording which version was used is what makes a plan
    #: reproducible after the fact.
    lineage_version: str
    lineage_as_of: datetime | None = None
    candidate_descendants: tuple[str, ...] = ()
    executable_descendants: tuple[str, ...] = ()
    excluded_assets: tuple[ExcludedAsset, ...] = ()
    #: Topological waves. Everything in one turn may run in parallel; turn N+1 waits for N.
    turns: tuple[tuple[str, ...], ...] = ()
    weakest_evidence: EvidenceClass = EvidenceClass.ABSENT
    cost_class: CostClass = CostClass.UNBOUNDED
    approval: ApprovalRequirement = ApprovalRequirement.REQUIRES_APPROVAL
    approval_reasons: tuple[str, ...] = ()
    config_hash: str = ""
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.incident_id.strip():
            raise ConfigError("RecoveryPlan.incident_id is required")
        AssetId.parse(self.root_asset)
        for raw in (*self.candidate_descendants, *self.executable_descendants):
            AssetId.parse(raw)
        extra = set(self.executable_descendants) - set(self.candidate_descendants)
        if extra:
            raise ConfigError(
                f"RecoveryPlan: {', '.join(sorted(extra))} listed as executable but not as a "
                f"candidate. The executable set is a SUBSET of the impacted set; anything "
                f"else means the planner invented a target.")
        for raw in self.executable_descendants:
            if AssetId.parse(raw).kind not in EXECUTABLE_KINDS:
                raise ConfigError(
                    f"RecoveryPlan: {raw} is not an executable kind. A BI report may be "
                    f"IMPACTED, but there is no job to rerun -- it belongs in "
                    f"excluded_assets with a reason.")
        flat = [a for turn in self.turns for a in turn]
        # Duplicates are checked FIRST. A repeated asset also fails the coverage check, and
        # reporting it as "the turns do not cover the executable set" sends the reader
        # looking for a missing asset rather than the repeated one.
        if len(set(flat)) != len(flat):
            dupes = sorted({a for a in flat if flat.count(a) > 1})
            raise ConfigError(
                f"RecoveryPlan: {', '.join(dupes)} appears in more than one turn. Running an "
                f"asset twice in one recovery is at best wasted work and at worst a second "
                f"rebuild over the output of the first.")
        if sorted(flat) != sorted(self.executable_descendants):
            raise ConfigError(
                "RecoveryPlan: the topological turns do not cover exactly the executable "
                "set. A turn list that drifts from the target list silently skips or "
                "repeats work.")

    @property
    def blast_radius(self) -> int:
        return len(self.candidate_descendants)

    @property
    def plan_id(self) -> str:
        body = json.dumps(self._body(), sort_keys=True, separators=(",", ":"))
        return "rp1:" + hashlib.sha256(body.encode()).hexdigest()[:16]

    def _body(self) -> dict:
        return {"incident_id": self.incident_id, "root_asset": self.root_asset,
                "scope": self.scope.payload(), "lineage_version": self.lineage_version,
                "candidates": sorted(self.candidate_descendants),
                "executable": sorted(self.executable_descendants),
                "excluded": [e.payload() for e in sorted(self.excluded_assets,
                                                         key=lambda x: x.asset)],
                "turns": [sorted(t) for t in self.turns],
                "weakest_evidence": self.weakest_evidence.value,
                "cost_class": self.cost_class.value,
                "config_hash": self.config_hash}

    def payload(self) -> dict:
        out = dict(self._body())
        out.update({"plan_id": self.plan_id, "blast_radius": self.blast_radius,
                    "approval": self.approval.value,
                    "approval_reasons": list(self.approval_reasons),
                    "lineage_as_of": self.lineage_as_of.isoformat() if self.lineage_as_of else None,
                    "created_at": self.created_at.isoformat() if self.created_at else None})
        return out


@dataclass(frozen=True)
class RecoveryExecution:
    """One ATTEMPT at one plan. Append-only; a retry is a new record, never an edit.

    `plan_id` is carried so an execution can be checked against the plan that was approved.
    """

    execution_id: str
    plan_id: str
    incident_id: str
    attempt: int
    status: str                      # PENDING | RUNNING | SUCCEEDED | FAILED | ABORTED
    approved_by: str = ""
    turn_results: tuple = ()         # ((turn_index, asset, status, run_id), ...)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        if self.attempt < 1:
            raise ConfigError("RecoveryExecution.attempt starts at 1")
        for name in ("execution_id", "plan_id", "incident_id"):
            if not str(getattr(self, name) or "").strip():
                raise ConfigError(f"RecoveryExecution.{name} is required")

    def payload(self) -> dict:
        return {"execution_id": self.execution_id, "plan_id": self.plan_id,
                "incident_id": self.incident_id, "attempt": self.attempt,
                "status": self.status, "approved_by": self.approved_by,
                "turn_results": [list(t) for t in self.turn_results],
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "detail": self.detail}


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #

def decide_approval(plan: RecoveryPlan, incident: Incident,
                    policy: RecoveryPolicy) -> tuple[ApprovalRequirement, tuple[str, ...]]:
    """The ten conditions from `docs/DATA_RELIABILITY_PLATFORM_TARGET.md` §6.

    Returns EVERY reason, not the first. An approver who fixes one blocker and resubmits
    only to hit the next is an approver who stops reading the reasons.
    """
    reasons: list[str] = []

    if incident.root_cause_status is RootCauseStatus.UNKNOWN:
        reasons.append("root cause is unknown; a rerun would be a guess")
    if not plan.scope.bounded:
        reasons.append("affected interval is not bounded; 'rebuild everything' is not a scope")
    if incident.failure_class not in policy.allowed_failure_classes:
        reasons.append(
            f"failure class {incident.failure_class.value} is not repairable by rerunning "
            f"(allowed: {', '.join(sorted(f.value for f in policy.allowed_failure_classes))})")
    if plan.weakest_evidence not in policy.allowed_evidence:
        reasons.append(
            f"the weakest lineage edge on this path is {plan.weakest_evidence.value}; "
            f"automatic recovery may only traverse "
            f"{', '.join(sorted(e.value for e in policy.allowed_evidence))}")
    if not plan.executable_descendants:
        reasons.append("no affected asset resolves to an executable job")
    if plan.blast_radius > policy.max_blast_radius:
        reasons.append(f"blast radius {plan.blast_radius} exceeds the limit "
                       f"{policy.max_blast_radius}")
    if COST_ORDER[plan.cost_class] > COST_ORDER[policy.max_cost_class]:
        reasons.append(f"cost class {plan.cost_class.value} exceeds the limit "
                       f"{policy.max_cost_class.value}")
    if incident.attempts >= policy.max_attempts:
        reasons.append(f"attempt limit reached ({incident.attempts}/{policy.max_attempts})")
    if incident.terminal:
        reasons.append(f"incident is in terminal status {incident.status.value}")
    if plan.incident_id != incident.incident_id:
        reasons.append(f"plan targets incident {plan.incident_id}, not {incident.incident_id}")

    if reasons:
        return ApprovalRequirement.REQUIRES_APPROVAL, tuple(reasons)
    return ApprovalRequirement.AUTOMATIC, ()
