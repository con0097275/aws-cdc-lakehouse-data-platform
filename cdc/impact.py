"""Lineage-driven impact analysis and the recovery PLANNER. Plans only; never executes.

DRP8. The division of labour is the design:

    DataHub lineage       WHAT is affected
    the job registry      HOW, and IN WHAT ORDER, the affected things can be rebuilt

Neither alone is enough. Lineage knows a Power BI report is downstream of a broken mart; it
does not know there is no job to run for it. The job graph knows the order of the reporting
jobs; it does not know a source table changed three layers below them.

So the planner intersects the two, keeps BOTH lists, and says why each exclusion happened.

WHY THE INTERSECTION IS THE SAFETY BOUNDARY
--------------------------------------------
An asset name from a catalogue never becomes a command. Every node in
`executable_descendants` has been resolved to a job the platform already registers
(`LineageGraph.Edge.job`, which is one of the nine names in
`governance/registry/openlineage.yaml`). Anything that does not resolve is an operator
decision. Without that rule, a tag edited in a web UI is a way to make the platform rewrite
data.

WHY A SOURCE DEFECT PRODUCES NO PLAN
-------------------------------------
If the value in Oracle is wrong, every rerun reproduces the same wrong answer, more
expensively. The incident goes to `WAITING_SOURCE_CORRECTION` and nothing is written back to
the source system -- `RecoveryPolicy` refuses `allow_source_writes` unconditionally. Recovery
is planned again once the corrected CDC arrives.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .incidents import (AffectedScope, ApprovalRequirement, CostClass, EvidenceClass,
                        ExcludedAsset, FailureClass, Incident, IncidentStatus,
                        RecoveryPlan, RecoveryPolicy, decide_approval)
from .lineage_graph import LineageGraph, classify
from .models import ConfigError

#: Node classes. `UNKNOWN` is a real answer and is never treated as safe to run.
EXECUTABLE_JOB = "EXECUTABLE_JOB"
DATASET = "DATASET"
SERVING_ASSET = "SERVING_ASSET"
BI_ASSET = "BI_ASSET"
UNKNOWN = "UNKNOWN"

#: Failure classes where a rerun cannot help because the input itself is wrong.
SOURCE_SIDE = (FailureClass.SOURCE_DEFECT,)

#: Blast-radius bands -> cost class. Coarse on purpose: a planner producing dollar figures
#: would be inventing precision it does not have.
def _cost_class(node_count: int, cob_count: int) -> CostClass:
    span = max(1, cob_count)
    if node_count <= 5 and span <= 1:
        return CostClass.SMALL
    if node_count <= 20 and span <= 7:
        return CostClass.MEDIUM
    if node_count <= 60:
        return CostClass.LARGE
    return CostClass.UNBOUNDED


@dataclass(frozen=True)
class ImpactNode:
    urn: str
    hops: int
    node_class: str
    job: str = ""

    @property
    def executable(self) -> bool:
        return self.node_class == EXECUTABLE_JOB and bool(self.job)

    def payload(self) -> dict:
        return {"urn": self.urn, "hops": self.hops, "class": self.node_class,
                "job": self.job, "executable": self.executable}


@dataclass(frozen=True)
class ImpactResult:
    root: str
    nodes: tuple
    max_hops: int
    environment: str
    column: str = ""

    def by_class(self) -> dict:
        out: dict = {}
        for n in self.nodes:
            out.setdefault(n.node_class, []).append(n.urn)
        return {k: sorted(v) for k, v in sorted(out.items())}

    @property
    def executable(self) -> tuple:
        return tuple(n for n in self.nodes if n.executable)

    @property
    def excluded(self) -> tuple:
        return tuple(n for n in self.nodes if not n.executable)

    def payload(self) -> dict:
        return {"root": self.root, "environment": self.environment, "column": self.column,
                "max_hops": self.max_hops, "by_class": self.by_class(),
                "nodes": [n.payload() for n in self.nodes]}


class LineageImpactService:
    """Reads a `LineageGraph`. Pure, and therefore testable with no metadata plane running --
    which matters because the plane is `disabled` by default."""

    def __init__(self, graph: LineageGraph, *, environment: str, minter=None,
                 registered_jobs: tuple = ()):
        self.graph = graph
        self.environment = environment
        #: Used to translate URNs back into `AssetId`s for the incident/recovery contracts.
        self.minter = minter
        #: The nine deterministic lineage job names. A node whose producing job is not one of
        #: them cannot be rerun by this platform, whatever the graph says.
        self.registered_jobs = tuple(registered_jobs) or self._jobs_from(graph)

    @staticmethod
    def _jobs_from(graph: LineageGraph) -> tuple:
        return tuple(sorted({e.job for e in graph.edges if e.job}))

    def producing_job(self, urn: str) -> str:
        """The job that WRITES this node, from the edges that end at it.

        Ambiguity is refused rather than resolved: two jobs writing one table means a rerun
        would pick one of them, and picking silently is how a recovery rebuilds a table with
        the wrong producer.
        """
        jobs = {e.job for e in self.graph.edges if e.downstream == urn and e.job}
        jobs.discard("debezium")           # not a platform job; it is a connector
        if len(jobs) > 1:
            raise ConfigError(
                f"{urn} is written by more than one job ({', '.join(sorted(jobs))}). A rerun "
                f"would have to choose, and choosing silently rebuilds a table with the "
                f"wrong producer.")
        return next(iter(jobs), "")

    def downstream(self, root: str, *, max_hops: int = 6, column: str = "") -> ImpactResult:
        if root not in self.graph.nodes:
            # Not an error: a root outside the graph means the catalogue does not know this
            # asset, which is itself the finding. Returning an empty result with the root
            # recorded lets the caller say so instead of crashing.
            return ImpactResult(root, (), max_hops, self.environment, column)

        nodes = []
        for urn, hops in self.graph.descendants(root, max_hops=max_hops):
            if column and not self._column_reaches(root, column, urn):
                continue
            cls = classify(urn)
            job = self.producing_job(urn) if cls == EXECUTABLE_JOB else ""
            if cls == EXECUTABLE_JOB and job not in self.registered_jobs:
                # Classified runnable, but no registered job produces it. That is not a
                # reason to improvise a command; it is a reason to ask a person.
                cls, job = UNKNOWN, ""
            nodes.append(ImpactNode(urn, hops, cls, job))
        return ImpactResult(root, tuple(nodes), max_hops, self.environment, column)

    def _column_reaches(self, root: str, column: str, target: str) -> bool:
        """Column-scoped impact, best effort and honest about it.

        Falls back to dataset scope when no column edge is known, because narrowing on an
        unknown mapping would EXCLUDE affected assets -- the dangerous direction.
        """
        known = [c for c in self.graph._columns if c.upstream_dataset == root
                 and c.upstream_column == column]
        if not known:
            return True
        reached = {c.downstream_dataset for c in known}
        frontier = set(reached)
        for _ in range(6):
            nxt = {c.downstream_dataset for c in self.graph._columns
                   if c.upstream_dataset in frontier}
            if nxt <= reached:
                break
            reached |= nxt
            frontier = nxt
        return target in reached or target not in {c.downstream_dataset
                                                   for c in self.graph._columns}

    # -- planning ---------------------------------------------------------
    def turns(self, urns) -> tuple:
        """Topological waves over the subgraph induced by `urns`.

        Everything in a turn may run in parallel; turn N+1 waits for N. A cycle raises --
        a topological rerun over a cycle is not a hard problem, it is an impossible one.
        """
        remaining = set(urns)
        waves: list = []
        while remaining:
            ready = sorted(n for n in remaining
                           if not (set(self.graph.upstreams(n)) & remaining))
            if not ready:
                raise ConfigError(
                    f"cannot order {len(remaining)} asset(s): every one depends on another "
                    f"in the set, which means a cycle. Fix the graph before recovering.")
            waves.append(tuple(ready))
            remaining -= set(ready)
        return tuple(waves)

    def plan(self, incident: Incident, *, root_urn: str, policy: RecoveryPolicy,
             lineage_version: str, scope: AffectedScope | None = None,
             max_hops: int = 6, column: str = "", config_hash: str = "",
             created_at: datetime | None = None) -> tuple:
        """Returns `(plan, approval, reasons)`. **Executes nothing.**

        A source-side defect returns `(None, REQUIRES_APPROVAL, ...)`: there is no plan to
        make, because every rerun reproduces the same wrong answer from the same wrong input.
        """
        scope = scope or incident.scope
        if incident.failure_class in SOURCE_SIDE:
            return (None, ApprovalRequirement.REQUIRES_APPROVAL,
                    (f"{incident.failure_class.value}: the source value itself is wrong. "
                     f"WAITING_SOURCE_CORRECTION — a rerun reproduces the same answer, and "
                     f"nothing is ever written back to Oracle or SQL Server.",))

        impact = self.downstream(root_urn, max_hops=max_hops, column=column)
        executable = [n.urn for n in impact.executable]
        excluded = tuple(
            ExcludedAsset(n.urn, _exclusion_reason(n)) for n in impact.excluded)

        weakest = EvidenceClass.OBSERVED
        for n in impact.nodes:
            path = self.graph.path(root_urn, n.urn, max_hops=max_hops + 1)
            if path:
                w = self.graph.weakest_evidence(path)
                if _rank(w) < _rank(weakest):
                    weakest = w
        if not impact.nodes:
            weakest = EvidenceClass.ABSENT

        # The graph speaks URNs; the recovery contracts speak `AssetId`. Translating at the
        # boundary keeps a URN from ending up where a kind:name belongs and failing deep
        # inside a validator instead of here.
        def aid(urn: str) -> str:
            return str(self.minter.asset_for(urn)) if self.minter else urn

        plan = RecoveryPlan(
            incident_id=incident.incident_id, root_asset=incident.dataset_id, scope=scope,
            lineage_version=lineage_version,
            candidate_descendants=tuple(sorted(aid(n.urn) for n in impact.nodes)),
            executable_descendants=tuple(sorted(aid(x) for x in executable)),
            excluded_assets=tuple(ExcludedAsset(aid(e.asset), e.reason) for e in excluded),
            turns=tuple(tuple(sorted(aid(x) for x in wave))
                        for wave in self.turns(executable)),
            weakest_evidence=weakest,
            cost_class=_cost_class(len(impact.nodes), len(scope.cob_dates) or 1),
            config_hash=config_hash, created_at=created_at)

        approval, reasons = decide_approval(plan, incident, policy)
        return plan, approval, reasons


def _rank(e: EvidenceClass) -> int:
    return {EvidenceClass.OBSERVED: 3, EvidenceClass.DERIVED: 2,
            EvidenceClass.DECLARED: 1, EvidenceClass.ABSENT: 0}[e]


def _exclusion_reason(node: ImpactNode) -> str:
    if node.node_class == BI_ASSET:
        return ("BI asset: impacted and not executable. There is no job to rerun, and "
                "refreshing a report is an outward-facing action")
    if node.node_class == SERVING_ASSET:
        return "serving asset: rebuilt by its upstream mart, not on its own"
    if node.node_class == DATASET:
        return "dataset with no platform job: upstream of the lake, or a Kafka topic"
    if node.node_class == UNKNOWN:
        return ("no registered executable job produces it. Not a reason to improvise a "
                "command -- a reason to ask a person")
    return "not executable"


def waiting_on_source(incident: Incident) -> Incident:
    """The one incident transition that is not a recovery."""
    from dataclasses import replace
    return replace(incident, status=IncidentStatus.REQUIRES_APPROVAL,
                   automatic_recovery_allowed=False,
                   detail=(incident.detail + " | WAITING_SOURCE_CORRECTION: the source value "
                           "is wrong; replan once corrected CDC arrives.").strip(" |"))
