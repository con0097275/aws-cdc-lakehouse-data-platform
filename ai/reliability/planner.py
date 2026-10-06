"""AIGR4 — the deterministic planner.

The LLM asks for a plan. This builds it. The distinction matters because every value in the
plan that could authorize a write -- job ids, turn order, scope, action type -- is computed
here from the lineage graph and the capability registry, not produced by a model.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from cdc.incidents import CostClass
from .control import ActionType, AiRecoveryPlan, PlannedAction
from .model import (CapabilityKind, LineageConfidence, RecoveryCapability, RecoveryScope,
                    RootCause, ScopeError, ScopeSource)

#: Which action rebuilds which layer. A layer the platform cannot rebuild has no entry, so
#: the planner refuses rather than inventing a plausible action name.
LAYER_ACTION: dict[str, ActionType] = {
    "realtime": ActionType.REALTIME_REBUILD,
    "eod": ActionType.EOD_REBUILD,
    "curated": ActionType.MART_RERUN,
    "mart": ActionType.MART_RERUN,
    "full_cdc": ActionType.STREAM_BATCH_REPLAY,
}


@dataclass(frozen=True)
class JobNode:
    """A registered, executable job. `layer` decides the action; `asset_id` ties it to lineage."""

    job_id: str
    asset_id: str
    layer: str
    capability: RecoveryCapability


class PlannerError(ScopeError):
    pass


class AiRecoveryPlanner:
    def __init__(self, *, jobs: dict[str, JobNode], turns: list[list[str]]):
        """`turns` is the topological order the platform computed (LineageImpactService),
        as a list of same-turn asset-id groups. The planner does not compute order itself:
        two places computing order is two places for it to drift."""
        self._jobs = jobs
        self._turns = turns

    #: Actions that VERIFY rather than rebuild. Emitted as their own turns so the
    #: quality barrier is part of the plan an approver reads, not an assumption about what
    #: happens afterwards:
    #:
    #:     root repair -> root DQ -> root reconciliation -> root certification
    #:                 -> descendants -> descendant DQ + reconciliation
    #:
    #: A plan that rebuilds and stops is a plan that cannot tell you whether it worked.
    VERIFY_ROOT = (ActionType.DQ_RECHECK, ActionType.RECONCILE, ActionType.CERTIFY)
    VERIFY_CHILD = (ActionType.DQ_RECHECK, ActionType.RECONCILE)

    def plan(self, *, incident_id: str, root_asset_id: str, root_cause: RootCause,
             scope: RecoveryScope, impacted: set[str], excluded: list[tuple[str, str]],
             environment: str = "dev", quality_gates: bool = True) -> AiRecoveryPlan:
        if not root_cause.may_recover:
            raise PlannerError(
                f"{root_cause.category.value} disposition is "
                f"{root_cause.disposition.value}; refusing to build a plan. "
                "A plan that exists invites submission.")

        actions: list[PlannedAction] = []
        for turn_index, group in enumerate(self._turns):
            for asset in sorted(group):
                if asset not in impacted:
                    continue
                node = self._job_for(asset)
                if node is None:
                    # Impacted but not executable: recorded as excluded, never silently
                    # dropped -- a descendant nobody can rebuild is a fact the approver
                    # needs, not a gap in the plan.
                    excluded.append((asset, "impacted but no registered executable job"))
                    continue
                kind = node.capability.smallest_supported(scope.desired_kinds())
                actions.append(PlannedAction(
                    job_id=node.job_id, action=self._action_for(node),
                    scope=self._narrow(scope, kind), turn=turn_index,
                    granularity=kind.value, asset_id=asset))

        # Anything impacted that never appeared in a turn would otherwise vanish without
        # trace. That is the precise shape of the bug this design exists to prevent: a
        # descendant silently omitted is a descendant that never gets rebuilt, and the plan
        # still looks complete. Account for every impacted asset or say why not.
        seen = {a for group in self._turns for a in group}
        for asset in sorted(impacted - seen):
            excluded.append((asset, "impacted but absent from the topological order; "
                                    "no execution path is known for it"))

        if not actions:
            raise PlannerError("no impacted asset maps to a registered executable job")
        actions = self._compact_turns(actions)
        if quality_gates:
            actions = self._with_quality_gates(actions, root_asset_id)
        return AiRecoveryPlan(incident_id=incident_id, root_asset_id=root_asset_id,
                              root_cause=root_cause, scope=scope, actions=tuple(actions),
                              excluded=tuple(excluded), environment=environment)

    # -- helpers ----------------------------------------------------------- #
    def _job_for(self, asset_id: str) -> JobNode | None:
        for n in self._jobs.values():
            if n.asset_id == asset_id:
                return n
        return None

    @staticmethod
    def _action_for(node: JobNode) -> ActionType:
        action = LAYER_ACTION.get(node.layer)
        if action is None:
            raise PlannerError(
                f"{node.job_id}: layer {node.layer!r} has no registered recovery action")
        return action

    @staticmethod
    def _narrow(scope: RecoveryScope, kind: CapabilityKind) -> RecoveryScope:
        """Express the scope at the granularity the job supports.

        Widening is honest, not lossy: if a job can only do COB_DATE, the plan says so and
        the approver sees that more than the offending keys will be rewritten.
        """
        from dataclasses import replace
        if kind is CapabilityKind.BUSINESS_KEY_SET:
            return scope
        if kind is CapabilityKind.FULL_TABLE:
            return replace(scope, business_keys=(), key_set_ref="", cob_dates=(),
                           affected_partitions=(),
                           reason=(scope.reason + " | widened to FULL_TABLE: the job "
                                   "declares no narrower capability").strip(" |"))
        return replace(scope, business_keys=(), key_set_ref="",
                       reason=(scope.reason + f" | widened to {kind.value}: the job "
                               "cannot recompute a key subset").strip(" |"))

    def _with_quality_gates(self, actions: list[PlannedAction],
                            root_asset_id: str) -> list[PlannedAction]:
        """Interleave verification turns between the rebuild turns.

        The root is validated BEFORE any descendant runs, which is the barrier the whole
        recovery model rests on: if the root did not come back clean, rebuilding everything
        below it just propagates the same wrong answer faster.
        """
        rebuild_turns = sorted({a.turn for a in actions})
        if not rebuild_turns:
            return actions
        root_turn = rebuild_turns[0]
        root_actions = [a for a in actions if a.turn == root_turn]
        root_scope = root_actions[0].scope
        root_job = root_actions[0].job_id

        out: list[PlannedAction] = []
        for a in actions:
            # every rebuild turn shifts down to leave room for the verification turns
            shift = 1 if a.turn > root_turn else 0
            out.append(replace(a, turn=a.turn + shift))

        for kind in self.VERIFY_ROOT:
            out.append(PlannedAction(job_id=root_job, action=kind, scope=root_scope,
                                     turn=root_turn + 1, granularity=root_actions[0].granularity,
                                     asset_id=root_asset_id))
        last = max(a.turn for a in out)
        if len(rebuild_turns) > 1:
            # The verifying job is whichever job PRODUCED that descendant. Hardcoding
            # "reporting" put a job name into the plan that the descendant's own pipeline
            # never used -- and the Recovery Control Service refuses a plan naming a job
            # outside the registry, so the whole plan would have been rejected at submit.
            # Each descendant is verified at ITS OWN scope and granularity. Copying the
            # root's would claim a check ran over a window the child never rebuilt.
            children = {a.asset_id: a for a in actions if a.turn > root_turn}
            for kind in self.VERIFY_CHILD:
                for child, act in sorted(children.items()):
                    out.append(PlannedAction(job_id=act.job_id, action=kind,
                                             scope=act.scope, turn=last + 1,
                                             granularity=act.granularity,
                                             asset_id=child))
        return self._compact_turns(out)

    @staticmethod
    def _compact_turns(actions: list[PlannedAction]) -> list[PlannedAction]:
        """Turn numbers must be contiguous from 0 after unexecutable assets drop out."""
        from dataclasses import replace
        used = sorted({a.turn for a in actions})
        remap = {old: new for new, old in enumerate(used)}
        return [replace(a, turn=remap[a.turn]) for a in actions]


def estimate_cost(plan: AiRecoveryPlan) -> CostClass:
    """Coarse on purpose. A planner that produced dollar figures would be inventing
    precision; what an approver needs is 'one partition' vs 'a history rebuild'."""
    if any(a.granularity == CapabilityKind.FULL_TABLE.value for a in plan.actions):
        return CostClass.LARGE if len(plan.actions) > 3 else CostClass.MEDIUM
    dates = len(plan.scope.cob_dates) or 1
    if dates > 7 or len(plan.actions) > 8:
        return CostClass.LARGE
    if dates > 1 or len(plan.actions) > 3:
        return CostClass.MEDIUM
    return CostClass.SMALL
