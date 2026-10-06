"""AIGR6 — the deterministic policy gate.

Runs BEFORE any mutation and takes no input from the model. Its inputs are facts the
platform produced: environment, root cause, scope size, lineage confidence, descendant
count, cost class, prior attempts, and the authenticated caller's roles.

Production is conservative by construction: `AUTO_EXECUTE_ALLOWED` in prod requires every
condition to hold, and any single failure downgrades -- never the reverse.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from cdc.incidents import CostClass
from .control import AiRecoveryPlan
from .model import Disposition, LineageConfidence, ScopeSource


class PolicyOutcome(str, Enum):
    AUTO_EXECUTE_ALLOWED = "AUTO_EXECUTE_ALLOWED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class PolicyLimits:
    """Defaults are lab-shaped. Production overrides tighten them; nothing loosens them
    at runtime, because a limit that a caller can raise is not a limit."""

    max_keys: int = 1000
    max_cob_dates: int = 7
    max_descendant_jobs: int = 10
    max_cost: CostClass = CostClass.MEDIUM
    max_prior_attempts: int = 0
    auto_execute_environments: frozenset[str] = frozenset({"dev"})


@dataclass(frozen=True)
class PolicyDecision:
    outcome: PolicyOutcome
    reasons: tuple[str, ...]

    @property
    def may_execute_without_approval(self) -> bool:
        return self.outcome is PolicyOutcome.AUTO_EXECUTE_ALLOWED

    def payload(self) -> dict:
        return {"outcome": self.outcome.value, "reasons": list(self.reasons)}


_COST_ORDER = {CostClass.SMALL: 1, CostClass.MEDIUM: 2, CostClass.LARGE: 3,
               CostClass.UNBOUNDED: 4}


def evaluate(plan: AiRecoveryPlan, *, cost: CostClass, roles: frozenset[str],
             prior_attempts: int = 0, limits: PolicyLimits | None = None) -> PolicyDecision:
    lim = limits or PolicyLimits()
    blocks: list[str] = []
    needs: list[str] = []

    # --- hard blocks: no approval can make these safe -------------------- #
    disp = plan.root_cause.disposition
    if disp is Disposition.NO_AUTOMATIC_RECOVERY:
        blocks.append("root cause is UNKNOWN; no automatic recovery is permitted")
    if disp is Disposition.CODE_FIX_REQUIRED:
        blocks.append("TRANSFORM_LOGIC_DEFECT: rerunning the same code reproduces the "
                      "same wrong answer, more expensively")
    if disp is Disposition.RULE_FIX_REQUIRED:
        blocks.append("DQ_RULE_DEFECT: the data may be correct; fix the rule first")
    if disp is Disposition.WAITING_SOURCE_CORRECTION:
        blocks.append("the defect is in the source; the platform must not write the source "
                      "and a rebuild would reproduce the bad value")
    if not plan.scope.bounded:
        blocks.append("scope is unbounded; 'rebuild everything' is the absence of a scope")
    if plan.scope.root_column and \
            plan.scope.scope_source is ScopeSource.COLUMN_LINEAGE and \
            plan.scope.scope_confidence is not LineageConfidence.VALIDATED:
        blocks.append("column-narrowed scope on non-validated lineage; descendants it "
                      "omits would never be rebuilt")
    if blocks:
        return PolicyDecision(PolicyOutcome.BLOCKED, tuple(blocks))

    # --- things that demand a human ------------------------------------- #
    if plan.environment not in lim.auto_execute_environments:
        needs.append(f"environment {plan.environment!r} always requires approval")
    if plan.scope.key_count > lim.max_keys:
        needs.append(f"{plan.scope.key_count} keys exceeds {lim.max_keys}")
    if len(plan.scope.cob_dates) > lim.max_cob_dates:
        needs.append(f"{len(plan.scope.cob_dates)} dates exceeds {lim.max_cob_dates}")
    jobs = {a.job_id for a in plan.actions}
    if len(jobs) > lim.max_descendant_jobs:
        needs.append(f"{len(jobs)} jobs exceeds {lim.max_descendant_jobs}")
    if _COST_ORDER[cost] > _COST_ORDER[lim.max_cost]:
        needs.append(f"cost class {cost.value} exceeds {lim.max_cost.value}")
    if prior_attempts > lim.max_prior_attempts:
        # A second automatic attempt at something that already failed automatically is how
        # one defect becomes a loop with a bill.
        needs.append(f"{prior_attempts} prior attempt(s); a repeat needs a human")
    if plan.scope.scope_confidence is LineageConfidence.DECLARED:
        needs.append("scope rests on DECLARED lineage, which nothing has observed")
    if "EXECUTE_NONPROD_RECOVERY" not in roles and "EXECUTE_PROD_RECOVERY" not in roles:
        needs.append("caller holds no execute role")

    if needs:
        return PolicyDecision(PolicyOutcome.APPROVAL_REQUIRED, tuple(needs))
    return PolicyDecision(PolicyOutcome.AUTO_EXECUTE_ALLOWED,
                          ("bounded scope, validated lineage, known repairable cause, "
                           "small blast radius, no prior failed attempt",))
