"""AIGR5/AIGR6 — the Recovery Control Service, and the approval it revalidates.

WHY A SERVICE AND NOT A DIRECT AIRFLOW CALL
-------------------------------------------
If the agent could call Airflow, the safety properties would live in the agent's prompt.
Here they live in code that runs after the agent has stopped talking: the plan hash is
re-checked, the approval is re-checked, the environment is re-checked, the job ids are
re-checked against a closed registry, and a duplicate in flight is rejected.

An approval is a statement about ONE plan. Any edit produces a different hash and the
approval no longer applies -- which is enforced here rather than remembered.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum

from .model import IncidentCategory, RecoveryScope, RootCause, ScopeError


class ActionType(str, Enum):
    """The complete set of things a plan may ask for. Not an arbitrary string."""

    REALTIME_REBUILD = "REALTIME_REBUILD"
    EOD_REBUILD = "EOD_REBUILD"
    AUTO_CORRECT = "AUTO_CORRECT"
    FULFILL = "FULFILL"
    MART_RERUN = "MART_RERUN"
    STREAM_BATCH_REPLAY = "STREAM_BATCH_REPLAY"
    DQ_RECHECK = "DQ_RECHECK"
    RECONCILE = "RECONCILE"
    CERTIFY = "CERTIFY"


class ExecutionState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"


class ControlError(RuntimeError):
    """Refused at the boundary. Every message says what was checked and what failed."""


@dataclass(frozen=True)
class PlannedAction:
    """One job, one target, one turn.

    `asset_id` is not decoration. One job rebuilds many assets -- `reporting` produces every
    mart -- so a plan carrying only job ids lists the same name repeatedly with no way to
    tell what each run touches. An approver cannot approve that, and an executor cannot
    execute it.
    """

    job_id: str
    action: ActionType
    scope: RecoveryScope
    turn: int
    granularity: str
    asset_id: str = ""

    @property
    def target(self) -> str:
        return self.asset_id or self.scope.root_asset_id

    def payload(self) -> dict:
        return {"job_id": self.job_id, "target": self.target,
                "action": self.action.value, "turn": self.turn,
                "granularity": self.granularity, "scope": self.scope.payload()}


@dataclass(frozen=True)
class AiRecoveryPlan:
    """Immutable. `plan_id` is derived from the content, so an edit is a different plan."""

    incident_id: str
    root_asset_id: str
    root_cause: RootCause
    scope: RecoveryScope
    actions: tuple[PlannedAction, ...]
    excluded: tuple[tuple[str, str], ...] = ()      # (asset, reason)
    environment: str = "dev"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not self.actions:
            raise ScopeError("a plan with no actions is not a plan")
        if not self.root_cause.may_recover:
            raise ScopeError(
                f"{self.root_cause.category.value} disposition is "
                f"{self.root_cause.disposition.value}; a plan must not be built for it")
        turns = sorted({a.turn for a in self.actions})
        if turns != list(range(len(turns))):
            raise ScopeError(f"turns must be contiguous from 0, got {turns}")

    @property
    def plan_hash(self) -> str:
        body = "|".join(
            f"{a.job_id}@{a.target}:{a.action.value}:{a.turn}:{a.scope.fingerprint()}"
            for a in sorted(self.actions, key=lambda x: (x.turn, x.job_id, x.target)))
        return hashlib.sha256(
            f"{self.incident_id}|{self.root_asset_id}|{self.environment}|"
            f"{self.root_cause.category.value}|{body}".encode()).hexdigest()[:16]

    @property
    def plan_id(self) -> str:
        return f"aigr1:{self.plan_hash}"

    @property
    def turn_count(self) -> int:
        return len({a.turn for a in self.actions})

    def payload(self) -> dict:
        return {"plan_id": self.plan_id, "plan_hash": self.plan_hash,
                "incident_id": self.incident_id, "root_asset_id": self.root_asset_id,
                "environment": self.environment,
                "root_cause": self.root_cause.payload(), "scope": self.scope.payload(),
                "actions": [a.payload() for a in self.actions],
                "excluded": [{"asset": a, "reason": r} for a, r in self.excluded],
                "turn_count": self.turn_count,
                "created_at": self.created_at.isoformat()}


@dataclass(frozen=True)
class ApprovalRecord:
    """An approval is about ONE plan hash, by ONE identity, for a bounded time."""

    approval_id: str
    plan_id: str
    plan_hash: str
    approver_identity: str
    approval_scope: str
    environment: str
    approved_at: datetime
    expires_at: datetime

    def valid_for(self, plan: AiRecoveryPlan, *, now: datetime | None = None) -> tuple[bool, str]:
        now = now or datetime.now(timezone.utc)
        if plan.plan_id != self.plan_id:
            return False, "approval is for a different plan"
        if plan.plan_hash != self.plan_hash:
            # The usual way this happens is an innocent edit between approval and submit.
            return False, ("plan hash changed since approval; an edited plan is a different "
                           "plan and the approval no longer applies")
        if plan.environment != self.environment:
            return False, "approval was granted for a different environment"
        if now >= self.expires_at:
            return False, "approval has expired"
        return True, "ok"

    def payload(self) -> dict:
        return {"approval_id": self.approval_id, "plan_id": self.plan_id,
                "plan_hash": self.plan_hash, "approver_identity": self.approver_identity,
                "approval_scope": self.approval_scope, "environment": self.environment,
                "approved_at": self.approved_at.isoformat(),
                "expires_at": self.expires_at.isoformat()}


@dataclass
class Execution:
    execution_id: str
    plan_id: str
    plan_hash: str
    idempotency_key: str
    state: ExecutionState = ExecutionState.PENDING
    turns_completed: int = 0
    detail: str = ""

    def payload(self) -> dict:
        return {"execution_id": self.execution_id, "plan_id": self.plan_id,
                "plan_hash": self.plan_hash, "state": self.state.value,
                "turns_completed": self.turns_completed, "detail": self.detail,
                "idempotency_key": self.idempotency_key}


class RecoveryControlService:
    """The only path from a plan to running work.

    `registered_jobs` is a closed set supplied by the platform, not by the caller: an action
    naming a job outside it is refused even if the plan is otherwise valid and approved.
    """

    APPROVAL_TTL = timedelta(hours=4)

    def __init__(self, *, registered_jobs: frozenset[str], environment: str = "dev",
                 executor=None):
        self._jobs = registered_jobs
        self._env = environment
        self._executor = executor           # injected; None = record only, run nothing
        self._plans: dict[str, AiRecoveryPlan] = {}
        self._approvals: dict[str, ApprovalRecord] = {}
        self._executions: dict[str, Execution] = {}
        self._by_idem: dict[str, str] = {}

    # -- plans ------------------------------------------------------------- #
    def create_plan(self, plan: AiRecoveryPlan) -> AiRecoveryPlan:
        unknown = sorted({a.job_id for a in plan.actions} - self._jobs)
        if unknown:
            raise ControlError(
                f"plan names jobs that are not registered: {unknown}. A job id that is not "
                "in the platform registry is refused here even with a valid approval.")
        if plan.environment != self._env:
            raise ControlError(
                f"plan environment {plan.environment!r} != service environment {self._env!r}")
        self._plans[plan.plan_id] = plan
        return plan

    def get_plan(self, plan_id: str) -> AiRecoveryPlan:
        if plan_id not in self._plans:
            raise ControlError(f"no such plan {plan_id!r}")
        return self._plans[plan_id]

    # -- approval ---------------------------------------------------------- #
    def approve(self, plan_id: str, *, approver: str, roles: frozenset[str],
                now: datetime | None = None) -> ApprovalRecord:
        plan = self.get_plan(plan_id)
        need = "APPROVE_PROD_RECOVERY" if self._env == "prod" else "PLAN_RECOVERY"
        if need not in roles:
            raise ControlError(f"{approver} lacks {need} and cannot approve in {self._env}")
        if approver == "agent" or approver.startswith("llm:"):
            # The model may relay an approval. It may not BE one.
            raise ControlError("an agent identity may not approve its own plan")
        now = now or datetime.now(timezone.utc)
        rec = ApprovalRecord(
            approval_id=f"apr:{uuid.uuid4().hex[:12]}", plan_id=plan.plan_id,
            plan_hash=plan.plan_hash, approver_identity=approver,
            approval_scope=plan.scope.fingerprint(), environment=plan.environment,
            approved_at=now, expires_at=now + self.APPROVAL_TTL)
        self._approvals[rec.approval_id] = rec
        return rec

    # -- execution --------------------------------------------------------- #
    def execute(self, plan_id: str, *, approval_id: str, idempotency_key: str,
                now: datetime | None = None) -> Execution:
        """Revalidate everything. The agent's word is not evidence at this boundary."""
        if idempotency_key in self._by_idem:
            # Converge rather than duplicate: the same approved retry returns the same
            # execution instead of running the work twice.
            return self._executions[self._by_idem[idempotency_key]]

        plan = self.get_plan(plan_id)
        approval = self._approvals.get(approval_id)
        if approval is None:
            raise ControlError(f"no such approval {approval_id!r}")
        ok, why = approval.valid_for(plan, now=now)
        if not ok:
            raise ControlError(f"approval rejected: {why}")

        unknown = sorted({a.job_id for a in plan.actions} - self._jobs)
        if unknown:
            raise ControlError(f"plan names unregistered jobs at execute time: {unknown}")

        for ex in self._executions.values():
            if ex.plan_id == plan_id and ex.state in (ExecutionState.PENDING,
                                                      ExecutionState.RUNNING):
                raise ControlError(
                    f"execution {ex.execution_id} for this plan is already {ex.state.value}")

        ex = Execution(execution_id=f"exe:{uuid.uuid4().hex[:12]}", plan_id=plan.plan_id,
                       plan_hash=plan.plan_hash, idempotency_key=idempotency_key)
        self._executions[ex.execution_id] = ex
        self._by_idem[idempotency_key] = ex.execution_id
        if self._executor is None:
            ex.detail = ("recorded; no executor injected, so nothing ran. This is a "
                         "dry surface, not a silent success.")
            return ex
        ex.state = ExecutionState.RUNNING
        self._executor(plan, ex)
        return ex

    def status(self, execution_id: str) -> Execution:
        if execution_id not in self._executions:
            raise ControlError(f"no such execution {execution_id!r}")
        return self._executions[execution_id]

    def request_cancel(self, execution_id: str) -> Execution:
        ex = self.status(execution_id)
        if ex.state in (ExecutionState.SUCCEEDED, ExecutionState.FAILED,
                        ExecutionState.CANCELLED):
            raise ControlError(f"execution is already {ex.state.value}")
        ex.state = ExecutionState.CANCEL_REQUESTED
        return ex
