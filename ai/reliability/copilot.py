"""AIGR7 — the LangGraph Data Reliability & Governance Copilot.

BOUNDED BY CONSTRUCTION
-----------------------
A fixed node sequence with explicit routes and hard ceilings. No ReAct loop, no agent
calling another agent, no step count that depends on what a model decided. The graph can
only end in `final_answer` or `refuse`.

The LLM appears in exactly two places: classifying intent, and composing the final prose
from an evidence pack that is already complete. Every fact in that pack came from a tool.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from cdc.incidents import CostClass
from .control import AiRecoveryPlan, ControlError, RecoveryControlService
from .model import Disposition, RootCause, ScopeError
from .planner import estimate_cost
from .policy import PolicyDecision, PolicyOutcome, evaluate
from .tools import AuthorizationContext


class Intent(str, Enum):
    EXPLAIN = "EXPLAIN"
    INVESTIGATE = "INVESTIGATE"
    PLAN = "PLAN"
    EXECUTE = "EXECUTE"
    STATUS = "STATUS"
    AMBIGUOUS = "AMBIGUOUS"


#: An ambiguous request may never mutate. "Fix it" without a resolved asset is a question,
#: not an instruction.
MUTATING_INTENTS = frozenset({Intent.EXECUTE})

#: Intents that describe no incident. "What does BALANCE feed?" is a lineage question, and
#: running a root-cause classifier over it produces the absurd answer this routing exists to
#: prevent: `Root cause UNKNOWN. No automatic recovery is permitted.` -- to a question that
#: never asked for a recovery. A diagnosis is only meaningful when something is wrong.
KNOWLEDGE_INTENTS = frozenset({Intent.EXPLAIN, Intent.STATUS})

#: An AMBIGUOUS request that nonetheless RESOLVED an asset is a lookup, not an incident
#: report. `"the customer snapshot"` names a table and describes no defect; diagnosing it
#: produced `Root cause UNKNOWN. No automatic recovery is permitted.` to someone who had
#: simply typed a table name. Describe what is known and say what else can be asked.
#:
#: Ambiguity still blocks every mutation -- that is enforced by MUTATING_INTENTS, which
#: this does not touch.
def is_knowledge(intent: Intent, *, asset_resolved: bool) -> bool:
    if intent in KNOWLEDGE_INTENTS:
        return True
    return intent is Intent.AMBIGUOUS and asset_resolved


class Label(str, Enum):
    """Every line of the final answer is labelled, so a reader can tell a measurement from
    an inference from a thing that has not happened yet."""

    FACT = "FACT"
    DIAGNOSIS = "DIAGNOSIS"
    PLAN = "PLAN"
    ACTION_EXECUTED = "ACTION_EXECUTED"
    VALIDATION_RESULT = "VALIDATION_RESULT"
    LIMITATION = "LIMITATION"


@dataclass
class Budget:
    """Ceilings, checked between nodes. A budget nothing enforces is a comment."""

    max_steps: int = 24
    max_tool_calls: int = 40
    max_recovery_attempts: int = 1
    agent_timeout_seconds: float = 300.0
    max_tokens: int = 120_000

    steps: int = 0
    tool_calls: int = 0

    def step(self, node: str) -> None:
        self.steps += 1
        if self.steps > self.max_steps:
            raise BudgetExceeded(f"max_steps {self.max_steps} exceeded at {node!r}")

    def tool(self, name: str) -> None:
        self.tool_calls += 1
        if self.tool_calls > self.max_tool_calls:
            raise BudgetExceeded(f"max_tool_calls {self.max_tool_calls} exceeded at {name!r}")


class BudgetExceeded(RuntimeError):
    """Terminal. The graph stops and says so rather than continuing more cheaply."""


@dataclass
class CopilotState:
    request_id: str = ""
    thread_id: str = ""
    authenticated_user: str = ""
    authorization_context: AuthorizationContext | None = None
    raw_request: str = ""
    normalized_intent: Intent = Intent.AMBIGUOUS
    resolved_assets: tuple[str, ...] = ()
    resolved_columns: tuple[str, ...] = ()
    resolved_time_scope: tuple[str, ...] = ()
    resolved_business_keys: tuple[str, ...] = ()
    incident_id: str = ""
    root_cause: RootCause | None = None
    evidence_refs: tuple[str, ...] = ()
    lineage_confidence: str = ""
    affected_assets: tuple[str, ...] = ()
    affected_jobs: tuple[str, ...] = ()
    excluded_assets: tuple[tuple[str, str], ...] = ()
    recovery_plan: AiRecoveryPlan | None = None
    policy_decision: PolicyDecision | None = None
    approval_id: str = ""
    execution_id: str = ""
    execution_status: str = ""
    dq_results: tuple[dict, ...] = ()
    reconciliation_results: tuple[dict, ...] = ()
    certification_results: tuple[dict, ...] = ()
    final_evidence_pack: dict = field(default_factory=dict)
    answer: tuple[tuple[str, str], ...] = ()       # (Label, text)
    errors: tuple[str, ...] = ()
    safety_events: tuple[str, ...] = ()
    budget: Budget = field(default_factory=Budget)
    agent_version: str = "aigr7.1"
    terminated: str = ""

    def say(self, label: Label, text: str) -> None:
        self.answer = self.answer + ((label.value, text),)

    def refuse(self, why: str) -> None:
        self.terminated = "refused"
        self.safety_events = self.safety_events + (why,)
        self.say(Label.LIMITATION, why)


# --------------------------------------------------------------------------- #
# Nodes. Each is deterministic except `classify_intent`, which may use a model.
# --------------------------------------------------------------------------- #

def node_authenticate(state: CopilotState) -> CopilotState:
    state.budget.step("authenticate")
    if state.authorization_context is None:
        state.refuse("no authenticated context; the copilot does not act for an "
                     "unidentified caller")
    return state


def node_classify_intent(state: CopilotState, *, classifier=None) -> CopilotState:
    state.budget.step("classify_intent")
    if classifier is not None:
        try:
            state.normalized_intent = Intent(classifier(state.raw_request))
        except (ValueError, Exception):            # a model returning nonsense is ambiguous
            state.normalized_intent = Intent.AMBIGUOUS
    return state


def node_resolve_scope(state: CopilotState, *, resolver=None) -> CopilotState:
    state.budget.step("resolve_scope")
    if resolver is not None:
        state.budget.tool("resolve_asset")
        resolved = resolver(state.raw_request)
        state.resolved_assets = tuple(resolved.get("assets", ()))
        state.resolved_columns = tuple(resolved.get("columns", ()))
        state.resolved_time_scope = tuple(resolved.get("dates", ()))
        state.resolved_business_keys = tuple(resolved.get("keys", ()))
    if not state.resolved_assets:
        # An unresolved asset must never reach a mutation: a guessed identifier is how a
        # recovery lands on the wrong table.
        state.normalized_intent = Intent.AMBIGUOUS
        state.refuse("could not resolve the request to a canonical asset; refusing rather "
                     "than guessing an identifier")
    return state


def node_answer_knowledge(state: CopilotState, *, describe=None) -> CopilotState:
    """Answer a question that is not about a defect, from resolved facts alone.

    No root cause, no plan, no policy gate -- none of those mean anything here, and
    printing an empty one invites the reader to think something was withheld.
    """
    state.budget.step("answer_knowledge")
    if describe is not None:
        for label, text in describe(state):
            state.say(label, text)
    state.terminated = "answered"
    return state


def node_classify_root_cause(state: CopilotState, *, classifier=None) -> CopilotState:
    if is_knowledge(state.normalized_intent, asset_resolved=bool(state.resolved_assets)):
        # Nothing is wrong, so there is nothing to diagnose.
        return state
    state.budget.step("classify_root_cause")
    if classifier is not None:
        state.budget.tool("classify_data_incident")
        state.root_cause = classifier(state)
    if state.root_cause is None:
        state.refuse("no root cause could be established from the evidence collected")
        return state
    state.evidence_refs = state.root_cause.evidence_refs
    d = state.root_cause.disposition
    if d is Disposition.WAITING_SOURCE_CORRECTION:
        state.terminated = "waiting_source_correction"
        state.say(Label.DIAGNOSIS, f"{state.root_cause.category.value}: the defect is in the "
                                   "source. The platform will not write the source, and a "
                                   "rebuild would reproduce the value.")
        state.say(Label.LIMITATION, "Recovery resumes once corrected CDC arrives.")
    elif d is Disposition.CODE_FIX_REQUIRED:
        state.terminated = "code_fix_required"
        state.say(Label.DIAGNOSIS, "TRANSFORM_LOGIC_DEFECT: rerunning the same code would "
                                   "reproduce the same wrong answer, more expensively.")
    elif d is Disposition.RULE_FIX_REQUIRED:
        state.terminated = "rule_fix_required"
        state.say(Label.DIAGNOSIS, "DQ_RULE_DEFECT: the data may be correct. Review the "
                                   "rule before touching business data.")
    elif d is Disposition.NO_AUTOMATIC_RECOVERY:
        state.terminated = "unknown_root_cause"
        state.say(Label.DIAGNOSIS, "Root cause UNKNOWN. No automatic recovery is permitted.")
    return state


def node_build_plan(state: CopilotState, *, planner=None) -> CopilotState:
    if planner is None or is_knowledge(state.normalized_intent,
                                       asset_resolved=bool(state.resolved_assets)):
        return state
    state.budget.step("build_plan")
    state.budget.tool("build_recovery_plan")
    try:
        state.recovery_plan = planner(state)
    except (ScopeError, ControlError) as e:
        state.refuse(f"no plan could be built: {e}")
    return state


def node_policy_gate(state: CopilotState, *, cost: CostClass | None = None,
                     prior_attempts: int = 0) -> CopilotState:
    plan = state.recovery_plan
    if plan is None:
        return state
    state.budget.step("policy_gate")
    roles = state.authorization_context.roles if state.authorization_context else frozenset()
    state.policy_decision = evaluate(plan, cost=cost or estimate_cost(plan), roles=roles,
                                     prior_attempts=prior_attempts)
    if state.policy_decision.outcome is PolicyOutcome.BLOCKED:
        state.terminated = "blocked"
        for r in state.policy_decision.reasons:
            state.say(Label.LIMITATION, r)
    return state


def node_execute(state: CopilotState, *, service: RecoveryControlService | None = None,
                 idempotency_key: str = "") -> CopilotState:
    """The single point where anything changes, and it changes nothing without an approval."""
    state.budget.step("execute")
    plan, decision = state.recovery_plan, state.policy_decision
    if plan is None or decision is None:
        return state
    if state.normalized_intent not in MUTATING_INTENTS:
        state.say(Label.PLAN, "Plan built. Not submitted: the request asked for a plan, "
                              "not an execution.")
        return state
    if decision.outcome is PolicyOutcome.APPROVAL_REQUIRED and not state.approval_id:
        state.terminated = "awaiting_approval"
        state.say(Label.PLAN, "Approval required before anything runs.")
        for r in decision.reasons:
            state.say(Label.LIMITATION, r)
        return state
    if service is None:
        state.say(Label.LIMITATION, "no Recovery Control Service wired; nothing was submitted")
        return state
    try:
        state.budget.tool("submit_recovery_plan")
        ex = service.execute(plan.plan_id, approval_id=state.approval_id,
                             idempotency_key=idempotency_key or plan.plan_hash)
        state.execution_id, state.execution_status = ex.execution_id, ex.state.value
        state.say(Label.ACTION_EXECUTED,
                  f"submitted {plan.plan_id} as {ex.execution_id} ({ex.state.value})")
    except ControlError as e:
        state.refuse(f"the control service refused the submission: {e}")
    return state


def node_build_evidence(state: CopilotState) -> CopilotState:
    # Deliberately NOT budgeted. The evidence pack is the account of what happened,
    # including the account of running out of budget. Charging for it means a run that
    # exceeds its ceiling produces no explanation of why -- the one output that is always
    # owed to the user.
    plan = state.recovery_plan
    state.final_evidence_pack = {
        "request": {"id": state.request_id, "text": state.raw_request,
                    "user": state.authenticated_user,
                    "intent": state.normalized_intent.value},
        "resolved": {"assets": list(state.resolved_assets),
                     "columns": list(state.resolved_columns),
                     "dates": list(state.resolved_time_scope),
                     "key_count": len(state.resolved_business_keys)},
        "root_cause": state.root_cause.payload() if state.root_cause else None,
        "evidence_refs": list(state.evidence_refs),
        "impact": {"affected_assets": list(state.affected_assets),
                   "affected_jobs": list(state.affected_jobs),
                   "excluded": [{"asset": a, "reason": r} for a, r in state.excluded_assets]},
        "plan": plan.payload() if plan else None,
        "policy": state.policy_decision.payload() if state.policy_decision else None,
        "approval_id": state.approval_id or None,
        "execution": {"id": state.execution_id, "status": state.execution_status},
        "verification": {"dq": list(state.dq_results),
                         "reconciliation": list(state.reconciliation_results),
                         "certification": list(state.certification_results)},
        "terminated": state.terminated or "completed",
        "safety_events": list(state.safety_events),
        "budget": {"steps": state.budget.steps, "tool_calls": state.budget.tool_calls},
        "agent_version": state.agent_version,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    return state


def node_final_answer(state: CopilotState, *, composer=None) -> CopilotState:
    """The model may write prose here. It may not add a fact.

    Anything it produces is appended as narration beside the labelled lines the
    deterministic nodes already wrote; it never replaces them.
    """
    # Not budgeted, for the same reason as the evidence pack.
    if composer is not None and not state.terminated == "refused":
        state.say(Label.FACT, composer(state.final_evidence_pack))
    return state


def run(state: CopilotState, *, classifier=None, resolver=None, cause_classifier=None,
        planner=None, service=None, composer=None, cost=None, describe=None,
        prior_attempts: int = 0) -> CopilotState:
    """The whole graph, executed deterministically.

    Written so the ordering and the stop conditions are testable without LangGraph
    installed -- the same reason the existing business agent keeps a tier-1 path.
    """
    try:
        for node in (
            lambda s: node_authenticate(s),
            lambda s: node_classify_intent(s, classifier=classifier),
            lambda s: node_resolve_scope(s, resolver=resolver),
            lambda s: (node_answer_knowledge(s, describe=describe)
                       if is_knowledge(s.normalized_intent,
                                       asset_resolved=bool(s.resolved_assets))
                       else node_classify_root_cause(s, classifier=cause_classifier)),
            lambda s: node_build_plan(s, planner=planner),
            lambda s: node_policy_gate(s, cost=cost, prior_attempts=prior_attempts),
            lambda s: node_execute(s, service=service),
        ):
            if state.terminated:
                break
            state = node(state)
    except BudgetExceeded as e:
        state.refuse(str(e))
    state = node_build_evidence(state)
    return node_final_answer(state, composer=composer)


def build_graph(**kw):
    """The LangGraph form, for the durable/interruptible production path.

    Imported lazily: the deterministic `run()` above must stay usable where LangGraph is
    not installed, which is how the existing agent is structured.
    """
    from langgraph.graph import END, StateGraph
    g = StateGraph(CopilotState)
    g.add_node("authenticate", node_authenticate)
    g.add_node("classify_intent", lambda s: node_classify_intent(s, classifier=kw.get("classifier")))
    g.add_node("resolve_scope", lambda s: node_resolve_scope(s, resolver=kw.get("resolver")))
    g.add_node("classify_root_cause",
               lambda s: node_classify_root_cause(s, classifier=kw.get("cause_classifier")))
    g.add_node("build_plan", lambda s: node_build_plan(s, planner=kw.get("planner")))
    g.add_node("policy_gate", node_policy_gate)
    g.add_node("execute", lambda s: node_execute(s, service=kw.get("service")))
    g.add_node("build_evidence", node_build_evidence)
    g.add_node("final_answer", lambda s: node_final_answer(s, composer=kw.get("composer")))
    g.set_entry_point("authenticate")
    order = ["authenticate", "classify_intent", "resolve_scope", "classify_root_cause",
             "build_plan", "policy_gate", "execute", "build_evidence"]
    for a, b in zip(order, order[1:]):
        # Every node can short-circuit to the evidence pack: a run that stops early still
        # owes the user an account of why.
        g.add_conditional_edges(a, lambda s, nxt=b: "build_evidence" if s.terminated else nxt,
                                {b: b, "build_evidence": "build_evidence"})
    g.add_edge("build_evidence", "final_answer")
    g.add_edge("final_answer", END)
    return g.compile(checkpointer=kw.get("checkpointer"))
