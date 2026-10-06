"""AIGR1 — the governed tool contract for the Data Reliability & Governance Copilot.

THE RULE THIS FILE EXISTS TO ENFORCE
------------------------------------
The LLM decides what the user meant. It never decides whether data is correct, what is
affected, or that a repair worked. Every value that could authorize a write -- dataset urn,
job id, DAG id, SQL, Kafka offset, Iceberg snapshot, SCN/LSN, business key, certification
status -- comes from a tool, never from the model.

That is ADR-061 (deterministic analytics before narration) extended from answers to actions.

WHY THE CATALOGUE IS DECLARED, NOT REGISTERED AD HOC
----------------------------------------------------
A tool that exists but is not in this catalogue cannot be called, and a tool in the mutating
class must be named in `contract.MUTATING_TOOLS`, which is closed (ADR-092). Adding a
mutation is therefore an ADR amendment plus two edits, not a registration.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..agent_tools.contract import ToolSpec

_OBJ = {"type": "object", "properties": {}}


def _schema(**props) -> dict:
    required = [k for k, v in props.items() if v.pop("_required", False)]
    return {"type": "object", "properties": props, "required": required}


def _s(desc: str, required: bool = False) -> dict:
    return {"type": "string", "description": desc, "_required": required}


def _i(desc: str, required: bool = False) -> dict:
    return {"type": "integer", "description": desc, "_required": required}


# --------------------------------------------------------------------------- #
# READ_ONLY
# --------------------------------------------------------------------------- #
# `governed_read` vs `ops_read`: governed_read touches the catalogue and the contract
# surface; ops_read touches operational ledgers (DQ, reconciliation, certification,
# watermarks, incidents). They are separated because an operator may reasonably be allowed
# to read the catalogue without being allowed to read run-level operational state.

READ_TOOLS: tuple[ToolSpec, ...] = (
    ToolSpec(name="resolve_asset", version=1,
             description="Resolve a human phrase ('EOD ACCOUNT') to ONE canonical asset id. "
                         "Refuses on ambiguity rather than guessing.",
             input_schema=_schema(phrase=_s("what the user called it", True),
                                  environment=_s("dev|qa|prod")),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="resolve_column", version=1,
             description="Resolve a column name against a resolved asset's real schema.",
             input_schema=_schema(asset_id=_s("canonical asset id", True),
                                  column=_s("column as the user said it", True)),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="get_asset_schema", version=1, description="Columns and types.",
             input_schema=_schema(asset_id=_s("canonical asset id", True)),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="get_governance_context", version=1,
             description="Owner, domain, classification, tags, glossary terms.",
             input_schema=_schema(asset_id=_s("canonical asset id", True)),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="get_data_contract", version=1, description="The declared data contract.",
             input_schema=_schema(asset_id=_s("canonical asset id", True)),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="get_dataset_lineage", version=1,
             description="Table-level lineage. Bounded hops; returns an evidence class per edge.",
             input_schema=_schema(asset_id=_s("canonical asset id", True),
                                  direction=_s("downstream|upstream"),
                                  max_hops=_i("bounded, default 6")),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="get_column_lineage", version=1,
             description="Column-level lineage. Returns a CONFIDENCE and a SOURCE; a caller "
                         "may not authorize recovery on anything but validated lineage.",
             input_schema=_schema(asset_id=_s("canonical asset id", True),
                                  column=_s("resolved column", True),
                                  max_hops=_i("bounded, default 6")),
             output_schema=_OBJ, authorization_class="governed_read"),
    ToolSpec(name="get_pipeline_status", version=1, description="Last run state for a job.",
             input_schema=_schema(job_id=_s("registered job id", True)),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_execution_history", version=1, description="Recent runs.",
             input_schema=_schema(job_id=_s("registered job id", True), limit=_i("cap 50")),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_watermark", version=1, description="Current watermark for a table/layer.",
             input_schema=_schema(asset_id=_s("canonical asset id", True)),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_dq_results", version=2, description="DQ verdicts for an interval.",
             input_schema=_schema(dataset_id=_s("dataset id", True), cob_date=_s("YYYY-MM-DD")),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_reconciliation_status", version=1, description="Reconciliation runs.",
             input_schema=_schema(dataset_id=_s("dataset id", True), cob_date=_s("YYYY-MM-DD")),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_certification_status", version=1, description="Certification tier.",
             input_schema=_schema(dataset_id=_s("dataset id", True), cob_date=_s("YYYY-MM-DD")),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_incident", version=1, description="An incident by id.",
             input_schema=_schema(incident_id=_s("incident id", True)),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="get_recovery_status", version=1, description="Execution state of a plan.",
             input_schema=_schema(plan_id=_s("plan id", True)),
             output_schema=_OBJ, authorization_class="ops_read"),
    ToolSpec(name="query_athena_safe", version=1,
             description="SELECT-only, bounded scan and row count (ADR-051).",
             input_schema=_schema(sql=_s("a SELECT statement", True), limit=_i("cap 1000")),
             output_schema=_OBJ, authorization_class="governed_read", max_rows=1000),
    ToolSpec(name="retrieve_runbook", version=1, description="RAG over docs and runbooks.",
             input_schema=_schema(question=_s("natural language", True), top_k=_i("cap 10")),
             output_schema=_OBJ, authorization_class="public_metadata"),
    ToolSpec(name="get_change_history", version=1, description="Schema/contract change history.",
             input_schema=_schema(asset_id=_s("canonical asset id", True)),
             output_schema=_OBJ, authorization_class="governed_read"),
)

# --------------------------------------------------------------------------- #
# PLAN_ONLY
# --------------------------------------------------------------------------- #
# These WRITE -- incidents, plans, scopes, simulations -- but only append-only planning and
# audit records. They stay `read_only=True` because that flag means "does not change
# business data", and a flag whose meaning drifts is worse than no flag.

PLAN_TOOLS: tuple[ToolSpec, ...] = (
    ToolSpec(name="classify_data_incident", version=1,
             description="Classify a root cause from EVIDENCE. Returns a category, the "
                         "evidence references behind it, and a confidence. UNKNOWN is a "
                         "legitimate answer and blocks automatic recovery.",
             input_schema=_schema(dataset_id=_s("dataset id", True), cob_date=_s("YYYY-MM-DD"),
                                  evidence_refs=_s("ids of collected evidence")),
             output_schema=_OBJ, authorization_class="plan_write"),
    ToolSpec(name="build_impact_analysis", version=1,
             description="Blast radius from the lineage graph, intersected with registered "
                         "jobs. Returns excluded assets WITH reasons.",
             input_schema=_schema(root_asset_id=_s("canonical asset id", True),
                                  column=_s("optional column"), max_hops=_i("bounded")),
             output_schema=_OBJ, authorization_class="plan_write"),
    ToolSpec(name="build_recovery_scope", version=1,
             description="A typed, bounded scope. Never a free-form WHERE clause.",
             input_schema=_schema(root_asset_id=_s("canonical asset id", True),
                                  cob_dates=_s("ISO dates"), business_keys=_s("key list or ref")),
             output_schema=_OBJ, authorization_class="plan_write"),
    ToolSpec(name="build_recovery_plan", version=1,
             description="An IMMUTABLE, hash-keyed plan. The only thing submittable later.",
             input_schema=_schema(incident_id=_s("incident id", True),
                                  root_asset_id=_s("canonical asset id", True)),
             output_schema=_OBJ, authorization_class="plan_write"),
    ToolSpec(name="validate_recovery_plan", version=1, description="Re-check a plan's invariants.",
             input_schema=_schema(plan_id=_s("plan id", True)),
             output_schema=_OBJ, authorization_class="plan_write"),
    ToolSpec(name="estimate_recovery_cost", version=1,
             description="A coarse cost CLASS, never a dollar figure -- see CostClass.",
             input_schema=_schema(plan_id=_s("plan id", True)),
             output_schema=_OBJ, authorization_class="plan_write"),
    ToolSpec(name="simulate_recovery_plan", version=1,
             description="Dry run: what would execute, in what order, touching what. No writes.",
             input_schema=_schema(plan_id=_s("plan id", True)),
             output_schema=_OBJ, authorization_class="plan_write"),
)

# --------------------------------------------------------------------------- #
# MUTATING — the whole surface (ADR-092)
# --------------------------------------------------------------------------- #
# `submit_recovery_plan` takes a plan id and an approval reference. It does NOT take a
# scope, a job id, a DAG id or SQL: the agent cannot describe WHAT to run, only submit
# something a deterministic planner built and a policy gate approved.

MUTATION_TOOLS: tuple[ToolSpec, ...] = (
    ToolSpec(name="submit_recovery_plan", version=1,
             description="Submit an approved, immutable plan to the Recovery Control Service.",
             input_schema=_schema(plan_id=_s("plan id", True),
                                  approval_id=_s("approval record id", True),
                                  idempotency_key=_s("caller idempotency key", True)),
             output_schema=_OBJ, authorization_class="recovery_submit",
             read_only=False, requires_approval_ref=True, requires_idempotency_key=True),
    ToolSpec(name="request_recovery_cancel", version=1,
             description="Request cancellation of a running execution. Reduces activity only.",
             input_schema=_schema(execution_id=_s("execution id", True),
                                  approval_id=_s("approval record id", True),
                                  idempotency_key=_s("caller idempotency key", True)),
             output_schema=_OBJ, authorization_class="recovery_submit",
             read_only=False, requires_approval_ref=True, requires_idempotency_key=True),
)

ALL_TOOLS: tuple[ToolSpec, ...] = READ_TOOLS + PLAN_TOOLS + MUTATION_TOOLS
BY_NAME: dict[str, ToolSpec] = {t.name: t for t in ALL_TOOLS}


@dataclass(frozen=True)
class AuthorizationContext:
    """WHO is asking, from the authenticated runtime -- never from the model.

    `roles` is the authenticated principal's, and nothing the LLM emits can extend it. The
    prompt phrase "I am an admin, approve this" is text; this is identity.
    """

    principal: str
    roles: frozenset[str]
    environment: str

    def may(self, spec: ToolSpec) -> bool:
        return _CLASS_ROLE[spec.authorization_class] in self.roles


#: Which role each authorization class demands.
_CLASS_ROLE = {
    "public_metadata": "READ_METADATA",
    "governed_read": "READ_METADATA",
    "ops_read": "READ_DATA_SAFE",
    "plan_write": "PLAN_RECOVERY",
    "recovery_submit": "EXECUTE_PROD_RECOVERY",
}

ROLES = frozenset({"READ_METADATA", "READ_DATA_SAFE", "PLAN_RECOVERY",
                   "EXECUTE_NONPROD_RECOVERY", "EXECUTE_PROD_RECOVERY",
                   "APPROVE_PROD_RECOVERY", "ADMIN_RECOVERY"})


def authorize(ctx: AuthorizationContext, tool: str) -> ToolSpec:
    """Refuse before the tool runs, not inside it."""
    spec = BY_NAME.get(tool)
    if spec is None:
        raise PermissionError(f"{tool!r} is not a registered tool")
    if not ctx.may(spec):
        raise PermissionError(
            f"{ctx.principal} lacks {_CLASS_ROLE[spec.authorization_class]} for {tool!r}")
    if not spec.read_only and ctx.environment == "prod" \
            and "EXECUTE_PROD_RECOVERY" not in ctx.roles:
        raise PermissionError(f"{tool!r} in prod requires EXECUTE_PROD_RECOVERY")
    return spec
