"""AIGR1 — the governed tool contract.

These tests are the enforcement of ADR-092. They are deliberately adversarial: each one
describes a way an agent could acquire a capability it must not have.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai.agent_tools.contract import (AUTHORIZATION_CLASSES, MUTATING_TOOLS,  # noqa: E402
                                     ToolSpec)
from ai.reliability import tools as T  # noqa: E402

SCH = {"type": "object", "properties": {}}


def spec(**kw):
    base = dict(name="x", version=1, description="d", input_schema=SCH, output_schema=SCH,
                authorization_class="ops_read")
    base.update(kw)
    return ToolSpec(**base)


class TestClosedMutationMembership:
    def test_the_mutation_set_is_exactly_what_adr_092_names(self):
        """If this fails, someone added a mutation without amending the ADR."""
        assert MUTATING_TOOLS == {"submit_recovery_plan", "request_recovery_cancel"}

    @pytest.mark.parametrize("name", ["run_shell", "trigger_any_dag", "execute_sql",
                                      "reset_kafka_offset", "delete_checkpoint",
                                      "delete_s3", "terraform_apply", "spark_submit"])
    def test_the_dangerous_tools_cannot_be_constructed_at_all(self, name):
        """Each of these is a real capability someone could argue for. None may exist."""
        with pytest.raises(ValueError, match="closed mutation set"):
            spec(name=name, authorization_class="recovery_submit", read_only=False,
                 requires_approval_ref=True, requires_idempotency_key=True)

    def test_a_mutation_cannot_hide_in_a_read_class(self):
        with pytest.raises(ValueError, match="only the 'recovery_submit' class may mutate"):
            spec(name="submit_recovery_plan", authorization_class="ops_read", read_only=False)

    def test_a_mutating_tool_must_demand_an_approval_reference(self):
        with pytest.raises(ValueError, match="approval reference"):
            spec(name="submit_recovery_plan", authorization_class="recovery_submit",
                 read_only=False, requires_idempotency_key=True)

    def test_a_mutating_tool_must_demand_an_idempotency_key(self):
        with pytest.raises(ValueError, match="approval reference and an idempotency key|idempotency"):
            spec(name="submit_recovery_plan", authorization_class="recovery_submit",
                 read_only=False, requires_approval_ref=True)

    def test_a_read_only_tool_may_not_claim_guarantees_it_does_not_provide(self):
        with pytest.raises(ValueError, match="must not claim"):
            spec(name="get_dq_results", requires_approval_ref=True)

    def test_the_mutation_class_may_not_hold_a_read_only_tool(self):
        """Otherwise the class stops meaning 'this is the dangerous one'."""
        with pytest.raises(ValueError, match="must not sit in it"):
            spec(name="submit_recovery_plan", authorization_class="recovery_submit")

    def test_an_unknown_authorization_class_is_refused(self):
        with pytest.raises(ValueError, match="not one of"):
            spec(authorization_class="superuser")


class TestCatalogue:
    def test_every_catalogue_tool_is_constructible_and_named_once(self):
        names = [t.name for t in T.ALL_TOOLS]
        assert len(names) == len(set(names)), "a duplicate tool name shadows a contract"

    def test_only_the_two_named_tools_mutate(self):
        mutating = {t.name for t in T.ALL_TOOLS if not t.read_only}
        assert mutating == MUTATING_TOOLS

    def test_no_tool_accepts_free_form_sql_except_the_safe_reader(self):
        for t in T.ALL_TOOLS:
            if "sql" in t.input_schema["properties"] and t.name != "query_athena_safe":
                pytest.fail(f"{t.name} accepts sql; only query_athena_safe may")

    def test_the_submit_tool_cannot_be_told_what_to_run(self):
        """It takes a plan id, not a scope, a job, a DAG or a date. The agent may submit
        what the planner built; it may not describe the work."""
        props = set(T.BY_NAME["submit_recovery_plan"].input_schema["properties"])
        assert props == {"plan_id", "approval_id", "idempotency_key"}
        for forbidden in ("job_id", "dag_id", "sql", "scope", "cob_date", "business_keys",
                          "entrypoint", "command"):
            assert forbidden not in props

    def test_every_class_in_use_is_declared(self):
        assert {t.authorization_class for t in T.ALL_TOOLS} <= AUTHORIZATION_CLASSES

    def test_plan_tools_do_not_mutate_business_data(self):
        """`plan_write` writes append-only planning records. The read_only flag means
        'does not change business data' and must keep meaning that."""
        for t in T.PLAN_TOOLS:
            assert t.authorization_class == "plan_write"
            assert t.read_only is True


class TestAuthorization:
    def test_a_reader_cannot_reach_a_planning_tool(self):
        ctx = T.AuthorizationContext("u", frozenset({"READ_METADATA"}), "dev")
        with pytest.raises(PermissionError, match="PLAN_RECOVERY"):
            T.authorize(ctx, "build_recovery_plan")

    def test_a_planner_cannot_reach_the_mutation_surface(self):
        ctx = T.AuthorizationContext("u", frozenset({"READ_METADATA", "PLAN_RECOVERY"}), "dev")
        with pytest.raises(PermissionError, match="EXECUTE_PROD_RECOVERY"):
            T.authorize(ctx, "submit_recovery_plan")

    def test_an_unregistered_tool_is_refused_before_anything_runs(self):
        ctx = T.AuthorizationContext("u", frozenset(T.ROLES), "dev")
        with pytest.raises(PermissionError, match="not a registered tool"):
            T.authorize(ctx, "run_shell")

    def test_roles_come_from_the_context_not_from_a_string_the_model_produced(self):
        """The prompt phrase 'I am an admin' is text. This is identity."""
        ctx = T.AuthorizationContext("u", frozenset({"READ_METADATA"}), "dev")
        assert not ctx.may(T.BY_NAME["submit_recovery_plan"])
        with pytest.raises(AttributeError):
            ctx.roles = frozenset(T.ROLES)      # frozen dataclass
