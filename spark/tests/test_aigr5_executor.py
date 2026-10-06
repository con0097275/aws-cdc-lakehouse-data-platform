"""AIGR5 — the executor, and the guard a live run forced into existence."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai.reliability.control import (ActionType, AiRecoveryPlan, Execution,  # noqa: E402
                                    ExecutionState, PlannedAction)
from ai.reliability.model import (CapabilityKind, IncidentCategory, RecoveryCapability,  # noqa: E402
                                  RecoveryScope, RootCause)
from ai.reliability.registry import (CAPABILITIES, JOB_LAYER, AthenaRecoveryExecutor,  # noqa: E402
                                     job_nodes)
from ai.reliability.control import ActionType as AT  # noqa: E402

COB = date(2026, 9, 28)


def _plan(job="eod_build"):
    scope = RecoveryScope(root_asset_id="eod:a", environment="dev", cob_dates=(COB,),
                          business_keys=("A001",))
    return AiRecoveryPlan(
        incident_id="i", root_asset_id="eod:a",
        root_cause=RootCause(IncidentCategory.DUPLICATE_CDC_EVENT, 0.9, ("dq:1",)),
        scope=scope,
        actions=(PlannedAction(job, ActionType.EOD_REBUILD, scope, 0, "COB_DATE"),))


def _ex():
    return Execution(execution_id="e", plan_id="p", plan_hash="h", idempotency_key="k")


class TestCapabilityRegistry:
    def test_every_registered_job_declares_a_capability_and_a_layer(self):
        assert set(CAPABILITIES) == set(JOB_LAYER)

    def test_eod_build_cannot_do_a_key_subset(self):
        """It takes --cob-date and has no key predicate. Claiming otherwise would make the
        planner promise a precision the job does not have."""
        assert CapabilityKind.BUSINESS_KEY_SET not in CAPABILITIES["eod_build"].supports
        assert CapabilityKind.COB_DATE in CAPABILITIES["eod_build"].supports

    def test_an_unknown_job_is_refused_rather_than_given_an_invented_capability(self):
        with pytest.raises(KeyError, match="no declared RecoveryCapability"):
            job_nodes({"some:asset": "a_job_nobody_declared"})


class TestScopeGuard:
    """The guard exists because a live run on 2026-09-30 got this wrong.

    A template repaired by a value heuristic (`balance > 350`) instead of the planned
    scope. It missed one affected account and corrupted an unaffected one: the mart went
    from wrong to differently wrong, and every gate upstream had passed.
    """

    def test_a_template_ignoring_the_scope_is_refused(self):
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "UPDATE t SET balance = balance / 2 WHERE balance > 350"},
            runner=lambda s: None, dry_run=True)
        out = ex(_plan(), _ex())
        assert out.state is ExecutionState.FAILED
        assert "references no scope placeholder" in out.detail

    def test_a_template_using_the_cob_is_accepted(self):
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "DELETE FROM t WHERE cob_date = DATE '{cob}'"},
            runner=lambda s: None, dry_run=True)
        out = ex(_plan(), _ex())
        assert out.state is not ExecutionState.FAILED
        assert "2026-09-28" in ex.statements[0]

    def test_a_template_using_the_keys_is_accepted(self):
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "DELETE FROM t WHERE acct_id IN ({keys})"},
            runner=lambda s: None, dry_run=True)
        ex(_plan(), _ex())
        assert "'A001'" in ex.statements[0]

    def test_a_job_with_no_template_fails_loudly(self):
        ex = AthenaRecoveryExecutor(templates={}, runner=lambda s: None, dry_run=True)
        out = ex(_plan(), _ex())
        assert out.state is ExecutionState.FAILED
        assert "no registered SQL template" in out.detail

    def test_the_executor_cannot_be_handed_sql_by_a_caller(self):
        """It takes a closed template map keyed by job id. There is no parameter through
        which a model-produced statement could arrive."""
        import inspect
        params = set(inspect.signature(AthenaRecoveryExecutor.__call__).parameters)
        assert params == {"self", "plan", "execution"}

    def test_dry_run_never_calls_the_runner(self):
        calls = []
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "DELETE FROM t WHERE cob_date = DATE '{cob}'"},
            runner=lambda s: calls.append(s), dry_run=True)
        ex(_plan(), _ex())
        assert calls == [] and len(ex.statements) == 1

    def test_a_failure_is_recorded_not_swallowed(self):
        def boom(_):
            raise RuntimeError("athena said no")
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "DELETE FROM t WHERE cob_date = DATE '{cob}'"},
            runner=boom, dry_run=False)
        out = ex(_plan(), _ex())
        assert out.state is ExecutionState.FAILED and "athena said no" in out.detail


class TestActionKeyedTemplates:
    """Keyed by job alone, a DQ_RECHECK ran the job's REBUILD SQL — a verification turn
    silently re-executing a DELETE + INSERT. The drill still passed, because an idempotent
    rebuild survives being run twice. That is luck, not correctness."""

    def test_a_verification_action_does_not_fall_back_to_the_rebuild_sql(self):
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "DELETE FROM t WHERE cob_date = DATE '{cob}'"},
            runner=lambda s: None, dry_run=True)
        plan = AiRecoveryPlan(
            incident_id="i", root_asset_id="eod:a",
            root_cause=RootCause(IncidentCategory.DUPLICATE_CDC_EVENT, 0.9, ("dq:1",)),
            scope=RecoveryScope(root_asset_id="eod:a", environment="dev", cob_dates=(COB,)),
            actions=(PlannedAction("eod_build", AT.DQ_RECHECK,
                                   RecoveryScope(root_asset_id="eod:a", environment="dev",
                                                 cob_dates=(COB,)), 0, "COB_DATE"),))
        out = ex(plan, _ex())
        assert out.state is ExecutionState.FAILED
        assert "refusing to fall back" in out.detail

    def test_the_right_action_template_is_chosen(self):
        ex = AthenaRecoveryExecutor(
            templates={("eod_build", AT.EOD_REBUILD): "DELETE FROM rebuild WHERE d='{cob}'",
                       ("eod_build", AT.DQ_RECHECK): "SELECT count(*) FROM check WHERE d='{cob}'"},
            runner=lambda s: None, dry_run=True)
        ex(_plan(), _ex())
        assert "rebuild" in ex.statements[0] and "SELECT" not in ex.statements[0]
