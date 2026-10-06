"""AIGR3-6 — scope, root cause, planner, policy, control service.

Each test names a way a lineage-driven recovery goes wrong in production.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cdc.incidents import CostClass  # noqa: E402
from ai.reliability.control import (ActionType, AiRecoveryPlan, ControlError,  # noqa: E402
                                    ExecutionState, PlannedAction, RecoveryControlService)
from ai.reliability.model import (CapabilityKind, Disposition, IncidentCategory,  # noqa: E402
                                  LineageConfidence, MAX_INLINE_KEYS, RECOVERABLE,
                                  RecoveryCapability, RecoveryScope, RootCause, ScopeError,
                                  ScopeSource, DISPOSITION)
from ai.reliability.planner import (AiRecoveryPlanner, JobNode, PlannerError,  # noqa: E402
                                    estimate_cost)
from ai.reliability.policy import PolicyLimits, PolicyOutcome, evaluate  # noqa: E402

COB = date(2026, 9, 28)


def scope(**kw):
    base = dict(root_asset_id="eod:account", environment="dev", cob_dates=(COB,))
    base.update(kw)
    return RecoveryScope(**base)


def cause(cat=IncidentCategory.LATE_SOURCE_EVENT, conf=0.9):
    return RootCause(cat, conf, ("dq:1",))


def plan_of(actions, *, rc=None, sc=None, env="dev", excluded=()):
    return AiRecoveryPlan(incident_id="inc", root_asset_id="eod:account",
                          root_cause=rc or cause(), scope=sc or scope(),
                          actions=tuple(actions), excluded=tuple(excluded), environment=env)


def action(job="j", kind=ActionType.EOD_REBUILD, turn=0, sc=None, gran="COB_DATE"):
    return PlannedAction(job, kind, sc or scope(), turn, gran)


class TestRootCauseDisposition:
    def test_every_category_has_a_disposition(self):
        for c in IncidentCategory:
            assert c in DISPOSITION, f"{c} has no disposition; it would fall through a gate"

    @pytest.mark.parametrize("cat,disp", [
        (IncidentCategory.TRANSFORM_LOGIC_DEFECT, Disposition.CODE_FIX_REQUIRED),
        (IncidentCategory.DQ_RULE_DEFECT, Disposition.RULE_FIX_REQUIRED),
        (IncidentCategory.BAD_SOURCE_VALUE, Disposition.WAITING_SOURCE_CORRECTION),
        (IncidentCategory.MISSING_SOURCE_EVENT, Disposition.WAITING_SOURCE_CORRECTION),
        (IncidentCategory.UNKNOWN, Disposition.NO_AUTOMATIC_RECOVERY),
    ])
    def test_the_categories_a_rerun_cannot_fix_are_not_recoverable(self, cat, disp):
        """These five are the whole reason the taxonomy is finer than FailureClass."""
        assert DISPOSITION[cat] is disp
        assert cat not in RECOVERABLE

    def test_late_arriving_data_is_recoverable_because_the_event_is_good(self):
        assert IncidentCategory.LATE_SOURCE_EVENT in RECOVERABLE

    def test_a_cause_without_evidence_is_refused(self):
        with pytest.raises(ValueError, match="no evidence reference"):
            RootCause(IncidentCategory.LATE_SOURCE_EVENT, 0.9, ())

    def test_unknown_may_be_asserted_without_evidence(self):
        """'I do not know' needs no proof; it also authorizes nothing."""
        rc = RootCause(IncidentCategory.UNKNOWN, 0.0, ())
        assert not rc.may_recover


class TestScope:
    def test_an_unbounded_scope_is_not_bounded(self):
        assert not RecoveryScope(root_asset_id="a", environment="dev").bounded

    def test_a_huge_key_list_must_become_a_reference(self):
        keys = tuple(f"K{i}" for i in range(MAX_INLINE_KEYS + 1))
        with pytest.raises(ScopeError, match="materialise a key-set reference"):
            scope(business_keys=keys)
        scope(business_keys=keys, key_set_ref="s3://.../keys.parquet")   # allowed

    def test_a_column_scope_on_unvalidated_lineage_is_refused_at_construction(self):
        """The refusal is here, not in the policy, so no such scope can even be built."""
        with pytest.raises(ScopeError, match="VALIDATED"):
            scope(root_column="balance", scope_source=ScopeSource.COLUMN_LINEAGE,
                  scope_confidence=LineageConfidence.DERIVED)

    def test_reversed_dates_are_refused(self):
        with pytest.raises(ScopeError, match="from_date is after to_date"):
            scope(from_date=date(2026, 9, 30), to_date=date(2026, 9, 1))

    def test_the_fingerprint_changes_when_the_scope_changes(self):
        a = scope(business_keys=("A",))
        b = scope(business_keys=("A", "B"))
        assert a.fingerprint() != b.fingerprint()


class TestCapabilityAndGranularity:
    def test_a_job_that_cannot_do_keys_is_widened_and_says_so(self):
        cob_only = RecoveryCapability("j", frozenset({CapabilityKind.COB_DATE,
                                                      CapabilityKind.FULL_TABLE}))
        sc = scope(business_keys=("A001",))
        assert cob_only.smallest_supported(sc.desired_kinds()) is CapabilityKind.COB_DATE

    def test_a_job_that_can_do_keys_gets_keys(self):
        rich = RecoveryCapability("j", frozenset({CapabilityKind.BUSINESS_KEY_SET,
                                                  CapabilityKind.COB_DATE}))
        sc = scope(business_keys=("A001",))
        assert rich.smallest_supported(sc.desired_kinds()) is CapabilityKind.BUSINESS_KEY_SET

    def test_a_job_supporting_nothing_relevant_falls_back_to_full_table(self):
        only_full = RecoveryCapability("j", frozenset({CapabilityKind.FULL_TABLE}))
        assert only_full.smallest_supported(scope().desired_kinds()) is CapabilityKind.FULL_TABLE

    def test_column_impact_does_not_imply_column_execution(self):
        """The trap this whole module exists to avoid: knowing only BALANCE is wrong does
        not mean Spark can rewrite one column."""
        sc = scope(root_column="balance", business_keys=("A001",),
                   scope_confidence=LineageConfidence.VALIDATED,
                   scope_source=ScopeSource.COLUMN_LINEAGE)
        cob_only = RecoveryCapability("j", frozenset({CapabilityKind.COB_DATE}))
        assert cob_only.smallest_supported(sc.desired_kinds()) is CapabilityKind.COB_DATE


class TestPlanner:
    def _planner(self):
        jobs = {
            "j_eod": JobNode("j_eod", "eod:account", "eod",
                             RecoveryCapability("j_eod", frozenset({CapabilityKind.COB_DATE,
                                                                    CapabilityKind.FULL_TABLE}))),
            "j_mart": JobNode("j_mart", "mart:bal", "mart",
                              RecoveryCapability("j_mart", frozenset({CapabilityKind.BUSINESS_KEY_SET,
                                                                      CapabilityKind.COB_DATE}))),
        }
        return AiRecoveryPlanner(jobs=jobs, turns=[["eod:account"], ["mart:bal"]])

    def test_it_refuses_to_plan_for_a_cause_a_rerun_cannot_fix(self):
        with pytest.raises(PlannerError, match="refusing to build a plan"):
            self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                 root_cause=cause(IncidentCategory.TRANSFORM_LOGIC_DEFECT),
                                 scope=scope(), impacted={"eod:account"}, excluded=[])

    def test_each_job_gets_the_smallest_scope_IT_supports(self):
        sc = scope(root_column="balance", business_keys=("A001", "A002"),
                   scope_confidence=LineageConfidence.VALIDATED,
                   scope_source=ScopeSource.COLUMN_LINEAGE)
        p = self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                 root_cause=cause(), scope=sc,
                                 impacted={"eod:account", "mart:bal"}, excluded=[])
        # A job now has a rebuild action AND verification actions, so filter to the
        # rebuild before comparing granularity.
        by_job = {a.job_id: a for a in p.actions
                  if a.action in (ActionType.EOD_REBUILD, ActionType.MART_RERUN)}
        assert by_job["j_eod"].granularity == "COB_DATE"
        assert by_job["j_mart"].granularity == "BUSINESS_KEY_SET"
        assert by_job["j_eod"].scope.business_keys == ()      # widened, keys dropped
        assert "widened to COB_DATE" in by_job["j_eod"].scope.reason
        assert by_job["j_mart"].scope.business_keys == ("A001", "A002")

    def test_turn_order_is_preserved_root_before_descendant(self):
        p = self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                 root_cause=cause(), scope=scope(),
                                 impacted={"eod:account", "mart:bal"}, excluded=[])
        turns = {a.job_id: a.turn for a in p.actions}
        assert turns["j_eod"] < turns["j_mart"]

    def test_an_unrelated_branch_never_enters_the_plan(self):
        p = self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                 root_cause=cause(), scope=scope(),
                                 impacted={"eod:account"},
                                 excluded=[("digital:events", "unrelated branch")])
        assert {a.job_id for a in p.actions} == {"j_eod"}
        assert ("digital:events", "unrelated branch") in p.excluded

    def test_an_impacted_asset_in_the_order_but_with_no_job_is_excluded_with_a_reason(self):
        """Path 1: it has a place in the topological order but nothing can execute it."""
        jobs = {"j_eod": JobNode("j_eod", "eod:account", "eod",
                                 RecoveryCapability("j_eod", frozenset({CapabilityKind.COB_DATE})))}
        pl = AiRecoveryPlanner(jobs=jobs, turns=[["eod:account"], ["mart:bal"]])
        p = pl.plan(incident_id="i", root_asset_id="eod:account", root_cause=cause(),
                    scope=scope(), impacted={"eod:account", "mart:bal"}, excluded=[])
        assert any("no registered executable job" in r for _, r in p.excluded)

    def test_an_impacted_asset_absent_from_the_order_is_excluded_not_dropped(self):
        """Path 2, and the dangerous one: it never appears in any turn, so a naive planner
        never visits it. A descendant silently omitted is one that never gets rebuilt while
        the plan still looks complete."""
        p = self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                 root_cause=cause(), scope=scope(),
                                 impacted={"eod:account", "bi:dashboard"}, excluded=[])
        assert any(a == "bi:dashboard" for a, _ in p.excluded)
        assert any("absent from the topological order" in r for _, r in p.excluded)

    def test_every_impacted_asset_is_either_planned_or_explained(self):
        """The invariant behind both paths: nothing impacted may vanish."""
        impacted = {"eod:account", "mart:bal", "bi:dashboard", "ghost:thing"}
        p = self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                 root_cause=cause(), scope=scope(),
                                 impacted=impacted, excluded=[])
        planned_assets = {"eod:account", "mart:bal"}          # the two with jobs
        explained = {a for a, _ in p.excluded}
        assert impacted <= (planned_assets | explained)

    def test_turns_are_contiguous_after_unexecutable_assets_drop_out(self):
        jobs = {"j_mart": JobNode("j_mart", "mart:bal", "mart",
                                  RecoveryCapability("j_mart", frozenset({CapabilityKind.COB_DATE})))}
        pl = AiRecoveryPlanner(jobs=jobs, turns=[["eod:account"], ["mart:bal"]])
        # gates off: this test is about turn COMPACTION after an asset drops out, not
        # about the verification turns, which are covered separately.
        p = pl.plan(incident_id="i", root_asset_id="eod:account", root_cause=cause(),
                    scope=scope(), impacted={"mart:bal"}, excluded=[], quality_gates=False)
        assert sorted({a.turn for a in p.actions}) == [0]

    def test_the_plan_hash_changes_when_anything_changes(self):
        p1 = plan_of([action()])
        p2 = plan_of([action(job="other")])
        assert p1.plan_hash != p2.plan_hash


class TestPolicy:
    def test_unknown_root_cause_is_blocked_not_merely_gated(self):
        with pytest.raises(ScopeError):
            plan_of([action()], rc=RootCause(IncidentCategory.UNKNOWN, 0.0, ()))

    def test_an_unbounded_scope_is_blocked(self):
        sc = RecoveryScope(root_asset_id="eod:account", environment="dev")
        d = evaluate(plan_of([action(sc=sc)], sc=sc), cost=CostClass.SMALL,
                     roles=frozenset({"EXECUTE_NONPROD_RECOVERY"}))
        assert d.outcome is PolicyOutcome.BLOCKED
        assert any("unbounded" in r for r in d.reasons)

    def test_production_always_requires_approval(self):
        d = evaluate(plan_of([action()], env="prod"), cost=CostClass.SMALL,
                     roles=frozenset({"EXECUTE_PROD_RECOVERY"}))
        assert d.outcome is PolicyOutcome.APPROVAL_REQUIRED

    def test_a_large_key_set_requires_approval(self):
        sc = scope(business_keys=tuple(f"K{i}" for i in range(400)))
        d = evaluate(plan_of([action(sc=sc)], sc=sc), cost=CostClass.SMALL,
                     roles=frozenset({"EXECUTE_NONPROD_RECOVERY"}),
                     limits=PolicyLimits(max_keys=100))
        assert d.outcome is PolicyOutcome.APPROVAL_REQUIRED

    def test_a_prior_failed_attempt_requires_a_human(self):
        d = evaluate(plan_of([action()]), cost=CostClass.SMALL,
                     roles=frozenset({"EXECUTE_NONPROD_RECOVERY"}), prior_attempts=1)
        assert d.outcome is PolicyOutcome.APPROVAL_REQUIRED
        assert any("prior attempt" in r for r in d.reasons)

    def test_declared_only_lineage_requires_approval(self):
        sc = scope(scope_confidence=LineageConfidence.DECLARED)
        d = evaluate(plan_of([action(sc=sc)], sc=sc), cost=CostClass.SMALL,
                     roles=frozenset({"EXECUTE_NONPROD_RECOVERY"}))
        assert d.outcome is PolicyOutcome.APPROVAL_REQUIRED

    def test_a_caller_with_no_execute_role_cannot_auto_execute(self):
        d = evaluate(plan_of([action()]), cost=CostClass.SMALL, roles=frozenset({"READ_METADATA"}))
        assert d.outcome is PolicyOutcome.APPROVAL_REQUIRED

    def test_the_happy_path_can_auto_execute_in_dev(self):
        d = evaluate(plan_of([action()]), cost=CostClass.SMALL,
                     roles=frozenset({"EXECUTE_NONPROD_RECOVERY"}))
        assert d.outcome is PolicyOutcome.AUTO_EXECUTE_ALLOWED


class TestControlService:
    def _svc(self, **kw):
        return RecoveryControlService(registered_jobs=frozenset({"j"}), **kw)

    def test_a_plan_naming_an_unregistered_job_is_refused(self):
        svc = self._svc()
        with pytest.raises(ControlError, match="not registered"):
            svc.create_plan(plan_of([action(job="rm_-rf")]))

    def test_an_agent_may_not_approve_its_own_plan(self):
        svc = self._svc(); p = svc.create_plan(plan_of([action()]))
        with pytest.raises(ControlError, match="may not approve its own plan"):
            svc.approve(p.plan_id, approver="agent", roles=frozenset({"PLAN_RECOVERY"}))

    def test_a_tampered_plan_invalidates_the_approval(self):
        svc = self._svc(); p = svc.create_plan(plan_of([action()]))
        appr = svc.approve(p.plan_id, approver="alice", roles=frozenset({"PLAN_RECOVERY"}))
        edited = plan_of([action(turn=0), action(job="j", turn=1)])
        ok, why = appr.valid_for(edited)
        assert not ok and "different plan" in why

    def test_an_expired_approval_is_refused(self):
        svc = self._svc(); p = svc.create_plan(plan_of([action()]))
        appr = svc.approve(p.plan_id, approver="alice", roles=frozenset({"PLAN_RECOVERY"}))
        later = datetime.now(timezone.utc) + timedelta(hours=5)
        with pytest.raises(ControlError, match="expired"):
            svc.execute(p.plan_id, approval_id=appr.approval_id,
                        idempotency_key="k", now=later)

    def test_the_same_idempotency_key_converges_rather_than_duplicating(self):
        svc = self._svc(); p = svc.create_plan(plan_of([action()]))
        a = svc.approve(p.plan_id, approver="alice", roles=frozenset({"PLAN_RECOVERY"}))
        e1 = svc.execute(p.plan_id, approval_id=a.approval_id, idempotency_key="k")
        e2 = svc.execute(p.plan_id, approval_id=a.approval_id, idempotency_key="k")
        assert e1.execution_id == e2.execution_id

    def test_a_second_concurrent_execution_of_one_plan_is_refused(self):
        svc = self._svc(); p = svc.create_plan(plan_of([action()]))
        a = svc.approve(p.plan_id, approver="alice", roles=frozenset({"PLAN_RECOVERY"}))
        svc.execute(p.plan_id, approval_id=a.approval_id, idempotency_key="k1")
        with pytest.raises(ControlError, match="already"):
            svc.execute(p.plan_id, approval_id=a.approval_id, idempotency_key="k2")

    def test_no_executor_means_nothing_ran_and_the_record_says_so(self):
        """A control service that returns SUCCEEDED having run nothing is the failure this
        whole platform keeps finding."""
        svc = self._svc(); p = svc.create_plan(plan_of([action()]))
        a = svc.approve(p.plan_id, approver="alice", roles=frozenset({"PLAN_RECOVERY"}))
        ex = svc.execute(p.plan_id, approval_id=a.approval_id, idempotency_key="k")
        assert ex.state is ExecutionState.PENDING
        assert "nothing ran" in ex.detail

    def test_an_approval_for_another_environment_does_not_transfer(self):
        svc = RecoveryControlService(registered_jobs=frozenset({"j"}), environment="dev")
        p = svc.create_plan(plan_of([action()]))
        appr = svc.approve(p.plan_id, approver="alice", roles=frozenset({"PLAN_RECOVERY"}))
        prod_plan = plan_of([action()], env="prod")
        ok, why = appr.valid_for(prod_plan)
        assert not ok


class TestQualityGatesAreInThePlan:
    """A plan that rebuilds and stops cannot tell you whether it worked.

    The action types DQ_RECHECK / RECONCILE / CERTIFY existed from the start and nothing
    emitted them: every plan was rebuild-only, and the quality barrier lived in prose. An
    approver reading such a plan cannot see that the root is validated before the
    descendants run.
    """

    def _planner(self):
        jobs = {
            "j_eod": JobNode("j_eod", "eod:account", "eod",
                             RecoveryCapability("j_eod", frozenset({CapabilityKind.COB_DATE}))),
            "j_mart": JobNode("j_mart", "mart:bal", "mart",
                              RecoveryCapability("j_mart", frozenset({CapabilityKind.COB_DATE}))),
        }
        return AiRecoveryPlanner(jobs=jobs, turns=[["eod:account"], ["mart:bal"]])

    def _plan(self, **kw):
        return self._planner().plan(incident_id="i", root_asset_id="eod:account",
                                    root_cause=cause(), scope=scope(),
                                    impacted={"eod:account", "mart:bal"}, excluded=[], **kw)

    def test_the_root_is_verified_before_any_descendant_rebuilds(self):
        p = self._plan()
        root_verify = max(a.turn for a in p.actions
                          if a.action in (ActionType.DQ_RECHECK, ActionType.RECONCILE,
                                          ActionType.CERTIFY)
                          and a.asset_id == "eod:account")
        child_rebuild = min(a.turn for a in p.actions
                            if a.action is ActionType.MART_RERUN)
        assert root_verify < child_rebuild, \
            "rebuilding descendants before the root is validated propagates the same " \
            "wrong answer faster"

    def test_the_root_is_certified_not_merely_rechecked(self):
        kinds = {a.action for a in self._plan().actions if a.asset_id == "eod:account"}
        assert {ActionType.DQ_RECHECK, ActionType.RECONCILE, ActionType.CERTIFY} <= kinds

    def test_descendants_get_dq_and_reconciliation_but_not_certification(self):
        """Certification is a statement about the closed business date, made once at the
        root. Certifying each mart separately would invent a tier for a derived asset."""
        p = self._plan()
        child = {a.action for a in p.actions if a.asset_id == "mart:bal"}
        assert ActionType.DQ_RECHECK in child and ActionType.RECONCILE in child
        assert ActionType.CERTIFY not in child

    def test_the_rebuild_still_comes_first(self):
        p = self._plan()
        assert min(a.turn for a in p.actions if a.action is ActionType.EOD_REBUILD) == 0

    def test_turns_stay_contiguous_with_the_gates_inserted(self):
        p = self._plan()
        assert sorted({a.turn for a in p.actions}) == list(range(p.turn_count))

    def test_gates_can_be_turned_off_for_a_bare_plan(self):
        p = self._plan(quality_gates=False)
        assert not [a for a in p.actions if a.action is ActionType.CERTIFY]

    def test_the_gates_change_the_plan_hash(self):
        """Two plans that differ in whether anything verifies the result are not the same
        plan, and an approval for one must not cover the other."""
        assert self._plan().plan_hash != self._plan(quality_gates=False).plan_hash

    def test_a_verification_action_names_the_job_that_produced_the_asset(self):
        """Hardcoding a verifying job puts a name into the plan that the descendant's own
        pipeline never used — and the control service refuses a plan naming an
        unregistered job, so the whole plan would be rejected at submit."""
        p = self._plan()
        for a in p.actions:
            if a.asset_id == "mart:bal":
                assert a.job_id == "j_mart"
        assert {a.job_id for a in p.actions} == {"j_eod", "j_mart"}
