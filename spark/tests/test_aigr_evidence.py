"""Root cause is read from the ledgers, not from the wording of the question."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai.reliability.evidence import Evidence, classify, collect  # noqa: E402
from ai.reliability.model import IncidentCategory as C  # noqa: E402

COB = date(2026, 9, 28)


def dq(check, sev="BLOCKER", failed="1", examined="670"):
    return {"check_id": check, "severity": sev, "failed": failed, "examined": examined}


def ev(**kw):
    base = dict(dataset="eod:acct", cob_date=COB, refs=["dq:r1"])
    base.update(kw)
    return Evidence(**base)


class TestClassifyFromLedgers:
    @pytest.mark.parametrize("check,expected", [
        ("uniqueness.one_active_row_per_key", C.DUPLICATE_CDC_EVENT),
        ("timeliness.late_event_applied", C.LATE_SOURCE_EVENT),
        ("freshness.max_lag", C.STALE_DATA),
        ("completeness.acct_id_not_null", C.MISSING_SOURCE_EVENT),
        ("schema.compatibility", C.SCHEMA_DRIFT),
        ("validity.operation_is_known", C.INGESTION_DEFECT),
    ])
    def test_the_check_family_decides_the_cause(self, check, expected):
        """The check id is what the platform itself assigned; prose is not consulted."""
        assert classify(ev(dq_failures=[dq(check)])).category is expected

    def test_a_check_that_examined_nothing_is_not_evidence_of_a_defect(self):
        """A live run diagnosed MISSING_SOURCE_EVENT from `completeness.*` at 0 of 0 rows
        while `uniqueness.*` at 1 of 670 sat in the same result set. A check that examined
        nothing proves nothing — the NOT_EVALUATED shape this platform already refuses to
        treat as a verdict."""
        e = ev(dq_failures=[dq("completeness.source_commit_ts", failed="0", examined="0"),
                            dq("uniqueness.one_active_row_per_key", failed="1", examined="670")])
        assert classify(e).category is C.DUPLICATE_CDC_EVENT

    def test_a_real_failure_outranks_a_zero_failure_check(self):
        e = ev(dq_failures=[dq("completeness.x", failed="0", examined="500"),
                            dq("uniqueness.y", failed="7", examined="500")])
        assert classify(e).category is C.DUPLICATE_CDC_EVENT

    def test_a_blocker_outranks_a_warning_at_equal_evidence(self):
        e = ev(dq_failures=[dq("completeness.x", sev="WARN", failed="1", examined="10"),
                            dq("uniqueness.y", sev="BLOCKER", failed="1", examined="10")])
        assert classify(e).category is C.DUPLICATE_CDC_EVENT

    def test_quarantined_rows_imply_an_ingestion_defect(self):
        assert classify(ev(quarantined=4)).category is C.INGESTION_DEFECT

    def test_a_reconciliation_break_is_its_own_category(self):
        e = ev(recon_breaks=[{"metric": "row_count", "source": "670", "target": "669"}])
        assert classify(e).category is C.RECONCILIATION_DEFECT

    def test_a_waiting_eod_run_is_a_readiness_defect_not_a_data_defect(self):
        assert classify(ev(eod_status="WAITING_SOURCE")).category is C.DEPENDENCY_READINESS_DEFECT

    def test_clean_ledgers_yield_unknown_and_authorize_nothing(self):
        rc = classify(ev())
        assert rc.category is C.UNKNOWN and not rc.may_recover

    def test_every_evidenced_cause_carries_its_ledger_refs(self):
        rc = classify(ev(dq_failures=[dq("uniqueness.k")]))
        assert rc.evidence_refs and rc.evidence_refs[0].startswith("dq:")


class TestStatedCause:
    def test_a_stated_cause_is_corroborated_when_the_ledgers_agree(self):
        rc = classify(ev(dq_failures=[dq("uniqueness.k")]), stated=C.LATE_SOURCE_EVENT)
        assert rc.category is C.LATE_SOURCE_EVENT and rc.confidence == 0.9
        assert "corroborated" in rc.detail

    def test_a_stated_cause_with_no_supporting_evidence_is_weaker(self):
        rc = classify(ev(), stated=C.LATE_SOURCE_EVENT)
        assert rc.confidence == 0.6 and "operator's word" in rc.detail

    def test_the_operator_cannot_state_their_way_past_the_dispositions(self):
        """Stating a cause changes the category, never what that category permits."""
        rc = classify(ev(), stated=C.TRANSFORM_LOGIC_DEFECT)
        assert not rc.may_recover


class TestCollectIsHonestAboutWhatItRead:
    def test_it_records_which_ledgers_it_read(self):
        e = collect("eod:acct", COB, runner=lambda *a, **k: [])
        assert set(e.looked) == {"dq_result_v2", "reconciliation_run",
                                 "dq_quarantine", "eod_run"}

    def test_an_unreadable_ledger_yields_no_findings_not_a_clean_bill(self):
        e = collect("eod:acct", COB, runner=lambda *a, **k: [])
        assert not e.found_anything
        assert classify(e).category is C.UNKNOWN
