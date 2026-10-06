"""AI v2 analytics: diagnosis, forecasting, governance.

These test the JUDGEMENTS, not just the plumbing. The three that matter most:

  * a metric drop caused by a short partition must be reported as a DATA problem, never
    decomposed as a business event;
  * a non-additive measure must REFUSE decomposition rather than emit parts that do not
    sum to the whole;
  * a forecast on too little history must refuse rather than produce a number.
"""
from __future__ import annotations

import math
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ai"))

from analytics.datasource import FrameSource                       # noqa: E402
from analytics.diagnose import diagnose                            # noqa: E402
from analytics.forecast import MIN_POINTS, forecast                # noqa: E402
from analytics.governance import review                            # noqa: E402
from analytics.registry import METRICS, MetricSpec, get_metric     # noqa: E402

BAL = get_metric("total_account_balance")
AVG = get_metric("avg_account_balance")


def flat_rows(days=7, start=date(2026, 8, 14), per_day=(("CURRENT", 1000),
                                                        ("SAVINGS", 500), ("LOAN", 200))):
    rows = []
    for i in range(days):
        d = (start + timedelta(days=i)).isoformat()
        for prod, v in per_day:
            rows.append({"business_date": d, "value": v, "processing_status": prod})
    return rows


class TestRegistry:
    def test_undeclared_dimension_is_refused(self):
        with pytest.raises(ValueError, match="not a declared dimension"):
            BAL.validate_dimension("customer_name")

    def test_unknown_metric_is_refused(self):
        with pytest.raises(KeyError, match="unknown metric"):
            get_metric("made_up_metric")

    def test_non_additive_metrics_are_declared_as_such(self):
        """AVG and COUNT(DISTINCT) do not sum across segments."""
        assert METRICS["avg_account_balance"].additive is False
        assert METRICS["active_account_count"].additive is False
        assert METRICS["total_account_balance"].additive is True


class TestDiagnosisPutsDataFirst:
    def test_a_short_partition_is_a_data_problem_not_a_business_event(self):
        rows = flat_rows()
        rows.append({"business_date": "2026-08-21", "value": 1000,
                     "processing_status": "CURRENT"})          # 1 row where 3 are typical
        d = diagnose(BAL, FrameSource(rows), "2026-08-21", dimension="processing_status")
        assert d.verdict == "DATA_INCOMPLETE"
        assert d.contributions == [], "must not attribute a delta on an incomplete partition"
        assert "PIPELINE" in " ".join(d.notes)

    def test_completeness_uses_raw_rows_not_aggregated_points(self):
        """Regression: deriving the count from `series` yields one point per date, so the
        ratio was always 1.0 and the guard could never fire."""
        rows = flat_rows()
        rows.append({"business_date": "2026-08-21", "value": 1, "processing_status": "CURRENT"})
        d = diagnose(BAL, FrameSource(rows), "2026-08-21")
        assert d.completeness["typical_rows"] == 3.0
        assert d.completeness["rows_today"] == 1

    def test_missing_date_is_NO_DATA(self):
        d = diagnose(BAL, FrameSource(flat_rows()), "2026-08-25")
        assert d.verdict == "NO_DATA"


class TestContributionsAreExact:
    def test_the_named_segment_carries_the_whole_delta(self):
        rows = flat_rows()
        for prod, v in (("CURRENT", 1000), ("SAVINGS", 50), ("LOAN", 200)):
            rows.append({"business_date": "2026-08-21", "value": v, "processing_status": prod})
        d = diagnose(BAL, FrameSource(rows), "2026-08-21", dimension="processing_status")
        assert d.verdict == "MOVED"
        top = d.contributions[0]
        assert top.segment == "SAVINGS"
        assert top.share_of_delta == pytest.approx(1.0, abs=1e-6)

    def test_segment_deltas_sum_to_the_total_delta(self):
        """A decomposition whose parts do not sum to the whole is worse than none."""
        rows = flat_rows()
        for prod, v in (("CURRENT", 700), ("SAVINGS", 400), ("LOAN", 100)):
            rows.append({"business_date": "2026-08-21", "value": v, "processing_status": prod})
        d = diagnose(BAL, FrameSource(rows), "2026-08-21", dimension="processing_status")
        assert sum(c.delta for c in d.contributions) == pytest.approx(d.delta, abs=1e-6)

    def test_a_new_segment_is_labelled_NEW(self):
        rows = flat_rows()
        for prod, v in (("CURRENT", 1000), ("SAVINGS", 500), ("LOAN", 200), ("FX", 900)):
            rows.append({"business_date": "2026-08-21", "value": v, "processing_status": prod})
        d = diagnose(BAL, FrameSource(rows), "2026-08-21", dimension="processing_status")
        assert [c.status for c in d.contributions if c.segment == "FX"] == ["NEW"]

    def test_non_additive_metric_refuses_decomposition(self):
        rows = flat_rows()
        for prod, v in (("CURRENT", 10), ("SAVINGS", 10), ("LOAN", 10)):
            rows.append({"business_date": "2026-08-21", "value": v, "processing_status": prod})
        d = diagnose(AVG, FrameSource(rows), "2026-08-21", dimension="processing_status")
        assert d.verdict == "NOT_DECOMPOSABLE"
        assert d.contributions == []


class TestForecastIsHonest:
    def _seasonal(self, n=30):
        rows = []
        d0 = date(2026, 7, 25)
        for i in range(n):
            rows.append({"business_date": (d0 + timedelta(days=i)).isoformat(),
                         "value": 1000 + 120 * math.sin(i * 2 * math.pi / 7) + i * 5,
                         "processing_status": "CURRENT"})
        return rows

    def test_refuses_when_history_is_too_short(self):
        rows = self._seasonal(n=8)
        f = forecast(BAL, FrameSource(rows), as_of="2026-08-01")
        assert f.point is None and f.refused
        assert str(MIN_POINTS) in f.refused

    def test_picks_the_method_with_the_lowest_backtest_error(self):
        f = forecast(BAL, FrameSource(self._seasonal()), as_of="2026-08-23")
        assert f.method == min(f.candidates, key=lambda k: f.candidates[k]["mape"])
        assert f.method == "seasonal_naive", "weekly series should prefer seasonal naive"

    def test_reports_an_interval_derived_from_its_own_measured_error(self):
        f = forecast(BAL, FrameSource(self._seasonal()), as_of="2026-08-23")
        assert f.lower < f.point < f.upper
        # Tolerance, not exact equality: point/lower/upper and the MAPE are each rounded to
        # 4dp on the way out, so the identity holds only up to that rounding.
        assert f.upper - f.point == pytest.approx(abs(f.point) * f.backtest_mape, abs=1e-2)

    def test_backtest_is_walk_forward_not_in_sample(self):
        """Every prediction must be made from strictly earlier points."""
        f = forecast(BAL, FrameSource(self._seasonal()), as_of="2026-08-23")
        assert 0 < f.backtest_n < f.history_points


class TestGovernanceProposesButNeverApplies:
    def test_a_short_partition_is_CRITICAL_with_a_proposed_fix(self):
        rows = [{"business_date": f"2026-08-{d:02d}", "value": 100, "processing_status": "C"}
                for d in range(14, 21) for _ in range(10)]
        rows.append({"business_date": "2026-08-21", "value": 100, "processing_status": "C"})
        r = review(BAL, FrameSource(rows), as_of="2026-08-21", watermark="2026-08-21")
        comp = [f for f in r.findings if f.check == "completeness"][0]
        assert comp.severity == "CRITICAL"
        assert comp.proposed_fix and comp.proposed_fix.startswith("bash ")
        assert r.verdict == "INVESTIGATE"

    def test_a_flat_baseline_does_not_blind_the_volume_check(self):
        """sd == 0 makes the z-score undefined; returning 0.0 marked it PASS exactly when
        the baseline was most trustworthy."""
        rows = [{"business_date": f"2026-08-{d:02d}", "value": 100, "processing_status": "C"}
                for d in range(14, 21) for _ in range(10)]
        rows.append({"business_date": "2026-08-21", "value": 100, "processing_status": "C"})
        r = review(BAL, FrameSource(rows), as_of="2026-08-21")
        vol = [f for f in r.findings if f.check == "volume_stability"][0]
        assert vol.severity != "PASS"

    def test_watermark_claiming_success_with_no_rows_is_CRITICAL(self):
        rows = [{"business_date": f"2026-08-{d:02d}", "value": 100, "processing_status": "C"}
                for d in range(14, 21) for _ in range(10)]
        r = review(BAL, FrameSource(rows), as_of="2026-08-21", watermark="2026-08-21")
        wm = [f for f in r.findings if f.check == "watermark_vs_reality"][0]
        assert wm.severity == "CRITICAL"

    def test_a_check_that_cannot_run_is_UNKNOWN_never_PASS(self):
        rows = [{"business_date": "2026-08-21", "value": 1, "processing_status": "C"}]
        r = review(BAL, FrameSource(rows), as_of="2026-08-21")
        wm = [f for f in r.findings if f.check == "watermark_vs_reality"][0]
        assert wm.severity == "UNKNOWN"

    def test_no_finding_executes_anything(self):
        """propose-only: the module must hold no write path."""
        src = (ROOT / "ai" / "analytics" / "governance.py").read_text()
        for forbidden in ("subprocess", "os.system", "boto3.client('s3').delete",
                          "run_query(", "popen"):
            assert forbidden not in src


class TestToolsAreRegisteredReadOnly:
    def test_the_new_tools_are_in_the_catalog_and_read_only(self):
        from agent_tools.catalog import CATALOG, WRITE_TOOLS
        for t in ("diagnose_metric", "forecast_metric", "governance_review", "list_metrics"):
            assert t in CATALOG
            assert CATALOG[t][0].read_only is True
        assert WRITE_TOOLS == {}

    def test_router_sends_the_new_questions_to_the_new_intents(self):
        from agent.router import Intent, route
        assert route("why is the total account balance so small today") == Intent.DIAGNOSIS
        assert route("what will the total balance be tomorrow") == Intent.FORECAST
        assert route("what should i check, the mart looks wrong") == Intent.GOVERNANCE

    def test_a_pipeline_why_question_still_routes_to_pipeline_ops(self):
        """Regression: DIAGNOSIS absorbed 'Why is EOD stale?' when precedence was too wide."""
        from agent.router import Intent, route
        assert route("Why is EOD stale?") == Intent.PIPELINE_OPS


class TestRouterSafetyAfterNarrowingDrop:
    """`drops?` was bare and refused "why did the balance drop yesterday".

    Narrowing it to require a DDL object is security-relevant, so every mutation phrasing the
    suite has ever relied on is re-asserted here alongside the analytical sense.
    """

    @pytest.mark.parametrize("q", [
        "drop this table", "drop these tables", "DROP TABLE mart.dim_customer",
        "drop the mart table", "drop database prod", "drop schema staging",
        "reset Kafka offsets", "delete the checkpoints", "truncate table x",
        "terraform destroy", "rm -rf /opt/checkpoints", "insert into mart.x values (1)",
    ])
    def test_mutation_phrasings_are_still_refused(self, q):
        from agent.router import Intent, route
        assert route(q) == Intent.UNSAFE, f"{q!r} leaked past the deny pattern"

    @pytest.mark.parametrize("q", [
        "why did the balance drop yesterday",
        "why did revenue fall last week",
        "why is the total account balance so small today",
    ])
    def test_the_noun_sense_reaches_diagnosis(self, q):
        from agent.router import Intent, route
        assert route(q) == Intent.DIAGNOSIS

    def test_an_architecture_why_question_is_still_knowledge(self):
        """Regression: a bare `why is/are` sent "Why are REALTIME and EOD siblings?" to
        DIAGNOSIS -- a documentation question answered with a metric decomposition."""
        from agent.router import Intent, route
        assert route("Why are REALTIME and EOD siblings rather than a chain?") == Intent.KNOWLEDGE
