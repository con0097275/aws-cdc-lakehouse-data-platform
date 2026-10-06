"""Column lineage is VALIDATED by checking, not by relabelling.

`DERIVED` blocks column-narrowed recovery. There were two ways to unblock it: change the
string, or confirm both endpoints of every edge exist. These tests hold the second.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cdc.column_validation import (EdgeVerdict, GlueSchemaReader,  # noqa: E402
                                   PayloadJsonReader, parse_dataset_urn, summarise,
                                   validate_column_edges)
from cdc.lineage_graph import ColumnEdge, LineageGraph  # noqa: E402
from cdc.models import ConfigError  # noqa: E402

UP = "urn:li:dataset:(urn:li:dataPlatform:glue,db.eod_account,DEV)"
DOWN = "urn:li:dataset:(urn:li:dataPlatform:glue,db.curated_account,DEV)"


class FakeGlue:
    def __init__(self, tables): self._t = tables
    def columns(self, db_table): return self._t.get(db_table)


class FakePayload:
    def __init__(self, keys): self._k = keys
    def keys(self, db_table): return self._k.get(db_table)


def graph_with(edge_kw):
    from cdc.lineage_graph import EvidenceClass
    g = LineageGraph()
    base = dict(upstream_dataset=UP, upstream_column="BALANCE",
                downstream_dataset=DOWN, downstream_column="balance",
                evidence=EvidenceClass.DERIVED, source="test")
    base.update(edge_kw)
    g.add_column_edge(ColumnEdge(**base))
    return g


class TestUrnParsing:
    def test_a_dataset_urn_yields_platform_table_and_fabric(self):
        assert parse_dataset_urn(UP) == ("glue", "db.eod_account", "DEV")

    def test_a_non_dataset_urn_is_refused(self):
        with pytest.raises(ValueError, match="not a dataset urn"):
            parse_dataset_urn("urn:li:corpuser:alice")


class TestValidation:
    def test_a_top_level_column_on_both_sides_validates(self):
        v = validate_column_edges(
            graph_with({}),
            FakeGlue({"db.eod_account": {"balance"}, "db.curated_account": {"balance"}}))
        assert v[0].ok and v[0].reason == ""

    def test_a_missing_upstream_table_is_a_verdict_not_a_crash(self):
        """An edge pointing at a table nobody created is exactly the drift this is for."""
        v = validate_column_edges(
            graph_with({}), FakeGlue({"db.curated_account": {"balance"}}))
        assert not v[0].ok and "does not exist" in v[0].reason

    def test_a_missing_column_is_reported_with_the_table_named(self):
        v = validate_column_edges(
            graph_with({}),
            FakeGlue({"db.eod_account": {"other"}, "db.curated_account": {"balance"}}))
        assert not v[0].ok and "has no column BALANCE" in v[0].reason

    def test_a_column_inside_payload_after_json_validates(self):
        """CDC layers store the source row as JSON, so a business column is invisible to a
        schema lookup while being present in every row. Calling that unvalidatable would be
        wrong; calling it validated without looking would be worse."""
        v = validate_column_edges(
            graph_with({}),
            FakeGlue({"db.eod_account": {"payload_after", "op"},
                      "db.curated_account": {"balance"}}),
            FakePayload({"db.eod_account": {"balance", "account_id"}}))
        assert v[0].ok

    def test_a_key_absent_from_the_payload_does_not_validate(self):
        v = validate_column_edges(
            graph_with({}),
            FakeGlue({"db.eod_account": {"payload_after"}, "db.curated_account": {"balance"}}),
            FakePayload({"db.eod_account": {"account_id"}}))
        assert not v[0].ok and "payload_after has no key" in v[0].reason

    def test_an_unreadable_payload_does_not_silently_validate(self):
        """An empty table yields no keys. That is 'cannot confirm', never 'confirmed'."""
        v = validate_column_edges(
            graph_with({}),
            FakeGlue({"db.eod_account": {"payload_after"}, "db.curated_account": {"balance"}}),
            FakePayload({"db.eod_account": None}))
        assert not v[0].ok and "unreadable" in v[0].reason

    def test_without_a_payload_reader_a_json_column_is_not_assumed_valid(self):
        v = validate_column_edges(
            graph_with({}),
            FakeGlue({"db.eod_account": {"payload_after"}, "db.curated_account": {"balance"}}))
        assert not v[0].ok


class TestSummary:
    def test_the_summary_counts_and_never_rounds_a_failure_away(self):
        v = (EdgeVerdict("a", "x", "b", "y", True),
             EdgeVerdict("a", "z", "b", "w", False, "table a does not exist in Glue"))
        s = summarise(v)
        assert s["total"] == 2 and s["validated"] == 1 and s["failed"] == 1
        assert s["validated_pct"] == 50.0
        assert sum(s["failure_reasons"].values()) == 1

    def test_an_empty_graph_does_not_divide_by_zero(self):
        assert summarise(())["validated_pct"] == 0.0


class TestRecordedArtifact:
    """The copilot reads confidence from a recorded run, so the artifact is load-bearing."""

    ART = ROOT / "artifacts" / "validation" / "data-reliability" / "column-lineage-validation.json"

    def test_the_artifact_exists_and_names_its_method(self):
        assert self.ART.exists(), "confidence must come from a recorded run, not an assertion"

    def test_the_balance_edge_the_copilot_narrows_on_is_actually_validated(self):
        import json
        data = json.loads(self.ART.read_text())
        bal = [v for v in data["verdicts"]
               if v["upstream_column"].lower() == "balance"
               and "corebank_account" in v["upstream"]]
        assert bal, "no BALANCE edge recorded for the account table"
        assert all(v["validated"] for v in bal), \
            "the copilot narrows recovery on this edge; it must be confirmed, not assumed"

    def test_unvalidated_edges_are_kept_not_discarded(self):
        """Hiding the failures would make the artifact look like a clean bill of health."""
        import json
        data = json.loads(self.ART.read_text())
        assert any(not v["validated"] for v in data["verdicts"])
