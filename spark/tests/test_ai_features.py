"""Feature platform tests (AI-P5).

The point-in-time tests run against REAL local Spark, not a mock. A PIT join is a window
function over a left join with an inequality predicate; mocking it would test the mock.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "spark"))

from features.lineage import feature_lineage, model_lineage
from features.materialize import (ALLOWED_BATCH_SOURCE_LAYERS, MERGE_KEYS,
                                  MaterializationSpec, SourcePolicyViolation, merge_sql,
                                  resolve_source_layer, stamp_audit_columns,
                                  validate_frame)
from features.online_store import (DisabledOnlineStore, DynamoDbOnlineStore, FeatureRecord,
                                   InMemoryOnlineStore, OnlineStoreDisabled,
                                   build_online_store)
from features.point_in_time import (FutureLeakage, assert_event_time_column,
                                    assert_no_future_leakage, assert_no_horizon_leakage,
                                    point_in_time_join)

T = datetime(2026, 8, 20, 12, 0, 0)


# The `spark` fixture is session-scoped in conftest.py. It used to be defined here
# too, and a per-module `getOrCreate()` does NOT get the configs it asks for --
# there is one JVM per pytest run, so the first session built wins.


def _labels(spark, rows):
    return spark.createDataFrame(rows, "entity_id string, label_event_time timestamp, label int")


def _features(spark, rows):
    return spark.createDataFrame(
        rows, "entity_id string, feature_event_time timestamp, feature_version string, v double")


SPEC = MaterializationSpec(
    feature_group="customer_behavior", entity_keys=("entity_id",),
    feature_columns=("v",), feature_group_version="feature_group:abc123",
    source_layer="CURATED", source_tables=("curated.fact_account_daily_snapshot",),
    target_table="feature_offline.customer_behavior", cob_date="2026-08-20",
    run_id="run-1", watermark="2026-08-20T00:00:00Z")


# ------------------------------------------------------------- source policy
class TestSourcePolicy:
    @pytest.mark.parametrize("layer", ALLOWED_BATCH_SOURCE_LAYERS)
    def test_stable_layers_are_allowed_for_batch(self, layer):
        assert resolve_source_layer(layer) == layer

    @pytest.mark.parametrize("layer", ["FULL_CDC", "REALTIME", "STREAM"])
    def test_batch_features_may_not_read_raw_cdc(self, layer):
        """A batch feature job reading the change stream would make the feature store a
        second interpretation of it."""
        with pytest.raises(SourcePolicyViolation):
            resolve_source_layer(layer)

    def test_streaming_may_read_realtime(self):
        assert resolve_source_layer("REALTIME", streaming=True) == "REALTIME"

    def test_streaming_may_not_read_a_mart(self):
        with pytest.raises(SourcePolicyViolation):
            resolve_source_layer("MART", streaming=True)


# --------------------------------------------------------------- POINT IN TIME
class TestPointInTimeJoin:
    def test_latest_knowable_feature_is_selected(self, spark):
        lab = _labels(spark, [("e1", T, 1)])
        fea = _features(spark, [("e1", T - timedelta(days=3), "v1", 1.0),
                                ("e1", T - timedelta(days=1), "v1", 2.0),
                                ("e1", T + timedelta(days=1), "v1", 99.0)])
        r = point_in_time_join(lab, fea, entity_keys=["entity_id"]).collect()
        assert len(r) == 1 and r[0]["v"] == 2.0

    def test_feature_stamped_exactly_at_T_is_included(self, spark):
        """`<=`, not `<`: a value stamped at T was knowable at T."""
        lab = _labels(spark, [("e1", T, 1)])
        fea = _features(spark, [("e1", T, "v1", 7.0)])
        assert point_in_time_join(lab, fea, entity_keys=["entity_id"]).collect()[0]["v"] == 7.0

    def test_future_features_are_never_selected(self, spark):
        lab = _labels(spark, [("e1", T, 1)])
        fea = _features(spark, [("e1", T + timedelta(seconds=1), "v1", 99.0)])
        r = point_in_time_join(lab, fea, entity_keys=["entity_id"]).collect()
        assert len(r) == 1 and r[0]["v"] is None

    def test_missing_feature_yields_null_not_a_dropped_label(self, spark):
        """A left join: a label with no feature must survive, or the training set silently
        shrinks and the class balance moves."""
        lab = _labels(spark, [("e1", T, 1), ("e2", T, 0)])
        fea = _features(spark, [("e1", T - timedelta(days=1), "v1", 5.0)])
        r = {x["entity_id"]: x["v"] for x in
             point_in_time_join(lab, fea, entity_keys=["entity_id"]).collect()}
        assert r == {"e1": 5.0, "e2": None}

    def test_late_arriving_feature_is_used_only_from_its_event_time(self, spark):
        lab = _labels(spark, [("e1", T - timedelta(days=5), 1), ("e1", T, 1)])
        fea = _features(spark, [("e1", T - timedelta(days=1), "v1", 3.0)])
        got = sorted((x["label_event_time"], x["v"]) for x in
                     point_in_time_join(lab, fea, entity_keys=["entity_id"]).collect())
        assert got[0][1] is None and got[1][1] == 3.0

    def test_version_breaks_a_timestamp_tie_deterministically(self, spark):
        lab = _labels(spark, [("e1", T, 1)])
        fea = _features(spark, [("e1", T, "v1", 1.0), ("e1", T, "v2", 2.0)])
        assert point_in_time_join(lab, fea, entity_keys=["entity_id"]).collect()[0]["v"] == 2.0

    def test_processing_time_is_refused_as_a_join_key(self, spark):
        with pytest.raises(FutureLeakage):
            assert_event_time_column("created_at")
        lab = _labels(spark, [("e1", T, 1)])
        fea = _features(spark, [("e1", T, "v1", 1.0)])
        with pytest.raises(FutureLeakage):
            point_in_time_join(lab, fea, entity_keys=["entity_id"], event_time="created_at")

    def test_leakage_assertion_catches_a_bad_join(self, spark):
        bad = spark.createDataFrame(
            [("e1", T, T + timedelta(days=1))],
            "entity_id string, label_event_time timestamp, feature_event_time timestamp")
        with pytest.raises(FutureLeakage):
            assert_no_future_leakage(bad)

    def test_horizon_leakage_is_caught_when_plain_inequality_is_not(self, spark):
        """The check that catches REAL leakage on a forward label."""
        df = spark.createDataFrame(
            [("e1", T, T + timedelta(days=5))],
            "entity_id string, label_event_time timestamp, feature_event_time timestamp")
        assert_no_horizon_leakage(df, horizon_days=0)          # no horizon -> no-op
        with pytest.raises(FutureLeakage):
            assert_no_horizon_leakage(df, horizon_days=30)

    def test_missing_entity_key_is_refused(self, spark):
        lab = _labels(spark, [("e1", T, 1)])
        fea = _features(spark, [("e1", T, "v1", 1.0)])
        with pytest.raises(ValueError):
            point_in_time_join(lab, fea, entity_keys=["nope"])


# ------------------------------------------------------------ materialization
class TestMaterialization:
    def test_audit_columns_are_stamped(self, spark):
        df = _features(spark, [("e1", T, "x", 1.0)]).drop("feature_version")
        out = stamp_audit_columns(df, SPEC)
        for c in ("feature_version", "source_cob_date", "source_watermark",
                  "materialization_run_id", "created_at"):
            assert c in out.columns

    def test_merge_key_excludes_the_run_id(self):
        """Including the run id would make every rerun insert new rows and destroy the
        idempotence the run id exists to audit."""
        assert "materialization_run_id" not in MERGE_KEYS
        assert MERGE_KEYS == ("entity_id", "feature_event_time", "feature_version")

    def test_merge_sql_matches_on_the_grain_and_updates_the_rest(self):
        sql = merge_sql(SPEC, "src")
        assert "MERGE INTO feature_offline.customer_behavior" in sql
        for k in MERGE_KEYS:
            assert f"t.{k} = s.{k}" in sql
        assert "WHEN MATCHED THEN UPDATE SET" in sql and "WHEN NOT MATCHED THEN INSERT" in sql

    def test_duplicate_grain_is_rejected_before_writing(self, spark):
        df = stamp_audit_columns(
            _features(spark, [("e1", T, "x", 1.0), ("e1", T, "x", 2.0)]).drop("feature_version"),
            SPEC)
        with pytest.raises(ValueError, match="duplicate"):
            validate_frame(df, SPEC)

    def test_missing_event_time_is_rejected(self, spark):
        df = spark.createDataFrame([("e1", 1.0)], "entity_id string, v double")
        with pytest.raises(ValueError, match="feature_event_time"):
            validate_frame(stamp_audit_columns(df, SPEC), SPEC)

    def test_missing_declared_feature_column_is_rejected(self, spark):
        df = spark.createDataFrame(
            [("e1", T)], "entity_id string, feature_event_time timestamp")
        with pytest.raises(ValueError, match="missing feature columns"):
            validate_frame(stamp_audit_columns(df, SPEC), SPEC)

    def test_feature_version_change_produces_a_distinct_row(self, spark):
        """A version bump must not overwrite history -- point-in-time reconstruction of an
        older training set depends on the old values still being there."""
        a = stamp_audit_columns(_features(spark, [("e1", T, "x", 1.0)]).drop("feature_version"), SPEC)
        b = stamp_audit_columns(_features(spark, [("e1", T, "x", 2.0)]).drop("feature_version"),
                                MaterializationSpec(**{**SPEC.__dict__,
                                                       "feature_group_version": "feature_group:def456"}))
        keys = {(r["entity_id"], r["feature_event_time"], r["feature_version"])
                for r in a.collect() + b.collect()}
        assert len(keys) == 2


# ------------------------------------------------------------- online store
class TestOnlineStore:
    def test_default_is_disabled_and_fails_loudly(self):
        s = build_online_store("customer_behavior")
        assert isinstance(s, DisabledOnlineStore)
        with pytest.raises(OnlineStoreDisabled):
            s.get("customer_behavior", "e1")

    def test_flag_on_without_a_table_still_disabled(self, monkeypatch):
        monkeypatch.setenv("ENABLE_AI_ONLINE_FEATURE_STORE", "true")
        monkeypatch.delenv("AI_ONLINE_FEATURE_TABLE", raising=False)
        assert isinstance(build_online_store("g"), DisabledOnlineStore)

    def test_in_memory_round_trip(self):
        s = InMemoryOnlineStore()
        r = FeatureRecord("e1", "g", T, "v1", {"txn_count_7d": 3})
        s.put(r)
        assert s.get("g", "e1").values["txn_count_7d"] == 3
        assert s.get("g", "missing") is None

    def test_a_late_write_never_regresses_a_newer_value(self):
        """Out-of-order arrival is normal; silently going backwards is not."""
        s = InMemoryOnlineStore()
        s.put(FeatureRecord("e1", "g", T, "v1", {"v": "new"}))
        s.put(FeatureRecord("e1", "g", T - timedelta(days=1), "v1", {"v": "old"}))
        assert s.get("g", "e1").values["v"] == "new"

    def test_get_many_skips_absent_entities(self):
        s = InMemoryOnlineStore()
        s.put(FeatureRecord("e1", "g", T, "v1", {"v": 1}))
        assert set(s.get_many("g", ["e1", "e2"])) == {"e1"}

    def test_dynamodb_requires_a_table_name(self):
        with pytest.raises(ValueError):
            DynamoDbOnlineStore("")

    def test_dynamodb_without_a_client_fails_loudly(self):
        with pytest.raises(OnlineStoreDisabled):
            DynamoDbOnlineStore("tbl").get("g", "e1")

    def test_dynamodb_put_is_conditional_on_event_time(self):
        seen = {}
        class C:
            def put_item(self, **kw): seen.update(kw)
        DynamoDbOnlineStore("tbl", client=C()).put(
            FeatureRecord("e1", "g", T, "v1", {"v": 1}))
        assert "ConditionExpression" in seen
        assert "feature_event_time <= :t" in seen["ConditionExpression"]


# ----------------------------------------------------------------- lineage
class TestFeatureLineage:
    def test_model_lineage_is_read_from_the_dbt_manifest(self):
        ml = model_lineage(ROOT / "dbt" / "target" / "manifest.json")
        assert ml, "expected dbt models"
        assert all("refs" in v and "sources" in v for v in ml.values())

    def test_missing_manifest_yields_empty_not_error(self):
        assert model_lineage(ROOT / "nope.json") == {}

    def test_feature_lineage_resolves_against_dbt(self):
        import yaml
        g = yaml.safe_load((ROOT / "aiplatform" / "features" /
                            "customer_behavior.yaml").read_text())
        lin = feature_lineage(g, ROOT / "dbt" / "target" / "manifest.json")
        assert lin["feature_group"] == "customer_behavior"
        assert lin["features"] and all("sources" in f for f in lin["features"])

    def test_unresolvable_lineage_is_reported_not_hidden(self):
        g = {"name": "g", "features": [
            {"name": "f", "source_lineage": ["nonexistent_model.some_col"]}]}
        lin = feature_lineage(g, ROOT / "dbt" / "target" / "manifest.json")
        assert lin["unresolved"] == ["nonexistent_model.some_col"]

    def test_dbt_sources_resolve_not_only_models(self):
        """A feature computed from a dbt SOURCE reported `resolved: false` while the claim
        was perfectly valid — a resolver that knows half the graph reports the other half
        as broken. Found by running the lineage on the real pilot, 2026-08-26."""
        ml = model_lineage(ROOT / "dbt" / "target" / "manifest.json")
        kinds = {v.get("kind") for v in ml.values()}
        assert "source" in kinds and "model" in kinds
        assert "fact_transaction" in ml and ml["fact_transaction"]["kind"] == "source"

    def test_pilot_lineage_fully_resolves(self):
        import yaml
        g = yaml.safe_load((ROOT / "aiplatform" / "features" /
                            "customer_behavior.yaml").read_text())
        lin = feature_lineage(g, ROOT / "dbt" / "target" / "manifest.json")
        assert lin["unresolved"] == []

    def test_lineage_is_declared_a_projection_not_a_second_graph(self):
        lin = feature_lineage({"name": "g", "features": []},
                              ROOT / "dbt" / "target" / "manifest.json")
        assert "manifest.json" in lin["derived_from"]
