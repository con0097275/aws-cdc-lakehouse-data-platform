"""AI platform contract tests (AI-P1).

The point-in-time tests are the ones that matter. Everything else here guards a rule that
would produce a wrong config; the PIT tests guard a rule that would produce a model which
scores well and is worthless -- the failure mode that survives longest because nothing
looks broken.

No AWS, no Spark, no network. Runs in well under a second.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from aiplatform.agents import (AgentDefinition, AuthorizationClass, ToolContract,
                               WRITE_TOOLS)
from aiplatform.classification import Classification, CostClass
from aiplatform.evaluation import EvalQuestion, EvaluationDataset, ExpectedMode
from aiplatform.features import (FeatureDefinition, FeatureGroup, FutureLeakage,
                                 assert_join_key_is_event_time,
                                 assert_label_horizon_excluded,
                                 assert_no_future_leakage, select_point_in_time)
from aiplatform.knowledge import Chunk, ContractViolation, KnowledgeSource, is_excluded
from aiplatform.models import (InferenceMode, ModelDefinition, SplitStrategy,
                               TrainingDataset)
from aiplatform.ops import ALL_TABLES, TABLES_TO_CREATE, render_ddl
from aiplatform.versioning import content_hash, typed_version

T = datetime(2026, 8, 20, 12, 0, 0)
SCHEMA = {"type": "object", "properties": {"q": {"type": "string"}}}


def _feature(name="txn_count_7d"):
    return FeatureDefinition(name, "bigint", "desc", "risk-data")


def _group(**kw):
    base = dict(name="customer_behavior", entity_keys=("customer_sk",),
                event_time_column="feature_event_time", owner="risk-data",
                domain="banking", features=(_feature(),))
    base.update(kw)
    return FeatureGroup(**base)


def _tool(**kw):
    base = dict(tool_name="retrieve_knowledge", description="d", input_schema=SCHEMA,
                output_schema=SCHEMA,
                authorization_class=AuthorizationClass.PUBLIC_METADATA)
    base.update(kw)
    return ToolContract(**base)


# ----------------------------------------------------------------- versioning
class TestVersioning:
    def test_version_is_content_addressed_not_random(self):
        assert typed_version("corpus", {"a": 1}) == typed_version("corpus", {"a": 1})

    def test_key_order_does_not_change_the_version(self):
        assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})

    def test_different_content_gives_a_different_version(self):
        assert typed_version("corpus", {"a": 1}) != typed_version("corpus", {"a": 2})

    def test_unknown_kind_is_rejected(self):
        with pytest.raises(ValueError):
            typed_version("not_a_kind", {})


# ------------------------------------------------------------ classification
class TestClassification:
    def test_unknown_classification_fails_closed_to_restricted(self):
        assert Classification.parse("nonsense") is Classification.RESTRICTED

    def test_missing_classification_fails_closed(self):
        assert Classification.parse(None) is Classification.RESTRICTED

    def test_restricted_is_not_retrievable(self):
        assert not Classification.RESTRICTED.retrievable_by_ai()

    def test_always_on_requires_cost_review(self):
        assert CostClass.ALWAYS_ON.requires_cost_review()
        assert not CostClass.PER_REQUEST.requires_cost_review()


# ---------------------------------------------------------------- knowledge
class TestKnowledgeContracts:
    def test_valid_source(self):
        assert KnowledgeSource("adr_corpus", "adr", ("docs/adr/*.md",),
                               "data-platform", "ops").version.startswith("corpus:")

    @pytest.mark.parametrize("pattern", ["terraform/*.tf", "artifacts/x.txt",
                                         "env/.env", "keys/id_rsa"])
    def test_excluded_paths_are_rejected(self, pattern):
        assert is_excluded(pattern)
        with pytest.raises(ContractViolation):
            KnowledgeSource("s_x", "adr", (pattern,), "o", "d")

    def test_restricted_source_is_never_indexed(self):
        with pytest.raises(ContractViolation):
            KnowledgeSource("s_y", "adr", ("docs/*.md",), "o", "d",
                            classification=Classification.RESTRICTED)

    def test_unknown_document_type_rejected(self):
        with pytest.raises(ContractViolation):
            KnowledgeSource("s_z", "not_a_type", ("docs/*.md",), "o", "d")

    def test_chunk_without_classification_is_rejected(self):
        """Absent classification lands on RESTRICTED, so the chunk is withheld."""
        with pytest.raises(ContractViolation):
            Chunk("c1", "d1", 0, "h", "chunking:v1", "text", metadata={})

    def test_chunk_id_is_deterministic(self):
        a = Chunk.make_id("doc", 3, "hello")
        assert a == Chunk.make_id("doc", 3, "hello") != Chunk.make_id("doc", 3, "world")

    def test_document_rejects_a_non_hex_commit(self):
        from aiplatform.knowledge import KnowledgeDocument
        with pytest.raises(ContractViolation):
            KnowledgeDocument("d", "adr", "docs/a.md", "s3://x", "not-a-sha", "h",
                              "o", "ops", "dev", Classification.INTERNAL, "v1",
                              "corpus:x", T, T)


# ------------------------------------------------------------------ features
class TestFeatureContracts:
    def test_valid_group(self):
        assert _group().group_version.startswith("feature_group:")

    def test_event_time_is_mandatory(self):
        with pytest.raises(ContractViolation):
            _group(event_time_column="")

    def test_processing_time_cannot_be_the_event_time_column(self):
        with pytest.raises(ContractViolation):
            _group(event_time_column="created_at")

    def test_processing_time_cannot_be_a_feature(self):
        with pytest.raises(ContractViolation):
            FeatureDefinition("created_at", "timestamp", "d", "o")

    def test_partitioning_on_the_entity_key_is_rejected(self):
        with pytest.raises(ContractViolation):
            _group(partition_by=("customer_sk",))

    def test_duplicate_feature_names_rejected(self):
        with pytest.raises(ContractViolation):
            _group(features=(_feature(), _feature()))

    def test_streaming_features_are_not_available(self):
        with pytest.raises(ContractViolation):
            _group(streaming_enabled=True)

    def test_online_store_without_offline_is_rejected(self):
        with pytest.raises(ContractViolation):
            _group(batch_enabled=False, online_store_enabled=True)

    def test_row_columns_keep_event_and_processing_time_distinct(self):
        cols = _group().row_columns()
        assert "feature_event_time" in cols and "created_at" in cols
        assert cols.index("feature_event_time") < cols.index("created_at")


# -------------------------------------------------------------- POINT IN TIME
class TestPointInTimeCorrectness:
    """The three leakage tests AI-P6 must satisfy, contracted here first."""

    def test_a_feature_from_the_past_is_allowed(self):
        assert_no_future_leakage(T - timedelta(days=1), T)

    def test_a_feature_stamped_exactly_at_T_is_allowed(self):
        """`<=`, not `<`: a value stamped at T was knowable at T."""
        assert_no_future_leakage(T, T)

    def test_a_future_feature_is_rejected(self):
        with pytest.raises(FutureLeakage):
            assert_no_future_leakage(T + timedelta(seconds=1), T)

    def test_processing_time_is_never_a_valid_join_key(self):
        with pytest.raises(FutureLeakage):
            assert_join_key_is_event_time("created_at")

    def test_label_horizon_window_is_excluded(self):
        """The test that catches REAL leakage on a forward-looking label."""
        horizon = T + timedelta(days=30)
        with pytest.raises(FutureLeakage):
            assert_label_horizon_excluded(T + timedelta(days=5), T, horizon)

    def test_feature_before_the_label_is_outside_the_horizon(self):
        assert_label_horizon_excluded(T - timedelta(days=5), T, T + timedelta(days=30))

    def test_pit_select_picks_the_latest_knowable_row(self):
        rows = [{"feature_event_time": T - timedelta(days=3), "v": "old"},
                {"feature_event_time": T - timedelta(days=1), "v": "newest"},
                {"feature_event_time": T + timedelta(days=1), "v": "FUTURE"}]
        assert select_point_in_time(rows, T)["v"] == "newest"

    def test_pit_select_returns_none_when_nothing_was_knowable(self):
        rows = [{"feature_event_time": T + timedelta(days=1), "v": "future"}]
        assert select_point_in_time(rows, T) is None

    def test_pit_tie_is_broken_by_feature_version_not_arbitrarily(self):
        rows = [{"feature_event_time": T, "feature_version": 1, "v": "v1"},
                {"feature_event_time": T, "feature_version": 2, "v": "v2"}]
        assert select_point_in_time(rows, T)["v"] == "v2"


# -------------------------------------------------------------------- models
class TestModelContracts:
    def _ds(self, **kw):
        base = dict(name="lapse_30d", label_name="lapsed", label_event_time_column="t",
                    feature_groups=("customer_behavior",), train_start="2026-01-01",
                    train_end="2026-07-31", label_horizon_days=30)
        base.update(kw)
        return TrainingDataset(**base)

    def test_random_split_is_rejected(self):
        with pytest.raises(ContractViolation):
            self._ds(split=SplitStrategy.RANDOM)

    def test_realtime_inference_is_rejected(self):
        with pytest.raises(ContractViolation):
            ModelDefinition("lapse_model", "d", "o", self._ds(), "logreg",
                            inference_mode=InferenceMode.REALTIME)

    def test_model_without_metrics_is_rejected(self):
        with pytest.raises(ContractViolation):
            ModelDefinition("lapse_model", "d", "o", self._ds(), "logreg", metrics=())

    def test_model_version_changes_with_the_dataset(self):
        a = ModelDefinition("lapse_model", "d", "o", self._ds(), "logreg")
        b = ModelDefinition("lapse_model", "d", "o", self._ds(train_end="2026-08-31"), "logreg")
        assert a.version_id != b.version_id


# -------------------------------------------------------------- agent / tools
class TestAgentToolContracts:
    def test_write_tools_is_empty_and_must_stay_empty(self):
        assert WRITE_TOOLS == {}

    def test_mutating_authorization_class_is_rejected(self):
        with pytest.raises(ContractViolation):
            _tool(tool_name="t_mut", authorization_class=AuthorizationClass.MUTATING)

    def test_there_is_no_shell_tool(self):
        with pytest.raises(ContractViolation):
            _tool(tool_name="t_sh", authorization_class=AuthorizationClass.SHELL)

    def test_read_only_cannot_be_switched_off(self):
        with pytest.raises(ContractViolation):
            _tool(tool_name="t_rw", read_only=False)

    def test_audit_cannot_be_disabled(self):
        with pytest.raises(ContractViolation):
            _tool(tool_name="t_na", audited=False)

    def test_unvalidated_input_schema_is_rejected(self):
        with pytest.raises(ContractViolation):
            _tool(tool_name="t_ns", input_schema={})

    @pytest.mark.parametrize("kw", [{"timeout_seconds": 0}, {"timeout_seconds": 10_000},
                                    {"max_rows": 0}, {"max_rows": 99_999}])
    def test_limits_are_bounded(self, kw):
        with pytest.raises(ContractViolation):
            _tool(tool_name="t_lim", **kw)

    def test_agent_model_id_must_be_pinned(self):
        with pytest.raises(ContractViolation):
            AgentDefinition("copilot", "d", (_tool(),), "anthropic.claude-latest", "p:v1")

    def test_agent_session_memory_is_off_in_v1(self):
        with pytest.raises(ContractViolation):
            AgentDefinition("copilot", "d", (_tool(),), "pinned-model", "p:v1",
                            session_memory=True)

    def test_agent_requires_at_least_one_tool(self):
        with pytest.raises(ContractViolation):
            AgentDefinition("copilot", "d", (), "pinned-model", "p:v1")

    def test_agent_version_changes_when_a_tool_changes(self):
        a = AgentDefinition("copilot", "d", (_tool(),), "pinned-model", "p:v1")
        b = AgentDefinition("copilot", "d", (_tool(timeout_seconds=29),), "pinned-model", "p:v1")
        assert a.version_id != b.version_id


# ---------------------------------------------------------------- evaluation
class TestEvaluationContracts:
    def _q(self, **kw):
        base = dict(question_id="q1", question="who owns mart.x",
                    expect_mode=ExpectedMode.TOOL, expect_tool="explain_dataset")
        base.update(kw)
        return EvalQuestion(**base)

    def _adv(self):
        return EvalQuestion("adv1", "drop table mart.x", ExpectedMode.REFUSAL,
                            adversarial=True)

    def test_tool_question_needs_an_expected_tool(self):
        with pytest.raises(ContractViolation):
            EvalQuestion("q", "x", ExpectedMode.TOOL)

    def test_retrieval_question_needs_a_grounding_source(self):
        with pytest.raises(ContractViolation):
            EvalQuestion("q", "x", ExpectedMode.RETRIEVAL)

    def test_dataset_without_adversarial_questions_is_rejected(self):
        with pytest.raises(ContractViolation):
            EvaluationDataset("golden", (self._q(),))

    def test_duplicate_question_ids_rejected(self):
        with pytest.raises(ContractViolation):
            EvaluationDataset("golden", (self._q(), self._q(), self._adv()))

    def test_retrieval_gate_needs_at_least_fifty_questions(self):
        d = EvaluationDataset("golden", (self._q(), self._adv()))
        assert not d.meets_retrieval_gate()


# ---------------------------------------------------------------------- ops
class TestOpsContracts:
    def test_every_table_names_a_consumer(self):
        assert all(t.consumer.strip() for t in ALL_TABLES)

    def test_feature_materialization_run_is_deferred_not_created(self):
        """It would duplicate ops.job_master_execution_hist. AI-P5 justifies or reuses."""
        names = [t.name for t in TABLES_TO_CREATE]
        assert "ops.feature_materialization_run" not in names

    def test_four_tables_are_created(self):
        assert len(TABLES_TO_CREATE) == 4

    def test_ddl_is_iceberg_v2_and_partitioned(self):
        for t in TABLES_TO_CREATE:
            ddl = render_ddl(t)
            assert "USING iceberg" in ddl
            assert "'format-version' = '2'" in ddl
            assert f"PARTITIONED BY ({t.partition_by})" in ddl

    def test_agent_history_stores_a_question_hash_not_the_question(self):
        cols = [c for c, _ in
                next(t for t in ALL_TABLES if t.name.endswith("agent_execution_hist")).columns]
        assert "question_hash" in cols and "question" not in cols


# ------------------------------------------------------------------ compiler
class TestCompiler:
    def test_repo_config_compiles(self):
        from aiplatform.compile import compile_plan
        plan = compile_plan()
        assert plan["objects"]["features"] and plan["objects"]["agents"]

    def test_plan_hash_is_deterministic_and_ignores_the_timestamp(self):
        from aiplatform.compile import compile_plan
        a, b = compile_plan(), compile_plan()
        assert a["plan_hash"] == b["plan_hash"]
        assert a["generated_at"] or True  # present, but not part of the hash

    def test_serialization_round_trip(self):
        import json
        from aiplatform.compile import compile_plan
        plan = compile_plan()
        assert json.loads(json.dumps(plan, sort_keys=True))["plan_hash"] == plan["plan_hash"]


# ------------------------------------------------------- deletability of ai/
class TestAiRemainsDeletable:
    def test_aiplatform_does_not_import_the_assistant(self):
        """`ai/` must stay deletable; aiplatform may be imported by pipeline code."""
        import pathlib
        root = pathlib.Path(__file__).resolve().parents[2] / "aiplatform"
        for p in root.rglob("*.py"):
            src = p.read_text()
            assert "import ai\n" not in src and "from ai " not in src, p
