"""Agent tool layer tests (AI-P7).

The adversarial Athena tests matter most. Everything else guards a wrong answer; those guard
a mutation reaching the warehouse. Backends are injected, so nothing here needs AWS.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ai"))

from guards import GuardViolation
from agent_tools.athena_tool import (DEFAULT_LIMIT, MAX_LIMIT, enforce_limit, prepare, run_query)
from agent_tools.catalog import CATALOG, NOT_IMPLEMENTED, WRITE_TOOLS, call
from agent_tools.contract import (AUDIT_LOG, InputRejected, PermissionDenied, ResultTooLarge,
                            ToolError, ToolSpec, ToolTimeout, invoke)

MART = "kafka_dev_lab_dev_mart.mart_account_balance_daily"


@pytest.fixture(autouse=True)
def _clear_audit():
    AUDIT_LOG.clear()
    yield


# --------------------------------------------------- ATHENA ADVERSARIAL SUITE
class TestAthenaSafety:
    @pytest.mark.parametrize("sql,attack", [
        (f"SELECT 1 FROM {MART}; DROP TABLE x",                    "semicolon injection"),
        (f"SELECT 1 FROM {MART};SELECT 2 FROM {MART}",             "statement stacking"),
        (f"DROP TABLE {MART}",                                     "DDL: DROP"),
        (f"ALTER TABLE {MART} ADD COLUMN x INT",                   "DDL: ALTER"),
        (f"CREATE TABLE {MART}_z AS SELECT * FROM {MART}",         "CTAS"),
        (f"INSERT INTO {MART} VALUES (1)",                         "DML: INSERT"),
        (f"UPDATE {MART} SET x = 1",                               "DML: UPDATE"),
        (f"DELETE FROM {MART}",                                    "DML: DELETE"),
        (f"MERGE INTO {MART} t USING s ON 1=1",                    "DML: MERGE"),
        ("UNLOAD (SELECT 1) TO 's3://x' WITH (format='PARQUET')",  "UNLOAD"),
        (f"CALL system.rewrite_data_files('{MART}')",              "CALL"),
        (f"/* SELECT */ DELETE FROM {MART}",                       "comment hides a write"),
        (f"SELECT 1 FROM {MART} -- \nUNION SELECT 1 FROM {MART}\n;DROP TABLE x", "comment + stack"),
        (f"GRANT SELECT ON {MART} TO x",                           "GRANT"),
        (f"TRUNCATE TABLE {MART}",                                 "TRUNCATE"),
    ])
    def test_mutation_attempts_are_blocked(self, sql, attack):
        with pytest.raises((GuardViolation, PermissionDenied)):
            prepare(sql)

    @pytest.mark.parametrize("sql", [
        f"SELECT * FROM {MART}",
        f"SELECT business_date, count(*) FROM {MART} GROUP BY business_date",
        f"WITH x AS (SELECT 1 AS a FROM {MART}) SELECT * FROM x JOIN {MART} ON 1=1",
    ])
    def test_legitimate_selects_and_ctes_are_allowed(self, sql):
        assert "LIMIT" in prepare(sql).upper()

    def test_raw_cdc_databases_are_denied(self):
        for db in ("kafka_dev_lab_dev_full_cdc", "kafka_dev_lab_dev_stream",
                   "kafka_dev_lab_dev_quarantine"):
            with pytest.raises(PermissionDenied, match="denied|allow-listed"):
                prepare(f"SELECT * FROM {db}.cdc_events")

    def test_unapproved_database_is_denied(self):
        with pytest.raises(PermissionDenied):
            prepare("SELECT * FROM some_other_db.t")

    def test_unqualified_table_is_refused(self):
        """An unqualified name resolves against whatever the session default happens to be."""
        with pytest.raises(PermissionDenied):
            prepare("SELECT 1")

    def test_oversized_query_is_refused(self):
        with pytest.raises(PermissionDenied, match="bytes"):
            prepare(f"SELECT {'x,' * 3000} 1 FROM {MART}")

    def test_missing_limit_is_injected(self):
        assert prepare(f"SELECT * FROM {MART}").rstrip().endswith(f"LIMIT {DEFAULT_LIMIT}")

    def test_existing_larger_limit_is_tightened(self):
        assert "LIMIT 100" in prepare(f"SELECT * FROM {MART} LIMIT 99999", 100)

    def test_existing_smaller_limit_is_respected(self):
        assert "LIMIT 5" in prepare(f"SELECT * FROM {MART} LIMIT 5", 100)

    def test_limit_is_revalidated_after_injection(self):
        """A guard that checks the input then rewrites it has validated a different string."""
        with pytest.raises(PermissionDenied):
            enforce_limit(f"SELECT * FROM {MART}", MAX_LIMIT + 1)

    def test_trailing_semicolon_is_handled_not_stacked(self):
        assert "LIMIT" in prepare(f"SELECT * FROM {MART};")


class TestAthenaExecution:
    class _Client:
        def __init__(self, state="SUCCEEDED", rows=2, scanned=1234):
            self.state, self.rows, self.scanned = state, rows, scanned
            self.stopped = False
        def start_query_execution(self, **kw):
            self.sql = kw["QueryString"]; return {"QueryExecutionId": "qid-1"}
        def get_query_execution(self, **kw):
            return {"QueryExecution": {"Status": {"State": self.state,
                                                  "StateChangeReason": "boom"},
                                       "Statistics": {"DataScannedInBytes": self.scanned}}}
        def get_query_results(self, **kw):
            hdr = {"Data": [{"VarCharValue": "a"}]}
            return {"ResultSet": {"Rows": [hdr] + [{"Data": [{"VarCharValue": str(i)}]}
                                                   for i in range(self.rows)]}}
        def stop_query_execution(self, **kw): self.stopped = True

    def test_happy_path_returns_rows_and_query_id(self):
        r = run_query(f"SELECT a FROM {MART}", client=self._Client())
        assert r["row_count"] == 2 and r["resource_ids"] == ["qid-1"]
        assert r["bytes_scanned"] == 1234

    def test_failed_query_raises_with_the_reason(self):
        with pytest.raises(ToolError, match="boom"):
            run_query(f"SELECT a FROM {MART}", client=self._Client(state="FAILED"))

    def test_timeout_cancels_the_query_rather_than_abandoning_it(self):
        """An abandoned query keeps scanning and keeps billing."""
        c = self._Client(state="RUNNING")
        with pytest.raises(ToolTimeout):
            run_query(f"SELECT a FROM {MART}", client=c, timeout_seconds=0.01,
                      poll_seconds=0.01)
        assert c.stopped is True

    def test_result_limit_is_enforced(self):
        with pytest.raises(ResultTooLarge):
            run_query(f"SELECT a FROM {MART}", client=self._Client(rows=50), max_rows=10)

    def test_empty_result_is_not_an_error(self):
        class Empty(self._Client.__mro__[0]):
            def get_query_results(self, **kw): return {"ResultSet": {"Rows": []}}
        r = run_query(f"SELECT a FROM {MART}", client=Empty())
        assert r["row_count"] == 0 and r["rows"] == []


# ---------------------------------------------------------------- contract
class TestToolContract:
    SPEC = ToolSpec(name="t", version=1, description="d",
                    input_schema={"type": "object",
                                  "properties": {"q": {"type": "string", "maxLength": 10}},
                                  "required": ["q"]},
                    output_schema={"type": "object", "properties": {}},
                    authorization_class="public_metadata", max_result_bytes=200)

    def test_missing_required_argument_is_rejected(self):
        with pytest.raises(InputRejected, match="missing required"):
            invoke(self.SPEC, lambda q: {"ok": q}, {})

    def test_unknown_argument_is_rejected(self):
        """A silently-ignored key is a parameter the caller believes is in effect."""
        with pytest.raises(InputRejected, match="unknown argument"):
            invoke(self.SPEC, lambda q: {"ok": q}, {"q": "x", "sneaky": 1})

    def test_wrong_type_is_rejected(self):
        with pytest.raises(InputRejected):
            invoke(self.SPEC, lambda q: {"ok": q}, {"q": 123})

    def test_maxlength_is_enforced(self):
        with pytest.raises(InputRejected, match="maxLength"):
            invoke(self.SPEC, lambda q: {"ok": q}, {"q": "x" * 50})

    def test_oversized_result_is_refused_not_truncated(self):
        with pytest.raises(ResultTooLarge):
            invoke(self.SPEC, lambda q: {"big": "y" * 5000}, {"q": "x"})

    def test_a_mutation_cannot_hide_in_a_read_class(self):
        """ADR-057 refused EVERY mutating tool; ADR-092 permits exactly one class.

        What this test protected is unchanged and still holds: a read class may not mutate.
        Only the error message moved, because the rule became "the mutation class, or
        nothing" rather than "nothing".
        """
        with pytest.raises(ValueError, match="only the 'recovery_submit' class may mutate"):
            ToolSpec(name="w", version=1, description="d",
                     input_schema={"type": "object", "properties": {}},
                     output_schema={"type": "object", "properties": {}},
                     authorization_class="ops_read", read_only=False)

    def test_an_arbitrary_mutating_tool_still_cannot_be_declared(self):
        """The heart of ADR-057, preserved under ADR-092: a tool this repository has not
        named cannot mutate, whatever class it claims."""
        with pytest.raises(ValueError, match="closed mutation set"):
            ToolSpec(name="w", version=1, description="d",
                     input_schema={"type": "object", "properties": {}},
                     output_schema={"type": "object", "properties": {}},
                     authorization_class="recovery_submit", read_only=False,
                     requires_approval_ref=True, requires_idempotency_key=True)

    def test_an_unknown_authorization_class_is_refused(self):
        with pytest.raises(ValueError, match="not one of"):
            ToolSpec(name="m", version=1, description="d",
                     input_schema={"type": "object", "properties": {}},
                     output_schema={"type": "object", "properties": {}},
                     authorization_class="mutating")


# ------------------------------------------------------------------- audit
class TestAudit:
    SPEC = TestToolContract.SPEC

    def test_success_is_audited_with_duration_and_hash(self):
        invoke(self.SPEC, lambda q: {"ok": q}, {"q": "hello"}, actor="s1")
        a = AUDIT_LOG[-1]
        assert a.status == "SUCCEEDED" and a.actor == "s1"
        assert a.duration_ms >= 0 and len(a.input_hash) == 16
        assert a.tool_name == "t" and a.tool_version == 1

    def test_rejection_is_audited_too(self):
        with pytest.raises(InputRejected):
            invoke(self.SPEC, lambda q: {"ok": q}, {})
        assert AUDIT_LOG[-1].status == "REJECTED"

    def test_backend_error_is_audited_as_error(self):
        def boom(q): raise RuntimeError("backend down")
        with pytest.raises(RuntimeError):
            invoke(self.SPEC, boom, {"q": "x"})
        assert AUDIT_LOG[-1].status == "ERROR"
        assert AUDIT_LOG[-1].error_class == "RuntimeError"

    def test_audit_never_records_the_raw_input(self):
        """A question can contain a customer name; its hash cannot."""
        invoke(self.SPEC, lambda q: {"ok": q}, {"q": "secretval"})
        assert "secretval" not in str(AUDIT_LOG[-1].to_dict())

    def test_request_id_is_carried_through(self):
        invoke(self.SPEC, lambda q: {"ok": q}, {"q": "x"}, request_id="req-42")
        assert AUDIT_LOG[-1].request_id == "req-42"


# ---------------------------------------------------------------- catalog
class TestCatalog:
    def test_write_tools_is_empty(self):
        assert WRITE_TOOLS == {}

    def test_every_tool_is_read_only_and_v1_authorised(self):
        for name, (spec, _) in CATALOG.items():
            assert spec.read_only, name
            assert spec.authorization_class in ("public_metadata", "governed_read", "ops_read")
            assert spec.timeout_seconds > 0 and spec.max_rows > 0

    def test_unimplemented_tools_are_absent_and_explained(self):
        """A tool that answers plausibly from nothing is worse than a missing tool."""
        for name in ("get_reconciliation_status", "get_feature_value", "run_model_inference"):
            assert name not in CATALOG
            assert NOT_IMPLEMENTED[name]

    def test_calling_an_unimplemented_tool_explains_why(self):
        with pytest.raises(ToolError, match="not implemented"):
            call("run_model_inference", {})

    def test_unknown_tool_is_refused(self):
        with pytest.raises(ToolError, match="unknown tool"):
            call("rm_minus_rf", {})

    def test_no_shell_or_terraform_tool_exists(self):
        joined = " ".join(CATALOG).lower()
        for banned in ("shell", "exec", "terraform", "delete", "write", "apply"):
            assert banned not in joined

    def test_knowledge_tool_returns_citations(self):
        r = call("retrieve_knowledge", {"question": "point in time correctness", "top_k": 2})
        assert r["chunks"] and all(c["citation"] and c["source_path"] for c in r["chunks"])
        assert r["corpus_version"].startswith("corpus:")

    def test_table_schema_denies_a_bi_denied_dataset(self):
        """The AI plane inherits the BI role's access."""
        with pytest.raises(PermissionDenied, match="bi_access"):
            call("get_table_schema", {"dataset": "snapshot.banking_customer"})

    def test_table_schema_happy_path(self):
        r = call("get_table_schema", {"dataset": "mart.fact_transaction"})
        assert r["classification"] and r["grain"]

    def test_lineage_reports_declared_edges_as_declared(self):
        r = call("get_data_lineage", {"dataset": "mart.dim_customer"})
        assert "declared" in r["note"]

    def test_unknown_dataset_is_an_explicit_error(self):
        with pytest.raises(ToolError):
            call("get_table_schema", {"dataset": "does.not_exist"})

    def test_feature_definition_includes_lineage(self):
        r = call("get_feature_definition", {"feature_group": "customer_behavior"})
        assert r["features"] and r["unresolved_lineage"] == []

    def test_model_status_flags_a_synthetic_label(self):
        r = call("get_model_status", {})
        assert any(m["synthetic_label"] for m in r["models"])
        assert "synthetic" in r["caveat"]

    def test_the_new_package_does_not_shadow_the_session_16_assistant(self):
        """`ai/agent_tools/` was originally `ai/tools/`, which SHADOWED the existing
        `ai/tools.py`. The Session-16 assistant imports `tools` and calls `tools.call`,
        so its evaluation broke with AttributeError while every new test still passed —
        a regression invisible from inside the new suite. Found 2026-08-26."""
        import importlib
        sys.path.insert(0, str(ROOT / "ai"))
        legacy = importlib.import_module("tools")
        assert hasattr(legacy, "call") and hasattr(legacy, "TOOLS")
        assert not (ROOT / "ai" / "tools").is_dir(), \
            "ai/tools/ as a package shadows ai/tools.py"

    def test_pipeline_status_rejects_an_invalid_job_id(self):
        with pytest.raises(PermissionDenied):
            call("get_pipeline_status", {"job_id": "../../etc/passwd"})
