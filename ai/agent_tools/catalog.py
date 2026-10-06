"""The V1 tool catalog. Read-only, every tool backed by something that actually exists.

A tool that returns a plausible answer from nothing is worse than a missing tool: the model
cannot tell the difference, and neither can the reader of its answer. So each entry below
names the artifact it reads, and tools whose backing does not exist are ABSENT rather than
stubbed.
"""

from __future__ import annotations

import glob
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ai"))
sys.path.insert(0, str(ROOT / "spark"))

from agent_tools.athena_tool import DEFAULT_LIMIT, MAX_LIMIT, run_query
from agent_tools.contract import PermissionDenied, ToolError, ToolSpec, invoke

_S = lambda **p: {"type": "object", "properties": p, "additionalProperties": False}


# --------------------------------------------------------------- knowledge
SPEC_RETRIEVE = ToolSpec(
    name="retrieve_knowledge", version=1,
    description="Search the published knowledge corpus. Returns cited chunks.",
    input_schema={**_S(question={"type": "string", "maxLength": 1000},
                       top_k={"type": "integer"},
                       document_type={"type": "string", "maxLength": 40}),
                  "required": ["question"]},
    output_schema=_S(chunks={"type": "array"}, corpus_version={"type": "string"}),
    authorization_class="public_metadata", timeout_seconds=15.0, max_rows=20)


def retrieve_knowledge(question: str, top_k: int = 5, document_type: str | None = None) -> dict:
    from ai.retrieval.service import RetrievalRequest, build_default
    dirs = sorted(glob.glob(str(ROOT / "ai" / "knowledge" / "corpus_*")))
    if not dirs:
        raise ToolError("no published corpus — run `make ai-corpus-build`")
    manifest = json.loads(Path(dirs[-1], "manifest.json").read_text())
    backend = build_default(dirs[-1])
    filters = {"document_type": document_type} if document_type else {}
    hits = backend.search(RetrievalRequest(question, top_k=min(top_k, 20), filters=filters))
    return {
        "chunks": [{
            "text": h.text[:2000],
            "score": h.score,
            "citation": h.citation.render(),
            "source_path": h.citation.source_path,
            "document_id": h.citation.document_id,
            "heading_path": list(h.citation.heading_path),
        } for h in hits],
        "corpus_version": manifest["corpus_version"],
        "chunking_version": manifest["chunking_version"],
        "backend": backend.name,
    }


# ------------------------------------------------------------------ athena
SPEC_QUERY_ATHENA = ToolSpec(
    name="query_athena", version=1,
    description="Run a READ-ONLY SELECT over allow-listed MART/CURATED/OPS databases.",
    input_schema={**_S(sql={"type": "string", "maxLength": 4000},
                       limit={"type": "integer"}),
                  "required": ["sql"]},
    output_schema=_S(rows={"type": "array"}, row_count={"type": "integer"}),
    authorization_class="governed_read", timeout_seconds=60.0, max_rows=MAX_LIMIT)


def query_athena(sql: str, limit: int = DEFAULT_LIMIT, _spec=None) -> dict:
    return run_query(sql, limit, timeout_seconds=(_spec.timeout_seconds if _spec else 60.0),
                     max_rows=(_spec.max_rows if _spec else MAX_LIMIT))


# ------------------------------------------------------------ table schema
SPEC_TABLE_SCHEMA = ToolSpec(
    name="get_table_schema", version=1,
    description="Columns, owner, grain, classification and retention for one dataset.",
    input_schema={**_S(dataset={"type": "string", "maxLength": 120}),
                  "required": ["dataset"]},
    output_schema=_S(columns={"type": "array"}),
    authorization_class="public_metadata", timeout_seconds=10.0)


def get_table_schema(dataset: str) -> dict:
    import yaml
    reg = yaml.safe_load((ROOT / "governance" / "catalog" / "domains.yml").read_text())
    entry = next((d for d in reg["datasets"] if d["name"] == dataset), None)
    if entry is None:
        close = [d["name"] for d in reg["datasets"] if dataset.lower() in d["name"].lower()]
        raise ToolError(f"{dataset!r} is not in the registry"
                        + (f"; did you mean {close}?" if close else ""))
    if entry.get("bi_access") == "denied":
        raise PermissionDenied(
            f"{dataset}: bi_access=denied. The AI plane inherits the BI role's access, so a "
            "dataset BI may not read is not readable here either.")
    return {"dataset": dataset, "layer": entry["layer"], "domain": entry["domain"],
            "grain": entry["grain"], "primary_key": entry["primary_key"],
            "classification": entry["classification"],
            "retention_days": entry["retention_days"],
            "pii_columns": entry.get("pii_columns", []),
            "columns": entry.get("columns", []),
            "source": "governance/catalog/domains.yml"}


# ---------------------------------------------------------------- lineage
SPEC_LINEAGE = ToolSpec(
    name="get_data_lineage", version=1,
    description="Upstream and downstream for a dataset, from the lineage graph and dbt.",
    input_schema={**_S(dataset={"type": "string", "maxLength": 120}),
                  "required": ["dataset"]},
    output_schema=_S(upstream={"type": "array"}, downstream={"type": "array"}),
    authorization_class="public_metadata", timeout_seconds=15.0)


def get_data_lineage(dataset: str) -> dict:
    import yaml
    graph = yaml.safe_load(
        (ROOT / "governance" / "lineage" / "openlineage.yml").read_text())["lineage_graph"]
    up = [{"job": j["job"], "type": j["type"], "evidence": j["evidence"],
           "reads": j["inputs"]} for j in graph if dataset in j["outputs"]]
    down = [{"job": j["job"], "writes": j["outputs"]} for j in graph if dataset in j["inputs"]]
    if not up and not down:
        raise ToolError(f"{dataset!r} does not appear in the lineage graph")
    return {"dataset": dataset, "upstream": up, "downstream": down,
            "note": "edges marked evidence=declared are asserted from config, not observed "
                    "telemetry — Debezium emits no OpenLineage",
            "source": "governance/lineage/openlineage.yml"}


# --------------------------------------------------------- pipeline status
SPEC_PIPELINE = ToolSpec(
    name="get_pipeline_status", version=1,
    description="Execution status and watermark for a reporting job, from runtime state.",
    input_schema={**_S(job_id={"type": "string", "maxLength": 80},
                       flow_mode={"type": "string", "maxLength": 24}),
                  "required": ["job_id"]},
    output_schema=_S(watermark={"type": "string"}, executions={"type": "array"}),
    authorization_class="ops_read", timeout_seconds=20.0, max_rows=25)


def get_pipeline_status(job_id: str, flow_mode: str = "EOD", ddb=None) -> dict:
    if not job_id.replace("_", "").isalnum():
        raise PermissionDenied(f"invalid job_id {job_id!r}")
    if ddb is None:
        import boto3
        ddb = boto3.client("dynamodb")
    pfx = os.environ.get("AI_DDB_PREFIX", "kafka-dev-lab-dev")
    wm = ddb.get_item(TableName=f"{pfx}-job-watermark-state",
                      Key={"watermark_key": {"S": f"{job_id}#{flow_mode}"}}).get("Item")
    return {
        "job_id": job_id, "flow_mode": flow_mode,
        "watermark": (wm or {}).get("last_success_date_of_data", {}).get("S"),
        "last_success_execution_id": (wm or {}).get("last_success_execution_id", {}).get("S"),
        "updated_at": (wm or {}).get("updated_at", {}).get("S"),
        "executions": [],
        "caveat": "a watermark is a CLAIM. Cross-check the target table exists and has rows "
                  "— a job can report SUCCEEDED and produce nothing (docs/VERIFY_END_TO_END.md §3).",
        "source": "DynamoDB runtime state (ADR-036)",
    }


# ----------------------------------------------------- analytics (AI v2)
# These three answer the questions a bare SUM cannot: why a metric moved, what it will be,
# and what to check when the mart looks wrong. The COMPUTATION is deterministic and lives in
# ai/analytics/; these are thin, audited wrappers. No model is involved, so the answers are
# reproducible and reviewable — and they cost $0 beyond the Athena scan.

SPEC_DIAGNOSE = ToolSpec(
    name="diagnose_metric", version=1,
    description="Explain why a declared metric moved on a date: completeness first, then "
                "baseline comparison, then per-dimension contribution.",
    input_schema={**_S(metric={"type": "string", "maxLength": 80},
                       target_date={"type": "string", "maxLength": 10},
                       dimension={"type": "string", "maxLength": 60},
                       baseline_days={"type": "integer"}),
                  "required": ["metric", "target_date"]},
    output_schema=_S(verdict={"type": "string"}, contributions={"type": "array"}),
    authorization_class="governed_read", timeout_seconds=45.0)


def diagnose_metric(metric: str, target_date: str, dimension: str | None = None,
                    baseline_days: int = 7, source=None) -> dict:
    from analytics.datasource import AthenaSource
    from analytics.diagnose import diagnose
    from analytics.registry import get_metric
    spec = get_metric(metric)
    return diagnose(spec, source or AthenaSource(), target_date,
                    baseline_days=baseline_days, dimension=dimension).to_dict()


SPEC_FORECAST = ToolSpec(
    name="forecast_metric", version=1,
    description="Forecast the next value of a declared metric, with the backtested error "
                "of the chosen method. Refuses on insufficient history.",
    input_schema={**_S(metric={"type": "string", "maxLength": 80},
                       as_of={"type": "string", "maxLength": 10},
                       history_days={"type": "integer"}),
                  "required": ["metric", "as_of"]},
    output_schema=_S(point={"type": "number"}, method={"type": "string"}),
    authorization_class="governed_read", timeout_seconds=45.0)


def forecast_metric(metric: str, as_of: str, history_days: int = 28, source=None) -> dict:
    from analytics.datasource import AthenaSource
    from analytics.forecast import forecast
    from analytics.registry import get_metric
    return forecast(get_metric(metric), source or AthenaSource(),
                    as_of=as_of, history_days=history_days).to_dict()


SPEC_GOVERNANCE = ToolSpec(
    name="governance_review", version=1,
    description="Rank what to check on a dataset: freshness, completeness, watermark-vs-"
                "reality, volume. PROPOSES fixes; executes nothing.",
    input_schema={**_S(metric={"type": "string", "maxLength": 80},
                       as_of={"type": "string", "maxLength": 10},
                       watermark={"type": "string", "maxLength": 10}),
                  "required": ["metric", "as_of"]},
    output_schema=_S(verdict={"type": "string"}, findings={"type": "array"}),
    authorization_class="ops_read", timeout_seconds=45.0)


def governance_review(metric: str, as_of: str, watermark: str | None = None,
                      source=None) -> dict:
    from analytics.datasource import AthenaSource
    from analytics.governance import review
    from analytics.registry import get_metric
    return review(get_metric(metric), source or AthenaSource(),
                  as_of=as_of, watermark=watermark).to_dict()


SPEC_LIST_METRICS = ToolSpec(
    name="list_metrics", version=1,
    description="The declared metric registry: what the copilot may reason about.",
    input_schema=_S(), output_schema=_S(metrics={"type": "array"}),
    authorization_class="public_metadata", timeout_seconds=5.0)


def list_metrics() -> dict:
    from analytics.registry import METRICS
    return {"metrics": [{"name": m.name, "dataset": m.dataset, "measure": m.measure,
                         "grain": m.grain, "owner": m.owner, "additive": m.additive,
                         "dimensions": list(m.dimensions), "unit": m.unit,
                         "description": m.description} for m in METRICS.values()]}


# ------------------------------------------------------------------- DQ
SPEC_DQ = ToolSpec(
    name="get_dq_results", version=1,
    description="Data-quality rules and severities declared for a dataset.",
    input_schema={**_S(dataset={"type": "string", "maxLength": 120}),
                  "required": ["dataset"]},
    output_schema=_S(checks={"type": "array"}),
    authorization_class="ops_read", timeout_seconds=10.0)


def get_dq_results(dataset: str) -> dict:
    import yaml
    rules = yaml.safe_load((ROOT / "governance" / "dq" / "rules.yml").read_text())
    entry = next((d for d in rules["datasets"] if d["name"] == dataset), None)
    if entry is None:
        raise ToolError(f"no DQ rules declared for {dataset!r}")
    return {"dataset": dataset, "layer": entry.get("layer"),
            "predicate": entry.get("predicate"), "checks": entry.get("checks", []),
            "note": "these are DECLARED rules. Observed results live in ops.dq_result.",
            "source": "governance/dq/rules.yml"}


# ------------------------------------------------------- feature definition
SPEC_FEATURE_DEF = ToolSpec(
    name="get_feature_definition", version=1,
    description="Definition, owner, entity keys and lineage for a feature group.",
    input_schema={**_S(feature_group={"type": "string", "maxLength": 80}),
                  "required": ["feature_group"]},
    output_schema=_S(features={"type": "array"}),
    authorization_class="public_metadata", timeout_seconds=10.0)


def get_feature_definition(feature_group: str) -> dict:
    import yaml
    from features.lineage import feature_lineage
    p = ROOT / "aiplatform" / "features" / f"{feature_group}.yaml"
    if not p.exists():
        avail = [x.stem for x in (ROOT / "aiplatform" / "features").glob("*.yaml")]
        raise ToolError(f"no feature group {feature_group!r}; available: {avail}")
    g = yaml.safe_load(p.read_text())
    lin = feature_lineage(g, ROOT / "dbt" / "target" / "manifest.json")
    return {"feature_group": g["name"], "owner": g["owner"], "domain": g["domain"],
            "entity_keys": g["entity_keys"], "event_time_column": g["event_time_column"],
            "online_store_enabled": g.get("online_store", {}).get("enabled", False),
            "features": [{"name": f["name"], "type": f["type"],
                          "description": f.get("description", "").strip()}
                         for f in g["features"]],
            "lineage": lin["features"], "unresolved_lineage": lin["unresolved"],
            "source": str(p.relative_to(ROOT))}


# ---------------------------------------------------------- model status
SPEC_MODEL_STATUS = ToolSpec(
    name="get_model_status", version=1,
    description="Trained model versions, metrics and provenance.",
    input_schema=_S(model_name={"type": "string", "maxLength": 80}),
    output_schema=_S(models={"type": "array"}),
    authorization_class="ops_read", timeout_seconds=10.0)


def get_model_status(model_name: str | None = None) -> dict:
    out = []
    for p in sorted(glob.glob(str(ROOT / "artifacts" / "models" / "*" / "model.json"))):
        d = json.loads(Path(p).read_text())
        if model_name and d["model_name"] != model_name:
            continue
        out.append({k: d[k] for k in ("model_name", "model_version", "dataset_version",
                                      "feature_versions", "code_version", "metrics",
                                      "owner", "created_at", "synthetic_label")})
    if not out:
        raise ToolError(f"no trained model found"
                        + (f" for {model_name!r}" if model_name else ""))
    return {"models": out,
            "caveat": "models with synthetic_label=true were trained on a DEMO target and "
                      "carry no predictive meaning — see spark/ml/dataset.py.",
            "source": "artifacts/models/"}


# ---------------------------------------------------------------- registry
CATALOG: dict[str, tuple[ToolSpec, object]] = {
    "retrieve_knowledge":      (SPEC_RETRIEVE,       retrieve_knowledge),
    "query_athena":            (SPEC_QUERY_ATHENA,   query_athena),
    "get_table_schema":        (SPEC_TABLE_SCHEMA,   get_table_schema),
    "get_data_lineage":        (SPEC_LINEAGE,        get_data_lineage),
    "get_pipeline_status":     (SPEC_PIPELINE,       get_pipeline_status),
    "get_dq_results":          (SPEC_DQ,             get_dq_results),
    "get_feature_definition":  (SPEC_FEATURE_DEF,    get_feature_definition),
    "get_model_status":        (SPEC_MODEL_STATUS,   get_model_status),
    # AI v2 analytics — deterministic, no model call
    "diagnose_metric":         (SPEC_DIAGNOSE,       diagnose_metric),
    "forecast_metric":         (SPEC_FORECAST,       forecast_metric),
    "governance_review":       (SPEC_GOVERNANCE,     governance_review),
    "list_metrics":            (SPEC_LIST_METRICS,   list_metrics),
}

#: NOT implemented in V1, and deliberately absent rather than stubbed:
#:   get_reconciliation_status  ops.metric_variance / ops.data_certification hold no rows yet
#:   get_feature_value          feature_offline has not been materialised (AI-P5 blocker)
#:   run_model_inference        the only model carries a SYNTHETIC label; exposing it as an
#:                              agent tool would invite an answer built on a meaningless score
NOT_IMPLEMENTED = {
    "get_reconciliation_status": "ops.metric_variance / ops.data_certification are empty",
    "get_feature_value": "feature_offline is not materialised yet",
    "run_model_inference": "the only model has a synthetic label and no predictive meaning",
}

#: A test asserts this stays empty (ADR-057).
WRITE_TOOLS: dict = {}


def call(tool: str, payload: dict, *, actor: str = "anonymous", **kw) -> dict:
    if tool not in CATALOG:
        why = NOT_IMPLEMENTED.get(tool)
        raise ToolError(f"unknown tool {tool!r}" + (f" — not implemented: {why}" if why else ""))
    spec, fn = CATALOG[tool]
    return invoke(spec, fn, payload, actor=actor, **kw)
