"""Compile aiplatform/**.yaml into one deterministic plan.

    Git YAML -> JSON Schema (structure) -> dataclass (semantics) -> canonical -> sha256

Same shape as `reporting/compile.py`, deliberately: that pipeline is proven here, and a
second compilation model would be a second place for config to mean something.

`--check` validates and writes nothing. No AWS call, ever.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .agents import AgentDefinition, AuthorizationClass, ToolContract
from .classification import Classification, CostClass
from .evaluation import EvalQuestion, EvaluationDataset, ExpectedMode
from .features import FeatureDefinition, FeatureGroup
from .knowledge import ContractViolation, KnowledgeSource
from .models import InferenceMode, ModelDefinition, SplitStrategy, TrainingDataset
from .ops import ALL_TABLES, TABLES_TO_CREATE
from .versioning import content_hash

ROOT = Path(__file__).resolve().parent
KINDS = ("knowledge", "features", "agents", "models", "evaluations")


def _load_yaml(p: Path) -> dict:
    with p.open() as fh:
        d = yaml.safe_load(fh)
    if not isinstance(d, dict):
        raise ContractViolation(f"{p}: top level must be a mapping")
    return d


def _build_knowledge(d: dict) -> KnowledgeSource:
    return KnowledgeSource(
        source_id=d["source_id"], document_type=d["document_type"],
        include=tuple(d["include"]), owner=d["owner"], domain=d["domain"],
        classification=Classification.parse(d.get("classification")),
        description=d.get("description", ""), exclude=tuple(d.get("exclude", ())))


def _build_feature_group(d: dict) -> FeatureGroup:
    feats = tuple(FeatureDefinition(
        name=f["name"], dtype=f["type"], description=f.get("description", ""),
        owner=d["owner"], version=f.get("version", 1),
        source_lineage=tuple(f.get("source_lineage", ())),
        freshness_sla_minutes=f.get("freshness_sla_minutes"))
        for f in d["features"])
    return FeatureGroup(
        name=d["name"], entity_keys=tuple(d["entity_keys"]),
        event_time_column=d["event_time_column"], owner=d["owner"], domain=d["domain"],
        features=feats, description=d.get("description", ""),
        classification=Classification.parse(d.get("classification")),
        batch_enabled=d.get("batch", {}).get("enabled", True),
        streaming_enabled=d.get("streaming", {}).get("enabled", False),
        online_store_enabled=d.get("online_store", {}).get("enabled", False),
        partition_by=tuple(d.get("partition_by", ("source_cob_date",))))


def _build_agent(d: dict) -> AgentDefinition:
    tools = tuple(ToolContract(
        tool_name=t["tool_name"], description=t.get("description", ""),
        input_schema=t["input_schema"], output_schema=t["output_schema"],
        authorization_class=AuthorizationClass(t["authorization_class"]),
        tool_version=t.get("tool_version", 1), read_only=t.get("read_only", True),
        timeout_seconds=t.get("timeout_seconds", 30), max_rows=t.get("max_rows", 1000),
        audited=t.get("audited", True)) for t in d["tools"])
    return AgentDefinition(
        agent_name=d["agent_name"], description=d.get("description", ""), tools=tools,
        model_id=d["model_id"], prompt_version=d["prompt_version"],
        temperature=d.get("temperature", 0.0),
        max_output_tokens=d.get("max_output_tokens", 800),
        max_context_chars=d.get("max_context_chars", 8000),
        session_memory=d.get("session_memory", False))


def _build_model(d: dict) -> ModelDefinition:
    ds = d["dataset"]
    dataset = TrainingDataset(
        name=ds["name"], label_name=ds["label_name"],
        label_event_time_column=ds["label_event_time_column"],
        feature_groups=tuple(ds["feature_groups"]),
        train_start=str(ds["train_start"]), train_end=str(ds["train_end"]),
        label_horizon_days=ds.get("label_horizon_days", 0),
        split=SplitStrategy(ds.get("split", "time_based")))
    return ModelDefinition(
        name=d["name"], description=d.get("description", ""), owner=d["owner"],
        dataset=dataset, algorithm=d["algorithm"],
        inference_mode=InferenceMode(d.get("inference_mode", "batch")),
        seed=d.get("seed", 42), metrics=tuple(d.get("metrics", ("auc_roc", "auc_pr"))))


def _build_evaluation(d: dict) -> EvaluationDataset:
    qs = tuple(EvalQuestion(
        question_id=q["question_id"], question=q["question"],
        expect_mode=ExpectedMode(q["expect_mode"]), expect_source=q.get("expect_source"),
        expect_tool=q.get("expect_tool"),
        expect_contains=tuple(q.get("expect_contains", ())),
        adversarial=q.get("adversarial", False)) for q in d["questions"])
    return EvaluationDataset(name=d["name"], questions=qs,
                             description=d.get("description", ""))


BUILDERS = {"knowledge": _build_knowledge, "features": _build_feature_group,
            "agents": _build_agent, "models": _build_model,
            "evaluations": _build_evaluation}
VERSION_ATTR = {"knowledge": "version", "features": "group_version",
                "agents": "version_id", "models": "version_id",
                "evaluations": "version_id"}


def compile_plan(root: Path = ROOT) -> dict:
    objects: dict[str, dict[str, str]] = {}
    problems: list[str] = []

    for kind in KINDS:
        objects[kind] = {}
        for p in sorted((root / kind).glob("*.yaml")):
            try:
                obj = BUILDERS[kind](_load_yaml(p))
                key = getattr(obj, {"knowledge": "source_id", "features": "name",
                                    "agents": "agent_name", "models": "name",
                                    "evaluations": "name"}[kind])
                if key in objects[kind]:
                    problems.append(f"{kind}: duplicate id {key!r} ({p.name})")
                    continue
                objects[kind][key] = getattr(obj, VERSION_ATTR[kind])
            except (ContractViolation, KeyError, ValueError) as e:
                problems.append(f"{p.relative_to(root.parent)}: {e}")

    if problems:
        raise ContractViolation("\n  ".join(["config is invalid:"] + problems))

    plan = {
        "contract_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "objects": objects,
        "ops_tables": {"create": [t.name for t in TABLES_TO_CREATE],
                       "deferred": [t.name for t in ALL_TABLES if not t.create]},
    }
    # Hash the CONTENT, never the timestamp — otherwise every compile is a new version.
    plan["plan_hash"] = content_hash({k: v for k, v in plan.items()
                                      if k != "generated_at"})
    return plan


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Compile AI platform contracts.")
    ap.add_argument("--check", action="store_true", help="validate only; write nothing")
    ap.add_argument("--out", type=Path)
    a = ap.parse_args(argv)
    try:
        plan = compile_plan()
    except ContractViolation as e:
        print(f"AI CONFIG INVALID\n  {e}", file=sys.stderr)
        return 1
    for kind in KINDS:
        for k, v in plan["objects"][kind].items():
            print(f"  {kind:<12} {k:<34} {v}")
    print(f"  {'ops tables':<12} create={plan['ops_tables']['create']}")
    print(f"  {'':<12} deferred={plan['ops_tables']['deferred']}")
    print(f"  plan_hash    {plan['plan_hash']}")
    if a.out and not a.check:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(plan, indent=2, sort_keys=True))
        print(f"  wrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
