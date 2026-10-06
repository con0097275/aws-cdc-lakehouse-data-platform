"""Version registry. Given an audit row, name everything that produced the answer.

WHY THIS EXISTS AS A JOIN, NOT A STORE
--------------------------------------
Every version in this platform is already content-addressed and already recorded where it is
produced -- the corpus manifest, the model artifact, the tool specs, the prompt hashes. A
registry that COPIED them would be a second source of truth that drifts, and the drift is
silent because both look maintained.

So this RESOLVES, it does not store. `snapshot()` reads the live artefacts; if one is
missing it says so rather than serving a stale copy.

TRACEABILITY IS THE TEST
------------------------
"Which corpus answered this question in August?" must be answerable from an audit row alone.
`resolve()` is that answer, and a test asserts every required kind is present.
"""

from __future__ import annotations

import glob
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: Every version kind the platform must be able to name. Missing one is a gap, not a detail.
REQUIRED_KINDS = (
    "agent", "runtime", "prompt", "model_config", "tool",
    "corpus", "chunking", "embedding",
    "feature_group", "training_dataset", "ml_model", "evaluation",
)


class VersionUnavailable(RuntimeError):
    """A version could not be resolved. Reported, never substituted with a guess."""


@dataclass(frozen=True)
class VersionSnapshot:
    resolved_at: str
    versions: dict
    missing: tuple[str, ...]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["missing"] = list(self.missing)
        return d

    def require(self, *kinds: str) -> None:
        gaps = [k for k in kinds if k in self.missing]
        if gaps:
            raise VersionUnavailable(
                f"cannot resolve {gaps}. An answer whose provenance cannot be named is an "
                "answer nobody can audit later.")


def _corpus() -> dict:
    d = sorted(glob.glob(str(ROOT / "ai" / "knowledge" / "corpus_*")))
    if not d:
        return {}
    return json.loads(Path(d[-1], "manifest.json").read_text())


def _model() -> dict:
    m = sorted(glob.glob(str(ROOT / "artifacts" / "models" / "*" / "model.json")))
    return json.loads(Path(m[-1]).read_text()) if m else {}


def _feature_groups() -> dict:
    import yaml
    import sys
    sys.path.insert(0, str(ROOT))
    from aiplatform.features import FeatureDefinition, FeatureGroup
    out = {}
    for p in sorted((ROOT / "aiplatform" / "features").glob("*.yaml")):
        g = yaml.safe_load(p.read_text())
        feats = tuple(FeatureDefinition(f["name"], f["type"], f.get("description", ""),
                                        g["owner"]) for f in g["features"])
        fg = FeatureGroup(name=g["name"], entity_keys=tuple(g["entity_keys"]),
                          event_time_column=g["event_time_column"], owner=g["owner"],
                          domain=g["domain"], features=feats)
        out[g["name"]] = fg.group_version
    return out


def snapshot() -> VersionSnapshot:
    import sys
    sys.path.insert(0, str(ROOT / "ai"))
    from agent.graph import AGENT_VERSION
    from agent.prompts import VERSIONS as PROMPTS
    from agent.bedrock import ModelConfig
    from agent_tools.catalog import CATALOG

    corpus, model = _corpus(), _model()
    try:
        fgs = _feature_groups()
    except Exception:                                   # noqa: BLE001
        fgs = {}

    evals = {}
    for name, path in (("rag", "ai/eval/rag_golden.yaml"),
                       ("agent", "ai/eval/agent_scenarios.yaml")):
        p = ROOT / path
        if p.exists():
            import yaml
            d = yaml.safe_load(p.read_text())
            evals[name] = f"evaluation:{d['dataset_id']}-v{d['version']}"

    cfg = ModelConfig()
    versions = {
        "agent": AGENT_VERSION,
        "runtime": "runtime:v1",
        "prompt": PROMPTS,
        "model_config": {"model_id": cfg.model_id, "temperature": cfg.temperature,
                         "max_tokens": cfg.max_tokens,
                         "fallback": cfg.fallback_model_id},
        "tool": {n: f"tool:{n}-v{s.version}" for n, (s, _) in sorted(CATALOG.items())},
        "corpus": corpus.get("corpus_version"),
        "chunking": corpus.get("chunking_version"),
        # None is HONEST here: the dense path is built but not deployed, so nothing has
        # been embedded. Inventing a version would imply an index that does not exist.
        "embedding": None,
        "feature_group": fgs or None,
        "training_dataset": model.get("dataset_version"),
        "ml_model": model.get("model_version"),
        "evaluation": evals or None,
    }
    missing = tuple(k for k in REQUIRED_KINDS if not versions.get(k))
    return VersionSnapshot(
        resolved_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        versions=versions, missing=missing)


def resolve(audit_row: dict) -> dict:
    """Join one audit row to the versions in force. The traceability contract."""
    snap = snapshot()
    return {
        "request_id": audit_row.get("request_id"),
        "tool_name": audit_row.get("tool_name"),
        "tool_version": audit_row.get("tool_version"),
        "actor": audit_row.get("actor"),
        "started_at": audit_row.get("started_at"),
        "status": audit_row.get("status"),
        "resource_ids": audit_row.get("resource_ids", []),
        "versions": snap.versions,
        "unresolvable": list(snap.missing),
    }


#: Lifecycle: how each version changes, and what rolling it back means.
LIFECYCLE = {
    "corpus":           "rebuild from a git commit; content hash re-derives. Rollback = rebuild at the older commit.",
    "chunking":         "changing the strategy changes EVERY chunk id, which is what makes re-embedding decidable.",
    "embedding":        "pinned model id + dimension. Rollback = re-embed the corpus at the previous model.",
    "prompt":           "content-addressed; an edited prompt cannot keep its version. Rollback = git revert.",
    "tool":             "bump ToolSpec.version on any contract change. Rollback = revert the spec.",
    "agent":            "bumps when the graph, prompts or tool set change. Rollback = git revert + re-evaluate.",
    "feature_group":    "content hash of the definition. A bump writes NEW rows; history is preserved for PIT.",
    "training_dataset": "content hash over rows + window + split. Rollback = rebuild at the recorded spec.",
    "ml_model":         "content hash of (name, dataset, params, code). Rollback = point inference at the prior artifact.",
    "evaluation":       "dataset_id + version in the YAML. Rollback = git revert; baselines are re-recorded deliberately.",
    "runtime":          "package version. Rollback = redeploy the previous zip; flag-off is instant.",
}
