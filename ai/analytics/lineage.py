"""Metric lineage, read from dbt's own manifest.

BAI-P1 §9: do not create a second incompatible lineage graph. dbt already computes the
dependency chain when it compiles, so this walks `manifest.json` rather than re-deriving
anything. If a model is renamed or re-parented, lineage follows automatically — a
hand-maintained copy would drift silently and be believed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "dbt" / "target" / "manifest.json"


@dataclass
class MetricLineage:
    metric_id: str
    dbt_node: str
    relation: str
    upstream: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    resolved: bool = True
    note: str = ""

    def to_dict(self) -> dict:
        return {"metric_id": self.metric_id, "dbt_node": self.dbt_node,
                "relation": self.relation, "upstream": self.upstream,
                "sources": self.sources, "resolved": self.resolved, "note": self.note}


def _load(path: Path | str | None = None) -> dict | None:
    p = Path(path or MANIFEST)
    if not p.exists():
        return None
    import json
    return json.loads(p.read_text())


def lineage_for(metric, manifest_path: Path | str | None = None) -> MetricLineage:
    """Walk upstream from the metric's dbt node to the raw sources."""
    man = _load(manifest_path)
    if man is None:
        return MetricLineage(metric.metric_id, metric.dbt_node, metric.source_relation,
                             resolved=False,
                             note="dbt/target/manifest.json absent — run `dbt compile`. "
                                  "Lineage is reported as UNRESOLVED rather than guessed.")
    nodes, sources = man.get("nodes", {}), man.get("sources", {})
    if metric.dbt_node not in nodes:
        return MetricLineage(metric.metric_id, metric.dbt_node, metric.source_relation,
                             resolved=False,
                             note=f"{metric.dbt_node} is not in the manifest; the registry "
                                  "and the dbt project have drifted apart.")

    seen: list[str] = []
    src: list[str] = []
    stack = [metric.dbt_node]
    while stack:
        nid = stack.pop()
        for dep in nodes.get(nid, {}).get("depends_on", {}).get("nodes", []):
            if dep in seen or dep in src:
                continue
            if dep.startswith("source."):
                s = sources.get(dep, {})
                src.append(f"{s.get('source_name', '?')}.{s.get('name', dep)}")
            else:
                seen.append(dep)
                stack.append(dep)
    return MetricLineage(metric.metric_id, metric.dbt_node, metric.source_relation,
                         upstream=seen, sources=sorted(src))
