"""Feature lineage — DERIVED from the dbt manifest, never a second graph.

The architecture forbids a second lineage system. dbt already knows what a model reads;
`governance/lineage/openlineage.yml` already covers what dbt cannot see. Feature lineage is
therefore a PROJECTION of those, joined to the feature registry -- not a new store with its
own truth.

What this adds that neither has: the mapping from a FEATURE to the source model and columns
it was computed from, plus the run and version that produced a given value.
"""

from __future__ import annotations

import json
from pathlib import Path


def _fqn(uid: str) -> str:
    return uid.split(".")[-1]


def model_lineage(manifest_path: Path) -> dict[str, dict]:
    """name -> {kind, refs, sources, columns, schema} for dbt MODELS **and SOURCES**.

    Sources are indexed too, and that was a defect when they were not: a feature computed
    from `fact_transaction` -- a dbt SOURCE, not a model -- reported `resolved: false`, so
    a perfectly valid lineage claim looked broken. A resolver that only knows half the graph
    reports the other half as missing, which is worse than reporting nothing because it
    looks like a registry error.
    """
    if not manifest_path.exists():
        return {}
    m = json.loads(manifest_path.read_text())
    nodes, sources = m.get("nodes", {}), m.get("sources", {})
    out: dict[str, dict] = {}

    for uid, sn in sources.items():
        out[sn["name"]] = {
            "kind": "source",
            "refs": [], "sources": [],
            "columns": sorted((sn.get("columns") or {}).keys()),
            "schema": sn.get("schema"),
        }

    for uid, n in nodes.items():
        if n.get("resource_type") != "model":
            continue
        deps = n.get("depends_on", {}).get("nodes", [])
        out[n["name"]] = {
            "kind": "model",
            "refs": sorted({_fqn(d) for d in deps if d.startswith("model.")}),
            "sources": sorted({sources[d]["name"] for d in deps
                               if d.startswith("source.") and d in sources}),
            "columns": sorted((n.get("columns") or {}).keys()),
            "schema": n.get("schema"),
        }
    return out


def feature_lineage(group: dict, manifest_path: Path) -> dict:
    """Resolve one feature group's declared lineage against the dbt graph.

    `resolved: false` on a source means the registry names something dbt does not know
    about. That is reported, not silently dropped -- an unresolvable lineage claim is a
    defect in the registry, and hiding it makes the graph look complete when it is not.
    """
    models = model_lineage(manifest_path)
    feats = []
    for f in group.get("features", []):
        entries = []
        for ref in f.get("source_lineage", []):
            # forms: "db.model.column" | "model.column" | "model"
            parts = ref.split(".")
            col = parts[-1] if len(parts) > 1 else None
            model = parts[-2] if len(parts) > 2 else (parts[0] if len(parts) > 1 else ref)
            known = models.get(model)
            entries.append({
                "ref": ref, "model": model, "column": col,
                "resolved": known is not None,
                "kind": (known or {}).get("kind"),
                "model_reads": (known or {}).get("refs", []),
                "model_sources": (known or {}).get("sources", []),
            })
        feats.append({"feature": f["name"], "sources": entries})
    return {
        "feature_group": group["name"],
        "entity_keys": group.get("entity_keys", []),
        "event_time_column": group.get("event_time_column"),
        "owner": group.get("owner"),
        "features": feats,
        "unresolved": [e["ref"] for fe in feats for e in fe["sources"]
                       if not e["resolved"]],
        "derived_from": "dbt/target/manifest.json (projection, not a second graph)",
    }
