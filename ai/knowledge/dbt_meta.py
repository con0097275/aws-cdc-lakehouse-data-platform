"""Human-usable knowledge from the dbt manifest — one document per MODEL, not one blob.

WHY NOT INDEX manifest.json
---------------------------
It is 688 KB of mostly compiled SQL and internal graph state. As one chunk it matches every
query and answers none; split by characters it produces fragments of JSON. Neither is
knowledge.

So the manifest is READ as structured data and rendered into one small document per model:
what the model is, what its columns mean, what it reads, how it is tested.

DEPENDENCY TRUTH IS NOT DUPLICATED. `refs`/`sources` are recorded as the model's OWN
declared inputs, which is what a person asking "what feeds this mart" wants. The
authoritative execution graph stays in `target/reporting/plan.json` and the manifest itself;
this is documentation of it, and says so.
"""

from __future__ import annotations

import json
from pathlib import Path


def _fqn(unique_id: str) -> str:
    return unique_id.split(".")[-1]


def extract(manifest_path: Path) -> list[dict]:
    """One knowledge document per dbt model that carries documentation worth indexing."""
    if not manifest_path.exists():
        return []
    m = json.loads(manifest_path.read_text())
    nodes = m.get("nodes", {})
    sources = m.get("sources", {})

    # test coverage per model, so "how is this validated" is answerable
    tests: dict[str, list[str]] = {}
    for uid, n in nodes.items():
        if n.get("resource_type") != "test":
            continue
        for dep in n.get("depends_on", {}).get("nodes", []):
            tests.setdefault(dep, []).append(n.get("name", _fqn(uid)))

    docs: list[dict] = []
    for uid, n in nodes.items():
        if n.get("resource_type") != "model":
            continue
        desc = (n.get("description") or "").strip()
        cols = {c: (v.get("description") or "").strip()
                for c, v in (n.get("columns") or {}).items()}
        # A model with neither a description nor documented columns has nothing a reader
        # could learn that the SQL does not already say. Skipping it keeps the corpus
        # signal-dense rather than padding it with empty stubs.
        if not desc and not any(cols.values()):
            continue

        refs = sorted({_fqn(d) for d in n.get("depends_on", {}).get("nodes", [])
                       if d.startswith("model.")})
        srcs = sorted({sources[d]["name"] for d in n.get("depends_on", {}).get("nodes", [])
                       if d.startswith("source.") and d in sources})

        lines = [f"# dbt model: {n['name']}", ""]
        if desc:
            lines += ["## Description", "", desc, ""]
        if cols:
            lines += ["## Columns", ""]
            lines += [f"- `{c}` — {d}" if d else f"- `{c}`" for c, d in sorted(cols.items())]
            lines.append("")
        if refs or srcs:
            lines += ["## Reads", ""]
            lines += [f"- model `{r}`" for r in refs] + [f"- source `{s}`" for s in srcs]
            lines += ["", "  Declared inputs from the dbt manifest. The authoritative "
                      "execution order is the compiled plan, not this list.", ""]
        if tests.get(uid):
            lines += ["## Tests", ""] + [f"- {t}" for t in sorted(set(tests[uid]))] + [""]
        meta = n.get("meta") or {}
        if n.get("tags") or meta:
            lines += ["## Metadata", ""]
            if n.get("tags"):
                lines.append(f"- tags: {', '.join(n['tags'])}")
            for k, v in sorted(meta.items()):
                lines.append(f"- {k}: {v}")
            lines.append("")

        docs.append({
            "source_path": n.get("original_file_path", f"dbt/models/{n['name']}.sql"),
            "document_type": "table_doc",
            "text": "\n".join(lines).rstrip() + "\n",
            "table_name": n.get("alias") or n["name"],
            "database_name": n.get("schema"),
            "owner": (meta.get("owner") or "data-platform"),
            "domain": "ops",
        })
    return sorted(docs, key=lambda d: d["table_name"])
