"""Build the retrieval corpus from documentation and governance metadata.

WHAT GOES IN — AND WHAT DELIBERATELY DOES NOT
---------------------------------------------
IN:  docs/*.md, docs/adr/*.md, governance/*/*.yml, the runbook, the decision log.
OUT: terraform/ (tfvars, state, provider config), spark/ job code, any data file,
     anything under artifacts/ that captured a live command.

The exclusion is the PRIMARY secret control, not the redaction pass in guards.py. Redaction
catches what it recognises; not indexing a file catches everything in it. A corpus built
only from prose and metadata cannot leak a credential it never contained.

CHUNKING
--------
By markdown SECTION, not by fixed character count. A fixed window splits a runbook procedure
across two chunks, so retrieval returns half an instruction — and half a recovery procedure
is worse than none. Sections are the unit a human wrote and the unit a citation should
point at.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from guards import redact  # noqa: E402

INCLUDE = [
    ("docs", ".md"),
    ("docs/adr", ".md"),
    ("governance/catalog", ".yml"),
    ("governance/dq", ".yml"),
    ("governance/lineage", ".yml"),
]
INCLUDE_FILES = ["DECISION_LOG.md", "CLAUDE.md", "PROJECT_STATE.md", "SESSION_HANDOFF.md"]

# Never indexed. Each would be a credential or live-account leak.
EXCLUDE_DIRS = {"terraform", "artifacts", "spark", "dbt", "airflow", ".git",
                "spark-warehouse", ".pytest_cache", "observability"}
EXCLUDE_PATTERNS = [re.compile(p) for p in (
    r"\.tfvars", r"\.tfstate", r"terraform\.lock", r"\.env", r"credentials",
)]

MIN_CHUNK_CHARS = 80          # a heading with no body is not a retrievable answer


def _sections(text: str, path: str) -> list:
    """Split markdown on ## / ### headings, keeping the heading with its body."""
    chunks = []
    parts = re.split(r"^(#{2,3} .+)$", text, flags=re.M)
    preamble = parts[0].strip()
    if len(preamble) >= MIN_CHUNK_CHARS:
        chunks.append({"heading": os.path.basename(path), "body": preamble})
    for i in range(1, len(parts) - 1, 2):
        heading = parts[i].lstrip("# ").strip()
        body = parts[i + 1].strip()
        if len(body) >= MIN_CHUNK_CHARS:
            chunks.append({"heading": heading, "body": body})
    return chunks


def _yaml_chunks(text: str, path: str) -> list:
    """YAML is indexed as ONE chunk per file.

    Splitting a registry entry from its surrounding structure produces fragments that
    retrieve well and cite badly — "retention_days: 90" is a true fact attached to nothing.
    """
    return [{"heading": os.path.basename(path), "body": text.strip()}]


def collect(root: str) -> list:
    docs = []
    seen = set()

    def add(path: str):
        rel = os.path.relpath(path, root)
        if rel in seen:
            return
        if any(p.search(rel) for p in EXCLUDE_PATTERNS):
            return
        if rel.split(os.sep)[0] in EXCLUDE_DIRS:
            return
        seen.add(rel)
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = redact(fh.read())
        maker = _yaml_chunks if path.endswith((".yml", ".yaml")) else _sections
        for i, c in enumerate(maker(text, path)):
            docs.append({
                "id": f"{rel}#{i}",
                "source": rel,
                "heading": c["heading"],
                "text": c["body"],
                # Anchor lets a citation link to the exact section, not just the file.
                "anchor": re.sub(r"[^a-z0-9]+", "-", c["heading"].lower()).strip("-"),
            })

    for d, ext in INCLUDE:
        full = os.path.join(root, d)
        if not os.path.isdir(full):
            continue
        for f in sorted(os.listdir(full)):
            if f.endswith(ext):
                add(os.path.join(full, f))
    for f in INCLUDE_FILES:
        p = os.path.join(root, f)
        if os.path.exists(p):
            add(p)
    return docs


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the RAG corpus")
    ap.add_argument("--root", default=os.path.join(os.path.dirname(__file__), "..", ".."))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "corpus.json"))
    args = ap.parse_args()

    docs = collect(os.path.abspath(args.root))
    with open(args.out, "w") as fh:
        json.dump({"version": 1, "chunks": docs}, fh, indent=1)

    by_source = {}
    for d in docs:
        by_source[d["source"]] = by_source.get(d["source"], 0) + 1
    print(f"  {len(docs)} chunks from {len(by_source)} files -> {args.out}")
    for s, n in sorted(by_source.items(), key=lambda kv: -kv[1])[:8]:
        print(f"    {n:3d}  {s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
