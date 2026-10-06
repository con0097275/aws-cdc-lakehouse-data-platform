"""Publish the approved knowledge corpus. Deterministic, offline, dry-run by default.

    python3 -m ai.knowledge.publish --dry-run      # default: report, write nothing
    python3 -m ai.knowledge.publish --build        # write ai/knowledge/<corpus_version>/

    ai/knowledge/<corpus_version>/
        documents.jsonl   one row per source document
        chunks.jsonl      one row per retrievable chunk
        manifest.json     what was built, from what, and what was refused

WHAT MAKES THIS REPRODUCIBLE
----------------------------
`corpus_version` is a content hash over every document's content hash plus the chunking
version -- never a timestamp, never a counter. Two builds from the same tree produce the
same version and byte-identical output, which is what makes "re-embed only what changed"
decidable in AI-P3 instead of a guess.

A DIRTY WORKING TREE IS RECORDED, NOT HIDDEN. Stamping a commit the content does not match
is worse than having no commit at all, so `git_dirty` goes in the manifest.

NO AWS CALL. Publication to S3 is AI-P13, after the prefix exists in Terraform.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from chunking import CHUNKING_VERSION, Piece, chunk_markdown, chunk_structured
from dbt_meta import extract as dbt_extract
from redaction import scan
from sources import MAX_DOCUMENT_BYTES, REGISTRY, is_excluded

ROOT = Path(__file__).resolve().parents[2]
OUT_ROOT = ROOT / "ai" / "knowledge"
ARCHITECTURE_VERSION = "ai-1.0.0"
ENVIRONMENT = "dev"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                              text=True, timeout=15).stdout.strip()
    except Exception:
        return ""


def discover() -> tuple[list[dict], list[dict]]:
    """(documents, refusals). Allow-list first, then per-file exclusion."""
    docs, refused, seen = [], [], set()

    for src in REGISTRY:
        for pattern in src.globs:
            for p in sorted(glob(str(ROOT / pattern), recursive=True)):
                path = Path(p)
                if not path.is_file():
                    continue
                rel = str(path.relative_to(ROOT))
                if rel in seen:
                    continue
                reason = is_excluded(rel)
                if reason:
                    refused.append({"source_path": rel, "reason": reason}); continue
                size = path.stat().st_size
                if size > MAX_DOCUMENT_BYTES:
                    refused.append({"source_path": rel,
                                    "reason": f"too large ({size} bytes) — a dataset, not a document"})
                    continue
                try:
                    text = path.read_text(encoding="utf-8")
                except (UnicodeDecodeError, OSError) as e:
                    refused.append({"source_path": rel, "reason": f"unreadable: {e}"}); continue
                seen.add(rel)
                docs.append({
                    "source_path": rel, "document_type": src.document_type,
                    "owner": src.owner, "domain": src.domain,
                    "classification": src.classification, "source_id": src.source_id,
                    "text": text, "table_name": None, "database_name": None,
                })

    for d in dbt_extract(ROOT / "dbt" / "target" / "manifest.json"):
        rel = f"dbt::{d['table_name']}"
        if rel in seen:
            continue
        seen.add(rel)
        docs.append({**d, "source_path": rel, "classification": "internal",
                     "source_id": "dbt_models"})
    return docs, refused


def build() -> dict:
    raw, refused = discover()
    commit = _git("rev-parse", "HEAD") or "0" * 40
    dirty = bool(_git("status", "--porcelain"))
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    documents, chunks, quarantined = [], [], []

    for d in raw:
        findings = scan(d["text"])
        if findings:
            # QUARANTINE the whole document. Publishing the rest would be a bet that the
            # pattern list is complete, and it is not.
            quarantined.append({"source_path": d["source_path"],
                                "findings": [{"rule": f.rule, "line": f.line_no,
                                              "excerpt": f.excerpt} for f in findings]})
            continue

        content_hash = _sha(d["text"])
        doc_id = f"{d['source_path']}@{content_hash[:12]}"
        documents.append({
            "document_id": doc_id, "document_type": d["document_type"],
            "source_path": d["source_path"],
            "source_uri": f"repo://{d['source_path']}",
            "git_commit": commit, "content_hash": content_hash,
            "owner": d["owner"], "domain": d["domain"],
            "environment": ENVIRONMENT, "classification": d["classification"],
            "table_name": d.get("table_name"), "database_name": d.get("database_name"),
            "architecture_version": ARCHITECTURE_VERSION,
            "created_at": now, "updated_at": now,
        })

        sp = d["source_path"]
        pieces: list[Piece] = (chunk_structured(d["text"], sp)
                               if sp.endswith((".yml", ".yaml", ".json"))
                               else chunk_markdown(d["text"]))
        for pc in pieces:
            ctext = pc.text.strip()
            chunks.append({
                "chunk_id": f"{doc_id}#{pc.index:04d}:{_sha(ctext)[:12]}",
                "document_id": doc_id, "chunk_index": pc.index,
                "content_hash": _sha(ctext), "chunking_version": CHUNKING_VERSION,
                "embedding_version": None, "text": ctext,
                "metadata": {
                    "heading_path": list(pc.heading_path),
                    "source_path": sp, "document_type": d["document_type"],
                    "owner": d["owner"], "domain": d["domain"],
                    "classification": d["classification"],
                    "table_name": d.get("table_name"),
                    "database_name": d.get("database_name"),
                    "git_commit": commit,
                    "architecture_version": ARCHITECTURE_VERSION,
                },
            })

    corpus_version = "corpus:" + hashlib.sha256(
        json.dumps({"docs": sorted(d["content_hash"] for d in documents),
                    "chunking": CHUNKING_VERSION,
                    "arch": ARCHITECTURE_VERSION},
                   sort_keys=True).encode()).hexdigest()[:16]

    by_source: dict[str, int] = {}
    for d in raw:
        by_source[d["source_id"]] = by_source.get(d["source_id"], 0) + 1

    manifest = {
        "corpus_version": corpus_version,
        "chunking_version": CHUNKING_VERSION,
        "architecture_version": ARCHITECTURE_VERSION,
        "git_commit": commit,
        "git_dirty": dirty,
        "generated_at": now,
        "document_count": len(documents),
        "chunk_count": len(chunks),
        "source_breakdown": dict(sorted(by_source.items())),
        "excluded_count": len(refused),
        "excluded": refused[:50],
        "quarantined_count": len(quarantined),
        "quarantined": quarantined,
        "s3_target": "s3://<lake>/ai/knowledge/" + corpus_version + "/",
    }
    return {"manifest": manifest, "documents": documents, "chunks": chunks}


def write(result: dict, out_root: Path = OUT_ROOT) -> Path:
    d = out_root / result["manifest"]["corpus_version"].replace(":", "_")
    d.mkdir(parents=True, exist_ok=True)
    for name, rows in (("documents.jsonl", result["documents"]),
                       ("chunks.jsonl", result["chunks"])):
        with (d / name).open("w") as fh:
            for r in rows:
                fh.write(json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n")
    (d / "manifest.json").write_text(
        json.dumps(result["manifest"], indent=2, sort_keys=True) + "\n")
    return d


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Publish the RAG knowledge corpus.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="report only; write nothing (DEFAULT)")
    g.add_argument("--build", action="store_true", help="write the corpus")
    a = ap.parse_args(argv)

    r = build()
    m = r["manifest"]
    print(f"  corpus_version    {m['corpus_version']}")
    print(f"  chunking_version  {m['chunking_version']}")
    print(f"  git_commit        {m['git_commit'][:12]}"
          + ("  (DIRTY — recorded in the manifest)" if m["git_dirty"] else ""))
    print(f"  documents         {m['document_count']}")
    print(f"  chunks            {m['chunk_count']}")
    print("  by source:")
    for k, v in m["source_breakdown"].items():
        print(f"      {k:<20} {v}")
    print(f"  excluded          {m['excluded_count']}")
    print(f"  quarantined       {m['quarantined_count']}")
    for q in m["quarantined"]:
        rules = ", ".join(sorted({f['rule'] for f in q['findings']}))
        print(f"      REFUSED {q['source_path']}  ({rules})")

    if a.build:
        out = write(r)
        print(f"  wrote             {out.relative_to(ROOT)}")
    else:
        print("  DRY RUN — nothing written. Re-run with --build.")
    return 1 if m["quarantined_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
