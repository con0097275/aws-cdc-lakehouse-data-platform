"""Corpus sync: upload a published corpus and start a Knowledge Base ingestion.

    python3 ai/knowledge/sync.py --dry-run        # DEFAULT: plan only, no AWS call
    python3 ai/knowledge/sync.py --execute        # upload + ingest

IDEMPOTENCE
-----------
`corpus_version` is a content hash, so re-syncing an unchanged corpus is decidably a no-op:
the S3 prefix already exists with the same objects, and the ingestion job is skipped unless
`--force`. That is what makes a retry safe rather than a second bill.

Every run appends to `ops.ai_knowledge_sync` -- corpus_version, embedding_version, index
identity, counts -- so staleness is queryable from Athena instead of inferred.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _manifest(corpus_dir: Path) -> dict:
    p = corpus_dir / "manifest.json"
    if not p.exists():
        raise SystemExit(f"no manifest.json in {corpus_dir} — run publish.py --build first")
    return json.loads(p.read_text())


def plan(corpus_dir: Path, bucket: str, kb_id: str, ds_id: str,
         embedding_model: str, index_arn: str) -> dict:
    m = _manifest(corpus_dir)
    prefix = f"ai/knowledge/{m['corpus_version'].replace(':', '_')}/"
    return {
        "corpus_version": m["corpus_version"],
        "chunking_version": m["chunking_version"],
        "embedding_version": f"embedding:{embedding_model}",
        "vector_index_arn": index_arn,
        "knowledge_base_id": kb_id,
        "data_source_id": ds_id,
        "document_count": m["document_count"],
        "chunk_count": m["chunk_count"],
        "git_commit": m["git_commit"],
        "git_dirty": m["git_dirty"],
        "s3_uri": f"s3://{bucket}/{prefix}",
        "objects": ["documents.jsonl", "chunks.jsonl", "manifest.json"],
        "planned_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def execute(p: dict, corpus_dir: Path, *, force: bool = False, s3=None, agent=None) -> dict:
    """Upload then ingest. Clients are injected so the unit tests need no AWS."""
    import boto3
    s3 = s3 or boto3.client("s3")
    agent = agent or boto3.client("bedrock-agent")

    bucket = p["s3_uri"].split("/")[2]
    prefix = p["s3_uri"].split(f"{bucket}/", 1)[1]
    kms = os.environ.get("AI_LAKE_KMS_KEY_ARN")

    uploaded, skipped = [], []
    for name in p["objects"]:
        key = prefix + name
        if not force:
            try:
                s3.head_object(Bucket=bucket, Key=key)
                skipped.append(key)
                continue
            except Exception:
                pass
        extra = {"ServerSideEncryption": "aws:kms"}
        if kms:
            extra["SSEKMSKeyId"] = kms
        s3.put_object(Bucket=bucket, Key=key,
                      Body=(corpus_dir / name).read_bytes(), **extra)
        uploaded.append(key)

    ingestion_job_id = None
    if uploaded or force:
        if p["knowledge_base_id"] and p["data_source_id"]:
            r = agent.start_ingestion_job(knowledgeBaseId=p["knowledge_base_id"],
                                          dataSourceId=p["data_source_id"],
                                          clientToken=p["corpus_version"][:32])
            ingestion_job_id = r["ingestionJob"]["ingestionJobId"]
    return {**p, "uploaded": uploaded, "skipped": skipped,
            "ingestion_job_id": ingestion_job_id,
            "status": "SUCCEEDED" if (uploaded or skipped) else "NOOP"}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Sync the knowledge corpus to S3 + Bedrock KB.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="plan only; no AWS call (DEFAULT)")
    g.add_argument("--execute", action="store_true")
    ap.add_argument("--corpus-dir")
    ap.add_argument("--force", action="store_true", help="re-upload and re-ingest")
    a = ap.parse_args(argv)

    if a.corpus_dir:
        cdir = Path(a.corpus_dir)
    else:
        found = sorted((ROOT / "ai" / "knowledge").glob("corpus_*"))
        if not found:
            print("no built corpus — run: make ai-corpus-build", file=sys.stderr)
            return 1
        cdir = found[-1]

    p = plan(cdir, os.environ.get("AI_LAKE_BUCKET", "<lake>"),
             os.environ.get("AI_KNOWLEDGE_BASE_ID", ""),
             os.environ.get("AI_DATA_SOURCE_ID", ""),
             os.environ.get("AI_EMBEDDING_MODEL", "cohere.embed-english-v3"),
             os.environ.get("AI_VECTOR_INDEX_ARN", ""))
    for k in ("corpus_version", "chunking_version", "embedding_version",
              "document_count", "chunk_count", "s3_uri", "knowledge_base_id"):
        print(f"  {k:<20} {p[k] or '(unset)'}")
    if p["git_dirty"]:
        print("  WARNING              corpus built from a DIRTY tree; git_commit is not "
              "a faithful pointer")

    if not a.execute:
        print("  DRY RUN — no AWS call made. Re-run with --execute once AI-P13 has applied "
              "the infrastructure.")
        return 0
    if not p["knowledge_base_id"]:
        print("  REFUSING: AI_KNOWLEDGE_BASE_ID unset — infrastructure is not deployed "
              "(RUNTIME_PENDING_INFRA).", file=sys.stderr)
        return 2
    r = execute(p, cdir, force=a.force)
    print(f"  uploaded {len(r['uploaded'])}  skipped {len(r['skipped'])}  "
          f"ingestion {r['ingestion_job_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
