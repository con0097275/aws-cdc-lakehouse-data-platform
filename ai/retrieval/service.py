"""Framework-neutral retrieval service.

FRAMEWORK-NEUTRAL ON PURPOSE
----------------------------
AI-P8 wraps this in LangGraph. If retrieval returned LangChain `Document` objects, the
retrieval layer would depend on the agent framework, and swapping or removing the framework
would mean rewriting retrieval. It returns plain dataclasses instead; the adapter is AI-P8's
problem, not this module's.

BACKENDS
--------
`Bm25Backend`      always available, $0, no AWS. THE DEFAULT AND THE FALLBACK.
`S3VectorsBackend` dense retrieval via Bedrock embeddings + S3 Vectors. Requires the
                   `enable_ai_vector_index` flag AND deployed infrastructure.
`HybridBackend`    both, fused. Ships only if AI-P4 shows it beats BM25 (ADR-049).

The assistant must answer correctly with the vector index absent, unreachable, or the flag
off. That property is why tier 1 costs nothing, and it is not negotiable -- so every dense
path degrades to BM25 rather than raising.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Protocol

MAX_TOP_K = 50
DEFAULT_TOP_K = 5


class RetrievalError(RuntimeError):
    """A backend failed in a way the caller must see. Degradation is NOT an error."""


@dataclass(frozen=True)
class Citation:
    """Everything the agent needs to attribute an answer. Never stripped at index time."""
    document_id: str
    source_path: str
    heading_path: tuple[str, ...]
    document_type: str
    git_commit: str
    owner: str = ""
    domain: str = ""
    table_name: str | None = None

    def render(self) -> str:
        where = " > ".join(self.heading_path) if self.heading_path else ""
        return f"{self.source_path}" + (f" § {where}" if where else "") + \
               f" @{self.git_commit[:8]}"


@dataclass(frozen=True)
class RetrievedChunk:
    chunk_id: str
    text: str
    score: float | None          # None when the backend exposes no score
    citation: Citation
    backend: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetrievalRequest:
    query: str
    top_k: int = DEFAULT_TOP_K
    filters: dict[str, Any] = field(default_factory=dict)
    corpus_version: str | None = None
    environment: str = "dev"
    #: Drop hits at or below this score. Measured, not chosen: a sweep over the AI-P4
    #: golden set showed a raw BM25 cutoff CANNOT separate unanswerable questions from
    #: real ones -- "what is our Istio mesh configuration" scores 13.19, higher than most
    #: genuine questions, because Kubernetes vocabulary really is in this corpus. The only
    #: defensible floor is 0.0: no lexical overlap at all means no match, and that costs
    #: zero real hits. Anything higher trades good answers for a cosmetic improvement on
    #: four negative cases. Separating "we don't have this" from "we have something wordy
    #: about this" needs semantics, not a threshold -- which is evidence for ADR-049.
    min_score: float = 0.0

    def __post_init__(self) -> None:
        if not self.query or not self.query.strip():
            raise RetrievalError("empty query")
        if not isinstance(self.top_k, int) or self.top_k < 1 or self.top_k > MAX_TOP_K:
            raise RetrievalError(f"top_k must be 1..{MAX_TOP_K}, got {self.top_k!r}")
        for k, v in self.filters.items():
            if not isinstance(k, str) or not k:
                raise RetrievalError(f"filter key must be a non-empty string: {k!r}")
            if isinstance(v, (dict, list, set)):
                raise RetrievalError(
                    f"filter {k!r} must be a scalar; nested filters are not supported by "
                    "every backend and a filter that silently does nothing is worse than "
                    "one that is refused")


class Backend(Protocol):
    name: str
    def search(self, req: RetrievalRequest) -> list[RetrievedChunk]: ...


def _citation_from(meta: dict, document_id: str) -> Citation:
    return Citation(
        document_id=document_id,
        source_path=meta.get("source_path", "<unknown>"),
        heading_path=tuple(meta.get("heading_path") or ()),
        document_type=meta.get("document_type", "unknown"),
        git_commit=meta.get("git_commit", ""),
        owner=meta.get("owner", ""),
        domain=meta.get("domain", ""),
        table_name=meta.get("table_name"),
    )


def _matches(meta: dict, filters: dict) -> bool:
    return all(meta.get(k) == v for k, v in filters.items())


class Bm25Backend:
    """Lexical retrieval over the published corpus. No AWS, no network, $0."""
    name = "bm25"

    def __init__(self, chunks: list[dict]):
        self._chunks = chunks
        self._index = self._build(chunks)

    @staticmethod
    def _tok(text: str) -> list[str]:
        import re
        stop = {"the", "a", "an", "is", "are", "of", "to", "in", "on", "for", "and", "or",
                "it", "this", "that", "with", "as", "by", "at", "from", "what", "which",
                "how", "why", "when", "where", "does", "do", "can", "i", "we", "be"}
        toks = re.findall(r"[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*", text.lower())
        # Fold a trailing plural: `conflict` and `conflicts` are one term, or the coverage
        # rule below rejects the very chunk that answers the question best. Not a stemmer --
        # `certified` and `certification` stay distinct, and identifiers are left alone.
        return [(t[:-1] if (len(t) > 3 and t.endswith("s") and not t.endswith("ss")
                            and "_" not in t and "." not in t) else t)
                for t in toks if t not in stop and len(t) > 1]

    def _build(self, chunks):
        from collections import Counter
        import math
        docs, df = [], Counter()
        for c in chunks:
            t = self._tok(c["text"])
            docs.append(Counter(t))
            df.update(set(t))
        n = max(len(docs), 1)
        idf = {w: math.log(1 + (n - c + 0.5) / (c + 0.5)) for w, c in df.items()}
        avg = sum(sum(d.values()) for d in docs) / n if docs else 1.0
        return {"docs": docs, "idf": idf, "avgdl": avg or 1.0}

    #: How much of the QUESTION a chunk must account for, weighted by word rarity.
    #: `s > 0` is not a match: one shared common word clears it. "What does total deposits
    #: mean?" returned docs/RUNBOOK.md#iceberg-commit-conflicts as a CITATION under the
    #: answer -- "deposits" appears in 0 of 708 chunks, so the subject of the question was
    #: absent and the hit came from "total" and "mean" alone. A citation that does not
    #: support the answer is worse than none: it invites a check that can prove nothing.
    #:
    #: An absolute score floor cannot do this. Measured here, the answerable query
    #: "watermark" scores 7.19 and "full_cdc" 4.63, while that unanswerable one scores 5.45.
    #: Magnitude does not separate them; coverage does.
    MIN_QUERY_COVERAGE = 0.30


    def _covering(self, q, idf, scored):
        """Drop hits that leave most of the question's rare-word weight unmatched.

        `s > 0` is not a match: one shared common word clears it.

        A term the corpus has NEVER seen is maximally informative, not minimally. BM25
        reaches it through `idf.get(w, 0.0)` and scores an unseen word like a ubiquitous
        one, so a question about something absent looks like a question about something
        everywhere. Counting it at the corpus maximum instead makes the unmatched weight
        visible in the ratio.

        The threshold is DELIBERATELY LOOSE. It trims a weak tail; it does not decide
        relevance. Measured on `ai/eval/evaluate_rag.py`, 0.30 holds recall@5 at the recorded
        0.6842, lifts groundedness 0.5614 -> 0.5789, and takes negative_correct 0.25 -> 0.75.
        Tightening to 0.65 handles one more unanswerable query and costs recall@5
        0.68 -> 0.42, because a real question's rare words are rarely all in one chunk. That
        trade was measured and refused: a filter that silently drops a third of the right
        answers is a worse defect than a weak citation.
        """
        if not scored or not q:
            return scored
        unknown = max(idf.values(), default=0.0)
        weight = {t: idf.get(t, unknown) for t in set(q)}
        demanded = sum(weight.values())
        if demanded <= 0:
            return scored
        docs = self._index["docs"]
        return [(s_, i) for s_, i in scored
                if sum(w for t, w in weight.items() if t in docs[i]) / demanded
                >= self.MIN_QUERY_COVERAGE]

    def search(self, req: RetrievalRequest) -> list[RetrievedChunk]:
        k1, b = 1.5, 0.75
        q = self._tok(req.query)
        idf, avgdl = self._index["idf"], self._index["avgdl"]
        scored = []
        for i, tf in enumerate(self._index["docs"]):
            meta = self._chunks[i].get("metadata", {})
            if req.filters and not _matches(meta, req.filters):
                continue
            dl = sum(tf.values()) or 1
            s = sum(idf.get(w, 0.0) * (tf[w] * (k1 + 1)) /
                    (tf[w] + k1 * (1 - b + b * dl / avgdl)) for w in q if w in tf)
            if s > req.min_score:
                scored.append((s, i))
        scored.sort(key=lambda x: (-x[0], x[1]))
        scored = self._covering(q, idf, scored)
        out = []
        for s, i in scored[: req.top_k]:
            c = self._chunks[i]
            out.append(RetrievedChunk(
                chunk_id=c["chunk_id"], text=c["text"], score=round(s, 4),
                citation=_citation_from(c.get("metadata", {}), c["document_id"]),
                backend=self.name, metadata=c.get("metadata", {})))
        return out


class S3VectorsBackend:
    """Dense retrieval. Constructed lazily so importing this module needs no AWS."""
    name = "s3vectors"

    def __init__(self, *, index_arn: str, embedding_model_id: str,
                 region: str = "ap-southeast-1", client=None, embed_client=None,
                 timeout_seconds: float = 10.0):
        if not index_arn:
            raise RetrievalError("S3VectorsBackend requires an index_arn")
        self.index_arn = index_arn
        self.embedding_model_id = embedding_model_id
        self.region = region
        self._client = client
        self._embed = embed_client
        self.timeout_seconds = timeout_seconds

    def embed(self, text: str) -> list[float]:
        if self._embed is None:
            raise RetrievalError("no embedding client configured")
        return self._embed(text)

    def search(self, req: RetrievalRequest) -> list[RetrievedChunk]:
        if self._client is None:
            raise RetrievalError(
                "s3vectors client unavailable — the index is not deployed, or botocore is "
                "too old to address it. Callers should fall back to BM25.")
        vector = self.embed(req.query)
        raw = self._client.query_vectors(
            indexArn=self.index_arn, topK=req.top_k, queryVector={"float32": vector},
            filter=req.filters or None, returnMetadata=True, returnDistance=True)
        out = []
        for v in raw.get("vectors", []):
            meta = v.get("metadata", {}) or {}
            out.append(RetrievedChunk(
                chunk_id=v.get("key", ""), text=meta.get("text", ""),
                # distance -> similarity, so a HIGHER score is always better regardless of
                # backend. Mixing conventions across backends is how a fusion silently
                # inverts its own ranking.
                score=(1.0 - v["distance"]) if "distance" in v else None,
                citation=_citation_from(meta, meta.get("document_id", "")),
                backend=self.name, metadata=meta))
        return out


class HybridBackend:
    """BM25 + dense, fused by reciprocal rank. DEGRADES TO BM25, never raises."""
    name = "hybrid"
    RRF_K = 60

    def __init__(self, lexical: Bm25Backend, dense: Backend | None):
        self.lexical, self.dense = lexical, dense

    def search(self, req: RetrievalRequest) -> list[RetrievedChunk]:
        lex = self.lexical.search(req)
        if self.dense is None:
            return lex
        try:
            dense = self.dense.search(req)
        except Exception:
            # Deliberately broad: a dense-path failure of ANY kind must degrade, not break.
            # The degradation is visible in `backend` on every returned chunk.
            return lex
        ranks: dict[str, float] = {}
        keep: dict[str, RetrievedChunk] = {}
        for hits in (lex, dense):
            for rank, c in enumerate(hits, 1):
                ranks[c.chunk_id] = ranks.get(c.chunk_id, 0.0) + 1.0 / (self.RRF_K + rank)
                keep.setdefault(c.chunk_id, c)
        order = sorted(ranks.items(), key=lambda kv: (-kv[1], kv[0]))
        return [RetrievedChunk(**{**keep[cid].__dict__, "score": round(sc, 6),
                                  "backend": self.name}) for cid, sc in order[: req.top_k]]


def load_corpus_chunks(corpus_dir: str) -> list[dict]:
    import json
    from pathlib import Path
    p = Path(corpus_dir) / "chunks.jsonl"
    if not p.exists():
        raise RetrievalError(f"no chunks.jsonl under {corpus_dir}")
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def build_default(corpus_dir: str) -> Backend:
    """BM25 unless the vector index is BOTH flagged on and reachable (ADR-049)."""
    lexical = Bm25Backend(load_corpus_chunks(corpus_dir))
    if os.environ.get("ENABLE_AI_VECTOR_INDEX", "false").lower() != "true":
        return lexical
    arn = os.environ.get("AI_VECTOR_INDEX_ARN", "")
    if not arn:
        return lexical
    try:
        import boto3
        client = boto3.client("s3vectors", region_name=os.environ.get(
            "AWS_DEFAULT_REGION", "ap-southeast-1"))
        dense = S3VectorsBackend(
            index_arn=arn,
            embedding_model_id=os.environ.get("AI_EMBEDDING_MODEL", "cohere.embed-english-v3"),
            client=client)
        return HybridBackend(lexical, dense)
    except Exception:
        return lexical
