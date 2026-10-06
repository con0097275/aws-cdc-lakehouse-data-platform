"""Retrieval service + corpus sync tests (AI-P3).

No AWS. Backends that would call AWS are constructed with injected fakes, so the tests
prove the DEGRADATION contract rather than the happy path of a service we cannot reach.
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ai" / "knowledge"))

from ai.retrieval.service import (MAX_TOP_K, Bm25Backend, Citation, HybridBackend,
                                  RetrievalError, RetrievalRequest, RetrievedChunk,
                                  S3VectorsBackend, load_corpus_chunks)

CORPUS = sorted(glob.glob(str(ROOT / "ai" / "knowledge" / "corpus_*")))


@pytest.fixture(scope="module")
def chunks():
    if not CORPUS:
        pytest.skip("no built corpus; run make ai-corpus-build")
    return load_corpus_chunks(CORPUS[-1])


@pytest.fixture(scope="module")
def bm25(chunks):
    return Bm25Backend(chunks)


class _Boom:
    name = "dense"
    def search(self, req):
        raise RuntimeError("backend exploded")


class _Fake:
    name = "dense"
    def __init__(self, hits): self.hits = hits
    def search(self, req): return self.hits[: req.top_k]


# ------------------------------------------------------------------ validation
class TestRequestValidation:
    def test_empty_query_is_refused(self):
        for q in ("", "   ", "\n"):
            with pytest.raises(RetrievalError):
                RetrievalRequest(q)

    @pytest.mark.parametrize("k", [0, -1, MAX_TOP_K + 1, "5", 2.5, None])
    def test_top_k_bounds(self, k):
        with pytest.raises(RetrievalError):
            RetrievalRequest("q", top_k=k)

    def test_top_k_within_bounds_is_accepted(self):
        assert RetrievalRequest("q", top_k=MAX_TOP_K).top_k == MAX_TOP_K

    def test_nested_filter_is_refused(self):
        """A filter no backend can honour must be refused, not silently dropped."""
        with pytest.raises(RetrievalError):
            RetrievalRequest("q", filters={"meta": {"a": 1}})

    def test_empty_filter_key_is_refused(self):
        with pytest.raises(RetrievalError):
            RetrievalRequest("q", filters={"": "x"})


# --------------------------------------------------------------------- bm25
class TestBm25Retrieval:
    def test_known_architecture_fact_is_retrieved(self, bm25):
        hits = bm25.search(RetrievalRequest("point in time feature correctness", top_k=3))
        assert hits
        assert any("ADR-054" in h.citation.source_path for h in hits)

    def test_known_runbook_fact_is_retrieved(self, bm25):
        hits = bm25.search(RetrievalRequest("stop the lab for the night", top_k=3))
        assert any("runbooks/stop-and-resume" in h.citation.source_path for h in hits)

    def test_dbt_model_documentation_is_retrievable(self, bm25):
        hits = bm25.search(RetrievalRequest("mart_account_balance_daily grain", top_k=5))
        assert any(h.citation.source_path.startswith("dbt::") for h in hits)

    def test_no_result_query_returns_empty_not_error(self, bm25):
        assert bm25.search(RetrievalRequest("zzzqqq unmatchable xyzzy", top_k=5)) == []

    def test_top_k_is_honoured(self, bm25):
        assert len(bm25.search(RetrievalRequest("data", top_k=3))) <= 3

    def test_scores_are_descending(self, bm25):
        hits = bm25.search(RetrievalRequest("iceberg partition", top_k=6))
        assert hits == sorted(hits, key=lambda h: -h.score)

    def test_metadata_filter_restricts_results(self, bm25):
        hits = bm25.search(RetrievalRequest("recovery", top_k=10,
                                            filters={"document_type": "runbook"}))
        assert hits
        assert all(h.metadata["document_type"] == "runbook" for h in hits)

    def test_a_filter_matching_nothing_yields_no_results(self, bm25):
        assert bm25.search(RetrievalRequest("data", top_k=5,
                                            filters={"document_type": "nope"})) == []


# ----------------------------------------------------------------- citations
class TestCitationContract:
    def test_every_hit_carries_a_usable_citation(self, bm25):
        for h in bm25.search(RetrievalRequest("EOD watermark", top_k=5)):
            c = h.citation
            assert c.source_path and c.source_path != "<unknown>"
            assert c.document_id and c.document_type
            assert h.chunk_id

    def test_citation_renders_path_section_and_commit(self, bm25):
        h = bm25.search(RetrievalRequest("stop the lab for the night", top_k=1))[0]
        r = h.citation.render()
        assert h.citation.source_path in r and "@" in r

    def test_indexing_never_strips_source_identity(self, chunks):
        for c in chunks:
            assert c["metadata"].get("source_path")
            assert c.get("document_id")


# ----------------------------------------------------- degradation / failures
class TestDenseAndDegradation:
    def test_hybrid_degrades_to_bm25_when_dense_raises(self, bm25):
        h = HybridBackend(bm25, _Boom())
        hits = h.search(RetrievalRequest("point in time correctness", top_k=3))
        assert hits and all(x.backend == "bm25" for x in hits)

    def test_hybrid_with_no_dense_backend_is_bm25(self, bm25):
        hits = HybridBackend(bm25, None).search(RetrievalRequest("iceberg", top_k=3))
        assert hits and all(x.backend == "bm25" for x in hits)

    def test_hybrid_fuses_when_dense_works(self, bm25):
        fake = RetrievedChunk("dense-1", "text", 0.9,
                              Citation("d", "docs/x.md", (), "adr", "abc123"), "dense")
        hits = HybridBackend(bm25, _Fake([fake])).search(
            RetrievalRequest("iceberg partition", top_k=5))
        assert any(x.chunk_id == "dense-1" for x in hits)
        assert all(x.backend == "hybrid" for x in hits)

    def test_s3vectors_without_a_client_raises_so_callers_can_fall_back(self):
        b = S3VectorsBackend(index_arn="arn:aws:s3vectors:::index/x",
                             embedding_model_id="cohere.embed-english-v3")
        with pytest.raises(RetrievalError):
            b.search(RetrievalRequest("q"))

    def test_s3vectors_requires_an_index_arn(self):
        with pytest.raises(RetrievalError):
            S3VectorsBackend(index_arn="", embedding_model_id="m")

    def test_dense_distance_is_converted_to_a_higher_is_better_score(self):
        class C:
            def query_vectors(self, **kw):
                return {"vectors": [{"key": "k1", "distance": 0.25,
                                     "metadata": {"text": "t", "source_path": "docs/a.md",
                                                  "document_id": "d1"}}]}
        b = S3VectorsBackend(index_arn="arn:x", embedding_model_id="m",
                             client=C(), embed_client=lambda t: [0.0] * 4)
        assert b.search(RetrievalRequest("q", top_k=1))[0].score == pytest.approx(0.75)

    def test_default_backend_is_bm25_when_the_flag_is_off(self, monkeypatch):
        from ai.retrieval import service
        monkeypatch.delenv("ENABLE_AI_VECTOR_INDEX", raising=False)
        assert service.build_default(CORPUS[-1]).name == "bm25"

    def test_flag_on_but_no_index_arn_still_degrades_to_bm25(self, monkeypatch):
        from ai.retrieval import service
        monkeypatch.setenv("ENABLE_AI_VECTOR_INDEX", "true")
        monkeypatch.delenv("AI_VECTOR_INDEX_ARN", raising=False)
        assert service.build_default(CORPUS[-1]).name == "bm25"


# ---------------------------------------------------------------- corpus sync
class TestCorpusSync:
    def _plan(self):
        import sync
        return sync.plan(Path(CORPUS[-1]), "lake-bucket", "KB123", "DS456",
                         "cohere.embed-english-v3", "arn:aws:s3vectors:::index/k")

    def test_plan_records_all_versions(self):
        p = self._plan()
        for k in ("corpus_version", "chunking_version", "embedding_version",
                  "vector_index_arn", "knowledge_base_id", "chunk_count", "git_commit"):
            assert p[k], k

    def test_embedding_version_names_the_pinned_model(self):
        assert self._plan()["embedding_version"] == "embedding:cohere.embed-english-v3"

    def test_s3_uri_is_versioned_by_corpus(self):
        p = self._plan()
        assert p["corpus_version"].replace(":", "_") in p["s3_uri"]

    def test_resync_of_an_unchanged_corpus_uploads_nothing(self):
        """Idempotence: head_object succeeds, so every object is skipped and no ingestion
        job starts. A retry must not be a second bill."""
        import sync
        class S3:
            def head_object(self, **kw): return {}
            def put_object(self, **kw): raise AssertionError("must not upload")
        class Agent:
            def start_ingestion_job(self, **kw): raise AssertionError("must not ingest")
        r = sync.execute(self._plan(), Path(CORPUS[-1]), s3=S3(), agent=Agent())
        assert r["uploaded"] == [] and r["ingestion_job_id"] is None
        assert len(r["skipped"]) == 3

    def test_changed_corpus_uploads_and_ingests_once(self):
        import sync
        calls = {"put": 0, "ingest": 0}
        class S3:
            def head_object(self, **kw): raise RuntimeError("missing")
            def put_object(self, **kw): calls["put"] += 1
        class Agent:
            def start_ingestion_job(self, **kw):
                calls["ingest"] += 1
                assert kw["clientToken"], "client token makes the retry idempotent"
                return {"ingestionJob": {"ingestionJobId": "job-1"}}
        r = sync.execute(self._plan(), Path(CORPUS[-1]), s3=S3(), agent=Agent())
        assert calls == {"put": 3, "ingest": 1}
        assert r["ingestion_job_id"] == "job-1"

    def test_upload_always_names_kms(self):
        import sync
        seen = []
        class S3:
            def head_object(self, **kw): raise RuntimeError("missing")
            def put_object(self, **kw): seen.append(kw)
        class Agent:
            def start_ingestion_job(self, **kw): return {"ingestionJob": {"ingestionJobId": "j"}}
        sync.execute(self._plan(), Path(CORPUS[-1]), s3=S3(), agent=Agent())
        assert all(k["ServerSideEncryption"] == "aws:kms" for k in seen)

    def test_dry_run_makes_no_aws_call(self):
        import sync
        assert sync.main(["--dry-run"]) == 0

    def test_execute_refuses_without_deployed_infrastructure(self, monkeypatch):
        import sync
        monkeypatch.delenv("AI_KNOWLEDGE_BASE_ID", raising=False)
        assert sync.main(["--execute"]) == 2
