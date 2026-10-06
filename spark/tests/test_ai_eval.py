"""Evaluator correctness + dataset grounding (AI-P4).

An evaluator that scores a known miss as a hit is worse than no evaluator: it converts a
retrieval regression into a green build. So the evaluator is tested against fakes with
known-correct answers, not only run against the real corpus.
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ai.eval.evaluate_rag import evaluate
from ai.retrieval.service import (Bm25Backend, Citation, RetrievalRequest, RetrievedChunk,
                                  load_corpus_chunks)

GOLDEN = yaml.safe_load((ROOT / "ai" / "eval" / "rag_golden.yaml").read_text())
CORPUS = sorted(glob.glob(str(ROOT / "ai" / "knowledge" / "corpus_*")))


def _chunk(path, text="body text", score=1.0, cid="c1"):
    return RetrievedChunk(cid, text, score,
                          Citation("d1", path, ("H",), "adr", "abc12345"), "fake")


class _Fixed:
    name = "fake"
    def __init__(self, hits): self.hits = hits
    def search(self, req): return self.hits[: req.top_k]


class _Broken:
    name = "fake"
    def search(self, req): raise TimeoutError("backend timed out")


CASE = {"id": "t1", "category": "architecture", "question": "q",
        "expect_source": "docs/A.md", "key_facts": ["FULL_CDC"]}


# ------------------------------------------------------- evaluator correctness
class TestEvaluatorCorrectness:
    def test_known_hit_is_scored_as_a_hit(self):
        r = evaluate(_Fixed([_chunk("docs/A.md", "FULL_CDC is canonical")]), [CASE])
        assert r["hit_rate"] == 1.0 and r["recall_at_1"] == 1.0
        assert r["groundedness"] == 1.0

    def test_known_miss_is_scored_as_a_miss(self):
        r = evaluate(_Fixed([_chunk("docs/WRONG.md", "FULL_CDC is canonical")]), [CASE])
        assert r["hit_rate"] == 0.0 and r["source_correctness"] == 0.0

    def test_right_source_but_missing_fact_is_not_grounded(self):
        """Source correctness and groundedness are different failures and must not be
        collapsed: the right document with the wrong content is still wrong."""
        r = evaluate(_Fixed([_chunk("docs/A.md", "unrelated prose")]), [CASE])
        assert r["source_correctness"] == 1.0
        assert r["groundedness"] == 0.0

    def test_empty_retrieval_is_a_miss_not_an_error(self):
        r = evaluate(_Fixed([]), [CASE])
        assert r["hit_rate"] == 0.0 and r["error_rate"] == 0.0

    def test_backend_error_is_recorded_not_raised(self):
        r = evaluate(_Broken(), [CASE])
        assert r["error_rate"] == 1.0 and r["hit_rate"] == 0.0
        assert "TimeoutError" in r["per_case"][0]["error"]

    def test_rank_is_the_position_of_the_expected_source(self):
        hits = [_chunk("docs/X.md", cid="a"), _chunk("docs/Y.md", cid="b"),
                _chunk("docs/A.md", "FULL_CDC", cid="c")]
        r = evaluate(_Fixed(hits), [CASE], top_k=5)
        assert r["per_case"][0]["rank"] == 3
        assert r["recall_at_1"] == 0.0 and r["recall_at_3"] == 1.0

    def test_recall_at_k_respects_top_k(self):
        hits = [_chunk(f"docs/{i}.md", cid=str(i)) for i in range(4)] + \
               [_chunk("docs/A.md", "FULL_CDC", cid="last")]
        assert evaluate(_Fixed(hits), [CASE], top_k=3)["hit_rate"] == 0.0
        assert evaluate(_Fixed(hits), [CASE], top_k=5)["hit_rate"] == 1.0

    def test_negative_case_is_correct_only_when_nothing_is_returned(self):
        neg = {"id": "n1", "category": "negative", "question": "q",
               "expect_no_answer": True}
        assert evaluate(_Fixed([]), [neg])["negative_correct"] == 1.0
        assert evaluate(_Fixed([_chunk("docs/A.md")]), [neg])["negative_correct"] == 0.0

    def test_forbidden_fact_raises_the_hallucination_rate(self):
        case = {**CASE, "forbidden_facts": ["a failed run advances the watermark"]}
        r = evaluate(_Fixed([_chunk("docs/A.md",
                                    "FULL_CDC. a failed run advances the watermark")]),
                     [case])
        assert r["hallucination_rate"] == 1.0

    def test_latency_is_measured(self):
        r = evaluate(_Fixed([_chunk("docs/A.md", "FULL_CDC")]), [CASE])
        assert r["latency_p50_ms"] >= 0.0 and "latency_p95_ms" in r

    def test_paraphrase_filter_selects_only_matching_cases(self):
        cases = [{**CASE, "id": "p", "paraphrase": True}, {**CASE, "id": "n"}]
        hit = _Fixed([_chunk("docs/A.md", "FULL_CDC")])
        assert evaluate(hit, cases, only_paraphrase=True)["cases"] == 1
        assert evaluate(hit, cases, only_paraphrase=False)["cases"] == 1
        assert evaluate(hit, cases)["cases"] == 2


# ------------------------------------------------------------ dataset grounding
class TestDatasetGrounding:
    """Every expected fact must actually exist in its source. This is what stops the
    dataset drifting into questions whose answers we invented."""

    @pytest.fixture(scope="class")
    def doc_text(self):
        if not CORPUS:
            pytest.skip("no built corpus")
        by = {}
        for line in Path(CORPUS[-1], "chunks.jsonl").read_text().splitlines():
            c = json.loads(line)
            by.setdefault(c["metadata"]["source_path"], []).append(c["text"])
        return {k: "\n".join(v) for k, v in by.items()}

    def test_dataset_has_at_least_fifty_cases(self):
        """ADR-049/059: 14 questions cannot resolve a retrieval delta."""
        assert len(GOLDEN["cases"]) >= 50

    def test_every_required_category_is_covered(self):
        cats = {c["category"] for c in GOLDEN["cases"]}
        for required in ("architecture", "reporting_modes", "watermark", "dbt",
                         "recovery", "runbook", "security", "cost", "data_contract"):
            assert required in cats, required

    def test_case_ids_are_unique(self):
        ids = [c["id"] for c in GOLDEN["cases"]]
        assert len(ids) == len(set(ids))

    def test_every_expected_source_exists_in_the_corpus(self, doc_text):
        for c in GOLDEN["cases"]:
            if c.get("expect_no_answer"):
                continue
            assert c["expect_source"] in doc_text, f"{c['id']}: {c['expect_source']}"

    def test_every_key_fact_appears_in_its_source(self, doc_text):
        for c in GOLDEN["cases"]:
            if c.get("expect_no_answer"):
                continue
            body = doc_text[c["expect_source"]].lower()
            for f in c.get("key_facts", []):
                assert f.lower() in body, f"{c['id']}: {f!r} absent from {c['expect_source']}"

    def test_paraphrased_and_negative_cases_are_present(self):
        assert sum(1 for c in GOLDEN["cases"] if c.get("paraphrase")) >= 5
        assert sum(1 for c in GOLDEN["cases"] if c.get("expect_no_answer")) >= 3

    def test_dataset_version_is_stable(self):
        assert GOLDEN["version"] and GOLDEN["dataset_id"]


# ------------------------------------------------------------------- baseline
class TestBaselineAndGate:
    def test_baseline_file_records_the_versions_it_was_measured_on(self):
        p = ROOT / "ai" / "eval" / "rag_baseline.json"
        if not p.exists():
            pytest.skip("no baseline recorded yet")
        b = json.loads(p.read_text())
        for k in ("corpus_version", "chunking_version", "evaluation_version", "backend"):
            assert b.get(k) or k == "embedding_version", k

    def test_real_corpus_meets_the_recorded_baseline(self):
        p = ROOT / "ai" / "eval" / "rag_baseline.json"
        if not p.exists() or not CORPUS:
            pytest.skip("no baseline or corpus")
        b = json.loads(p.read_text())
        backend = Bm25Backend(load_corpus_chunks(CORPUS[-1]))
        r = evaluate(backend, GOLDEN["cases"], top_k=5)
        assert r["recall_at_5"] >= b["recall_at_5"] - 0.02
        assert r["hallucination_rate"] <= b["hallucination_rate"] + 0.001

    def test_paraphrased_recall_is_measurably_worse_than_repo_vocabulary(self):
        """The ADR-049 finding, asserted so it cannot be quietly lost: BM25 collapses on
        questions phrased in synonyms. If this ever stops being true, the case for
        embeddings has changed and the ADR must be revisited."""
        if not CORPUS:
            pytest.skip("no corpus")
        backend = Bm25Backend(load_corpus_chunks(CORPUS[-1]))
        native = evaluate(backend, GOLDEN["cases"], top_k=5, only_paraphrase=False)
        para = evaluate(backend, GOLDEN["cases"], top_k=5, only_paraphrase=True)
        assert para["recall_at_5"] < native["recall_at_5"]
