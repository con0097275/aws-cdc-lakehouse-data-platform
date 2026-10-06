"""RAG retrieval evaluation. Retrieval only — generation is NOT evaluated here.

    python3 ai/eval/evaluate_rag.py                 # human-readable, BM25 baseline
    python3 ai/eval/evaluate_rag.py --json out.json # machine-readable
    python3 ai/eval/evaluate_rag.py --sweep         # top_k / filter / paraphrase baselines
    python3 ai/eval/evaluate_rag.py --gate          # exit non-zero below the recorded floor

WHY RETRIEVAL AND GENERATION ARE SCORED SEPARATELY
--------------------------------------------------
A fluent answer built from the wrong chunk and a terse answer built from the right one look
similar to an end-to-end score, and only one of them is fixable by changing the retriever.
Mixing them means a retrieval regression can be masked by a better-phrased answer.

So this measures ONLY: did the right document come back, at what rank, how fast, at what
cost. Generation quality is AI-P10's problem, after an agent exists.

THRESHOLDS ARE MEASURED, NOT CHOSEN
-----------------------------------
`--gate` fails against a floor recorded FROM a baseline run, not from an aspiration. A gate
set above what the system has ever achieved fails on day one and gets disabled, which is how
a quality gate stops existing.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ai.retrieval.service import Bm25Backend, RetrievalRequest, load_corpus_chunks

GOLDEN = ROOT / "ai" / "eval" / "rag_golden.yaml"
BASELINE = ROOT / "ai" / "eval" / "rag_baseline.json"


def _corpus_dir() -> Path:
    found = sorted((ROOT / "ai" / "knowledge").glob("corpus_*"))
    if not found:
        raise SystemExit("no built corpus — run: make ai-corpus-build")
    return found[-1]


def evaluate(backend, cases: list[dict], top_k: int = 5,
             only_paraphrase: bool | None = None) -> dict:
    per, lat = [], []
    for c in cases:
        if only_paraphrase is not None and bool(c.get("paraphrase")) != only_paraphrase:
            continue
        req = RetrievalRequest(c["question"], top_k=top_k, filters=c.get("filters", {}))
        t0 = time.perf_counter()
        try:
            hits = backend.search(req)
            err = None
        except Exception as e:               # a backend fault is a RESULT, not a crash
            hits, err = [], f"{type(e).__name__}: {e}"
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        lat.append(elapsed_ms)

        sources = [h.citation.source_path for h in hits]
        if c.get("expect_no_answer"):
            # Correct behaviour is either nothing, or something the caller can see is weak.
            hit, rank, grounded = (len(hits) == 0), None, (len(hits) == 0)
        else:
            want = c["expect_source"]
            hit = want in sources
            rank = sources.index(want) + 1 if hit else None
            joined = "\n".join(h.text for h in hits if h.citation.source_path == want)
            grounded = hit and all(f.lower() in joined.lower()
                                   for f in c.get("key_facts", []))
        forbidden = [f for f in c.get("forbidden_facts", [])
                     if any(f.lower() in h.text.lower() for h in hits)]

        per.append({"id": c["id"], "category": c["category"],
                    "difficulty": c.get("difficulty", "medium"),
                    "paraphrase": bool(c.get("paraphrase")),
                    "negative": bool(c.get("expect_no_answer")),
                    "hit": hit, "rank": rank, "grounded": grounded,
                    "forbidden_present": forbidden, "error": err,
                    "latency_ms": round(elapsed_ms, 2),
                    "top_sources": sources[:3]})

    n = len(per) or 1
    pos = [p for p in per if not p["negative"]]
    neg = [p for p in per if p["negative"]]
    npos = len(pos) or 1

    def recall_at(k: int) -> float:
        return sum(1 for p in pos if p["rank"] and p["rank"] <= k) / npos

    return {
        "cases": len(per),
        "top_k": top_k,
        "hit_rate": round(sum(1 for p in per if p["hit"]) / n, 4),
        "recall_at_1": round(recall_at(1), 4),
        "recall_at_3": round(recall_at(3), 4),
        "recall_at_5": round(recall_at(5), 4),
        "source_correctness": round(sum(1 for p in pos if p["hit"]) / npos, 4),
        "groundedness": round(sum(1 for p in pos if p["grounded"]) / npos, 4),
        "negative_correct": round(sum(1 for p in neg if p["hit"]) / (len(neg) or 1), 4),
        "hallucination_rate": round(
            sum(1 for p in per if p["forbidden_present"]) / n, 4),
        "error_rate": round(sum(1 for p in per if p["error"]) / n, 4),
        "mean_reciprocal_rank": round(
            sum(1 / p["rank"] for p in pos if p["rank"]) / npos, 4),
        "latency_p50_ms": round(statistics.median(lat), 2) if lat else 0.0,
        "latency_p95_ms": round(sorted(lat)[int(len(lat) * 0.95)], 2) if lat else 0.0,
        "retrieval_cost_usd": 0.0,      # BM25 is in-process; no service, no tokens
        "embedding_cost_usd": 0.0,      # no embedding call on the lexical path
        "per_case": per,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Evaluate RAG retrieval.")
    ap.add_argument("--json", type=Path)
    ap.add_argument("--sweep", action="store_true", help="baseline comparison")
    ap.add_argument("--gate", action="store_true", help="fail below the recorded floor")
    ap.add_argument("--top-k", type=int, default=5)
    a = ap.parse_args(argv)

    cdir = _corpus_dir()
    chunks = load_corpus_chunks(str(cdir))
    manifest = json.loads((cdir / "manifest.json").read_text())
    golden = yaml.safe_load(GOLDEN.read_text())
    backend = Bm25Backend(chunks)

    result = evaluate(backend, golden["cases"], top_k=a.top_k)
    header = {
        "dataset_id": golden["dataset_id"],
        "evaluation_version": f"evaluation:{golden['dataset_id']}-v{golden['version']}",
        "corpus_version": manifest["corpus_version"],
        "chunking_version": manifest["chunking_version"],
        "embedding_version": None,          # lexical baseline embeds nothing
        "backend": backend.name,
    }
    out = {**header, **result}

    print(f"  dataset            {header['evaluation_version']}")
    print(f"  corpus             {header['corpus_version']}")
    print(f"  chunking           {header['chunking_version']}")
    print(f"  backend            {header['backend']}   top_k={result['top_k']}")
    print(f"  cases              {result['cases']}")
    for k in ("recall_at_1", "recall_at_3", "recall_at_5", "source_correctness",
              "groundedness", "mean_reciprocal_rank", "negative_correct",
              "hallucination_rate", "error_rate"):
        print(f"  {k:<20} {result[k]}")
    print(f"  latency p50/p95    {result['latency_p50_ms']} / {result['latency_p95_ms']} ms")
    print(f"  cost               retrieval ${result['retrieval_cost_usd']:.6f}  "
          f"embedding ${result['embedding_cost_usd']:.6f}")

    misses = [p for p in result["per_case"] if not p["hit"]]
    if misses:
        print(f"\n  MISSES ({len(misses)}):")
        for p in misses:
            print(f"    {p['id']:<10} {p['category']:<16} "
                  f"{'paraphrase' if p['paraphrase'] else '':<11} -> {p['top_sources'][:2]}")

    if a.sweep:
        print("\n  BASELINE SWEEP")
        print(f"    {'variant':<26}{'recall@1':>10}{'recall@3':>10}{'recall@5':>10}{'MRR':>8}")
        for k in (1, 3, 5, 10, 20):
            r = evaluate(backend, golden["cases"], top_k=k)
            print(f"    {'top_k=' + str(k):<26}{r['recall_at_1']:>10}{r['recall_at_3']:>10}"
                  f"{r['recall_at_5']:>10}{r['mean_reciprocal_rank']:>8}")
        for label, flag in (("repo-vocabulary only", False), ("paraphrased only", True)):
            r = evaluate(backend, golden["cases"], top_k=5, only_paraphrase=flag)
            print(f"    {label:<26}{r['recall_at_1']:>10}{r['recall_at_3']:>10}"
                  f"{r['recall_at_5']:>10}{r['mean_reciprocal_rank']:>8}")

    if a.json:
        a.json.parent.mkdir(parents=True, exist_ok=True)
        a.json.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
        print(f"\n  wrote {a.json.relative_to(ROOT)}")

    if a.gate:
        if not BASELINE.exists():
            print("\n  NO BASELINE RECORDED — run with --json "
                  "ai/eval/rag_baseline.json first. Refusing to invent a threshold.",
                  file=sys.stderr)
            return 2
        floor = json.loads(BASELINE.read_text())
        failed = []
        for metric in ("recall_at_5", "groundedness", "negative_correct"):
            if result[metric] < floor[metric] - 0.02:      # 2pp tolerance for tie-breaks
                failed.append(f"{metric} {result[metric]} < floor {floor[metric]}")
        for metric in ("hallucination_rate", "error_rate"):
            if result[metric] > floor[metric] + 0.001:
                failed.append(f"{metric} {result[metric]} > floor {floor[metric]}")
        if failed:
            print("\n  QUALITY GATE FAILED:", file=sys.stderr)
            for f in failed:
                print(f"    {f}", file=sys.stderr)
            return 1
        print("\n  quality gate PASSED against the recorded baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
