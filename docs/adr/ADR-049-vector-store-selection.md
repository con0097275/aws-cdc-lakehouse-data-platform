# ADR-049 — Vector store — S3 Vectors, conditional on beating BM25

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

The corpus is ~700 chunks of one team's jargon-dense prose, and the questions reuse that vocabulary. That is the regime BM25 is strongest in and semantic search least needed.

## Options

OpenSearch Serverless — rejected on arithmetic: its minimum standing capacity exceeds the entire monthly budget many times over. Aurora pgvector — rejected, always-on. Local FAISS — viable interim.

## Decision

Bedrock Knowledge Base + S3 source + a PINNED Cohere embedding model + S3 Vectors, behind `enable_ai_vector_index`, DEFAULT OFF. It ships only if hybrid retrieval measurably beats BM25 on a golden set of at least 50 questions. BM25 remains the default and the fallback.

## Consequences

Retrieval keeps working with the vector index absent, unreachable, or the flag off. Two retrieval paths must be kept honest by one evaluation.

## Cost

Embedding ~700 chunks is a sub-cent one-off; the index is ~3 MB. Near-zero idle. The cost that matters is spend without a measured gain.

## Security

Vectors inherit the corpus classification; a chunk excluded by classification never enters the candidate set. `bedrock:InvokeModel` is scoped to the pinned model id, never a wildcard.

## Rollback

Set the flag false — BM25 never stopped working. Delete the index prefix.

## Validation

AI-P4 runs both paths over the same expanded set and records the numbers here. `meets_retrieval_gate()` enforces the >=50 threshold in code.

---

## Amendment — 2026-08-26 (AI-P3): the toolchain blocker was misattributed

AI-P1 and AI-P2 carried blocker **B1** as "S3 Vectors cannot be addressed; provider pinned
at 6.56.0". Verified directly against the provider schema during AI-P3, that was **wrong for
Terraform** and right only for the operator CLI:

| Probe | Result |
|---|---|
| `aws s3vectors` (CLI, botocore 1.35.79) | **unavailable** |
| `aws bedrock-agentcore-control` (CLI) | **unavailable** |
| provider 6.56.0 `aws_s3vectors_vector_bucket` / `_index` | **present** |
| provider 6.56.0 `aws_bedrockagent_knowledge_base` | **present** |
| its `storage_configuration` backends | 8, including **`s3_vectors_configuration`** |
| provider 6.56.0 `aws_bedrockagentcore_agent_runtime` | **present** (relevant to ADR-056) |

The CLI gap affects ad-hoc operator commands, not infrastructure. **The approved path is
buildable exactly as specified**, with no substitution and no provider upgrade.

**B2 stands and is load-bearing.** `aws bedrock list-foundation-models --by-output-modality
EMBEDDING` returns only `cohere.embed-v4:0`, `cohere.embed-english-v3` and
`cohere.embed-multilingual-v3`. Titan is not offered to this account in `ap-southeast-1`,
and Bedrock Knowledge Bases commonly default to `amazon.titan-embed-text-v2:0` — that
default would fail at apply. The module therefore **pins Cohere and validates it**.

**B4 is CLEARED.** `cohere.embed-english-v3` was invoked successfully on 2026-08-26 —
the first real Bedrock call this repository has made — returning **1024-dimensional**
vectors. `embedding_dimension` is pinned to that measured width, not to a documented one.

### Why not any of the other seven storage backends

`storage_configuration` also offers OpenSearch Serverless, OpenSearch managed, RDS,
Pinecone, Neptune Analytics, MongoDB Atlas and Redis Enterprise. All seven are always-on and
priced per hour, and are disqualified by arithmetic against ADR-030 before any other
consideration. `s3_vectors_configuration` is storage-priced with no standing capacity, which
is the property that made S3 Vectors the choice in the first place.

**The measurement gate is unchanged.** This amendment records that the path is *buildable*.
It does not authorise turning it on: `enable_ai_vector_index` stays `false` until AI-P4
shows hybrid retrieval measurably beats BM25 on a golden set of at least 50 questions.

---

## AI-P4 measurement — the gate resolves to CONDITIONALLY ADOPT

Measured 2026-08-26 on `corpus:b639b715eabf0e37`, `chunking:v2-heading-aware`, **61 cases**
(57 answerable + 4 negative), BM25 lexical baseline, `top_k=5`:

| Metric | Value |
|---|---|
| recall@1 / @3 / @5 | 0.368 / 0.561 / **0.684** |
| groundedness | 0.561 |
| MRR | 0.486 |
| hallucination rate | **0.000** |
| error rate | **0.000** |
| latency p50 / p95 | 1.4 / 1.9 ms |
| retrieval + embedding cost | **$0.000000** |

### The finding that decides it

| Question phrasing | recall@1 | recall@5 | MRR |
|---|---|---|---|
| repository vocabulary | 0.408 | **0.755** | 0.535 |
| **paraphrased (synonyms)** | 0.125 | **0.250** | 0.188 |

**A 50-point recall collapse** the moment a question stops reusing the corpus's own words.
That is BM25 behaving exactly as its own docstring predicted — strongest where lexical
overlap is high — and it is the first evidence in this project that the regime actually
matters, because real operators paraphrase.

### A second, independent finding

BM25 cannot tell "we do not have this" from "we have something wordy about this".
`negative_correct` is **0.25**: only the case with zero lexical overlap is refused.

A score threshold was swept and **rejected on the measurement**: the unanswerable
"what is our Istio mesh configuration" scores **13.19**, higher than most genuine questions,
because Kubernetes vocabulary genuinely is in this corpus. Any cutoff that suppresses it
destroys real answers. `min_score` is therefore left at **0.0** — no lexical overlap at all
is the only defensible floor. Separating those two cases needs semantics, not a threshold.

### Decision

**The gate is met: hybrid retrieval is justified — and still ships OFF.**

- `enable_ai_vector_index` remains **false**. AI-P3 built the path; AI-P13 must apply it
  before dense retrieval can be measured at all.
- The comparison that turns the flag on is BM25-vs-hybrid **on this same 61-case set**,
  with the paraphrased subset as the metric that matters. That comparison is **not yet
  possible** — the index is not deployed.
- BM25 remains the default and the fallback regardless of the outcome, because the
  assistant must answer with the vector index absent.

**What would reverse this:** if hybrid fails to improve paraphrased recall@5 above 0.250 by
a margin worth its cost, the dense path is removed from the default flow and BM25 stands
alone. A measured null result is a valid outcome and must be recorded here, not retried
until it passes.

