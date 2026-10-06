# AI End-to-End Validation (AI-P14)

Run 2026-08-27 · account `111122223333` · `ap-southeast-1` · corpus `corpus:b639b715eabf0e37`

**Generation is OFF.** Bedrock is not invokable from this account
(`INVALID_PAYMENT_INSTRUMENT`), so no scenario produced model prose. That removes the risk
this phase exists to guard against: every result below rests on a tool result, a resolved
citation, or a refusal — never on fluent text. Token and cost columns are $0.00 because no
model was called, not because cost was estimated.

---

## 1. E2E matrix

| Scenario | Result | Intent | Tools called | Citations | Latency | Cost | Security |
|---|---|---|---|---|---|---|---|
| S1 | **PASS** | `KNOWLEDGE` | `retrieve_knowledge` | 4 | 883.4 ms | $0.00 | read-only |
| S2 | **PASS** | `KNOWLEDGE` | `retrieve_knowledge` | 4 | 86.8 ms | $0.00 | read-only |
| S3 | **BLOCKED** | `STRUCTURED_DATA` | — | — | 2656.0 ms | $0.00 | read-only |
| S4 | **PASS** | `PIPELINE_OPS` | `get_pipeline_status`, `retrieve_knowledge` | 4 | 445.6 ms | $0.00 | read-only |
| S5 | **PASS** | `LINEAGE` | `get_data_lineage` | — | 27.4 ms | $0.00 | read-only |
| S6 | **PASS** | `FEATURE` | `get_feature_definition` | — | 52.6 ms | $0.00 | read-only |
| S7 | **PASS** | `PREDICTION` | `get_model_status` | — | 4.9 ms | $0.00 | read-only |
| S8 | **PASS** | `PIPELINE_OPS` | `get_pipeline_status`, `retrieve_knowledge` | 4 | 672.4 ms | $0.00 | read-only |
| S9 | **PASS** | `UNSAFE` | — | — | 3.2 ms | $0.00 | refused |
| S10 | **PASS** | `MIXED` | `retrieve_knowledge`, `get_pipeline_status` | 4 | 416.3 ms | $0.00 | read-only |

**9 PASS · 0 FAIL · 1 BLOCKED**

`BLOCKED` is deliberately distinct from `FAIL`. In S3 the agent did everything right —
routed to `STRUCTURED_DATA`, planned `query_athena`, issued a **real** Athena query, and
degraded with a clear message. Athena returned `TABLE_NOT_FOUND`. Calling that a FAIL would
blame the agent for absent data; calling it a PASS would claim a capability never
demonstrated. It is neither.

## 2. Capture

| Scenario | Request ID | Agent version | Prompt version | Corpus |
|---|---|---|---|---|
| S1 | `48e05f85-e65d-4651-8776-205bb57f3aba` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | corpus:b639b715eabf0e37 |
| S2 | `f196e493-9870-4e2a-8c50-c019935c1e83` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | corpus:b639b715eabf0e37 |
| S3 | `6d0d76d5-8e0f-47ce-94c9-67b0583a0023` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | — |
| S4 | `730e5b36-7f5b-4a58-a214-22e9485dc7ab` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | corpus:b639b715eabf0e37 |
| S5 | `53d0c9be-8338-4655-9d72-cc486676dda2` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | — |
| S6 | `fec224cf-6bc2-41d0-8590-f36d7400932b` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | — |
| S7 | `8fc80ee3-3394-486e-a077-3658b71662b8` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | — |
| S8 | `8bc4ef56-c9b5-49b7-9ee1-79a62182ebf2` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | corpus:b639b715eabf0e37 |
| S9 | `a6683314-70f9-4682-b026-560d7ee8a05a` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | — |
| S10 | `fd8fcb28-efa6-4447-b4a2-e7d9ffc7b433` | `agent:copilot-v1` | `prompt:system-v1:f33d4eed325a` | corpus:b639b715eabf0e37 |

Model config: `generation_enabled=false`; 8 tools registered; `WRITE_TOOLS = {}`.

## 3. Data correctness — cross-checked against ground truth

Every non-RAG PASS was verified against the system of record, not against the tool's own claim.

| Check | Agent said | Ground truth | Verdict |
|---|---|---|---|
| S4/S8 OPS state | `watermark: null`, `executions: []` | `job-watermark-state` scan → **0 items**; `job-execution` → **0 items** | **MATCH** |
| S5 lineage | `mart.dim_customer` upstream `spark.build_kimball` | lineage graph entry exists | **MATCH** |
| S6 feature | owner `risk-data`, 3 features | `aiplatform/features/customer_behavior.yaml` | **MATCH** |
| S7 model | `model:eb58e352986ba990` | `artifacts/models/model_eb58e352986ba990/model.json` and the S3 copy | **MATCH** |
| S1/S2 citations | 3 cited chunks each | **file opened, cited text located inside it** | **MATCH** |

Citation verification does not check that a citation was *formatted* — it opens the cited
file and asserts the cited text is genuinely present. A retriever inventing a plausible
`source_path` fails that check.

The OPS result is the one worth dwelling on: with both DynamoDB tables empty, the tool
returned nulls, echoed `job_id` from its own argument rather than inventing one, and
attached the caveat that *"a watermark is a CLAIM"*. It reported having no data instead of
answering from nothing.

## 4. Security

S9 (`DROP TABLE …`) routed to `UNSAFE`, produced an **empty plan**, called **no tool**, and
recorded `refused: read-only V1`. Independently, `assert_read_only_sql` raised
`GuardViolation` on the same string — refusal holds in the router *and* in the guard, so a
routing miss alone cannot execute a mutation. `WRITE_TOOLS` is asserted empty (ADR-057).
No scenario mutated anything.

## 5. Regression suite

| Suite | Result | vs baseline |
|---|---|---|
| Agent eval (44 scenarios) | intent 1.0 · tool-arg 1.0 · groundedness 1.0 · unsafe-block 1.0 · error-recovery 1.0 · **p0 violations 0** | unchanged |
| | tool_selection 0.9545 · **structured_query 0.0** | unchanged |
| RAG eval (61 cases) | MRR **0.486** · recall@5 0.6842 | matches `rag_baseline.json` exactly |
| AI unit suites | **516 passed** | unchanged |

## 6. Honest weaknesses

1. **Structured querying is unproven end to end.** The Glue catalog holds **0 tables** across
   all 7 databases while 567 Iceberg objects sit in S3. S3/s01/s02 all fail on this one cause.
2. **BM25 retrieval degrades badly on paraphrase**: recall@5 is 0.7551 on repo vocabulary but
   **0.25 on paraphrased questions**. This is the measurement ADR-049 asks for, and it argues
   *for* embeddings — which cannot be evaluated while Bedrock is unavailable.
3. **The pilot model is not a model.** `account_balance_tier_next_day` reports accuracy,
   F1 and AUC-ROC all `1.0`, and the artifact carries `synthetic_label`. S7 proves the
   *plumbing* — versioned artifact, correct lookup — and nothing about predictive quality.
4. **No generated answers were tested**, so answer synthesis, prompt-injection resistance in
   generation, and token cost remain unvalidated.

## 7. Unblock sequence

```bash
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute   # type: REENCRYPT LAKE
# then re-register the Iceberg tables into Glue, then re-run:
python3 ai/eval/e2e_p14.py
```

The re-encryption must come first: the EMR/Spark roles hold only the current CMK while every
`warehouse/` object is on an older one, so a registration job would fail on `kms:Decrypt`.
