# BUSINESS_AI_IMPLEMENTATION_STATE

Track D — Business Decision Intelligence. Sits alongside `AI_PLATFORM_STATE.md` (Track C);
neither replaces the other.

**Start here: [`docs/PLATFORM_AND_AI_GUIDE.md`](../PLATFORM_AND_AI_GUIDE.md)** — architecture, operating
guide, testing, cost and the four traps, in one place.

## Current state

| | |
|---|---|
| **Last checkpoint** | **`BUSINESS_AI_PLATFORM_NOT_READY`** |
| **Current phase** | **BAI-P9 — Final Gate — ⛔ NOT READY (2 of 7 E2E cannot run: no platform)** |
| **Next phase** | **Blocked.** Cancel the CMK deletion, rebuild, then re-run BAI-P9. |
| **Date** | 2026-08-28 |
| **Branch** | `session-02-prerequisites` |
| **Metrics governed** | **7** metrics · **14 tools** · **4 engines** · **4 KPIs** · **2 pilots** · **784 tests** |
| **AWS mutations** | **none** |
| **AWS cost** | **$0.00** |

## What exists

```text
BAI-P7
ai/eval/business_golden.yaml               25 cases, 19 categories
ai/eval/evaluate_business.py               expected values computed INDEPENDENTLY
make business-ai-eval                      JSON + Markdown + machine-readable metrics
artifacts/validation/bai-p7/               business_eval.json / .md

BAI-P6
ai/business_agent/tools.py                 14 tools, all backed; WRITE_TOOLS = {}
ai/business_agent/graph.py                 bounded LangGraph, 9 nodes, no ReAct loop
ai/business_agent/verify.py                evidence gate: insufficient -> say so
ai/business_agent/answer.py                answer contract + advisory-only guard
spark/tests/test_business_copilot.py       31 tests

BAI-P5
aiplatform/features/account_behavior.yaml  14 features, mart-grounded, AI-P1 contract
ai/business_ml/features.py                 offline store + PIT join + horizon guard
ai/business_ml/pilots.py                   anomaly (median/MAD) + next-best-investigation
ai/business_ml/registry.py                 content-addressed artifact; edits fail to load
ai/business_ml/inference.py                batch scoring, schema-drift reporting
spark/tests/test_business_ml.py            32 tests

BAI-P4
aiplatform/insights/insight_config.yaml    4 KPIs; the YAML IS the registration
ai/insights/config.py                      strict loader; unknown metric refused
ai/insights/record.py                      BusinessInsight + deterministic insight_id
ai/insights/summarise.py                   LLM summary, causal-claim guard, det. fallback
ai/insights/pipeline.py                    readiness -> certification -> analytics -> record
ai/insights/serving.py                     idempotent MERGE (never delete-then-insert)
ai/insights/digest.py                      executive digest from records only
airflow/dags/business_insights.py          ONE dag for every KPI (ADR-039 property)
serving/athena/views/ai_business_insight.sql  table + BI view + ops view
spark/tests/test_business_insights.py      29 tests

BAI-P3
ai/analytics/core.py                       deterministic math; None over a fabricated value
ai/analytics/drivers.py                    contribution, coverage, unexplained remainder
ai/analytics/anomaly.py                    zscore · IQR · EWMA · weekday baseline
ai/analytics/forecast.py                   6 methods, walk-forward backtest
ai/analytics/budget.py                     per-request query/dimension/bytes ceiling + cache
ai/analytics/engine.py                     orchestrator -> EvidencePack v2
spark/tests/test_business_engine.py        46 tests

BAI-P2
ai/analytics/request.py                    6 intents, validated before execution
ai/analytics/plan.py                       bounded plans (max 2 queries) + EvidencePack
ai/analytics/formatter.py                  structured answer, no LLM
ai/analytics/rag_boundary.py               RAG explains, never supplies the number
spark/tests/test_business_analytics.py     40 tests

BAI-P1
aiplatform/metrics/business_metrics.yaml   7 metrics, 40 aliases, 7 dimensions
ai/analytics/semantic.py                   loader + contract validation
ai/analytics/resolver.py                   phrase -> ONE metric, or AMBIGUOUS
ai/analytics/timespec.py                   canonical business-time windows
ai/analytics/compiler.py                   metric -> single bounded SELECT
ai/analytics/lineage.py                    reads dbt/target/manifest.json
ai/analytics/validate_metrics.py           actual-value check vs independent arithmetic
spark/tests/test_business_metrics.py       42 tests
docs/BUSINESS_METRIC_LAYER.md              usage + mermaid flow
docs/adr/ADR-061                           deterministic analytics before narration
```

## Infra state (BAI-P8, audited 2026-08-28)

```text
terraform state      3 resources        EC2 0   MSK 0   Glue 0   Lambda 0
Bedrock KBs 0        SageMaker 0        OpenSearch 0   NAT 0   EKS 0
S3 lake              1,332 objects      the only surviving asset
plan                 248 add / 0 change / 0 destroy / 0 replace   NOT APPLIED
```

**The lake CMK `e66f4dfa` is PendingDeletion, 2026-09-03 10:49 UTC.** Every one of those
1,332 objects is encrypted with it. This is the only irreversible item in the project.

## Blockers for BAI-P2

```text
1. NO LIVE VALIDATION. Athena is unreachable: the platform is destroyed and the lake CMK
   e66f4dfa is PendingDeletion (2026-09-03). Metric arithmetic is proven against an
   independent computation over a fixture mirroring the real mart schema; it is NOT proven
   against live data.

2. DIMENSIONS NOT MATERIALISED. product_code / branch_id / segment_code are declared and
   correctly REFUSED at compile time. Driver analysis by business dimension -- the core of
   the brief -- stays blocked until curated.dim_account and dim_customer exist.

3. FOUR BUSINESS DATES. 2026-08-17, 20, 21, 22 with a hole. Rolling averages, anomaly
   baselines and forecasting remain out of reach.

4. BEDROCK NOT INVOKABLE. Narration is unavailable; the layer is deterministic and works
   without it.
```

## Before BAI-P2

```bash
aws kms cancel-key-deletion --key-id e66f4dfa-3c59-4a63-a637-1bc4836e1866
aws kms enable-key --key-id e66f4dfa-3c59-4a63-a637-1bc4836e1866
```

Then decide the history strategy from BAI-P0 §11 — it determines whether BAI-P3 onward can
be honest.

## Re-verify (~30s, $0)

```bash
python3 -m pytest spark/tests/test_business_*.py -q          # expect 220 passed
python3 -m pytest spark/tests/test_ai_*.py -q                # expect 564 passed
```
