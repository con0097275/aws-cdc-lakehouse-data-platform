# Business AI Evaluation — business_golden_v1

Run 2026-09-17T16:36:19+00:00 · as-of 2026-08-22

**25/25 cases passed** · P0 0 · P1 0

## Versions

- `dataset`: business_golden_v1
- `metric_registry`: business_metrics_v1
- `tools`: 14 tools
- `agent`: business_copilot_v1
- `analytics_engine`: analytics:core-v1
- `model`: none (Bedrock unavailable)
- `prompt`: none (deterministic summary)
- `feature_group`: feature_group:cba09fa84cea1307
- `corpus`: corpus_b639b715eabf0e37

## Accuracy by check

| check | accuracy |
|---|---|
| anomaly_agreement | 100% |
| certification | 100% |
| citation | 100% |
| citations_present | 100% |
| dq_warning | 100% |
| driver_ranking | 100% |
| fabricated_prediction | 100% |
| fabricated_result | 100% |
| forecast_refused | 100% |
| hallucination | 100% |
| intent | 100% |
| lineage_source | 100% |
| metric_id | 100% |
| numerical_value | 100% |
| period | 100% |
| refusal_reason | 100% |
| refused | 100% |
| source | 100% |
| tool_choice | 100% |
| trend_rows | 100% |
| unsafe_blocked | 100% |

## Cost and latency

- latency p50 **86.6 ms** / p95 **175.2 ms**
- tool calls per question: **0.96**
- Athena bytes per question: **7209.0**
- LLM tokens per question: **0** (no model invoked — Bedrock unavailable)
- estimated cost per question: **$0.0000**

## Failures

None.
