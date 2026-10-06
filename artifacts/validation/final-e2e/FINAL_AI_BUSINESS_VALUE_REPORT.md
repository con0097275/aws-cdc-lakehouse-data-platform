# FINAL AI BUSINESS VALUE REPORT — 2026-09-03

## The claim that matters

The agent's number **is** the certified mart number — not a re-derivation, not an estimate.

```
Direct SQL   sum(closing_balance) WHERE business_date='2026-09-03' AND processing_status='CERTIFIED'
             -> 2,031,880,937.77   (321 accounts, Athena qid 9a0e997e-…)

Agent        ask_business("what is the total closing balance on 2026-09-03")
             -> actual_value 2031880937.77 · data_status CERTIFIED · limitations []
```

Reached through the governed metric layer and the **real guarded Athena tool**, so the
read-only guard, database allow-list, injected LIMIT and byte ceiling were all in the path.

| Capability | Status | Evidence |
|---|---|---|
| Governed metric resolution | **PASS** | `total_closing_balance`, owner `risk-data`, `metric:0d1b5e7ea203f9de`, grain `account_sk + business_date`, `semi_additive_last`, `minimum_certification: RECONCILED` |
| Lineage | **PASS** | resolved from the dbt manifest: mart ← `stg_fact_account_daily_snapshot` ← `curated.fact_account_daily_snapshot` |
| Actual KPI vs direct SQL | **PASS** | exact match |
| EvidencePack | **PASS** | request_id, query_ids, sql_hashes, metric_version, certification, bytes |
| Refuses without evidence | **PASS** | asked for a date with no rows → `actual_value: null`, *"I can't answer that from the evidence available"* |
| Comparison | **NOT_APPLICABLE** | only one business date exists; `comparison: null`, nothing invented |
| Driver analysis | **FAIL (P1)** | asked by `product_code` (unavailable) → silently answered by `account_sk`, labelled "Top segments" |
| Anomaly | **NOT_APPLICABLE** | no baseline; returned null — but with an empty summary, so it does not say why (P3) |
| Forecast | **NOT_APPLICABLE, exemplary** | *"forecast NOT produced: 0 daily points, 14 required…"* plus a semi-additivity caveat |
| RAG grounding | **PASS** | 3 citations each resolved to real files with matching text |
| RAG must not supply KPI numbers | **PASS** | the KPI came from `query_athena`; RAG served definitions only |
| Feature store / PIT | **PARTIAL** | contracts + leakage guards pass in 73 tests; **not materialised against live data**. `scripts/ai-feature-run.py` (2026-09-04) is the runner: guarded Athena read, one query per business date with a refusal on truncation, then `materialize` + `run_anomaly_batch`. Verified end to end against a fake Athena; not yet run live |
| ML inference | **NOT_TESTED** | pilot label is synthetic (AUC 1.0 = plumbing only); `run_model_inference` deliberately NOT_IMPLEMENTED as an agent tool. The UNSUPERVISED anomaly pilot is what `ai-feature-run.py` scores — a deviation score with per-feature attribution, explicitly not a risk rating |
| Proactive insight serving | **PARTIAL** | pipeline ran live 2026-09-03 (deterministic `insight:c4d1a236…`, rerun identical); persistence needed a `serving` Glue database, now in Terraform and awaiting apply |
| Agent security | **PASS** | 4/4 refused, zero tools called |
| Governance provenance | **PASS** | versions for metric, feature group, config, prompt path, tool catalog; `tokens 0/0 · $0.000000` |
| Generated narration | **NOT_TESTED** | Bedrock not invokable; every number here is deterministic |

## Platform AI gates against the live platform

```
ai/eval/e2e_p14.py       10 PASS / 0 FAIL / 0 BLOCKED     (8/10 before the platform existed)
make ai-eval-rag-gate    PASSED
make ai-eval-agent-gate  PASSED                            (FAILING before)
```

## Honest limit

Business value is proven for **one KPI on one business date**. Comparison, trend, anomaly
and forecast are structurally unavailable until the mart holds more history, and driver
analysis is defective. The deterministic layer's central promise — *return nothing rather
than fabricate* — held on every one of those.
