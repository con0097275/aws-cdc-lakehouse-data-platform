# AI Governance

Normative for the AI/Business-AI plane. Verified live 2026-09-03; evidence in
`artifacts/validation/final-e2e/ai/`.

## 1. Knowledge source governance

The corpus is built **only** from documentation and governance metadata — never from
tfvars, Terraform state, or the data itself. Redaction runs in both directions (into the
model and out of it). Pattern redaction is explicitly the *second* layer; the primary
control is what is admitted to the corpus at all.

## 2. Metric ownership

Every governed metric declares owner, grain, measure, unit, time-additivity, allowed
dimensions, minimum certification and a content-addressed version. Example, live:

```
total_closing_balance · owner risk-data · grain account_sk + business_date
measure SUM(closing_balance) · semi_additive_last · minimum_certification RECONCILED
version metric:0d1b5e7ea203f9de
```

A dimension declared `available: false` carries a `blocked_reason` and must fail **loudly**
at compile time. **This is currently violated** — see finding B2 in
`docs/validation/BUSINESS_AI_E2E.md`.

## 3. Lineage

Read from dbt's own `manifest.json`, never duplicated into a hand-maintained list. Resolved
live: `mart_account_balance_daily ← stg_fact_account_daily_snapshot ← curated.fact_account_daily_snapshot`.

## 4. Read-only boundary

`WRITE_TOOLS = {}`, asserted by test (ADR-057). The boundary is enforced in **code on the
model's output**, not in prompt wording:

- statement allow-list (`SELECT/WITH/SHOW/DESCRIBE/EXPLAIN`), comment stripping, anti-stacking
- database allow-list by suffix, with `_stream` / `_full_cdc` / `_quarantine` explicitly denied
- **every FROM/JOIN operand must be qualified** — an unqualified name is refused
- injected `LIMIT`, re-validated after injection; 4 KiB SQL ceiling; workgroup byte cutoff
- IAM denies the raw databases outright and is the **first** control; the allow-list is the second

Any future write capability requires a deliberate code change plus human approval — there is
no runtime toggle.

## 5. Point-in-time feature correctness

`event_time_column` is mandatory; processing-time columns are **rejected** as join keys.
`assert_no_future_leakage` and `assert_label_horizon_excluded` catch the leak that
`feature_event_time <= label_event_time` silently permits when the label is stamped at the
end of its horizon. 73 tests cover this.

## 6. Versioning and provenance

Every answered request carries: `request_id`, intent, `metric_id` + version,
`feature_group` version, `config_version`, dbt node + upstream lineage, Athena query ids,
sql hashes, bytes scanned, tool calls, and token/cost. Live example: request
`f7b3804a-…`, `metric:0d1b5e7ea203f9de`, `feature_group:bded816b072c310d`,
`cfg-1682b193a20ce2ae`, `tokens 0/0 · $0.000000`.

## 7. Evaluation and tracing

`e2e_p14` (10 scenarios), `drills_p15` (20 failure drills, 0 P0), RAG gate against a
recorded baseline, agent gate, and a 25-case business golden set whose expected values are
computed **independently** of the code under test. All passing live.

## 8. Answer discipline

- Deterministic analytics **before** narration (ADR-061). The number never comes from the LLM.
- RAG explains; it never supplies a current KPI value.
- Insufficient evidence → say so. Proven live: a query returning no rows produced
  `actual_value: null` and *"I can't answer that from the evidence available"* rather than
  reaching for an adjacent date.
- Insufficient history → refuse with the requirement stated, e.g.
  *"forecast NOT produced: 0 daily points, 14 required."*
- Causal language is avoided: contribution is reported as "contributed to", never "caused".

## 9. Cost

12 components costed, **none always-on**. Generation is currently unavailable (Bedrock
`INVALID_PAYMENT_INSTRUMENT`), so every figure the platform produces today is deterministic
and free; token cost is `0`.

## 10. Known gaps

`B2` (dimension substitution), generated-narration validation, live feature materialisation
and the proactive-insight serving path. See `docs/KNOWN_LIMITATIONS.md`.
