# BAI-P0 — Business Data / KPI Discovery

Discovery only. **Nothing was implemented.** Evidence is the repository (dbt models, the
Kimball dimension registry, serving DDL, reporting job registry) and the surviving S3
warehouse listing. Athena could not be queried: the platform is destroyed and the lake CMK
is pending deletion, so nothing in the lake is currently readable.

---

## 0. Two blocking facts, before any plan

**The lake CMK deletes on 2026-09-03.** `e66f4dfa` is `PendingDeletion` and every one of the
1,332 lake objects is encrypted with it. If it lapses, all data below ceases to exist —
including the only materialised mart. This is not a risk to manage later:

```bash
aws kms cancel-key-deletion --key-id e66f4dfa-3c59-4a63-a637-1bc4836e1866
aws kms enable-key --key-id e66f4dfa-3c59-4a63-a637-1bc4836e1866
```

**The models are excellent; the data is not there yet.** That is the headline of this
discovery, and it changes the order of the plan.

---

## 1. Available business marts

| dbt model | Grain | Materialised in S3? | Reporting job |
|---|---|---|---|
| `mart_account_balance_daily` | account_sk + business_date | **✅ yes** | ✅ registered |
| `mart_account_balance_monthly` | account_sk + month_start | ❌ no | ✅ registered |
| `mart_channel_engagement_daily` | channel_sk + business_date | ❌ no | ✅ registered |
| `mart_customer_360_daily` | customer_sk + business_date | ❌ no | ❌ |
| `mart_channel_performance_daily` | channel_sk + business_date | ❌ no | ❌ |
| `mart_transaction_monitoring_10m` | 10-min bucket + channel_sk | ❌ no | ❌ |

**Six marts designed, one built.**

## 2. Available measures — verified from model SQL

### `mart_account_balance_daily` (the only one with data)

| Column | Type | Additivity | Note |
|---|---|---|---|
| `closing_balance` | measure | **SEMI-ADDITIVE** | schema.yml: *"Across time use LAST, never SUM"* |
| `debit_amount` | measure | additive | |
| `credit_amount` | measure | additive | |
| `txn_count` | measure | additive | |
| `processing_status` | governance | — | the certification tier |
| `account_sk`, `customer_sk` | key | — | **surrogate keys only** |

Measures designed but **not materialised**: `txn_amount`, `distinct_customers`,
`avg_txn_amount` (NULL-safe), `session_count`, `digital_event_count`, `account_count`,
`total_balance`, `max_txn_amount`.

### ⚠ A correction to work I already delivered

My AI-v2 metric registry declares `total_account_balance = SUM(closing_balance)` with
`additive=True`. That is correct **across accounts within one date** and **wrong across
dates** — the dbt contract says semi-additive, use LAST. Nothing has produced a wrong number
yet (the diagnosis engine only ever compares within a date), but the registry needs a
`time_additivity` field before any trend or monthly rollup is built on it. Recorded here
rather than quietly patched, because it changes a declared contract.

## 3. Available dimensions

Six SCD2 dimensions are **designed** in `spark/dimensions/dim_builder.py` with genuinely
useful business attributes:

| Dimension | Business attributes | Materialised? |
|---|---|---|
| `dim_customer` | `segment_code`, `status`, `branch_id`, `age_band` (PII: `full_name`, `dob`) | ❌ |
| `dim_account` | `product_code`, `currency`, `status` | ❌ |
| `dim_branch` | `branch_name`, `region_code`, `status` | ❌ |
| `dim_product` | `product_name`, `product_group` | ❌ |
| `dim_channel` | `channel_name`, `channel_group` | ❌ |
| `dim_merchant` | `merchant_name`, `category_code` | ❌ |

**None is materialised.** `warehouse/curated/` contains exactly one table:
`fact_account_daily_snapshot`.

**This is the single most important gap.** The marts carry surrogate keys only, so
*"which branches caused the decline"* and *"which segments contributed to growth"* — the
questions the brief is built around — require a mart↔dimension join that has no dimension to
join to. Driver analysis is **not implementable today** beyond `account_sk`, which is not a
business dimension.

## 4. Available time grains and history

| | |
|---|---|
| Grains designed | 10-minute, daily, monthly |
| Grains with data | **daily only** |
| Partitions present | `2026-08-17`, `2026-08-20`, `2026-08-21`, `2026-08-22` |
| **Distinct business dates** | **4**, and **non-contiguous** — 08-18 and 08-19 are missing |

## 5. What this history rules out

| Capability | Needs | Have | Verdict |
|---|---|---|---|
| Period comparison (D vs D-1) | 2 contiguous dates | 3 contiguous (20–22) | ⚠ marginal |
| 7-day rolling average | 7 | 4 | ❌ |
| 30-day average / trend | 30 | 4 | ❌ |
| Anomaly (z-score, IQR, EWMA) | ~14–30 for a baseline | 4 | ❌ |
| Forecast + backtest | ≥14 (2 weekly cycles) | 4 | ❌ |
| Driver analysis by business dim | dimensions | none | ❌ |
| Point-in-time ML features | months | 4 days | ❌ |

Six of the eight analytical intents in the brief cannot produce a defensible number on this
data. Building them now would produce a demo that computes confidently over four points —
which is exactly the "AI theatre" the brief is trying to move away from.

## 6. What IS ready, and is genuinely strong

**The certification ladder.** `processing_status` carries four tiers —
`PROVISIONAL_NRT` → `PROVISIONAL_CORRECTED` → `RECONCILED` → `CERTIFIED` — and
`mart_customer_360_daily` propagates the **weakest** status across joined facts. Most
platforms cannot answer *"is this number certified?"* at all. This one can, from a column
that already exists. §7 of the brief is the cheapest high-value item here.

**The serving contract.** Certified and provisional are **separate views** on purpose, so a
report cannot mix them behind one filter; `v_dim_customer_bi` tokenises PII rather than
dropping or nulling it. Both decisions are documented with their reasoning.

**Semi-additivity is already declared**, and `distinct_devices` was deliberately excluded
from a mart because it is non-additive. The semantic discipline the brief asks for partly
exists — it is in dbt contracts rather than a metric registry.

**OPS metadata**: watermarks, execution history, DQ, reconciliation, four DynamoDB tables.

## 7. Candidate metrics — only those the data supports

Ranked by whether they can be computed **today**:

| Metric | Source | Buildable now? |
|---|---|---|
| `total_closing_balance` | `SUM(closing_balance)` per date | ✅ (within-date only) |
| `total_debit_amount` | `SUM(debit_amount)` | ✅ |
| `total_credit_amount` | `SUM(credit_amount)` | ✅ |
| `total_txn_count` | `SUM(txn_count)` | ✅ |
| `active_account_count` | `COUNT(DISTINCT account_sk)` | ✅ non-additive |
| `avg_balance_per_account` | `AVG(closing_balance)` | ✅ non-additive |
| `net_flow` | `credit_amount - debit_amount` | ✅ derived |
| balance by **product / branch / segment** | needs `dim_account`/`dim_customer` | ❌ blocked |
| channel / digital metrics | needs marts built | ❌ blocked |

**Seven metrics are real today. All seven are on one mart, one grain, four dates.**

## 8. Data gaps, ranked by what they block

| # | Gap | Blocks |
|---|---|---|
| 1 | **No materialised dimensions** | all business-dimension driver analysis — the core of the brief |
| 2 | **4 business dates, with a hole** | forecasting, anomaly, rolling averages, trend, ML features |
| 3 | **5 of 6 marts never built** | customer 360, channel, digital, monthly, intraday |
| 4 | Platform destroyed + CMK expiring | everything, permanently, in 6 days |
| 5 | `serving.ai_business_insight_daily` does not exist | §17 Power BI insight serving |
| 6 | Bedrock not invokable | all LLM narration (§ the brief's final step) |

## 9. Top 3 highest-business-value AI use cases

Scored 1–5. **Data readiness is scored on today's data, not on intent.**

### #1 — Certified KPI movement explanation with certification status

*"Deposits moved −3.7% yesterday. 41% of it is Branch HCM-01. This number is CERTIFIED."*

| Business value | Data readiness | AI/ML value | Complexity | Cost | Portfolio |
|---|---|---|---|---|---|
| **5** | **2** (needs dims + history) | 3 | 3 | ~$0 | **5** |

The certification tier is the differentiator and it already exists. Blocked only by gaps 1–2.

### #2 — Pipeline-vs-business discrimination on KPI movement

*"The balance is down 60% — but only 4 of 12 expected rows landed. This is a pipeline
incident, not a business event."*

| Business value | Data readiness | AI/ML value | Complexity | Cost | Portfolio |
|---|---|---|---|---|---|
| **5** | **4** — works on today's data | 2 | 2 | ~$0 | **4** |

**Already implemented and tested** (AI-v2 `diagnose.py`, `DATA_INCOMPLETE`). Highest ratio of
value to remaining work in the entire brief, and the one thing here that most teams get wrong.

### #3 — Daily executive digest over certified EOD

*Top movements, drivers, certification, written after the EOD gate.*

| Business value | Data readiness | AI/ML value | Complexity | Cost | Portfolio |
|---|---|---|---|---|---|
| **4** | **2** | 3 | **4** (Airflow + serving table + idempotency) | low | **5** |

Depends on #1. Genuinely impressive in a portfolio because it proves orchestration, not just
a chat box.

### Deliberately ranked below the line

**Customer attrition / churn ML** — no honest label exists in four days of synthetic data.
Building it repeats the mistake already recorded against `account_balance_tier_next_day`
(AUC 1.0, synthetic label, no meaning).

**Transaction-risk anomaly ML** — `fact_transaction` is not materialised at all.

**KPI forecasting** — four points. A forecast here is a guess with a confidence interval
drawn around it.

---

## 10. Proposed BAI-P1 → BAI-P9

**The brief assumes the data exists. It does not.** So BAI-P1 is not the semantic layer — it
is making the data real. Building a decision-intelligence platform over four dates and zero
dimensions would produce exactly the demo the brief is trying to escape.

| Phase | Scope | Gate to pass before starting |
|---|---|---|
| **BAI-P1** | **Data foundation.** Cancel CMK deletion. Rebuild. Materialise the 6 dimensions and the 5 unbuilt marts. Generate **≥30 contiguous business dates** by replaying CDC across a date range. | CMK cancelled |
| **BAI-P2** | **Business metric registry.** Extend the AI-v2 registry with `time_additivity`, certification requirement, dimension bindings via `*_sk` joins, owner, unit, freshness SLA. Reuse dbt `meta` where it already carries the fact. | ≥30 dates, dims present |
| **BAI-P3** | **Metric resolver + safe query planner.** Aliases, ambiguity → clarification. Bounded plans over MART/SERVING only, reusing the existing Athena guards. | P2 |
| **BAI-P4** | **Evidence pack + certification.** Every answer carries query IDs, SQL hash, bytes scanned, as-of, and the tier. | P3 |
| **BAI-P5** | **Analytics engine on real dimensions.** Extend the existing diagnosis/forecast engines to dimension-aware driver analysis; enable anomaly + forecast now that history supports them. | P1 history |
| **BAI-P6** | **Proactive insight pipeline.** Airflow after the certified EOD gate → `serving.ai_business_insight_daily`, idempotent per business date. | P5 |
| **BAI-P7** | **Features + one ML pilot.** Only if P1 yields a defensible label. Otherwise formally skipped, with the reason recorded. | P1 |
| **BAI-P8** | **LangGraph business copilot + narration.** Requires Bedrock billing fixed; degrades to deterministic evidence without it. | Bedrock |
| **BAI-P9** | **Business evaluation + 3 E2E runs + production review.** Numerical correctness against direct SQL. | all |

### Cost

BAI-P1 is the only expensive phase (~$1.5/day plus MSK while rebuilding and replaying).
P2–P7 are local and ~$0. You are at **$69.75 of $100**, so P1 should be run in one focused
window and then destroyed — **after** re-running the CMK convergence.

---

## 11. What I recommend you decide first

Do not start BAI-P1 by rebuilding blindly. Decide the **history strategy**, because it
determines whether any of BAI-P5 onward is honest:

- **Replay CDC across 30+ dates** — real lineage end to end, most faithful, costs the most
  compute.
- **Backfill the marts directly from generated source history** — cheaper, still real
  dimensional data, skips proving CDC over the range.
- **Accept 4 dates** and formally descope forecasting/anomaly/ML, keeping #2 (pipeline-vs-
  business) as the flagship. Cheapest, and still portfolio-strong.

I would take the second for value-per-dollar, and the third if the $100 ceiling is firm.
