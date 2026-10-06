# AWS CDC Lakehouse — What This Project Is, in Five Minutes

A production-shaped change-data-capture lakehouse on AWS, with a governance and
bounded-recovery plane on top, and an AI copilot that can be asked in English to
investigate a data defect and repair only what it actually affected.

Built solo. Every number below was measured, not estimated.

---

## 1. The shape of it

```
Oracle ─┐                                          ┌─ Athena ── Power BI
        ├─ Debezium ─ MSK (KRaft) ─ Spark ─ Iceberg ┤
SQL Svr ┘                          Structured      └─ dbt ── marts
                                   Streaming
                                        │
                    ┌───────────────────┴────────────────────┐
                    │  DataHub · OpenLineage · data contracts │
                    │  DQ · reconciliation · certification    │
                    │  lineage-driven impact · bounded recovery│
                    └───────────────────┬────────────────────┘
                                        │
                              AI Reliability Copilot
```

| | |
|---|---|
| Ingest | Oracle + SQL Server → Debezium → MSK Kafka (KRaft), IAM + TLS + KMS, private brokers |
| Storage | Apache Iceberg v2 on S3, AWS Glue Data Catalog |
| Compute | Spark Structured Streaming + EMR Serverless, dbt for marts |
| Realtime serving | **STREAMING_RT** — four apps, two of them resident processes: a 30 s micro-batch stream, a correction pass, a nightly rebuild, a 45 s datamart |
| Orchestration | Airflow 3.2.2 on k3s, `KubernetesExecutor` |
| Serving | Athena (core), Power BI; Redshift/Trino behind feature flags |
| Governance | DataHub, OpenLineage, 84 governed assets, contracts, DQ, reconciliation, a 5-tier certification ladder |
| AI | A bounded LangGraph copilot with a closed mutation surface |

---

## 2. Scale, measured

| | |
|---|---|
| Python modules | **423** files — `cdc/` 46, `ai/` 84, `spark/` 94 |
| Tests | **3,529 passing, 0 failing** across 115 test files |
| Terraform | **14** modules, every one `enable_*` gated with a destroy path |
| Airflow DAGs | **17** — orchestration only, enforced by test |
| Operator scripts | **78**, every AWS-mutating one dry-run by default |
| Documentation | **154** docs · **82** ADRs · **11** validation reports |
| Decisions recorded | 91 ADR rows, 93 decision-log entries |
| Governed assets | **84**, 100% owned, classified and described |
| Lineage graph | **81 nodes, 80 edges, 67 column edges, 0 cycles** |
| AWS spend, metadata + AI programmes | **$0** |

---

## 3. What is actually proven, and how

The project's own rule: **no PASS without evidence**, and the *kind* of evidence is named.

| Capability | Evidence |
|---|---|
| CDC end to end | `REALTIME succeeded=7 rows=5770`, `EOD closed=10 certified=5 rows=3404`, readiness gate enforced |
| Resident realtime serving layer | the four apps run end to end on EMR Serverless **twice** (2026-09-03, and 2026-09-06 after a full platform rebuild); the late-dimension loop fired and was repaired both times; the datamart reconciles to the cent |
| Certified EOD on EMR | one run: 320 rows, `dq=PASS recon=PASS status=CERTIFIED`, Iceberg snapshot `80230069689276128 → 3683870259747946533` |
| Airflow runtime lineage | Helm rev 2, provider 2.20.2; a real KubernetesExecutor run emitted `START` + `FAIL` with `parent`/`root` facets |
| Spark runtime lineage | 12 real OpenLineage events, producer string matching the pin |
| DataHub | 373 governance + 87 lineage aspects; **18-asset** multi-hop traversal, mart at hop 7; 147 datasets after ingestion |
| Data quality | 160+ verdicts in `ops.dq_result_v2` from 35 datasets / 153 checks |
| Deterministic recovery | the full loop in **96 s** on real Iceberg/Glue/Athena; mart corrected 80.0 → 60.0 |
| **AI-driven recovery** | a natural-language request → real repair → mart **1600.0 → 1000.0**, violations 3 → 0 |
| Column lineage | **28 of 67** edges validated against real data — see §5 |

Two acceptance reviews, scored by the same rule:

```
DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY        25 pass · 0 fail · 1 N/A
AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY     27 pass · 0 fail · 1 blocked externally
```

Both returned **NOT_READY first** — DRP12 twice — and earned READY only on live evidence.

---

## 4. The AI copilot, and why it is not a chatbot

Ask it in English:

> *"A duplicate CDC event doubled the BALANCE column in EOD ACCOUNT for COB 2026-09-28 on
> accounts A001, A002 and A003. Investigate and repair only the affected downstream data."*

It resolves the asset against the real catalogue, reads the operational ledgers, diagnoses
from the rows it finds, checks whether the column lineage is strong enough to narrow on,
builds a plan where each job runs at **the narrowest scope that job supports**, and stops
for a human.

**The rule the whole design enforces:**

> The LLM decides what the user *meant*. It never decides whether data is correct, what is
> affected, or that a repair worked.

| | |
|---|---|
| Tools | **27** — 18 read-only, 7 planning, **2** mutating |
| Mutation surface | closed **by name** in a frozenset (ADR-092) |
| `run_shell`, `execute_sql`, `trigger_any_dag`, `reset_kafka_offset`… | **cannot be constructed** — 12 tests assert each raises |
| `submit_recovery_plan` takes | `plan_id`, `approval_id`, `idempotency_key` — **and nothing else** |
| Root causes | **14**, each with a disposition; **5 cannot produce a plan at all** |
| Safety matrix | **52** numbered attacks, each mapped to a test |
| Unauthorized mutations reachable | **0** |

Six causes where a rerun is the wrong answer and the platform refuses one:
`BAD_SOURCE_VALUE`, `MISSING_SOURCE_EVENT`, `TRANSFORM_LOGIC_DEFECT`, `DQ_RULE_DEFECT`,
`UNKNOWN`.

---

## 5. Three ideas I would want to be asked about

### Column impact and execution granularity are different questions

```
column lineage      →  WHICH jobs are affected     (dependency selection)
RecoveryCapability  →  WHAT a job can recompute    (execution granularity)
```

Knowing only `BALANCE` is wrong does **not** mean Spark can rewrite one column. It means
fewer jobs run — each at whatever scope it declares. A real plan therefore mixes
granularities: `eod_build` rebuilds a whole business date because it has no key predicate,
while `reporting` rebuilds **three keys**.

### Confidence has to be earned, not relabelled

Column lineage was `DERIVED`, which blocked column-narrowed recovery. Two ways forward:
change the evidence string, or check whether the columns are really there.

Checking meant handling a wrinkle: CDC layers store the source row as **JSON**, so
`payload_after` is a `string` and `BALANCE` is not a Glue column at all — it is a key inside
a blob, present in every row. A schema lookup alone confirmed **3 of 67** edges; reading the
JSON keys from real data confirmed **28**.

The remaining 39 stay `DERIVED` and **cannot narrow a recovery**: dbt `int_*`/`stg_*` models
are ephemeral, and some tables are empty. They are recorded as failures, not hidden.

### The stream does not fix its own mistakes

A fact can reach the fast path before its dimension does. Three options: hold the
micro-batch and retry (the retry builds backpressure and one missing dimension becomes an
outage), drop the row (a report that silently omits an account is wrong in the way nobody
can see), or **publish it flagged and record a pointer to it**.

The third is the only one whose failure mode is visible. A slower pass then re-enriches it
from the canonical layer — *not* from the stream's own output, which would faithfully
reproduce the stream's mistakes.

The write order matters more than the write: the **pointer goes first**. If the pointer
lands and the row does not, a correction repairs an account that was never published —
harmless. Reverse it and a crash publishes a wrong row with nothing flagging it.

→ [`REALTIME_STREAMING_RT.md`](REALTIME_STREAMING_RT.md)

---

## 6. Where to look

| I want to… | Read |
|---|---|
| use it, and check every claim | [`HOW_TO_USE_AND_VERIFY.md`](HOW_TO_USE_AND_VERIFY.md) |
| see real scenarios end to end | [`USE_CASES.md`](USE_CASES.md) |
| judge the engineering | [`ENGINEERING_JOURNAL.md`](ENGINEERING_JOURNAL.md) |
| see the resident streaming layer | [`REALTIME_STREAMING_RT.md`](REALTIME_STREAMING_RT.md) — four apps, two live windows, and the three rules that are easy to get backwards |
| see the copilot answer | [`AI_COPILOT_TRANSCRIPTS.md`](AI_COPILOT_TRANSCRIPTS.md) — real sessions, annotated |
| try the copilot | [`AI_COPILOT_SAMPLE_QUESTIONS.md`](AI_COPILOT_SAMPLE_QUESTIONS.md) |
| see the acceptance scoring | [`validation/DRP12_FINAL_REVIEW.md`](validation/DRP12_FINAL_REVIEW.md) · [`validation/AIGR12_FINAL_REVIEW.md`](validation/AIGR12_FINAL_REVIEW.md) |
| understand a decision | [`../DECISIONS.md`](../DECISIONS.md) (91 ADRs) |

---

## 7. What is not done, stated plainly

- **Bedrock invocation is gated** at the account level pending an Anthropic use-case form.
  Two calls succeeded before the gate applied, so the wiring is proven and the entitlement
  is not. Nothing in the recovery loop depends on it.
- **39 of 67 column edges remain `DERIVED`** and cannot narrow a recovery.
- **8 of 19 governance use cases are modelled**, 4 not built — each labelled.
- **Spark emits no dataset-level lineage on EMR Serverless**: OpenLineage and Iceberg load
  under different classloaders, and the platform *rejects* the documented cure. Needs a
  custom EMR image.
- **No Power BI workspace** exists in this account, so that connector is untestable here.
- **The resident stream has never run a full working-day window.** Every run is bounded
  by `--run-seconds` (240 s / 180 s); the reference runs 08:00→20:00. No freshness, throughput or
  recovery-time figure exists for it, and the layer enriches one dimension where the reference
  joins six — the mechanism is per-join, but only one is proven.

A section like this is the point of the project, not an apology for it.
