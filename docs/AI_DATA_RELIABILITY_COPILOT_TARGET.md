# AI DATA RELIABILITY & GOVERNANCE COPILOT — TARGET ARCHITECTURE

- Phase **AIGR0** · date **2026-09-30**
- Builds on **`DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY`**
- Companion audit: `AI_RECOVERY_CURRENT_STATE_AUDIT.md`

---

## 1. The one sentence this design exists to enforce

> **The LLM decides what the user meant. It never decides whether data is correct, what is
> affected, or that a repair worked.**

Every value that could authorize a write — dataset URN, job id, DAG id, SQL, Kafka offset,
Iceberg snapshot, SCN/LSN, business key, certification status — comes from a tool, never
from the model. This is ADR-061 (deterministic analytics before narration) extended from
*answers* to *actions*.

---

## 2. Where the copilot sits

```
                         USER / API / CHAT
                                │
                                ▼
                   AI Reliability Copilot  (LangGraph, bounded)
                                │
              ┌─────────────────┼──────────────────┐
              ▼                 ▼                  ▼
          READ TOOLS        PLAN TOOLS        MUTATION SURFACE
              │                 │                  │
              ▼                 ▼                  ▼
      DataHub / lineage   RecoveryPlan      submit_recovery_plan
      OPS / DQ / cert     Impact graph              │
      safe Athena         Policy engine             ▼
      contracts / RAG                    Recovery Control Service
              │                                     │
              └──────────────────┬──────────────────┘
                                 ▼
                    Airflow  →  Spark / dbt
                                 ▼
                       DQ → reconciliation → certification
```

The copilot sits **above** the deterministic plane and owns none of it.

---

## 3. The boundary, stated as a rule per layer

| Layer | The LLM may | The LLM may never |
|---|---|---|
| Intent | classify the request, ask a clarifying question | infer an environment or a date it was not told |
| Identity | propose a human name ("EOD ACCOUNT") | emit a URN, job id or DAG id |
| Evidence | summarise what tools returned | assert a DQ, reconciliation or certification result |
| Root cause | choose among *approved* categories, with tool evidence attached | declare a cause with no evidence reference |
| Impact | explain the blast radius | compute it, or widen/narrow it |
| Scope | explain why a scope is small | write a `WHERE` clause |
| Action | request that a plan be built, and relay an approval | trigger a DAG, run SQL, submit Spark |
| Outcome | report what verification returned | claim success |

---

## 4. Reuse map — what AIGR builds on rather than rebuilds

| AIGR need | Existing component | Change required |
|---|---|---|
| immutable plan + hash | `cdc/incidents.py::RecoveryPlan` | none |
| approval decision | `decide_approval()`, `RecoveryPolicy` | add a persisted **approval record** (identity, expiry) |
| blast radius, exclusions with reasons | `cdc/impact.py::LineageImpactService` | none |
| topological turns | `.turns()` | none |
| column lineage | `_column_reaches()` | surface a **confidence** and a **source** |
| URNs | `cdc/urns.py::UrnMinter` | none |
| DataHub | `cdc/datahub_client.py` | add typed read methods for owners/domains/tags |
| DQ / certification | `cdc/quality.py`, `cdc/certification.py`, `dq_catalog.evaluate_publish()` | none |
| tool envelope, audit, caps | `ai/agent_tools/contract.py::ToolSpec` + `invoke()` | **extend** authorization classes; see §5 |
| safe Athena | `ai/agent_tools/athena_tool.py` (ADR-051) | none |
| LangGraph | `ai/business_agent/graph.py` | extend with the reliability sub-graph |
| execution map | `JOB_ENTRYPOINTS` (closed map) | reuse as the only action target |

---

## 5. The decision that gates everything: ADR-057

`ToolSpec.__post_init__` refuses any tool with `read_only=False`. That is enforced in code,
not documented as a convention, and it is why this platform has never had an agent capable
of changing anything.

AIGR requires exactly one mutating capability: `submit_recovery_plan`. The target is
therefore **not** "remove the check". It is:

1. Keep `read_only=True` as the default and the overwhelming majority.
2. Add authorization classes `plan_write` and `recovery_submit`.
3. Allow `read_only=False` **only** for `recovery_submit`, and allow that class to contain
   exactly one tool name.
4. A tool in that class must carry an idempotency key, a plan hash, and an approval
   reference, or construction fails.

Point 3 is the unusual one and it is deliberate: the safest mutation surface is one whose
*membership* is closed, not merely one whose *behaviour* is reviewed.

---

## 6. Root-cause categories and what each is allowed to do

| Category | Rerun allowed? | Outcome |
|---|---|---|
| `LATE_SOURCE_EVENT` | yes, bounded | `AUTO_CORRECT` on affected keys/dates |
| `MISSING_SOURCE_EVENT` | no, until it arrives | `WAITING_SOURCE_CORRECTION` |
| `BAD_SOURCE_VALUE` | **no** | `WAITING_SOURCE_CORRECTION`; never write the source |
| `DUPLICATE_CDC_EVENT` / `OUT_OF_ORDER_EVENT` | yes, bounded | rebuild the affected window |
| `INGESTION_DEFECT` / `TARGET_WRITE_DEFECT` | yes, bounded | rebuild |
| `SCHEMA_DRIFT` | conditional | rebuild only after the contract is reconciled |
| `TRANSFORM_LOGIC_DEFECT` | **no** | `CODE_FIX_REQUIRED` — rerunning broken logic over good data reproduces the same wrong answer, more expensively |
| `DQ_RULE_DEFECT` | **no** | fix the rule; the data may be fine |
| `DEPENDENCY_READINESS_DEFECT` | yes | wait, then rerun |
| `STALE_DATA` | yes | rerun |
| `UNKNOWN` | **no** | no automatic recovery, ever |

The two that this platform cannot currently express — `TRANSFORM_LOGIC_DEFECT` and
`DQ_RULE_DEFECT` — are precisely the two where a rerun is actively harmful. Both currently
classify as `DQ_VIOLATION` or `JOB_FAILURE`, which `REPAIRABLE_BY_RERUN` treats as fixable.

---

## 7. Column impact vs execution granularity

These are different questions and conflating them is the main correctness trap.

```
column lineage  →  WHICH jobs are affected        (dependency selection)
RecoveryCapability  →  WHAT a job can recompute   (execution granularity)
```

Knowing that only `BALANCE` is wrong does **not** mean Spark can rewrite one column. The
planner therefore:

1. narrows the *job set* by validated column lineage;
2. picks, per job, the **smallest scope that job declares it supports**;
3. states the resulting scope, which may be wider than the defect.

If validated column lineage does not exist, the planner **downgrades to `TABLE_LEVEL_IMPACT`
and says so**. Fuzzy column lineage may never authorize automatic recovery — an impact
analysis that is confidently wrong is worse than one that admits its width, because the
descendants it silently omits are never rebuilt.

---

## 8. Bounded graph, no ReAct

One LangGraph with deterministic nodes and hard ceilings: max steps, max tool calls, max
recovery attempts, tool timeout, agent timeout, token budget, cost budget. No agent calls
another agent. No unbounded loop. The existing graph already terminates in an explicit
`refuse` node; the reliability sub-graph keeps that property.

---

## 9. Runtime decision

**Stay on the approved runtime.** ADR-056 defers AgentCore with a named revisit trigger —
multiple concurrent human users needing isolated sessions with managed identity — and that
trigger is not met by one operator running recoveries. AgentCore Gateway is not adopted in
this phase; the typed tool layer already provides the governed entry point it would offer.

Bedrock model access exists (`anthropic.claude-opus-5` and others are listed in
`ap-southeast-1`). Listing is not invoke access; AIGR7 must verify invocation before
claiming it.

---

## 10. Migration plan

| Phase | Deliverable | Depends on |
|---|---|---|
| AIGR1 | ADR superseding ADR-057; `plan_write` / `recovery_submit` classes; tool catalogue | — |
| AIGR2 | typed DataHub/OPS context tools, bounded hops, confidence surfaced | AIGR1 |
| AIGR3 | intent modes, canonical resolution, extended `FailureClass`, `RecoveryScope` | AIGR2 |
| AIGR4 | `RecoveryCapability` per job; smallest-scope selection; dry-run | AIGR3 |
| AIGR5 | Recovery Control Service (hash revalidation, idempotency, duplicate rejection) | AIGR4 |
| AIGR6 | policy engine + persisted approval with expiry; plan edit invalidates approval | AIGR5 |
| AIGR7 | LangGraph reliability sub-graph on the existing agent | AIGR6 |
| AIGR8 | runtime/IAM least privilege — **no AgentCore migration** | AIGR7 |
| AIGR9 | the 19 governance/reliability use cases | AIGR8 |
| AIGR10 | E2E cases A–J with evidence | AIGR9 |
| AIGR11 | evals, security, observability, cost | AIGR10 |
| AIGR12 | final review | AIGR11 |

---

## 11. Non-goals

- no autonomous multi-agent swarm
- no auto-deploy of code, ever
- no IAM change by the agent
- no source-database write
- no Kafka offset reset or checkpoint deletion reachable from the agent
- no metadata mutation without explicit review
- **no dependence on AI-P13**, which is blocked and mutates AWS
