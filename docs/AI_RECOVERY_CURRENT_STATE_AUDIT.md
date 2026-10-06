# AI RECOVERY — CURRENT STATE AUDIT (AIGR0)

- Date **2026-09-30** · account `111122223333` · `ap-southeast-1` · env `dev`
- Starting checkpoint: **`DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY`** (25 PASS · 0 FAIL · 1 N/A)
- **AUDIT ONLY.** No mutation tool added, no Airflow triggered, no recovery run, no AWS mutated.

---

## 1. Checkpoint reconstruction

### Data reliability plane

| Checkpoint | Status | Evidence |
|---|---|---|
| `DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY` | **PASS** | `artifacts/validation/data-reliability/drp12-acceptance.json`; 10/10 E2E scenarios live |

### AI platform (`AI_PLATFORM_STATE.md`)

| Checkpoint | Status | Note |
|---|---|---|
| `AI_AGENT_TOOLS_READY` (AI-P7) | **PASS** | 59 tests incl. 15 adversarial; 8 tools; live Athena verified |
| `AI_LANGGRAPH_AGENT_READY` (AI-P8) | **PASS** | LangGraph 0.2.60 pinned; tier-1 works without it |
| `AI_AGENTCORE_RUNTIME_READY` (AI-P9) | **PASS — but `DEFERRED_LAMBDA`** | module validates, **plan-only, 0 AI resources created** |
| `AI_AGENT_EVALUATED` (AI-P10) | **PASS** | 44/44 scenarios; unsafe-block rate 1.0; 0 P0 |
| `AI_GOVERNANCE_READY` (AI-P11) | **PASS** | 33 tests; 23/23 security checks |
| `AI_INFRA_PLAN_READY_FOR_REVIEW` (AI-P12) | **PASS** | plan-only; 14 AI creates, 0 destroy |
| `AI_INFRA_APPLIED_AND_VERIFIED` (AI-P13) | **NOT_STARTED** | **the only phase that mutates AWS**, and it is blocked — see §4 |
| `AI_E2E_VALIDATED` (AI-P14) | **PARTIAL** | `artifacts/validation/ai-p14/e2e_results.json` exists |
| `BUSINESS_AI_PRODUCTION_READY` | **UNKNOWN** | no such literal checkpoint in `PROJECT_STATE.md`; a business-AI track exists (`ai/business_agent/`, `artifacts/validation/bai-p7/`) but its checkpoint name differs from the one this prompt pack assumes |

> **A conflict worth naming.** The prompt pack lists `BUSINESS_AI_PRODUCTION_READY` as an
> existing checkpoint to reuse. It does not exist under that name. The business agent is
> real and tested; the checkpoint label is not. AIGR must not assume it.

---

## 2. What can be reused UNCHANGED

This is the important half of the audit: most of what AIGR needs is already built and live-tested.

| Need | Already exists | Where |
|---|---|---|
| immutable, hash-keyed recovery plan | `RecoveryPlan` with `plan_id` derived from a content hash | `cdc/incidents.py:258` |
| approval decision | `decide_approval()`, `ApprovalRequirement{AUTOMATIC, REQUIRES_APPROVAL}`, `RecoveryPolicy` | `cdc/incidents.py:386,131,137` |
| blast radius + exclusions | `LineageImpactService.downstream()`, `ImpactResult.excluded` with reasons | `cdc/impact.py:143` |
| topological turns | `LineageImpactService.turns()` | `cdc/impact.py:186` |
| lineage graph, 0 cycles | `LineageGraph`, 81 nodes / 80 edges / 67 column edges | `cdc/lineage_graph.py` |
| column lineage | `_column_reaches()` | `cdc/impact.py:163` |
| canonical URNs | `UrnMinter.dataset_urn()` / `.asset_for()` (exact inverse) | `cdc/urns.py` |
| DataHub adapter | `datahub_client.py` — Null/Recording/File/Rest transports, bounded retry, `get_aspect`, `relationships` | `cdc/datahub_client.py` |
| DQ verdicts / certification | `quality.py`, `certification.py`, `dq_catalog.evaluate_publish()` | `cdc/` |
| contracts | `contracts.py` with `Severity` | `cdc/contracts.py` |
| typed tool envelope | `ToolSpec` + `invoke()` with timeout, row cap, byte cap, audit record | `ai/agent_tools/contract.py` |
| safe Athena | `query_athena` (SELECT-only) | `ai/agent_tools/athena_tool.py`, ADR-051 |
| LangGraph orchestration | `StateGraph` with deterministic nodes and a `refuse` terminal | `ai/business_agent/graph.py:457` |
| deterministic-before-narration | ADR-061 | `docs/adr/ADR-061-*.md` |
| execution entrypoint map | `JOB_ENTRYPOINTS` closed map | `airflow/dags/recovery_coordinator.py` |

**Conclusion: AIGR is mostly an integration and boundary problem, not a green-field build.**

---

## 3. What must be EXTENDED, and the exact obstacle

### 3.1 The tool contract forbids mutation, in code

`ai/agent_tools/contract.py:64`:

```python
if not self.read_only:
    raise ValueError(f"{self.name}: V1 tools are read-only (ADR-057)")
if self.authorization_class not in ("public_metadata", "governed_read", "ops_read"):
```

This is not a gap — it is a **deliberate, enforced boundary** (ADR-057). AIGR1 cannot add a
mutating tool without superseding that ADR. That is the single most consequential decision
in this programme and must be an explicit ADR, not an edit.

### 3.2 Root-cause taxonomy is narrower than AIGR requires

`FailureClass` has **9** values; AIGR asks for **14**. Mapping:

| AIGR category | Today |
|---|---|
| `LATE_SOURCE_EVENT` | `LATE_ARRIVING_DATA` |
| `BAD_SOURCE_VALUE` | `SOURCE_DEFECT` |
| `SCHEMA_DRIFT` | `SCHEMA_CHANGE` |
| `DQ_RULE_DEFECT` | **missing** — today a bad rule looks like `DQ_VIOLATION`, i.e. a data problem |
| `TRANSFORM_LOGIC_DEFECT` | **missing** — would fall into `JOB_FAILURE` or `DQ_VIOLATION` and be rerun |
| `DUPLICATE_CDC_EVENT`, `OUT_OF_ORDER_EVENT`, `MISSING_SOURCE_EVENT`, `INGESTION_DEFECT`, `TARGET_WRITE_DEFECT`, `DEPENDENCY_READINESS_DEFECT`, `STALE_DATA` | **missing** |

The two missing ones that matter most are `DQ_RULE_DEFECT` and `TRANSFORM_LOGIC_DEFECT`,
because both currently classify as something a rerun "fixes" — and rerunning broken logic
over good data reproduces the same wrong answer, more expensively.

### 3.3 `AffectedScope` is narrower than `RecoveryScope`

Present: `cob_dates`, `interval_start/end`, `business_keys`, `bounded`.
Absent: `root_column`, `key_count`, `watermark_lower/upper`, `source_snapshot_start/end`,
`affected_partitions`, `scope_confidence`, `scope_source`, `request_id`, `requested_by`.

`scope_confidence` / `scope_source` are the load-bearing additions: without them there is no
way to distinguish "bounded by validated column lineage" from "bounded by a guess", and
§7 of the target forbids authorizing recovery on fuzzy lineage.

### 3.4 No `RecoveryCapability` per job

Nothing records whether a job can rebuild a `BUSINESS_KEY_SET`, only a `COB_DATE`, or only
`FULL_TABLE`. Without it, "smallest supported scope" cannot be computed and the planner
would either over-promise precision or always fall back to a full rebuild.

### 3.5 No Recovery Control API facade

`scripts/run-recovery.py` and `airflow/dags/recovery_coordinator.py` exist, but there is no
service boundary with plan-hash revalidation, approval expiry, idempotency keys and
concurrent-duplicate rejection.

### 3.6 Approval is a decision, not a record

`decide_approval()` returns a requirement. There is no persisted approval object carrying
`approver_identity`, `plan_hash`, `approval_scope`, `approved_at`, `expires_at`.

---

## 4. Blockers inherited from the AI track

`AI_PLATFORM_STATE.md` records AI-P13 as the only AWS-mutating AI phase, and it is blocked:

| Blocker | State on 2026-09-30 |
|---|---|
| budget `depends_on` defect causing 19 spurious replacements | **unverified today** — a latent repository defect in `modules/kafka_platform/`, needs an operator decision |
| "Bedrock unusable" (embeddings) | **partially stale** — `bedrock list-foundation-models` now returns Claude models including `anthropic.claude-opus-5`. Model *listing* is not the same as *invoke* access or embeddings working; not yet re-tested |
| `copilot.zip` missing, `aws_lambda_function.filename` points at it | **unverified today** |
| `legacy-lake-cmk-decrypt` inline policy drift | **unverified today** |

**AIGR must not depend on AI-P13.** The copilot should run on the currently approved runtime
(local CLI / Lambda per ADR-056), because AgentCore is `DEFERRED` with a stated revisit
trigger that has not been met.

---

## 5. What must NOT be coupled

1. **Do not build a second chatbot.** `ai/business_agent/` is the existing LangGraph agent.
   AIGR7 extends it; it does not fork it.
2. **Do not migrate to AgentCore because this prompt mentions it.** ADR-056 defers it with a
   named revisit trigger — *multiple concurrent human users needing isolated sessions with
   managed identity*. That trigger is not met by one operator running recoveries.
3. **Do not make DataHub MCP mandatory.** The deployment is DataHub OSS `v1.7.0.1`, started
   locally and flag-gated off by default.
4. **Do not let the copilot own correctness.** ADR-061 already fixes this: deterministic
   analytics before narration. The same rule extends to recovery — the LLM may request a
   plan; it may never decide that data is correct.

---

## 6. Live platform state at audit time

| | |
|---|---|
| DataHub | local `v1.7.0.1`, running; 147 datasets after the Glue + dbt recipes executed |
| Airflow | 3.2.2 on k3s, **revision 2**, image `airflow-openlineage:3.2.2-ol2.20.2`, provider **2.20.2**, `openlineage-python` **1.53.0** |
| Reliability ledgers | `dq_result_v2` 160 · `reconciliation_run` 5 · `dq_quarantine` 1 · `eod_watermark` 3 · `data_incident` 1 · `recovery_plan` 1 · `recovery_execution` 1 |
| EMR | application `00g941dubedckq25`, auto-stop; last certified close `eod-2026-09-20-032143Z`, 320 rows |
| Tests | **3,301 passed, 0 failed** |
| AWS spend, metadata programme | **$0** (four short EMR submissions this session) |

---

## 7. Exact next action

`AIGR1_GOVERNED_AGENT_TOOL_CONTRACT_READY` — and its first task is not code. It is an ADR
superseding ADR-057, because every later checkpoint depends on whether this platform is
willing to let an agent hold a mutating tool at all, and under what constraints.

`AIGR0_CURRENT_AI_RELIABILITY_CONTEXT_AUDITED`
