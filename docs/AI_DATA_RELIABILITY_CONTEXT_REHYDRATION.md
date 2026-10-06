# AI DATA RELIABILITY — CONTEXT REHYDRATION

Read this first in a new session. It is written to be **believed without re-deriving**, and
every number in it was measured on 2026-09-30.

---

## 1. LAST VERIFIED CHECKPOINT

| Checkpoint | Status | Evidence |
|---|---|---|
| `DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY` | **PASS** | 25 · 0 · 1 of 26 gates, all live |
| `AIGR0_CURRENT_AI_RELIABILITY_CONTEXT_AUDITED` | **PASS** | `AI_RECOVERY_CURRENT_STATE_AUDIT.md` |
| `AIGR1_GOVERNED_AGENT_TOOL_CONTRACT_READY` | **PASS** | ADR-092; 25 tests |
| `AIGR2_DATA_GOVERNANCE_CONTEXT_TOOLS_READY` | **PASS** | wired to the real inventory and lineage graph; column confidence read from a recorded validation run |
| `AIGR3_INCIDENT_SCOPE_RESOLVER_READY` | **PASS** | 14 categories, typed scope; 41 tests |
| `AIGR4_AI_RECOVERY_PLANNER_READY` | **PASS** | capability-aware planner |
| `AIGR5_RECOVERY_CONTROL_API_READY` | **PASS** | revalidating facade |
| `AIGR6_APPROVAL_POLICY_ENGINE_READY` | **PASS** | policy + approval records |
| `AIGR7_LANGGRAPH_RELIABILITY_COPILOT_READY` | **PASS** | bounded graph; 27 tests |
| `AIGR8_MANAGED_RUNTIME_GATEWAY_READY` | **PASS (decision)** | stay on the approved runtime (ADR-056); no new IAM to grant |
| `AIGR9_GOVERNANCE_AGENT_USECASES_READY` | **PASS (core)** | 7 tested · 8 modelled · 4 not built, each labelled |
| `AIGR10_AI_RECOVERY_E2E_PASS` | **PASS (LIVE)** | mart 1600.0 → 1000.0, violations 3 → 0 |
| `AIGR11_AI_RELIABILITY_EVAL_SECURITY_READY` | **PASS (safety)** | 52 safety rows; accuracy needs the Bedrock entitlement |
| `AIGR12_..._PRODUCTION_READY` | **PRODUCTION_READY** | **27 pass · 0 fail · 1 blocked externally** |

---

## 2. CURRENT ARCHITECTURE

CDC data plane (Oracle/SQL Server → Debezium → MSK → Spark → Iceberg/Glue → dbt → Athena)
with a reliability plane (DataHub, OpenLineage, DQ, reconciliation, certification, incidents,
impact, recovery) — both live. The copilot sits **above** and owns none of it.

New in AIGR: `ai/reliability/` — `tools.py` (27 tools), `model.py` (categories, scope,
capability), `planner.py`, `control.py`, `policy.py`, `copilot.py`.

---

## 3. AI RUNTIME

Local CLI / Lambda per **ADR-056**. **AgentCore stays deferred** — its revisit trigger
(multiple concurrent users needing isolated managed-identity sessions) is not met by one
operator running recoveries. Do not migrate because a prompt mentions it.

Bedrock invocation **was verified** (two calls returned content and token usage), then the
account began returning *"Model use case details have not been submitted for this account"*.
**Submit the Anthropic use-case form in the Bedrock console** to re-enable it. Nothing in
the recovery loop depends on it: an absent classifier resolves to `AMBIGUOUS`, which cannot
mutate.

Two traps: a bare model id is rejected (`on-demand throughput isn't supported`) — an
**inference profile** is required; and *listing* a model is not *invoking* one.

---

## 4. TOOL GATEWAY

`ai/agent_tools/contract.py` — 5 authorization classes, and `MUTATING_TOOLS` closed to
`{submit_recovery_plan, request_recovery_cancel}`. A mutating tool under any other name
**fails construction**. ADR-092 supersedes ADR-057.

---

## 5. DATAHUB STATE

Local `v1.7.0.1`, running. **147 datasets** after the Glue (493 events) and dbt (248 events)
recipes executed for the first time. Flag-gated; `mode: disabled` by default is correct.

> The graph index is **eventually consistent**. A traversal seconds after a publish reached
> 3 assets; minutes later, 18. It does not fail — it silently under-reports the blast radius.

---

## 6. DQ / RECON / CERT STATE

```
ops.dq_result_v2  160 · ops.reconciliation_run 5 · ops.dq_quarantine 1
ops.eod_watermark   3 · ops.data_incident      1 · ops.recovery_plan  1
ops.recovery_execution 1
```

Last certified close: `eod-2026-09-20-032143Z`, 320 rows, `dq=PASS recon=PASS CERTIFIED`,
snapshot `80230069689276128` → `3683870259747946533`.

---

## 7. RECOVERY CONTROL STATE

`RecoveryControlService` exists and refuses correctly. **No executor is injected**, so a
submitted plan records `PENDING — nothing ran`. That is deliberate and the record says so.

---

## 8. OPEN INCIDENTS / PENDING APPROVALS

One incident from the DRP drill (`inc-b3-proof`, resolved). **No pending approvals.** No
AI-created incident exists.

---

## 9. TESTS

**3,529 passing, 0 failed** — measured, not derived. Two ADR-057 tests asserted the behaviour ADR-092 replaced and had to be updated to the new truth while keeping what they protected.
(25 contract · 43 planner/policy · 27 copilot). Doc validator **14/14**.

---

## 10. TESTS REMAINING

Model accuracy only: intent, resolution on real phrasing, root-cause classification,
latency, tokens, cost. Blocked on the Bedrock use-case form, not on code.

---

## 11. FILES NOT TO LOSE

```
ai/reliability/{tools,model,planner,control,policy,copilot}.py
spark/tests/test_aigr{1,4,10}_*.py
docs/adr/ADR-092-governed-mutation-surface-with-closed-membership.md
docs/AI_{DATA_RELIABILITY_COPILOT_TARGET,RECOVERY_CURRENT_STATE_AUDIT}.md
docs/AI_AGENT_{TOOL_CATALOG,ACTION_BOUNDARY}.md
docs/AI_RECOVERY_{SCOPE_MODEL,PLANNER,APPROVAL_POLICY,SECURITY,OBSERVABILITY,COST}.md
docs/RECOVERY_CONTROL_API.md · docs/AI_DATA_RELIABILITY_{LANGGRAPH,EVAL}.md
docs/AI_GOVERNANCE_USE_CASES.md · docs/AI_{DATA_RECOVERY,COLUMN_LEVEL_IMPACT,TABLE_LEVEL_RECOVERY}_RUNBOOK.md
docs/validation/{AI_DATA_RECOVERY_E2E,AI_RECOVERY_SAFETY_MATRIX,AIGR12_FINAL_REVIEW}.md
scripts/airflow-enable-lineage.sh
artifacts/validation/data-reliability/*.json
```

---

## 12. INFRA / COST STATE

**$0** for AIGR. Airflow k3s revision 2 with the OpenLineage image; EMR Serverless
auto-stops; DataHub is local. The metadata programme has spent $0 in AWS throughout.

---

## 13. SIX THINGS A NEW SESSION WILL OTHERWISE GET WRONG

1. **The provider was never missing from Airflow.** The stock image ships 2.17.0. What the
   release lacked was any `AIRFLOW__OPENLINEAGE__*` config. Now deployed at 2.20.2.
2. **`jar_runtime_path` is empty on purpose.** EMR Serverless *rejects*
   `spark.driver.extraClassPath` at submit time; a value there stops every lineage job
   starting. A test holds it empty.
3. **`ToolSpec` refusing a mutation is a feature**, not a bug to route around. ADR-092
   defines the one way through.
4. **`TRANSFORM_LOGIC_DEFECT` and `DQ_RULE_DEFECT` must never reach a rerun.** They are the
   two causes where rerunning is actively harmful, and the old `FailureClass` called both
   repairable.
5. **28 of 67 column edges are now `VALIDATED`** — confirmed against real data by
   `cdc/column_validation.py`, including every edge on the account table. The other 39 stay
   `DERIVED` and still cannot narrow a recovery. CDC layers store the row as JSON, so a
   business column is a key inside `payload_after`, not a Glue column: schema lookup alone
   confirmed only 3.
6. **`BUSINESS_AI_PRODUCTION_READY` does not exist** under that name, and **AI-P13 is
   blocked** and mutates AWS. Nothing in AIGR may depend on it.

---

## 14. EXACT NEXT ACTION

```
1. rotate the Airflow UI admin password exposed by Helm NOTES  (P1, data plane)
2. submit the Anthropic use-case form in the Bedrock console   (unblocks model accuracy evals)
3. drive the kafka recipe from Python with an oauth_cb -- MSK IAM needs a callable
   that YAML cannot carry, so `datahub ingest -c kafka.yaml` alone will not authenticate
```

**Nothing in the recovery loop is blocked. It has run, column-narrowed, end to end.**

How to use and verify every capability: `docs/HOW_TO_USE_AND_VERIFY.md`.

`AI_DATA_RELIABILITY_CONTEXT_REHYDRATED_READY`
