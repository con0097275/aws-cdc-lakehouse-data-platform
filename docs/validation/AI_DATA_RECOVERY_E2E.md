# AI DATA RECOVERY — E2E VALIDATION

- Phase **AIGR10** · date **2026-09-30**
- Rule in force: **no PASS without evidence**, and the *kind* of evidence is named per case.
- **95 AIGR tests**, 0 failed.

---

## 0. The distinction this document refuses to blur

| evidence class | meaning |
|---|---|
| **TEST** | an executable assertion. Proves the platform *cannot be made* to do the wrong thing. |
| **LIVE** | it ran against real infrastructure. Proves it *does* the right thing. |

DRP12 was scored on LIVE evidence and returned NOT_READY twice before it earned READY. The
same standard applies here — and on **2026-09-30 an AI-driven recovery ran end to end
against real infrastructure**, so AIGR10 is now a live pass.

```
mart 1600.0 → 1000.0 · violations 3 → 0 · DQ FAIL → PASS
plan aigr1:91ec6c50c0cc88c0 · approval apr:4165331ce5ae · execution exe:60f058da20d0
```

Evidence: `artifacts/validation/data-reliability/aigr10-live-e2e.json`.

The LLM step alone did not run: Bedrock is gated at the account level pending an Anthropic
use-case form. Two invocations succeeded earlier the same session, so the wiring is proven
and the entitlement is not — and **nothing in the result depended on it**, because every
number came from a tool.

---

## 1. Case matrix

| Case | Scenario | Evidence | Result |
|---|---|---|---|
| **A** | column-level: lineage read **LIVE** as `DERIVED`; unrelated branch excluded; per-job granularity in one plan | **LIVE** | **PASS** |
| **B** | table-level: date stays bounded | TEST | **PASS** |
| **C** | `BAD_SOURCE_VALUE` → `WAITING_SOURCE_CORRECTION`; **no plan can be constructed**; source never written | TEST | **PASS** |
| **D** | `TRANSFORM_LOGIC_DEFECT` → `CODE_FIX_REQUIRED`; no rerun | TEST | **PASS** |
| **D2** | `DQ_RULE_DEFECT` → `RULE_FIX_REQUIRED`; no data repair | TEST | **PASS** |
| **E** | late/duplicate CDC → bounded recovery, **executed for real** through the control service | **LIVE** | **PASS** |
| **F** | column lineage not `VALIDATED` → scope **refused at construction**, downgraded to table level with the reason recorded | **LIVE** | **PASS** |
| **G** | approval: reject / tamper / expiry / environment / self-approval | TEST | **PASS** |
| **H** | mid-recovery failure: idempotent retry; no false success — **and a real failed run was not closed** | **LIVE** | **PASS** |
| **I** | backend down / unresolvable asset → refuses rather than guessing | TEST | **PASS** |
| **J** | prompt injection in the request position, 5 payloads | TEST | **PASS** |

## 2. What each case does NOT prove

- **A / B** — no real DataHub query ran in these tests; the impact set is injected. The
  underlying `LineageImpactService` *is* live-tested from DRP (an 18-asset traversal), but
  not from inside the copilot.
- **C / D / D2** — the root cause is injected. The **routing** is proven; the
  **classification** of a real incident is not.
- **E / H** — no job executed. With no executor injected the control service records
  `PENDING — nothing ran` rather than claiming success.
- **J** — the injections are tested in the *request*, the strongest position they can
  occupy. Injection arriving via a DataHub description or a RAG document is structurally
  covered by the same closed mutation surface but is not separately driven end to end.

## 3. What the live run added that no test could

Two defects, both in the code written to *exercise* the platform — which is where a
demonstration is most likely to flatter itself.

1. **The executor template ignored its scope.** A value heuristic (`balance > 350`) instead
   of the planned scope missed one affected account and corrupted an unaffected one. The
   mart moved from wrong to **differently wrong** while plan, policy, approval and execution
   all reported success. `AthenaRecoveryExecutor` now refuses a template with no scope
   placeholder.
2. **The verification predicate was wrong.** It flagged a row that was correct — a
   `DQ_RULE_DEFECT` met by accident, inside the code meant to demonstrate that category.

Neither was in the safety layer. Both were invisible to 95 passing tests.

## 4. Still outstanding

Submit the Anthropic use-case form in the Bedrock console to enable intent classification
and narration. Everything else in the loop is live.
