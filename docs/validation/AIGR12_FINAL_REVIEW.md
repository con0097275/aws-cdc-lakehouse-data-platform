# AIGR12 — FINAL AI DATA RELIABILITY COPILOT REVIEW

- Date **2026-09-30** · account `111122223333` · `ap-southeast-1` · env `dev`
- Verdict: **`AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY`**
- Score: **27 PASS · 0 FAIL · 1 BLOCKED-EXTERNALLY** of 28 acceptance items
- Column-narrowed recovery is **live**: the lineage it narrows on was *validated*, not relabelled
- **Re-verified 2026-09-30 after the copilot gained a CLI, a UI and quality gates.** Six further
  defects were found by using it; all are fixed and the live evidence was regenerated.
- Built on `DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY`

---

## 1. The verdict, and the one thing it is not claiming

An AI-driven recovery **ran end to end against real infrastructure** on 2026-09-30:

```
natural-language request
 → resolved against the real 84-asset governance inventory
 → real lineage graph: 16 impacted, 16 executable
 → column lineage VALIDATED against real data → scope narrowed to COLUMN_LINEAGE
 → real DQ verdict written to ops.dq_result_v2 (BLOCKER FAIL)
 → capability-driven plan: eod_build widened to COB_DATE, reporting kept BUSINESS_KEY_SET
 → policy AUTO_EXECUTE_ALLOWED, approval record persisted with an expiry
 → real Athena repair through the Recovery Control Service
 → verification: mart 1600.0 → 1000.0, violations 3 → 0, DQ PASS
```

Evidence: `artifacts/validation/data-reliability/aigr10-live-e2e.json`.

**The one item not passing is external to this platform.** Bedrock invocation is gated at
the account level — *"Model use case details have not been submitted for this account"* —
and only the account owner can submit that form. Two invocations **did** succeed earlier in
the same session, returning token usage, so the wiring is proven and the entitlement is not.

That item is marked `BLOCKED-EXTERNALLY` rather than `PASS`, and it does not gate the
verdict, because **the copilot's safety properties do not depend on a model**: an absent
classifier resolves to `AMBIGUOUS`, which cannot mutate, and an absent composer omits prose.
Every number in the live run came from a tool.

---

## 2. Acceptance checklist — 27 pass

| Item | Evidence |
|---|---|
| NL resolves canonical assets safely | **LIVE** — `"EOD ACCOUNT"` → `eod:oracle_coredb_corebank_account`; nonsense refused |
| exact column impact where validated lineage exists | **LIVE** — `BALANCE` confirmed on the real table, scope narrowed to `COLUMN_LINEAGE` |
| table fallback when column lineage missing | **LIVE** — a column whose edge is not validated still cannot build such a scope; construction raises |
| AI never invents lineage or job ids | `submit_recovery_plan` takes 3 fields, none of them a job |
| affected jobs = lineage ∩ execution graph | **LIVE** — 16 impacted intersected with the registered jobs |
| unrelated branches excluded | `stream:digital_events` excluded with a reason |
| smallest supported data scope selected | **LIVE** — `eod_build` COB_DATE, `reporting` BUSINESS_KEY_SET, in one plan |
| source bad value not auto-written | plan **cannot be constructed** for it |
| transform defect not blindly rerun | `CODE_FIX_REQUIRED` |
| DQ-rule defect not treated as a data defect | `RULE_FIX_REQUIRED` — and see §4, where this bit us for real |
| plan immutable / hash-protected | `aigr1:91ec6c50c0cc88c0`; hash changes on any edit |
| production mutation policy/approval enforced | prod always `APPROVAL_REQUIRED` |
| plan edit invalidates approval | by construction |
| no arbitrary shell / SQL / DAG trigger | **cannot be constructed** (12 tests) |
| no Kafka / checkpoint reset path | same |
| mutation only through the Recovery Control Service | ADR-092 closed membership |
| root DQ/recon before descendants | **LIVE** — and now *in the plan*: turn 0 rebuild, turn 1 `DQ_RECHECK → RECONCILE → CERTIFY`, turn 2 descendants, turn 3 `DQ_RECHECK → RECONCILE` |
| topological execution correct | **LIVE** — 4 turns, 7 actions, executed |
| failed recovery does not advance a watermark | no watermark advanced on the FAIL run |
| retry idempotent | same key converges |
| final DQ/recon/certification verified | **LIVE** — DQ FAIL → PASS, written to `ops.dq_result_v2` |
| incident closes only after verified success | the first run **did not pass** and was not closed |
| DataHub outage fails safe | unresolvable asset refuses |
| prompt injection blocked | 5 payloads, request position |
| actions fully audited | evidence pack always built |
| E2E evidence exists | **LIVE** — `aigr10-live-e2e.json` |
| docs/runbooks complete | 18 documents |

### Blocked externally — 1

| Item | State |
|---|---|
| eval thresholds pass (model accuracy) | Bedrock gated at the account level. Safety thresholds all hold; **no accuracy number is claimed**. |

---

## 3. What the live run cost

Four EMR-free Athena sandboxes, dropped at the end. **$0 of new AWS resource.**

---

## 4. Two defects the live run found, and neither was in the safety layer

Both were in the code I wrote to *exercise* it, which is exactly where a demonstration is
most likely to lie to you.

**1 — the executor template ignored its own scope.** The repair used a value heuristic
(`balance > 350`) instead of the planned scope. It missed `A001` and halved `A004`, which
was never corrupted. The mart went from wrong (1600) to **differently wrong** (900) while
every upstream gate passed — plan valid, policy allowed, approval valid, execution
`SUCCEEDED`.

> Fixed twice over: `AthenaRecoveryExecutor` now **refuses a template that references no
> scope placeholder**, and the EOD repair became a rebuild from FULL_CDC — correct by
> construction rather than by a predicate someone guessed.

**2 — the verification predicate was itself wrong.** `balance > 350` flagged `A004`, which
legitimately holds 400. The data was fully repaired and the *check* reported a violation.

> That is a `DQ_RULE_DEFECT` — one of the two categories this taxonomy exists to tell apart
> from a data defect — encountered by accident, in the code meant to demonstrate it. The
> check now compares EOD against FULL_CDC, which is the actual contract.

---

## 5. Findings

No **P0**. No **P1**.

| # | Sev | Finding |
|---|---|---|
| 1 | P2 | Bedrock gated at the account level; submit the Anthropic use-case form to enable intent classification and narration |
| 2 | P3 | **28 of 67** column edges are now `VALIDATED`. The other 39 are honest failures: dbt `int_*`/`stg_*` models are ephemeral (no Glue table), and some EOD tables are empty so their `payload_after` keys cannot be read. Those stay `DERIVED` and cannot narrow a recovery |
| 3 | P2 | 8 use cases `MODELLED`, 4 `NOT BUILT` (`AI_GOVERNANCE_USE_CASES.md`) |
| 4 | P3 | AgentCore remains deferred (ADR-056); its revisit trigger is still unmet |

---

## 5b. How column lineage was raised from DERIVED to VALIDATED

There were two ways. One changes a string. The other checks whether the columns are really
there. `cdc/column_validation.py` does the second, and it had to handle a wrinkle:

**CDC layers store the source row as JSON.** `payload_after` is a `string`, so `BALANCE` is
not a column on the EOD table at all — it is a key inside a blob, present in every row. A
Glue schema lookup alone validated **3 of 67** edges. Reading the JSON keys from real data
as well validated **28**, including all six on `eod_oracle_coredb_corebank_account`.

The remaining 39 are recorded as failures, not hidden:

| reason | count | why it is correct to fail |
|---|---|---|
| dbt `int_*` / `stg_*` has no Glue table | ~26 | ephemeral models; nothing can confirm them |
| `payload_after` unreadable | ~13 | the table is empty, so the keys cannot be read from data |

An unvalidated edge stays `DERIVED`, and a `DERIVED` edge still cannot narrow a recovery —
verified live, at scope construction.

---

## 5c. Re-verification, and the six defects it found

The first PRODUCTION_READY score was taken before the copilot had a CLI, a UI or quality
gates. Using it found six more defects. **None was in the safety boundary**; all were in the
layer that makes the boundary usable.

| # | Defect | Why it mattered |
|---|---|---|
| 1 | a lineage question was **diagnosed** | `"What does BALANCE feed?"` returned `Root cause UNKNOWN. No automatic recovery is permitted.` — the graph ran the diagnostic path unconditionally |
| 2 | the root cause came from **keywords in the question** | so "plan a recovery" returned UNKNOWN: the operator had not said what was wrong, because they were asking |
| 3 | a check that examined **0 rows** outranked one at 1 of 670 | the NOT_EVALUATED shape this platform already refuses, reappearing a layer up |
| 4 | a word in the asset's name became a **column** | `EOD ACCOUNT` yielded `column ACCOUNT`, and a spurious column changes what the scope narrows on |
| 5 | the plan had **no verification actions** | `DQ_RECHECK`/`RECONCILE`/`CERTIFY` existed and nothing emitted them; the barrier lived in prose |
| 6 | the executor keyed templates by **job, not action** | a `DQ_RECHECK` ran the job's REBUILD SQL. The drill still passed, because an idempotent rebuild survives being run twice — **luck, not correctness** |

Defect 6 is the one worth dwelling on: a verification turn silently re-executing a
`DELETE + INSERT`, and a green result that proved nothing about it. It was found by
re-running the drill and reading *"7 statements executed"* against a plan whose verification
actions should have read, not written.

Evidence regenerated against the current code:
`aigr10-live-e2e.json` — 4 turns, 7 actions
(`EOD_REBUILD`, `MART_RERUN`, `DQ_RECHECK ×2`, `RECONCILE ×2`, `CERTIFY`), mart
1600.0 → 1000.0, violations 3 → 0. The `source-update` scenario also passes: 1000.0 → 1125.0.

---

## 6. The honest sentence

> A natural-language request resolved a real asset, read the operational ledgers and
> diagnosed from the rows it found, read column lineage that had been **confirmed against
> real data rather than relabelled**, narrowed the scope to that column, built a
> capability-aware plan **with its quality barriers in it**, took a real approval, executed a
> real repair, and verified the mart against a contract — with **155 tests** and zero
> reachable unauthorized mutations behind it.
>
> Six defects were found by using it after it was first scored READY, and none was in the
> safety boundary. That is the pattern worth reporting: the boundary held while the parts
> that make it usable were wrong, repeatedly, until each was run.

`AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY`
