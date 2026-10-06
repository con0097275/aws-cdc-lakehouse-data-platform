# AI GOVERNANCE & RELIABILITY — USE CASES

Phase **AIGR9** · 19 cases · **status is per case and is not inflated**

`TESTED` = covered by an executable test in `spark/tests/test_aigr*.py`.
`MODELLED` = the types and gates exist; no test drives the whole case yet.
`NOT BUILT` = named here so it is not mistaken for done.

| # | Use case | Status | Note |
|---|---|---|---|
| 1 | wrong **column** + bounded recovery | **TESTED** | validated column lineage narrows jobs; per-job granularity chosen |
| 2 | wrong **table/date** + bounded recovery | **TESTED** | date stays bounded |
| 3 | late CDC → `AUTO_CORRECT` | **TESTED** | disposition routes to AUTO_CORRECT |
| 4 | DQ failure triage | **TESTED** | `DQ_RULE_DEFECT` blocks data repair |
| 5 | EOD not-certified diagnosis | **MODELLED** | certification tools declared; no live drive |
| 6 | quarantine spike diagnosis | **MODELLED** | `ops.dq_quarantine` exists and holds a row |
| 7 | freshness / SLA incident | **MODELLED** | `STALE_DATA` category and disposition exist |
| 8 | schema-change impact before deploy | **MODELLED** | `SCHEMA_DRIFT` → `CONTRACT_REVIEW_REQUIRED` |
| 9 | column / table deprecation impact | **MODELLED** | impact analysis reused; no deprecation flow |
| 10 | owner / domain governance gap report | **MODELLED** | `compile_inventory()` already reports 84/84 owned |
| 11 | PII downstream impact | **MODELLED** | classification carried; no dedicated flow |
| 12 | contract drift report | **MODELLED** | contracts exist; no drift reporter |
| 13 | lineage gap report | **TESTED** (partly) | impacted-but-unexecutable assets are excluded **with reasons** |
| 14 | recovery dry-run | **TESTED** | `PLAN` intent builds and never submits |
| 15 | recovery execution status | **TESTED** | `status()` |
| 16 | post-recovery evidence summary | **TESTED** | the evidence pack is always built |
| 17 | incident-ticket drafting | **NOT BUILT** | optional |
| 18 | data-owner notification drafting | **NOT BUILT** | optional |
| 19 | remediation PR/issue draft for transform defects | **NOT BUILT** | optional; **no auto-deploy under any circumstance** |

## Standing prohibitions

No automatic code deploy. No automatic IAM change. No automatic source-database write. No
metadata mutation without explicit review — the agent may *suggest* a metadata change and
must not apply one.

## The honest line

Four cases are `NOT BUILT` and eight are `MODELLED`. Listing them here rather than counting
them as delivered is the point: a use-case table where everything says "done" is the failure
mode this platform keeps finding.
