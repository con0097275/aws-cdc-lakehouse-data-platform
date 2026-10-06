# AI DATA RECOVERY — RUNBOOK

Phase **AIGR9/AIGR12**

## 1. Ask

> *"The BALANCE column in EOD ACCOUNT for COB 2026-09-28 is wrong for A001, A002, A003.
> Investigate and repair only the affected downstream data."*

## 2. What happens, and where you can stop it

| stage | you see | you can |
|---|---|---|
| resolve | the canonical asset and column the copilot resolved | correct it; an unresolved asset **refuses** rather than guessing |
| diagnose | a root-cause category **with evidence refs** | reject the diagnosis |
| impact | affected assets and jobs, **plus excluded ones with reasons** | question an exclusion |
| plan | per-job action, granularity and turn | ask for a narrower scope |
| policy | `AUTO_EXECUTE_ALLOWED` / `APPROVAL_REQUIRED` / `BLOCKED` with reasons | — |
| approval | the immutable plan summary and hash | **APPROVE · REJECT · EDIT_SCOPE** |
| execute | execution id and state | `request_recovery_cancel` |
| verify | DQ, reconciliation, certification | — |

## 3. Read the granularity column

A plan may say `BUSINESS_KEY_SET` for one job and `COB_DATE` for another. That is not an
inconsistency: it is each job's declared capability. A job showing `FULL_TABLE` will rewrite
the whole table — the `reason` field says why it had to widen.

## 4. When it refuses, it is usually right

| message | meaning |
|---|---|
| `WAITING_SOURCE_CORRECTION` | the value is wrong **in the source**. Fix it there; the platform will not write the source and a rebuild would reproduce it. |
| `CODE_FIX_REQUIRED` | a transform bug. Rerunning would cost money and reproduce the defect. |
| `RULE_FIX_REQUIRED` | the DQ rule may be wrong and the data fine. |
| `UNKNOWN` root cause | no automatic recovery. Investigate further. |
| unbounded scope | "rebuild everything" is not a scope. |

Do not work around these. They are the gates working — the same lesson as
`EOD_WAITING_SOURCE` and `EMPTY_WINDOW` in the data plane.

## 5. Approval

An approval binds **one plan hash**, for **4 hours**, in **one environment**. Edit the plan
and the approval stops applying — by construction. An agent identity cannot approve.

## 6. Retry

Re-submitting with the same idempotency key converges on the same execution. Any prior
attempt escalates the next one to a human.
