# AI COLUMN-LEVEL IMPACT — RUNBOOK

Phase **AIGR9**

## 1. The two questions, kept apart

```
column lineage      -> WHICH jobs are affected     (dependency selection)
RecoveryCapability  -> WHAT a job can recompute    (execution granularity)
```

Conflating them is the main correctness trap. Column impact narrows the *job set*. It does
not mean anything can rewrite a single column.

## 2. Check the confidence before trusting the narrowing

`get_column_lineage` returns a **confidence** and a **source**. Only `VALIDATED` may narrow
a recovery to columns. A `RecoveryScope` that tries to do otherwise **fails at
construction** — the refusal is in the type, not in a policy that could be bypassed.

## 3. When column lineage is missing

The copilot downgrades to `TABLE_LEVEL_IMPACT` and says so. It does not fabricate precision.
The recovery is wider and the approval requirement usually stricter — which is the correct
trade: an impact analysis that is confidently wrong is worse than one that admits its width,
because the descendants it silently omits are the ones that never get rebuilt.

## 4. Worked shape

```
BALANCE wrong, COB 2026-09-28, keys A001-A003
        ↓ validated column lineage
only models consuming BALANCE are affected; digital-branch consumers excluded, with reason
        ↓ per-job capability
j_eod  supports COB_DATE only          -> rebuild COB 2026-09-28 (widened, reason recorded)
j_mart supports BUSINESS_KEY_SET       -> rebuild 3 keys on that COB
        ↓
turn 0: j_eod   → DQ → reconciliation → certification
turn 1: j_mart
```

The root is validated before any descendant runs. If the root fails, no descendant runs.
