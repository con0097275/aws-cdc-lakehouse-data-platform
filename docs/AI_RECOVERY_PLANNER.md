# AI RECOVERY — PLANNER

Source: `ai/reliability/planner.py` · phase **AIGR4**

## 1. The LLM asks; this builds

Every value in a plan that could authorize a write — job ids, turn order, scope, action
type — is computed here from the lineage graph and the capability registry. None is produced
by a model.

## 2. Sequence

```
root cause permits recovery?        no -> refuse. A plan that exists invites submission.
        ↓
for each turn in the platform's topological order
        ↓
  asset impacted?                   no -> skip
        ↓
  a registered job produces it?     no -> EXCLUDE with a reason
        ↓
  smallest scope THAT JOB supports
        ↓
  action from the asset's layer     unknown layer -> refuse, do not invent an action name
        ↓
compact turns to be contiguous
        ↓
account for every impacted asset absent from the order -> EXCLUDE with a reason
```

## 3. Nothing impacted may vanish

Two exclusion paths, both explicit:

1. **in the order, no job** — nothing can rebuild it.
2. **absent from the order** — no execution path is known.

The second is the dangerous one: a naive planner iterating turns never visits such an asset,
drops it silently, and still produces a plan that looks complete. A test asserts the
invariant directly — *every impacted asset is either planned or explained*.

## 4. Turn order is not recomputed here

It comes from `LineageImpactService.turns()`. Two places computing order is two places for
it to drift, and the symptom would be a descendant rebuilt before its parent — which
produces a confidently wrong number rather than an error.

## 5. Widening is honest, not lossy

If a job supports only `COB_DATE`, the scope is re-expressed at that granularity and the
`reason` records *"widened to COB_DATE: the job cannot recompute a key subset"*. The
approver sees that more than the offending keys will be rewritten.

## 6. Cost is a class, not a number

`SMALL | MEDIUM | LARGE | UNBOUNDED`. A planner producing dollar figures would be inventing
precision. What an approver needs is *one partition* versus *a history rebuild*.
