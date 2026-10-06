# AI TABLE-LEVEL RECOVERY — RUNBOOK

Phase **AIGR9**

## 1. When you get here

Either the request was table-level, or column lineage was not `VALIDATED` and the copilot
downgraded — which it states rather than implies.

## 2. The scope must still be bounded

Table-level does **not** mean all history. A bounded date remains mandatory: `bounded` is
false without dates, keys, partitions or a watermark range, and an unbounded plan is
`BLOCKED` by policy, not merely gated.

## 3. Expect a wider approval

More jobs, more data, a higher `CostClass`. `PolicyLimits` will usually escalate to
`APPROVAL_REQUIRED` on descendant count or cost.

## 4. Order still holds

Root first, then `DQ → reconciliation → certification`, then turn 1, and so on. A failed
root stops everything below it. Independent branches fail in isolation.

## 5. Before approving, read the exclusions

Every impacted asset is either planned or explained. Two exclusion reasons appear:

- *"impacted but no registered executable job"* — nothing can rebuild it.
- *"impacted but absent from the topological order"* — no execution path is known.

Both mean the same thing operationally: **that asset will stay wrong** until someone acts.
It is listed so that is a decision rather than an oversight.
