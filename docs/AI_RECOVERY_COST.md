# AI RECOVERY — COST

Phase **AIGR11**

## 1. Bounded by construction

| dimension | bound | where |
|---|---|---|
| graph steps | 24 | `Budget.max_steps` |
| tool calls | 40 | `Budget.max_tool_calls` |
| recovery attempts | 1 automatic | `Budget`, and policy escalates on any prior attempt |
| agent wall clock | 300 s | `Budget` |
| tokens | 120,000 | `Budget` |
| Athena scan | workgroup bytes-scanned cutoff | ADR-051 |
| tool result | 1,000 rows / capped bytes | `ToolSpec` |
| lineage hops | bounded per call | `get_*_lineage` |
| recovery compute | `CostClass` gate | `PolicyLimits.max_cost`, default `MEDIUM` |

## 2. Recovery cost is classed, not priced

`SMALL` one COB on a few assets · `MEDIUM` several COBs or a full table ·
`LARGE` multi-table or history replay · `UNBOUNDED`.

A planner emitting dollar figures would be inventing precision it does not have.

## 3. What this phase spent

**$0.** No AWS resource was created for AIGR. No Bedrock invocation has been made by this
code yet — which is also why no model-cost figure is claimed.

## 4. The cost control that matters most

`max_recovery_attempts = 1`, plus a policy rule that escalates to a human on *any* prior
attempt. Automatic retry of a failing recovery is the only path here that can spend without
bound, and it is closed.
