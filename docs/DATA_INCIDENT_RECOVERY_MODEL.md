# DATA INCIDENT AND RECOVERY MODEL

- Phase: **DRP1** — contracts implemented; no planner, no executor (DRP8 / DRP9)
- Code: `cdc/incidents.py` · DDL: `spark/ops/ddl/reliability_tables.sql`
- Related: `docs/DATA_RELIABILITY_PLATFORM_TARGET.md` §6, `docs/LINEAGE_SOURCE_MATRIX.md` §5

---

## 1. The rule

> **A plan is a proposal. Execution is a separate, recorded act.**

`RecoveryPlan` is frozen and identified by `plan_id` — a hash of its own content. An edited
plan is a *different* plan, so "the plan we approved" and "the plan we ran" are comparable
rather than assumed equal, and an execution referencing a superseded id is visibly stale.

DRP1 writes the shape, not the engine. That order is deliberate: the dangerous part of
lineage-driven recovery is not computing a descendant set, it is deciding when the platform
may act on one without asking. That decision is a data structure with named conditions, so it
can be read, tested and refused.

## 2. Incident

| Field | Notes |
|---|---|
| `incident_id`, `dataset_id` | `dataset_id` is an `AssetId`, parsed and checked |
| `failure_class` | `dq_violation`, `reconciliation_break`, `freshness_breach`, `schema_change`, `late_arriving_data`, `job_failure`, `source_defect`, `infra`, `unknown` |
| `severity`, `status` | see the lifecycle below |
| `scope` | COB dates, interval, business keys |
| `source_run_id`, `dq_check_id`, `recon_run_id` | what raised it |
| `root_cause_status` | `unknown` \| `suspected` \| `confirmed` |
| `blast_radius_count`, `automatic_recovery_allowed`, `recovery_plan_id`, `attempts` | |
| `opened_at`, `updated_at`, `resolved_at` | `RESOLVED` without `resolved_at` is refused — an incident that cannot be aged stops being visible |

```
OPEN ──▶ PLANNED ──▶ RECOVERING ──▶ RECOVERED ──▶ RESOLVED
  │          │            │
  │          └──▶ REQUIRES_APPROVAL
  └──▶ CLOSED_NO_ACTION            └──▶ EXHAUSTED
```

`RESOLVED`, `EXHAUSTED` and `CLOSED_NO_ACTION` are terminal for automatic action.
**`EXHAUSTED` is what stops the DQ → rerun → DQ loop** the brief forbids: a reliability
platform that can loop is an outage generator with good intentions.

`SOURCE_DEFECT` and `INFRA` are deliberately outside `REPAIRABLE_BY_RERUN`. Rerunning a
pipeline over bad source data reproduces the same bad answer, more expensively.

The incident table stores one row per **state transition**, not one per incident. A table
UPDATEd in place answers "what is the state now" and loses "how did it get there", which is
where every post-mortem starts.

## 3. Recovery plan

| Field | Notes |
|---|---|
| `root_asset`, `scope` | an unbounded scope is refused by the approval gate |
| `lineage_version`, `lineage_as_of` | a plan is only as good as the graph it read, and graphs change |
| `candidate_descendants` | everything impacted |
| `executable_descendants` | a **subset** that resolves to a job |
| `excluded_assets` | each with a reason |
| `turns` | topological waves; must cover exactly the executable set, with no asset twice |
| `weakest_evidence`, `cost_class`, `approval`, `approval_reasons` | |

### Why a plan names things it cannot run

A Power BI report downstream of a broken mart is genuinely impacted and genuinely not
executable — there is no job. Dropping it would understate the blast radius; promoting it
into the executable set would make the planner try to run it. So both lists are stored and
`excluded_assets` says why each exclusion happened.

> An impact analysis that silently omits what it cannot fix is the one that gets someone a
> surprise on Monday.

`blast_radius` counts **candidates**, not executables.

## 4. The approval gate — ten conditions

`decide_approval()` returns `AUTOMATIC` only when every one holds, and otherwise returns
**every** reason:

1. root cause is known
2. the affected interval is bounded
3. the failure class is repairable by rerunning
4. **every edge on the path is `observed` or `derived`** — one `declared` edge downgrades the whole plan
5. at least one affected asset resolves to an executable job
6. blast radius is within the policy limit
7. cost class is within the policy limit
8. the attempt limit is not reached
9. the incident is not terminal
10. the plan targets this incident

Condition 4 is `docs/LINEAGE_SOURCE_MATRIX.md` §5 made executable. Acting on a `declared`
edge means rewriting data on the strength of a config file nobody verified. It makes every
mart-crossing recovery operator-approved, because ADR-086's STREAM_BATCH `source_tables` link
is `declared` — which is the correct outcome, since that link exists precisely because it was
once invisible and a mart served a stale number while looking healthy.

## 5. Policy — two limits that cannot be defaulted

`RecoveryPolicy` refuses to construct without `max_attempts` and `max_blast_radius`. A
silently defaulted limit is the one nobody reviews.

`allow_source_writes=True` is refused unconditionally. Recovery repairs the lakehouse;
writing back to Oracle or SQL Server is not a pipeline action and no automatic decision may
take it.

`allow_offset_reset` defaults `False`, and that is not theoretical. The 2026-09-29 rebuild
produced a Structured Streaming checkpoint that outlived its MSK cluster, and the ingest
reported `SUCCESS` with `batches: 0, rows: 0`
(`docs/PLATFORM_RESOURCE_INVENTORY.md` §0c). An automatic recovery permitted to reset
checkpoints would have **hidden** that, not fixed it.

## 6. Cost class

`small` · `medium` · `large` · `unbounded`. Coarse on purpose: a planner producing dollar
estimates would be inventing precision it does not have. What an approver needs is whether
this is a partition rewrite or a full history rebuild.

## 7. Execution

One row per **attempt**; a retry is a new record, never an edit. `attempt` starts at 1.
`plan_id` is carried so an execution can be checked against the plan that was approved.
`approved_by` is empty for an automatic execution and populated when a human decided — the
distinction has to survive in the record, because "who allowed this" is the first question
asked about any recovery that went wrong.

## 8. What DRP1 did not do

No incident is raised, no plan is computed, nothing executes, and none of the three tables
exists. There is no lineage graph yet, so `EvidenceClass` is vocabulary only — DRP6
populates it, DRP8 builds the planner, DRP9 runs an approved subgraph.
