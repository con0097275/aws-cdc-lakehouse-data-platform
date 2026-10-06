# AI AGENT — TOOL CATALOGUE

Phase **AIGR1** · source of truth `ai/reliability/tools.py` · boundary `AI_AGENT_ACTION_BOUNDARY.md`

**27 tools — 18 read-only, 7 planning, 2 mutating.**

Every tool carries: a typed input and output schema, an authorization class, a timeout, a
row cap, a result-byte cap, a version, and an audit record via `contract.invoke()`.

## READ_ONLY (18)

| tool | class | note |
|---|---|---|
| `resolve_asset` | governed_read | **refuses on ambiguity** rather than guessing |
| `resolve_column` | governed_read | resolved against the asset's real schema |
| `get_asset_schema` | governed_read | |
| `get_governance_context` | governed_read | owner, domain, classification, tags, glossary |
| `get_data_contract` | governed_read | |
| `get_dataset_lineage` | governed_read | bounded hops; evidence class per edge |
| `get_column_lineage` | governed_read | returns **confidence + source**; see §2 |
| `get_pipeline_status` | ops_read | |
| `get_execution_history` | ops_read | |
| `get_watermark` | ops_read | |
| `get_dq_results` | ops_read | |
| `get_reconciliation_status` | ops_read | |
| `get_certification_status` | ops_read | |
| `get_incident` | ops_read | |
| `get_recovery_status` | ops_read | |
| `query_athena_safe` | governed_read | SELECT-only, bounded (ADR-051) |
| `retrieve_runbook` | public_metadata | RAG: knowledge, never live operational truth |
| `get_change_history` | governed_read | |

## PLAN_ONLY (7)

Write **append-only planning and audit** records. They remain `read_only=True` because that
flag means *does not change business data* — and a flag whose meaning drifts is worse than
no flag.

| tool | note |
|---|---|
| `classify_data_incident` | returns evidence refs and a confidence; `UNKNOWN` is a legitimate answer and blocks automatic recovery |
| `build_impact_analysis` | blast radius ∩ registered jobs; returns **excluded assets with reasons** |
| `build_recovery_scope` | typed and bounded; never a free-form `WHERE` |
| `build_recovery_plan` | immutable, hash-keyed |
| `validate_recovery_plan` | re-checks invariants |
| `estimate_recovery_cost` | a coarse **class**, never a dollar figure |
| `simulate_recovery_plan` | dry run; no writes |

## MUTATING (2)

| tool | note |
|---|---|
| `submit_recovery_plan` | takes `plan_id`, `approval_id`, `idempotency_key` — **and nothing else** |
| `request_recovery_cancel` | mutates, but only ever *reduces* activity |

## 2. Column lineage carries its own confidence, on purpose

`get_column_lineage` returns a confidence and a source. A caller may narrow a recovery on
`validated` lineage only. Anything weaker downgrades to `TABLE_LEVEL_IMPACT` and says so.

Fuzzy column lineage must never authorize automatic recovery: an impact analysis that is
confidently wrong is worse than one that admits its width, because the descendants it
silently omits are the ones that never get rebuilt.
