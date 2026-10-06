# ADR-037 — Dependency graph and dbt manifest integration

- Status: **ACCEPTED** (Session 22)
- Related: ADR-034, ADR-038, `docs/REPORTING_REFERENCE_EVIDENCE.md` §7, §12 F3/F6

## Context

Repo 1 stores dependencies as comma-separated `VARCHAR(1000)` columns —
`before_direct`, `after_direct`, `before_full`, `after_full`, plus four `_by_mode`
variants and `dwh_dependencies` (`data_lake_init.sql:29-36`, `:44`). They are traversed in
SQL by substring match:

```sql
instr(concat(",", b.after_full, ","), concat(",", a.job_name, ",")) > 0
```

(`functions.py:106-115`). The transitive closure (`*_full`) is **hand-maintained
alongside** the direct edges, and the execution order is a hand-typed integer
(`order_by_default`, `order_by_mode`). There is no cycle detection anywhere in that
codebase.

Repo 6 is the counter-example: `class Job(nx.DiGraph)` with
`list(nx.topological_sort(self))` (`core/base.py:36`, `:108`) — a real graph, ordering
derived rather than declared.

Meanwhile dbt already knows the graph. Repo 4 produces `ttc_dbt/target/manifest.json`;
repo 5's `run_dbt_job.py` passes `-s job["modelFqn"]` from a DynamoDB row a human
maintains. **The graph exists twice and is reconciled by nobody** (finding F6).

## Options

| Option | Verdict |
|---|---|
| **Normalized DIRECT edges + computed closure, dbt manifest as an input** | **CHOSEN** |
| Comma-separated strings with hand-maintained closure (repo 1) | Rejected |
| dbt manifest as the sole source | Rejected |
| Hand-authored only, manifest ignored | Rejected |

dbt-only fails because real dependencies exist outside dbt: the EOD close of an upstream
layer, a Spark-built Kimball dimension, an external file arrival. Hand-authored-only
recreates F6.

## Decision

**`job_dependency` stores DIRECT edges only. Everything else is computed.**

Authored edge (in `reporting/jobs/<job>.yaml`):

```yaml
dependencies:
  - upstream_job_id: mart_account_balance_daily
    dependency_type: JOB          # JOB | DATASET | EOD_TABLE | REALTIME_TABLE
    flow_mode: EOD                #      | FULL_CDC_TABLE | EXTERNAL
    required: true
    kickoff_condition: SUCCEEDED  # SUCCEEDED | COMPLETED | WATERMARK_PAST
    lag_days: 0
```

Computed at compile time and written into `plan.json`, never authored:
`before_direct`, `after_direct`, `before_full`, `after_full` — all `ARRAY<STRING>`, and
per-mode variants derived by filtering the graph on `flow_mode`, not by four extra
columns.

**dbt manifest sync.** `make reporting-compile` runs `dbt parse`, reads
`target/manifest.json`, and for every model that is a registered job target walks
`nodes[<model>].depends_on.nodes`. Each resolved edge is emitted with
`source_of_definition = DBT_MANIFEST`. Edges the manifest cannot express (an EOD close, a
Spark-built dimension, an external arrival) are authored with
`source_of_definition = MANUAL_CONFIG`.

The reconciliation rule is asymmetric on purpose:

| Case | Verdict |
|---|---|
| In manifest, not authored | **auto-registered**, no warning — this is the point |
| In manifest, also authored identically | de-duplicated, no warning |
| In manifest, authored with different attributes | **compile FAILURE** — ambiguous intent |
| Authored `MANUAL_CONFIG`, absent from manifest | accepted — expected for non-dbt edges |
| A dbt `ref()` to a model that is not a registered job | **compile FAILURE** — an unscheduled upstream |

The last row is the one that catches the real defect: a mart that refs a model nobody
runs.

**Graph engine:** `networkx`. It is already a direct dependency of `dbt-core`
(`pip show dbt-core` → `Requires: ... networkx ...`; 2.6.3 present in this environment),
so the resolver adds no new dependency to pin under `CLAUDE.md` §3.9.

**Cycle detection is a hard compile failure.** `nx.simple_cycles` is run on the full graph
and on every per-mode subgraph; a cycle prints the offending path and exits non-zero.

## Consequences

- Adding a `ref()` in a model is enough to register a dependency. Nobody maintains the
  graph twice.
- `before_full` cannot drift from `before_direct` because it is not stored as input.
- A cyclic config never reaches Airflow.
- `job_dependency`'s `dependency_scope` column keeps `DIRECT` as the only authored value;
  `TRANSITIVE` appears in `plan.json` only.

## Cost

Compile-time only. `dbt parse` on a 10-model project is seconds, runs in CI, costs $0.

## Security

None. Manifest parsing is local; no credential is read.

## Rollback

Delete the manifest-sync step and author every edge. The schema is unchanged either way —
only `source_of_definition` values differ.

## Validation

- A cyclic fixture fails compile with a non-zero exit and the cycle path in stderr.
- A fixture with a `ref()` to an unregistered model fails compile.
- A fixture where manifest and manual edges conflict fails compile.
- `before_full` computed from a known graph matches a hand-checked expected closure.
- Per-mode subgraph filtering is tested: an edge declared `flow_mode: EOD` does not appear
  in the `STREAM_BATCH` ordering.
