# ADR-038 — Turn and topological ordering

- Status: **ACCEPTED** (Session 22)
- Related: ADR-037, ADR-039, ADR-043

## Context

Repo 1 has two unrelated things both called `turn`:

1. `job_master.order_by_default` / `order_by_mode` — hand-typed integers that the
   coordinator groups jobs by (`coordinator_graph_builder.py:18-28`). A wrong integer
   produces a wrong execution order silently.
2. `job_master_execution_hist.turn` — the **retry counter within a `date_of_data`**,
   incremented as `CASE WHEN status IS NULL THEN turn ELSE turn + 1 END`
   (`functions.py:986-988`), and used by `run_mode` to decide whether a dependency
   satisfied at an earlier turn counts (`pipeline_builder.py:543-656`).

The master prompt's §23 defines `turn` as the topological wave. Inheriting one column for
both meanings guarantees the confusion is inherited with it.

## Options

| Option | Verdict |
|---|---|
| **Computed generations for order; separate `attempt_number` for retries** | **CHOSEN** |
| Hand-authored `order_by_*` integers (repo 1) | Rejected |
| One `turn` column carrying both meanings (repo 1) | Rejected |

## Decision

**Two names, two meanings, never conflated.**

| Name | Meaning | Source |
|---|---|---|
| `turn` | topological wave within one coordinator run | computed |
| `attempt_number` | retry counter within `(job_id, flow_mode, date_of_data)` | incremented on rerun |

`turn` is `nx.topological_generations` over the graph **filtered to the flow_mode being
run**, computed at compile time and stored in `plan.json`. Generation *i* may run in
parallel; generation *i* starts only when every job it depends on has satisfied its
`kickoff_condition`.

`order_by_default` and `order_by_mode` are **not authored and not stored as input**. They
appear in `plan.json` as derived integers, retained under those names solely so the legacy
vocabulary maps cleanly (ADR-035's legacy table).

**Wiring policy.** Repo 1's three `coordinator_mode` values are kept as a per-flow policy
because they encode a real trade-off, and the conservative one is the default:

| `coordinator_mode` | Wiring | Default |
|---|---|---|
| `default` | generation *i-1* fans out to all of generation *i* | **yes** |
| `native` | true edges only, from `after_direct` | opt-in |
| `sequential` | strictly serial | opt-in, for debugging |

`default` is strictly more conservative than `native`: it can delay a job, never run one
early. `native` is opt-in per flow because it maximises parallelism at the cost of a
subtler failure mode.

**Concurrency is bounded independently of the graph.** A generation with eight jobs does
not submit eight EMR Serverless jobs if `max_concurrent_tasks` says four. Ordering
correctness and resource policy are separate controls.

## Consequences

- Adding a dependency reorders execution automatically; no integer to update.
- A cycle cannot reach runtime (ADR-037).
- `attempt_number` is explicit, so "third rerun of 2026-08-14" is a fact, not an inference
  from a mutated row.
- Repo 1's `run_mode` (`cross_turn` / `turn_by_turn`) is retained on `job_flow_config`
  but now unambiguously refers to **attempts**, not waves.

## Cost

Compile-time graph work on a graph of tens of nodes. Negligible.

## Security

None.

## Rollback

Pin `turn` manually in `plan.json` if the resolver is ever wrong; the coordinator reads
the plan, not the graph.

## Validation

- Turn assignment on a known diamond graph matches a hand-computed expectation.
- A job with no dependencies is always turn 1.
- Per-mode filtering changes the turn assignment when an edge is mode-scoped.
- `default` and `native` produce the same *result* on a fixture, and `default` produces a
  superset of the ordering constraints.
- `attempt_number` increments on rerun while `turn` does not change.
