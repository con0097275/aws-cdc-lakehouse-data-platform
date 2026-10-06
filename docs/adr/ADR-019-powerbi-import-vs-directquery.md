# ADR-019 — Power BI: Import by default, DirectQuery only after a benchmark

- Status: **ACCEPTED** (Session 01)
- Required by: `DECISIONS.md:33`
- Related: ADR-012, ADR-013, risk R5

## Context

Power BI is the visualization layer. Import caches data in the model; DirectQuery
issues a query per interaction. `CLAUDE.md` §8 makes Import the lab default and gates
DirectQuery on a benchmark of latency, concurrency and cost.

## Options

| Option | Cost per interaction | Freshness | Verdict |
|---|---|---|---|
| **Import** | zero — one scan per refresh | as of last refresh | **CHOSEN — default** |
| DirectQuery on Athena | one Athena query per visual per interaction | live | Rejected as default |
| DirectQuery on Redshift Serverless | RPU-hours while the workgroup is up | live | only after ADR-013's benchmark |
| Composite | mixed | mixed | out of scope for the lab |

## Decision

**Import**, refreshed from Athena, from Power BI Desktop on the operator's machine.
DirectQuery only after a recorded benchmark.

The cost mechanics are what settle it. Under DirectQuery on Athena, **every visual on
every page issues its own query on every filter change.** A ten-visual page with
three slicer interactions is thirty Athena queries. At lab data sizes each is
fractions of a cent, so the risk is not the money — it is that the pattern scales
badly and teaches the wrong habit, then gets carried into an environment where the
marts are 1000× larger and the same dashboard costs real money. Import scans once per
refresh and serves every interaction from the model.

Power BI **Desktop**, not Service, and this is a security decision as much as a cost
one: Service scheduled refresh against a private endpoint needs an on-premises data
gateway, and against a public endpoint needs stored credentials — which conflicts
directly with `CLAUDE.md` §3.1's prohibition on static credentials. Desktop uses the
operator's local AWS profile and stores nothing (risk R5).

## Consequences

- Freshness is bounded by refresh cadence, not by pipeline latency. The NRT flow's
  ≤10-minute freshness is only visible if someone refreshes — so the report must
  display `processing_status`, `business_date` and `last_updated_at` prominently
  (`ARCHITECTURE.md:213`), or a stale Import silently misrepresents a fresh pipeline.
- Certified vs provisional badging is required in the report, driven by the status
  precedence in `docs/DATA_CONTRACTS.md`.
- Model size is bounded by Import limits. Lab marts are far below any limit.
- Power BI reads **mart / serving views only** — never L1 or L2, enforced by Glue
  grants rather than by trust.

## Cost

One Athena scan per refresh: fractions of a cent at lab sizes. No gateway, no
Premium capacity, no stored credentials.

## Security

No stored credentials anywhere. The `athena` role grants read on `mart` and serving
schemas only. PII masked before the serving layer (Session 14).

## Rollback

Switching a model from Import to DirectQuery is a Power BI setting. The gate is
evidence, not effort: record query count, bytes scanned, p50/p95 latency and
projected monthly cost before flipping it.

## Validation

- Import refresh from Athena succeeds and renders.
- The report cannot query L1/L2 — the grant denies it (tested, not assumed).
- `processing_status` / `business_date` / `last_updated_at` visible.
- Certified vs provisional badge reflects the underlying status correctly.
