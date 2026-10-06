# ADR-012 — Athena as the core query engine

- Status: **ACCEPTED** (Session 01)
- Required by: `DECISIONS.md:30`
- Related: ADR-013, ADR-014, ADR-019, ADR-026

## Context

The lakehouse needs a SQL engine for ad-hoc queries, validation, reconciliation,
Iceberg inspection and a Power BI baseline. `CLAUDE.md` §8 makes Athena core and
default; Redshift Serverless and Trino optional.

## Options

| Option | Idle cost | Iceberg support | Verdict |
|---|---|---|---|
| **Athena** | **zero** | Iceberg v2 read/write, time travel | **CHOSEN — core** |
| Redshift Serverless | RPU-hours while a workgroup is active | Iceberg via Glue | ADR-013, optional |
| Trino | worker EC2 while running | strong | ADR-014, optional |
| Spark SQL only | per-job | strong | insufficient — no interactive SQL surface |

## Decision

Athena is the core, always-enabled engine (`enable_athena = true`).

The decisive property is that **Athena has no idle cost at all**. Everything else in
this architecture is billed for existing; Athena is billed only for bytes scanned.
`docs/PRICE_REFERENCE.md` §5 puts that at $5/TB, so at lab mart sizes — tens to
hundreds of MB per query — a query costs well under a cent, and a hundred queries
cost about $0.05 (`docs/COST.md` §3.1). Against a budget where a single forgotten
NAT Gateway would cost $43/month, Athena is effectively free.

## Consequences

- No provisioned concurrency. Queries queue under load, which is why the Power BI
  default is Import rather than DirectQuery (ADR-019).
- Athena's Iceberg support is good but trails Spark's. Table maintenance
  (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`) stays in Spark,
  where the full procedure set is available.
- Bytes-scanned cutoff of 10 GB per query is **mandatory**. Not for the $0.05 it
  saves, but because it converts an accidental unpartitioned scan of `full_cdc`
  from a silent cost into an immediate, legible failure.
- `enforce_workgroup_configuration = true` so a client cannot override the results
  location or encryption.

## Cost

$5.00/TB scanned. Query results in S3 at $0.025/GB-Mo with a 7-day lifecycle. The
workgroup itself is free. Practically: ~$0.05 per metered window.

## Security

- Dedicated `athena` IAM role; result location encrypted with the lake CMK.
- Power BI reads **mart and serving views only** — never L1 or L2 (`CLAUDE.md` §8).
  Enforced by Glue database grants, not by convention.
- Query audit via CloudTrail and workgroup separation.

## Rollback

None needed — Athena is additive and has no idle cost. If concurrency or latency
ever exceeds what it can serve, ADR-013's Redshift Serverless is the documented next
step, gated on a benchmark rather than a hunch.

## Validation

- Workgroup created with the cutoff and encryption enforced.
- A mart query over Iceberg returns correct rows.
- A query exceeding 10 GB scanned is **cancelled** — the guardrail proven, not
  assumed.
- Iceberg time travel and snapshot inspection work within Athena's supported subset.
