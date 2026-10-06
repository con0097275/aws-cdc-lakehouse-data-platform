# ADR-013 — Redshift Serverless as optional BI serving

- Status: **ACCEPTED as optional, DISABLED by default** (Session 01)
- Required by: `DECISIONS.md:31`
- Related: ADR-012, ADR-019, ADR-026

## Context

Athena has no provisioned concurrency, so a dashboard with many repeated visuals can
queue. Redshift Serverless is the AWS-native answer: it queries Iceberg through Glue,
supports local serving tables and materialized aggregates, and Power BI has a native
connector.

## Options

| Option | Cost when idle | Verdict |
|---|---|---|
| **Redshift Serverless behind a flag** | zero when destroyed; **$3.60/hr at 8 RPU while a workgroup exists** | **CHOSEN — optional** |
| Redshift Serverless always on | $2,628/month at 8 RPU | Rejected — 33× the budget |
| Redshift provisioned | node-hours continuous | Rejected |
| Athena only | zero | the default; ADR-012 |

## Decision

`enable_redshift_serverless = false`. Enabled only for a **single, time-boxed
benchmark session** with a decision gate, then destroyed.

The arithmetic is the whole argument. From `docs/PRICE_REFERENCE.md` §5,
$0.45/RPU-hour × 8 RPU base = $3.60/hr:

| Duration | Cost | Share of the $80 budget |
|---|---:|---:|
| 3-hour benchmark | $10.80 | 14 % |
| plus the concurrent lab stack | $20.25 | **25 %** |
| left on for a weekend | $172.80 | 216 % |
| 24/7 for a month | $2,628.00 | 3,285 % |

**One forgotten weekend costs more than two months of budget.** That is why the
usage limit below is mandatory rather than advisory.

## Consequences

- `redshift_max_rpu_hours = 24` with **`breachAction = deactivate`**, not `log`. A
  breach that only writes a log line is not a control; it is a record of the loss.
- `redshift_max_capacity_rpu = 16` caps burst.
- Private workgroup, no public endpoint, subnets and SG reused from the platform.
- IAM role scoped to Glue plus the `mart` S3 prefixes only — read-only by default.
- Base capacity minimum is assumed to be 8 RPU and **must be verified live** at
  Session 13B; regional minimums differ and this figure drives every number above.
- Mutually exclusive with Trino unless overridden (ADR-026).
- **Never claim this on a CV unless live-tested** (`MASTER_PLAN.md:53`).

## Cost

$3.60/hr at 8 RPU. Managed storage $0.0261/GB-Mo — which **survives the workgroup**,
so destroy verification must check for a leftover namespace, not just a workgroup.

## Security

Private networking, no public accessibility, CMK encryption, read-only IAM by
default. Power BI connects from Desktop over the private path or via a documented
gateway (ADR-019, risk R5).

## Rollback

`enable_redshift_serverless = false` and destroy. Verify **both**
`list-workgroups` **and** `list-namespaces` are empty — a namespace with managed
storage keeps billing after its workgroup is gone, which is the trap.

## Validation

- Private workgroup deploys; no public endpoint exists.
- Iceberg query via Glue succeeds, or a serving-mart load succeeds with a recorded
  reason for choosing that path.
- Power BI native connector completes a refresh.
- Benchmark records p50/p95 and concurrency **against Athena** — the comparison is
  the point; a benchmark of one engine decides nothing.
- Usage limit and `deactivate` action confirmed present.
- Destroy verified for workgroup **and** namespace.
