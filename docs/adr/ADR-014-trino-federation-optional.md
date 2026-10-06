# ADR-014 — Trino as optional federation engine

> **Superseded in part 2026-08-13 by [ADR-031](ADR-031-secret-store.md):**
> credentials live in **SSM Parameter Store SecureString**, not Secrets Manager.
> The runtime pattern below is unchanged — generated at apply, read at container
> start, never in outputs or user-data — only the store differs.


- Status: **ACCEPTED as optional, DISABLED by default** (Session 01)
- Required by: `DECISIONS.md:32`
- Related: ADR-012, ADR-026

## Context

A distinct use case from ADR-013: querying **across** S3/Iceberg and the operational
databases in one SQL statement, which neither Athena nor Redshift Serverless does.

## Options

| Option | Cost | Verdict |
|---|---|---|
| **Trino on k3s behind a flag** | $0.4224/hr while running | **CHOSEN — optional** |
| Trino on EKS | plus control plane | Rejected — ADR-011 |
| Athena Federated Query | Lambda connectors per source | Rejected — a different, more limited pattern |
| No federation | $0 | acceptable; it is optional for a reason |

## Decision

`enable_trino = false`. Enabled only for a time-boxed federation demonstration:
coordinator on `t3.xlarge`, two workers on `t3.large`.

Worth noting against ADR-013: **Trino is roughly half the cost of the Redshift
benchmark** — ~$10.74 for a session versus ~$20.25 (`docs/COST.md` §4–5) — because it
is plain EC2 with no managed-service premium. If only one optional module can be
afforded, Trino demonstrates more engineering per dollar; Redshift demonstrates the
more common enterprise serving pattern. Neither is required for the core release
(`MASTER_PLAN.md:53`).

## Consequences

- Coordinator is private-only, reached via SSM port forwarding.
- All catalogs **read-only** by default, including the Iceberg catalog. Trino is not
  a writer in this architecture; Spark is (`ARCHITECTURE.md:8`).
- The Kafka connector is **not configured**. `CLAUDE.md` §8 forbids querying Kafka
  for certified numbers, and the surest way to honour that is to make it impossible.
- Resource groups, per-query memory limits and spill configuration are required, not
  optional — an unbounded federated join against Oracle can pull far more than the
  cluster can hold.
- Federated queries push load onto the **source databases**. Read-only credentials
  and a query timeout protect the CDC source from the demo.
- Mutually exclusive with Redshift Serverless unless overridden (ADR-026).
- **Never claim this on a CV unless live-tested.**

## Cost

$0.4224/hr for three instances, plus ~$5.76/month of gp3 if volumes are left behind.

## Security

Private coordinator, zero-inbound SG, SSM-only access. Source credentials from
Secrets Manager at pod start. Read-only DB users with their own grants — not the
Debezium user, whose privileges are higher.

## Rollback

`enable_trino = false` and destroy, or scale the deployment to zero replicas for a
quick pause within a window. Verify no Trino pods and no leftover EBS.

## Validation

- Iceberg catalog query via Glue succeeds.
- At least one genuine cross-catalog federation query succeeds read-only —
  Iceberg joined to Oracle or SQL Server.
- A write attempt against any catalog is **rejected**.
- Resource-group and query-limit enforcement observed, not just configured.
- No Kafka catalog is present.
- Destroy verified: no pods, no volumes, no instances.
