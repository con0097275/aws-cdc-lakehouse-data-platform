# ADR-005 — Source database lab

> **Superseded in part 2026-08-13 by [ADR-031](ADR-031-secret-store.md):**
> credentials live in **SSM Parameter Store SecureString**, not Secrets Manager.
> The runtime pattern below is unchanged — generated at apply, read at container
> start, never in outputs or user-data — only the store differs.


- Status: **ACCEPTED** (Session 01)
- Related: risks R1, R15

## Context

CDC needs Oracle and SQL Server sources with transaction-log capture enabled.
`CLAUDE.md` §4.3 forbids RDS Oracle/SQL Server by default on cost grounds.

## Options

| Option | Cost | Verdict |
|---|---|---|
| **Containers on ephemeral EC2** | `t3.xlarge` at $0.2112/hr, destroyed with the window | **CHOSEN** |
| RDS Oracle SE2 / RDS SQL Server SE | licence-inclusive hourly, no stop for some editions | Rejected — `CLAUDE.md` §4.3 |
| Oracle on EC2 with a BYOL binary install | free binary, hours of setup, snowflake host | Rejected |

## Decision

Oracle Database Free and SQL Server Developer Edition as **containers** on one
ephemeral `t3.xlarge` (4 vCPU / 16 GB), **x86_64**, encrypted gp3.

`t3.xlarge`, not `t3.large`: Oracle Free needs roughly 2 GB resident just to open,
SQL Server Developer another ~2 GB, and both grow under load. 8 GB shared with the
OS and Docker leaves no headroom, and the failure is an OOM-kill mid-snapshot (risk
R1). The extra $0.1056/hr is cheap against a wasted metered window.

**x86_64 is a hard constraint, not a preference.** Neither Oracle Free nor SQL
Server Developer publishes an arm64 image, so this instance cannot follow the
Graviton choice made for MSK brokers (`kafka.m7g.large`) or for EMR Serverless
(ADR-008). Recording it here because "use Graviton everywhere for the 20 % saving"
is otherwise the obvious and wrong generalisation.

## Consequences

- Licensing: Oracle Free is production-usable within its resource limits; SQL Server
  Developer is **non-production only**. This is a portfolio lab, which is inside
  that boundary — but it must never be presented as a production deployment.
- Oracle needs `ARCHIVELOG` mode plus supplemental logging; SQL Server needs
  `sys.sp_cdc_enable_db` and `sp_cdc_enable_table` per table.
- Data is **synthetic and disposable**. Nothing survives the window, so seeding is
  part of the demo script, not a backup concern.
- Startup is slow (minutes). Start the two engines sequentially, and pre-pull images
  into the AMI or S3 so a window is not spent downloading multi-GB layers.

## Cost

$0.2112/hr while running. ~$4.80/month of gp3 if the volume is left behind.

## Security

- No inbound rules; database ports reachable only from the CDC runtime SG.
- Credentials generated at apply into Secrets Manager, read at container start,
  never in user-data or `docker run -e` plaintext (`CLAUDE.md` §3.1).
- Synthetic data only — no real PII, which removes an entire class of risk from the
  lab and is worth stating explicitly.

## Rollback

`enable_source_lab = false` and destroy. Nothing to preserve.

## Validation

- Oracle: `SELECT log_mode FROM v$database` returns `ARCHIVELOG`; supplemental
  logging confirmed at database and table level.
- SQL Server: `sys.databases.is_cdc_enabled = 1` and `sys.tables.is_tracked_by_cdc = 1`.
- **Both asserted before the connector is deployed** — a connector against a
  CDC-disabled table starts healthy and silently produces nothing (risk R15).
