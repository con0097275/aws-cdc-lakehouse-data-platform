# ADR-032 — Source lab: `t3a.xlarge` containers, and RDS priced out for good

- Status: **ACCEPTED** (Session 03, 2026-08-13)
- Refines: [ADR-005](ADR-005-source-database-lab.md) — confirms containers, fixes the instance type
- Closes: the "source lab at `t3.large` is unproven" limitation left open by ADR-030

## Context

Two questions were open going into Session 03:

1. **Is RDS Oracle / SQL Server actually necessary?** `CLAUDE.md` §4.3 forbids it by
   default and ADR-005 rejected it, but on a stated rather than a priced basis. The
   instruction for this session was to prove it, not cite it.
2. **`t3.large` or `t3.xlarge`?** ADR-005 said `xlarge`. ADR-030's cost package A- then
   assumed `t3.large` to fit the $30 budget, and `SESSION_HANDOFF.md` flagged the
   conflict as unproven. One of the two had to give.

## Options

Live prices, AWS Pricing API, `ap-southeast-1`, collected **2026-08-13**
(`artifacts/validation/session-03/pricing.txt`):

| Option | Hourly | vs chosen |
|---|---:|---:|
| **`t3a.xlarge`, both DBs as containers** | **$0.1888** | **— CHOSEN** |
| `t3.xlarge`, both DBs as containers | $0.2112 | +12% |
| `t3.large`, both DBs as containers | $0.1056 | −44%, **rejected on memory** |
| RDS Oracle SE2 BYOL + RDS SQL Server SE | $0.4160 + $0.4580 = **$0.8740** | **+363%** |
| RDS Oracle SE2 licence-included + RDS SQL Server SE | $0.7040 + $0.4580 = **$1.1620** | **+515%** |

## Decision

**One ephemeral `t3a.xlarge` running both databases as containers. RDS is rejected on
price, permanently.**

### RDS is 6.2× the cost, and that is the whole argument

Licence-included RDS for both engines is **$1.1620/hr against $0.1888/hr** — a
**$0.9732/hr** premium. Over the 19 source-lab hours in `docs/SESSION_DEPENDENCY_GRAPH.md`
§5 that is **$18.49**, which is **62% of the entire $30 monthly budget** spent on managed
database hosting for synthetic data that is thrown away at the end of every window.

Even BYOL — which this project has no licence for — is $0.8740/hr, +363%.

RDS is not *technically* wrong: RDS Oracle supports LogMiner CDC with `rdsadmin`
procedures, and RDS SQL Server supports CDC via `msdb.dbo.rds_cdc_enable_db`. Both would
work. **They are simply unaffordable here**, and the containers give the same
transaction-log semantics because they are the same database engines.

Two further points that make RDS worse than the hourly rate suggests:

- **RDS SQL Server cannot be stopped for more than 7 days**, after which AWS restarts it.
  Under ADR-027's destroy-between-windows model that is a standing leak.
- RDS instances take minutes to create *and* delete, so each metered window pays the
  latency twice.

**This closes the question. It should not be reopened without new pricing.**

### `t3a.xlarge`, not `t3.xlarge` — 10.6% off for nothing

`t3a` is AMD EPYC and **still x86_64**, so it satisfies ADR-005's hard constraint that
neither Oracle Database Free nor SQL Server Developer publishes an `arm64` image. Same
4 vCPU / 16 GiB, **$0.1888 against $0.2112 — a 10.6% saving with no behavioural
difference** for two database containers.

*Risk:* `t3a` is not offered in every AZ. The module therefore takes an ordered
`instance_type_candidates` list and a preflight check confirms availability in the target
AZ before apply, falling back to `t3.xlarge`.

### `t3.large` is rejected — the memory does not fit

This is the question ADR-030 left open, and the answer is no.

| Consumer | Requirement | Note |
|---|---:|---|
| Oracle Database Free | **2.0 GiB** | Hard cap — the Free edition licence limits RAM to 2 GB, 2 CPU threads, 12 GB user data |
| SQL Server Developer | 4.0 GiB | Minimum 2 GB; 4 GB is realistic under snapshot load |
| OS + Docker daemon | 1.5 GiB | Amazon Linux 2023 + containerd |
| **Subtotal** | **7.5 GiB** | |
| `t3.large` | 8.0 GiB | **0.5 GiB headroom — no page cache during a snapshot** |
| `t3a.xlarge` | 16.0 GiB | 8.5 GiB headroom |

`t3.large` is not impossible — capping SQL Server at 3 GiB via `MSSQL_MEMORY_LIMIT_MB`
brings the subtotal to 6.5 GiB. But the failure mode when it goes wrong is an **OOM-kill
mid-snapshot** (risk R1), which wastes the entire metered window it happens in. A wasted
4-hour window costs **$3.22**; the instance delta across the whole sequence costs
**$1.58**. Buying the headroom is cheaper than one failure.

## Consequences

**Accepted:**

- ADR-030's package A- moves from `t3.large` to `t3a.xlarge`: **+$0.0832/hr**, **+$1.58**
  across the 19 source-lab hours. The core release goes from ≈$28.25 to **≈$29.83** —
  still inside $30, but with the headroom essentially gone. **This reinforces ADR-030's
  two-month schedule rather than undermining it**; it does not change that decision.
- `docs/COST.md` §3.1's source-lab line changes from `t3.xlarge @ 0.2112` to
  `t3a.xlarge @ 0.1888`.
- The module must preflight `t3a.xlarge` availability per AZ, with a `t3.xlarge` fallback.
- Oracle Free's 12 GB user-data cap bounds the synthetic dataset. Session 03's generator
  must stay well inside it; a CDC lab needs *change volume*, not data volume.

**Licensing, stated plainly:** Oracle Database Free is production-usable within its
resource limits. **SQL Server Developer Edition is non-production only.** This is a
portfolio lab, which is inside that boundary — but it must never be presented as a
production deployment, and this ADR is where that is recorded.

## Cost

| | |
|---|---:|
| While running | **$0.1888/hr** |
| 4-hour window | $0.76 |
| 19 hours (full sequence) | **$3.59** |
| Left running 24/7 | $137.82/month — **4.6× the budget** |
| gp3 volume if left behind | ~$4.80/month for 50 GiB |

**RDS equivalent: $1.1620/hr, $22.08 for 19 hours, $848/month at 24/7 — 28× the budget.**

## Security

- **No inbound rules.** Database ports (1521, 1433) reachable only from the CDC runtime
  security group, SG-to-SG. Never `0.0.0.0/0` (`CLAUDE.md` §3.3).
- **No SSH, no key pair.** SSM Session Manager only (§3.4); port-forward for a local SQL
  client.
- **Credentials generated at apply into SSM SecureString** (ADR-031), read at container
  start. Never in user-data, never in `docker run -e` plaintext, never a Terraform output
  (§3.1, §3.6).
- **Synthetic data only.** No real PII, which removes an entire class of risk and is worth
  stating rather than assuming.
- Encrypted gp3 root volume, IMDSv2 enforced.

## Rollback

`enable_source_lab = false`, then `terraform apply`. Nothing to preserve — the data is
synthetic and regenerated by the seed script.

```bash
bash scripts/source-lab-destroy.sh --execute
aws ec2 describe-instances --profile my-aws-profile --region ap-southeast-1 \
  --filters Name=tag:Component,Values=source-lab \
  --query 'Reservations[].Instances[?State.Name!=`terminated`].InstanceId'   # must be []
aws ec2 describe-volumes --profile my-aws-profile --region ap-southeast-1 \
  --query 'Volumes[?State==`available`].VolumeId'                            # orphans bill
```

The second command matters: a detached gp3 volume keeps billing and does not appear in an
instance listing.

## Validation

- Prices re-derivable: `bash scripts/collect-pricing.sh` and the RDS/EC2 queries saved in
  `artifacts/validation/session-03/pricing.txt`.
- `terraform validate`, `checkov`, and a saved plan showing the source lab as
  **one** EC2 instance and **zero** RDS resources.
- A root `check` block asserts no `aws_db_instance` is ever planned.
- Oracle: `SELECT log_mode FROM v$database` → `ARCHIVELOG`, supplemental logging confirmed
  at database and table level.
- SQL Server: `sys.databases.is_cdc_enabled = 1`, `sys.tables.is_tracked_by_cdc = 1`.
- **Both asserted before any connector is deployed** — a Debezium connector pointed at a
  CDC-disabled table starts *healthy* and silently produces nothing (risk R15).
- Status: **`static-validated`**. No instance has been launched.
