# Source CDC Setup — Oracle Free + SQL Server Developer

- Session: 03
- Date: 2026-08-13
- Status: **`static-validated` and `planned`.** Nothing has been launched.
- Decisions: [ADR-005](adr/ADR-005-source-database-lab.md), [ADR-032](adr/ADR-032-source-lab-instance-sizing.md)

---

## 1. Why not RDS — the number

| | Hourly | 19 lab hours | 24/7 month |
|---|---:|---:|---:|
| **Containers on `t3a.xlarge`** | **$0.1888** | **$3.59** | $137.82 |
| RDS Oracle SE2 + RDS SQL Server SE, licence-included | $1.1620 | $22.08 | $848.26 |
| RDS, BYOL Oracle | $0.8740 | $16.61 | $637.90 |

**RDS is 6.2×.** Over the sequence that is $22.08 — **62% of the entire $30 monthly
budget** — for managed hosting of synthetic data that is destroyed at the end of every
window.

RDS is not technically wrong: RDS Oracle supports LogMiner CDC via `rdsadmin` procedures
and RDS SQL Server supports `msdb.dbo.rds_cdc_enable_db`. **It is simply unaffordable
here, and the containers run the same engines with the same transaction-log semantics.**

Two further strikes: **RDS SQL Server cannot be stopped for more than 7 days** (AWS
restarts it), which leaks money under ADR-027's destroy-between-windows model; and RDS
create/delete latency is paid twice per window.

Evidence: `artifacts/validation/session-03/pricing.txt`. A root `check` block fails the
plan if `source_lab_instance_type` ever looks like a `db.*` class.

## 2. Why `t3a.xlarge`, and why not `t3.large`

`t3a` is AMD EPYC and **still x86_64** — which matters, because neither Oracle Database
Free nor SQL Server Developer publishes an `arm64` image. This host cannot follow the
Graviton choice made for MSK and EMR Serverless. It is **10.6% cheaper than `t3.xlarge`**
for identical behaviour.

`t3.large` was assumed by ADR-030's cost package and is **rejected**:

| | GiB |
|---|---:|
| Oracle Free | 2.0 (hard licence cap: 2 GB RAM, 2 CPU threads, 12 GB user data) |
| SQL Server Developer | 4.0 |
| OS + Docker | 1.5 |
| **Total** | **7.5** |
| `t3.large` | 8.0 — **0.5 GiB headroom, no page cache during a snapshot** |
| `t3a.xlarge` | 16.0 |

The failure mode at 8 GiB is an **OOM-kill mid-snapshot** (risk R1), which wastes the
whole metered window. A wasted 4-hour window costs $3.22; the instance delta across the
sequence costs $1.58. **Buying the headroom is cheaper than one failure.**

## 3. What gets built

| Resource | Count | Notes |
|---|---:|---|
| `aws_instance` | 1 | `t3a.xlarge`, encrypted gp3 50 GiB, IMDSv2, **no key pair** |
| `aws_security_group` | 1 | **zero ingress rules** until Session 04 exists |
| `aws_ssm_parameter` (SecureString) | 4 | Oracle admin/CDC, SQL Server admin/CDC |
| `aws_s3_object` | 13 | bootstrap assets — see §4 |

### The 16 KB problem

The compose file and SQL scripts total **~35 KB base64 — more than double EC2's 16 KB
user-data hard limit.** Inlining them fails at apply with an opaque
`InvalidParameterValue`.

They are staged in `s3://<lake>/bootstrap/source-lab/` and pulled by an `aws s3 sync` in
a 2.9 KB user-data script. The `source_lab` role gets a **`bootstrap_read` policy scoped
to `bootstrap/*`** — deliberately narrower than `lake_read`, because the source lab has
no business reading warehouse data.

`etag = filemd5(...)` plus `user_data_replace_on_change` means a changed SQL file
re-uploads *and* rebuilds the instance, so it can never drift from the repo.

## 4. Schema

`reference/KIMBALL_SAMPLE_MODEL.md`. Oracle holds core banking, SQL Server holds digital
channel — genuinely separate systems, which is the point.

**Oracle `corebank`** (PDB `FREEPDB1`): `branch`, `customer`, `account`, `transaction`
**SQL Server `digital`**: `channel`, `merchant`, `app_user`, `digital_event`

Two modelling choices worth flagging:

- `app_user.customer_id` is **not** a foreign key. It points into Oracle. Modelling it as
  an FK would hide that the cross-source join is the mart's problem to solve.
- Oracle amounts are `NUMBER(18,2)`, never `FLOAT`. Debezium maps scaled `NUMBER` to a
  Connect `Decimal`; a float would make every downstream sum non-reproducible.

**The seed is deterministic** — fixed id ranges, a fixed base timestamp, no randomness.
Reconciling source counts against L1/L2 is impossible otherwise. 200 customers, 320
accounts, 2 000 transactions, 150 app users, 3 000 events: enough to exercise ordering and
partitioning, nowhere near Oracle Free's 12 GB cap. **A CDC lab needs change volume, not
data volume.**

## 5. CDC enablement — and the failure it prevents

**Risk R15, in both engines: a connector against a CDC-disabled table starts *healthy* and
silently produces nothing.** Everything below exists to make that impossible.

### Oracle

1. `ARCHIVELOG` mode — **requires a database restart**. Without it LogMiner has no redo to
   mine. `db_recovery_file_dest_size = 8G` bounds the recovery area so the container
   cannot fill its volume and hang.
2. Supplemental logging: `MIN` at database level, **`ALL COLUMNS` per captured table**.
   Without it, UPDATE events carry only changed columns, so the `before` image is
   incomplete and **L2 history is silently wrong**.
3. `c##dbzuser` — a least-privilege common user with `LOGMINING`, not `SYS`.

### SQL Server

1. **SQL Server Agent must be running.** CDC capture and cleanup *are* Agent jobs.
   `sp_cdc_enable_table` **succeeds with Agent stopped** and captures nothing — so
   `02-enable-cdc.sql` refuses to proceed unless `sys.dm_server_services` says Running.
2. `@supports_net_changes = 0`, deliberately. Net changes **collapse multiple updates
   within a capture interval into one row**, destroying exactly the I/U/D history L2
   exists to preserve (`CLAUDE.md` §5.3).
3. Cleanup retention **10 080 minutes (7 days)**, not the 3-day default. Kafka retention
   is 24 h (Gap 13), so CDC retention must be *longer* or a Kafka outage becomes
   unrecoverable without a full re-snapshot.
4. `dbzuser` with `db_datareader` + `cdc_reader`, not `sa`.

Verification lives in `99-verify-cdc.sql` for each engine and in `healthcheck.sh`, which
**exits non-zero unless both engines are up AND CDC is genuinely enabled**.

## 6. Workload scenarios

`bash scripts/source-lab.sh workload <n> --execute` runs the same scenario on both
engines.

| # | Scenario | What it proves |
|---|---|---|
| 1 | Steady I/U/D | Baseline; before/after images; a customer changing branch (SCD2 input) |
| 2 | **Burst — 500 rows, one commit** | All rows share a commit SCN/LSN. This is where ordering by commit alone is ambiguous and `CLAUDE.md` §5.5's tie-breakers decide. Wrong L2 ordering shows up here first |
| 3 | **Late-arriving reference** | Child inserted, parent 5 s later, FK disabled so it can genuinely happen. Session 10 must resolve to unknown SK `-1` and back-fill |
| 4 | Deletes | `d` envelope **and** tombstone. `CLAUDE.md` §5.7 — no ambiguous skipping |
| 5 | **Compatible DDL** | Additive nullable column. **Oracle: supplemental logging must be re-applied — `ADD COLUMN` does not inherit it. SQL Server: the existing capture instance keeps the OLD column list until a second one is created** |
| 6 | **INCOMPATIBLE DDL** | Narrowing a column. The registry **should reject** it. If the pipeline accepts it silently, the compatibility setting is wrong — which is what this scenario exists to prove |

Scenario 5's two footnotes are the ones that bite in production: both engines let a schema
change through in a way that looks fine and quietly stops capturing the new column.

## 7. Running it

```bash
# after apply, in order
bash scripts/source-lab.sh status                    # read-only
bash scripts/source-lab.sh enable-cdc --execute      # Oracle RESTARTS — several minutes
bash scripts/source-lab.sh seed --execute
bash scripts/source-lab.sh verify-cdc                # MUST pass before any connector
bash scripts/source-lab.sh workload 1 --execute
```

Every mutating command is dry-run by default and requires `--execute` plus a typed
confirmation phrase. Access is SSM only — **there is no SSH and no key pair anywhere**.

## 8. Security

| Control | How |
|---|---|
| No `0.0.0.0/0` inbound | The SG has **zero ingress rules**. Ports 1521/1433 open only via SG-to-SG rules that do not exist until Session 04 supplies a CDC runtime SG |
| No SSH | No `key_name`. SSM Session Manager and port-forwarding only |
| No plaintext credentials | Generated at apply into **SSM SecureString** (ADR-031), fetched at boot. The rendered user-data is 2.9 KB and contains **no password** — only an S3 URI |
| Least privilege | Debezium uses `c##dbzuser` / `dbzuser`, never `SYS` or `sa`. The instance role reads `bootstrap/*` only, not the warehouse |
| Encryption | gp3 encrypted; staged objects SSE-KMS on the lake CMK |
| IMDSv2 | `http_tokens = "required"` |
| No real data | Synthetic only — removes an entire class of risk |

**Licensing, stated plainly:** Oracle Database Free is production-usable within its
limits. **SQL Server Developer Edition is non-production only.** This lab is inside that
boundary and must never be presented as a production deployment.

## 9. Cost and destroy

**$0.1888/hr while running. $0.00 when destroyed** — the gp3 root has
`delete_on_termination = true`, so nothing survives.

```bash
bash scripts/source-lab.sh destroy --execute
# then verify — a destroy that reports success is not evidence
aws ec2 describe-instances --profile my-aws-profile --region ap-southeast-1 \
  --filters Name=tag:Component,Values=source-lab \
  --query 'Reservations[].Instances[?State.Name!=`terminated`].InstanceId'   # []
aws ec2 describe-volumes --profile my-aws-profile --region ap-southeast-1 \
  --query 'Volumes[?State==`available`].VolumeId'                            # []
```

The second command matters: **a detached gp3 volume keeps billing (~$4.80/month for
50 GiB) and never appears in an instance listing.**

The four SSM SecureString parameters survive a source-lab destroy and cost **$0** —
standard-tier parameters are free (ADR-031). They are regenerated on the next apply.
