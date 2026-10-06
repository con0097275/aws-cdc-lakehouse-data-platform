# ADR-036 — Reporting runtime state store

- Status: **ACCEPTED** (Session 22) — **requires operator sign-off before `terraform apply`**
- Related: ADR-034, ADR-035, `CLAUDE.md` §4

## Context

Repo 1's runtime tables live in **Impala/Kudu** — row-level upserts at millisecond
latency (`data_lake_init.sql`, `functions.py:779 update_execution_hist`). Kudu does not
exist on this platform. The two candidates here are Iceberg-on-S3 through Glue, and
DynamoDB.

The access pattern is the deciding factor, and it is not one pattern but two:

| Access | Shape | Frequency |
|---|---|---|
| Status transitions (`PLANNED → READY → SUBMITTED → RUNNING → VALIDATING → SUCCEEDED`) | single-row update | 6+ per job per run |
| Dependency gate polling | single-row read | every poke interval per waiting task |
| Watermark read/advance | single-row read, conditional write | 2 per run |
| "Why did day T change" | scan + join with lake data | ad hoc, rare |
| Config/run/lineage joins for a report | scan | ad hoc, rare |

The first three are OLTP. The last two are analytics. One store serving both serves
neither well.

Iceberg can do the OLTP part — Athena engine v3 supports `UPDATE`/`MERGE` on Iceberg
tables — but every statement creates a snapshot and a metadata file, so a run of ten marts
produces hundreds of snapshots a day on tiny tables and requires its own `expire_snapshots`
schedule. Reads are Athena queries: billed per statement and seconds of latency inside a
gate loop that runs every poke interval.

Repo 5 (the only AWS reference) chose DynamoDB for exactly these tables:
`aws_dynamodb_table "etl_execution"` with a `status-index` GSI, and `"dbt-job"`, both
`PAY_PER_REQUEST` (`IaC/modules/data_platform/ochestration.tf:184`, `:213`).

## Options

| Option | Verdict |
|---|---|
| **Split by access pattern: DynamoDB hot, Iceberg audit** | **CHOSEN** |
| Everything in Iceberg `ops.*` | Rejected |
| Everything in DynamoDB | Rejected |

Everything-in-Iceberg is *viable at lab scale* — the query cost is cents and the snapshot
count is manageable. It is rejected on latency inside the gate loop and on the standing
maintenance burden of expiring snapshots on eight small tables, not on cost.
Everything-in-DynamoDB loses the ability to join a run against the lake data it produced,
which is the one thing the audit tables exist for.

## Decision

| Table | Store | Key | Notes |
|---|---|---|---|
| `job_master`, `job_flow_config`, `job_dependency`, `resource_profile` | Iceberg `ops.*` | `config_version` partition | read-only projection of Git (ADR-034) |
| `job_master_execution_hist` **(live)** | DynamoDB | PK `execution_id`; GSI `job_flow_date`, GSI `status` | TTL 30 days |
| `job_master_execution_hist` **(audit)** | Iceberg `ops.job_master_execution_hist` | partitioned by `date_of_data` | **one write per completed execution**, append-only |
| `job_watermark_state` | DynamoDB | PK `job_id#flow_mode` | conditional writes only |
| `summary_config_v1` | DynamoDB | PK `job_id#flow_mode`, SK `coordinator_run_id` | the live resolved plan |
| `summary_config_hist_v1` | Iceberg `ops.summary_config_hist_v1` | partitioned by `execution_date` | append-only planning history |
| `streaming_app_state` | DynamoDB | PK `job_id`, SK `deployment_id` | ADR-041 |

The DynamoDB row is the working copy; the Iceberg row is the record. A terminal status
triggers exactly one Iceberg append. Nothing else writes Iceberg from the runtime path.

Four tables, all `PAY_PER_REQUEST`, all SSE with the lake CMK, PITR off, TTL on the two
that accumulate. Idle cost is **$0.00** — which preserves the property
`PROJECT_STATE.md` records for the current foundation (~$1.01/month, nothing hourly).

## Consequences

- A new AWS service class enters the account: new IAM surface, a new destroy path in
  `scripts/verify-destroy.sh`, a new line in `docs/COST.md`.
- Athena cannot read the DynamoDB tables. The query surface for history is the Iceberg
  side; the live tables are read through the framework's client. This is stated so nobody
  writes a report against a table Athena cannot see.
- `ops.job_master_execution_hist` is written once per execution, so it stays compactable
  by the existing `spark/eod/maintenance.py` path.

## Cost

At lab volume (≤10 marts, metered windows a few hours per month): well under $0.01/month
of request charges, $0 storage, **$0 idle**. On-demand billing means an unused window
costs nothing, matching ADR-027.

## Security

- One IAM role (`reporting`) with `dynamodb:GetItem/PutItem/UpdateItem/Query` scoped to
  the four table ARNs — no `Scan`, no `*`.
- SSE with the existing lake CMK, so the same key policy governs it.
- No secret is stored in any of these tables; `error_msg` is truncated and scrubbed before
  write.

## Rollback

The four tables are `enable_reporting_framework`-gated and independently destroyable. The
Iceberg audit tables survive their destruction, so history is not lost by turning the hot
store off. Reverting to Iceberg-only means pointing `ops_client.py` at Athena DML — a
single module, which is why it is behind one client class.

## Validation

- `terraform plan` shows four tables, `PAY_PER_REQUEST`, no PITR, tagged per ADR-023.
- `scripts/verify-destroy.sh` asserts they are gone after destroy.
- A test asserts `ops_client` issues exactly one Iceberg append per terminal execution.
- A test asserts the watermark write is conditional and fails on a stale
  `last_success_execution_id`.
- A test asserts no IAM policy grants `dynamodb:Scan` or a wildcard table ARN.
