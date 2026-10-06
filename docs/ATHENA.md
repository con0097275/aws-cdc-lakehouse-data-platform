# Athena — the core query and serving layer

- Session: 13
- Date: 2026-08-15
- Status: **workgroup, IAM and queries static-validated and PLANNED; nothing deployed**
- Decisions: ADR-012 (Athena is core, not optional)

```
S3 Iceberg  →  Glue Data Catalog  →  Athena  →  Power BI
```

---

## 1. Why Athena is the default, and the others are not

`CLAUDE.md` §8: Athena is **core**; Redshift Serverless and Trino are feature flags,
**both `false` in this session and not deployed**.

The reason is the billing model, not the feature set. Athena bills **per byte scanned** with
no idle cost — a lab that queries for an hour a day pays for an hour a day. Redshift
Serverless bills per RPU-hour once a namespace exists, and Trino needs a cluster that is
either running or not answering. For a $30/month budget (ADR-030), only one of these has a
floor of $0.

**Athena data scanned: $5.00/TB** in `ap-southeast-1` (`docs/PRICE_REFERENCE.md`).

## 2. Workgroup controls

Already built in Session 02 (`terraform/modules/athena`); this session verified and tested
them rather than rebuilding.

| Control | Value | Why |
|---|---|---|
| `bytes_scanned_cutoff_per_query` | **10 GiB** | ≈ $0.05 ceiling per query. Without it a single `SELECT *` on `full_cdc` can scan the whole lake |
| `enforce_workgroup_configuration` | **true** | **The setting that makes the rest real.** Without it a client overrides the workgroup and the cutoff, result location and encryption all become advisory |
| Result encryption | **SSE-KMS** | Query results are derived data and can contain anything the query selected |
| `expected_bucket_owner` | account id | Matches the lake's BucketOwnerEnforced setting |
| Results lifecycle | **7 days** | Every dashboard refresh writes a new result set; without expiry they accumulate as billed storage forever |
| Engine version | pinned | `CLAUDE.md` §3.9 — no `latest` |

The cutoff without enforcement is the trap: it looks configured in the console and does
nothing. `test_workgroup_configuration_is_enforced` pins it.

## 3. The BI role could read L1 and L2 — defect D13-1

The `athena_bi` role was attached to `lake_read`, which grants:

```hcl
actions   = ["s3:GetObject", "s3:GetObjectVersion"]
resources = ["${var.lake_bucket_arn}/*"]        # the WHOLE bucket
```

The attachment carried this comment:

> `# Athena BI: read-only. CLAUDE.md §8 — Power BI reads marts, never L1/L2.`

The comment described a rule the policy did not implement. Power BI's role could read raw
CDC — every `before`/`after` payload in L1 and L2, unmasked — and nothing failed, because
nothing was checking.

### The fix: a `mart_read` policy with two layers

**1. A narrow Allow** on `warehouse/{snapshot,curated,mart,ops}/` and `athena-results/`
only — including a **scoped `s3:prefix` condition on ListBucket**, because even a listing
leaks table names, partition dates and volumes.

**2. An explicit Deny** on `warehouse/stream/*`, `warehouse/full_cdc/*`,
`warehouse/quarantine/*` and `checkpoints/*`.

The Deny is not redundant. An explicit Deny **cannot be overridden by any later Allow**, in
this policy or any other attached to the same principal. Without it the guarantee depends on
nobody ever widening the Allow or attaching `lake_read` alongside — which is not a
guarantee, it is a hope.

**The Glue catalog is denied too.** With only S3 denied, a BI principal can still read L1/L2
*schemas* — column names and partition keys — which leaks structure and volume even when the
objects are unreadable.

The serving and raw layer lists are named **once** (`local.serving_layers`,
`local.raw_cdc_layers`) so the Allow and the Deny cannot drift apart.

Rendered policy: `artifacts/validation/session-13/mart-read-policy.json`.

### Quarantine is denied on purpose

`ops.quarantine` holds poison records — **raw payloads**. It is as much a PII path as an
operational one, so BI is denied it even though `ops` is otherwise a serving namespace.

## 4. Serving views

Two independent controls, because either alone is insufficient:

1. **IAM** — `athena_bi` is denied L1/L2 (above).
2. **Views** — BI is pointed at these, which project away PII and unfinished rows.

**A view is not a security boundary on its own.** If the caller can read the base table it
can bypass the view; the IAM deny is what makes it a boundary, and the view is what makes
the *right* data convenient.

| View | Purpose |
|---|---|
| `v_customer_360_certified` | `processing_status = 'CERTIFIED'` — the closing number for T-1 and earlier |
| `v_customer_360_provisional` | day T only, carries `last_updated_at` for the badge |
| `v_dim_customer_bi` | PII **tokenised** (`sha256`), `age_band` instead of `dob` |
| `v_account_balance_daily` | natural grain, **not** pre-aggregated |
| `v_channel_performance_daily` | joined to the channel dimension |

Certified and provisional are **separate views**, deliberately. One view behind a status
column is one careless filter away from a report that mixes them and presents the total as
final.

`v_account_balance_daily` deliberately does **not** roll up: `closing_balance` is
semi-additive (S10-8), and a view that pre-summed it across dates would bake the wrong
answer in where no downstream query could see or fix it.

## 5. Scan cost: partitions, then columns

Athena bills bytes scanned, so this is the cost section, not a performance section.

### Partition pruning must be proven, not assumed

`serving/athena/queries/02_partition_pruning.sql` ships a **negative case** for each rule,
because a single fast query proves nothing:

```sql
-- prunes: partition column bare on the left
WHERE business_date = DATE '2026-08-14'

-- does NOT prune: the cast defeats it
WHERE CAST(business_date AS VARCHAR) = '2026-08-14'

-- does NOT prune: a function on the partition column, and it looks completely normal
WHERE year(business_date) = 2026 AND month(business_date) = 8
```

All three return the correct answer. Two of them scan the whole table. **This is the most
common way a dashboard quietly starts costing money** — the results stay right, so nothing
prompts anyone to look.

The assertion is the **ratio** between the pruned and unpruned scans, not an absolute
number. If they scan the same bytes, the partition predicate is decorative.

### Column projection

Parquet is columnar, so scan cost is driven by columns as well as rows. `SELECT *` and
`SELECT transaction_id` return the same rows and bill very differently. The shipped queries
project explicitly — they are what a BI developer copies.

### File layout

`04_iceberg_metadata.sql` exposes `$files`, `$partitions`, `$snapshots` and `$manifests`.
**File layout cannot be tuned if it cannot be measured**: when average file size falls well
under the 128 MB target, footer and row-group overhead dominate and bytes-scanned climbs.
This is the input to deciding whether Session 12's maintenance DAG is keeping up.

### Benchmark harness

```bash
scripts/athena-benchmark.sh                 # dry-run
scripts/athena-benchmark.sh --execute       # runs, and BILLS
```

Records `DataScannedInBytes` as the **primary** metric and latency as secondary — the
reverse of a typical harness, deliberately. A query taking 8s and scanning 200 GB costs far
more than one taking 40s and scanning 200 MB, and only one of those appears on the invoice.

It refuses to run if the workgroup does not enforce its configuration or has no cutoff.

## 6. Query suite

| File | Purpose |
|---|---|
| `01_layer_smoke.sql` | visibility across L1, L2, L3, dims, facts, marts |
| `02_partition_pruning.sql` | pruning and projection, **with counter-cases** |
| `03_join_aggregate.sql` | star joins, semi-additive aggregate, certified/provisional |
| `04_iceberg_metadata.sql` | file layout, snapshots, manifests, **time travel** |
| `05_reconciliation.sql` | L1→L2 exact, duplicate `event_id`, L3 uniqueness, L3→mart, SCD2 overlap, RI |
| `06_bi_role_security.sql` | queries that **must fail** as `athena_bi` |

**`01` and `05` run as the governance role, not `athena_bi`** — they read L1/L2, which the BI
role is denied. That is the design, not an inconsistency: reconciliation is an operator
activity, and the role that can do it is deliberately not the role Power BI uses.

`06` is written so that **success is the failure**. The usual mistake is testing that the BI
user *can* read the mart, which proves nothing about what it cannot do.

## 7. Cost summary

| Item | Cost |
|---|---|
| Athena workgroup | **$0.00** — no idle cost |
| Data scanned | **$5.00/TB**; capped at 10 GiB/query ≈ **$0.05** |
| Query results storage | 7-day lifecycle; pennies |
| IAM policy | $0.00 |
| **Session 13 incremental** | **$0.00 until a query runs** |

A day of exploration at the cutoff ceiling — 20 queries × 10 GiB — is ~$1.00. A Power BI
**Import** refresh over one partition is a few MB. A **DirectQuery** dashboard with 10
visuals refreshing every 15 minutes is ~960 queries/day, which is why §8 makes DirectQuery a
gated decision rather than a toggle.

## 8. No Redshift, no Trino — precisely

The plan creates **zero engine resources**: no `aws_redshiftserverless_namespace`, no
workgroup, no Trino cluster, no ECS/EMR for either.

It **does** create 7 IAM objects named for them (roles, instance profile, policy
attachments) — those pre-date this session, come from `modules/lake_iam`, are **free**, and
exist so the engines can be enabled later without an IAM change. Saying "zero Redshift/Trino
resources" would be imprecise; saying "zero billable Redshift/Trino resources" is accurate.

### When to reconsider (13B / 13C triggers)

| Trigger | Consider |
|---|---|
| Athena p95 > 30s on a dashboard page after pruning is confirmed | 13B Redshift Serverless |
| Sustained concurrency > 20 dashboard users | 13B |
| Monthly scan spend approaches the compute cost of a small warehouse | 13B |
| A genuine federation need — joining the lake to an external RDBMS | 13C Trino |
| Import refresh window exceeds the load window | 13B, or fix partitioning first |

**Fix pruning before reaching for a bigger engine.** An unpruned query is not an Athena
limitation, and it will cost more on Redshift too.

## 9. Rollback

The workgroup holds no data (`force_destroy = true`; results live in the lake under their
own lifecycle).

```bash
terraform apply -var="enable_athena=false"      # removes the workgroup
# the mart_read policy is removed with modules/lake_iam
```

Reverting `athena_bi` to `lake_read` would restore defect D13-1 —
`test_athena_bi_is_not_attached_to_lake_read` fails if anyone does.
