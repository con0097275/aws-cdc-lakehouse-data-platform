# FinOps review — AWS CDC Lakehouse

- Session: 17
- Date: 2026-08-15
- Account: `111122223333` · Region: `ap-southeast-1` · Profile: `my-aws-profile`
- **Audit performed live against the account**, not derived from Terraform

---

## 1. Current cost drivers

### This project: **$0.00/month**

Sessions 02–16 have never been applied. Zero resources carry `Project=kafka-dev-lab`.

### The account it lives in: **~$0.35/month**

The audit swept the whole account, not just this project, because a bill does not
distinguish. What is actually there:

| Category | Found | Billing |
|---|---|---|
| MSK, EC2, EMR Serverless, EKS, RDS, Redshift | **0** | — |
| EBS volumes / snapshots / Elastic IPs | **0** | — |
| NAT gateways, VPC endpoints | **0** | — |
| **S3 buckets** | **44** (13.67 GB) | **~$0.34/mo** |
| **KMS keys** | 5 — **all AWS-managed** | **$0.00** |
| CloudWatch log groups | 3 (~1.5 MB) | negligible |
| Glue databases | 1 (the OPEN-20 orphan) | $0 under 1M objects |
| Athena workgroups | 1 (`primary` only) | $0 idle |

**Two things I initially got wrong and corrected by checking:**

1. **The 5 KMS keys are AWS-managed (`alias/aws/*`), which are free.** Only
   customer-managed keys bill ~$1/month. I flagged $5/month before verifying `KeyManager` —
   it was wrong.
2. **The 44 buckets are not this project's.** They are prior work — YouTube analytics,
   Redfin, Snowflake tests, demos. The CDC lakehouse bucket (`vannk-dev-lake-*`) does not
   exist. Largest: `store-raw-data-yml-operator` 5.6 GB, `vannk-dev-oracle-fusion-erp`
   3.6 GB.

### The one finding worth acting on

**All 3 CloudWatch log groups have `retentionInDays = null` — they never expire.** They are
tiny today (~1.5 MB) and belong to other projects (`/aws-glue/crawlers`,
`/aws/codebuild/airflow-*`), but unbounded retention is a slow leak. One command:

```bash
for lg in /aws-glue/crawlers /aws/codebuild/airflow-dev-pipeline /aws/codebuild/airflow-staging-pipeline; do
  aws logs put-retention-policy --log-group-name "$lg" --retention-in-days 30 \
    --profile my-aws-profile --region ap-southeast-1
done
```

Not applied automatically — they are outside this project's scope, and changing another
project's retention is the owner's call.

## 2. Expected cost while actively testing

Rates from `docs/PRICE_REFERENCE.md` (collected 2026-08-12 — **re-collect before any real
spend decision**). Quantities are ours; prices are AWS's and change.

| Component | Driver | While testing |
|---|---|---|
| MSK `kafka.t3.small` × 3 | per broker-hour | dominant — see below |
| Source lab `t3a.xlarge` | per hour | metered window only |
| CDC runtime `t3.large` | per hour | metered window only |
| Airflow k3s `t3.large` | per hour | $0.1056/hr |
| EMR Serverless | vCPU-hour + GB-hour, **auto-stops** | per job, seconds |
| Athena | $5.00/TB scanned, **10 GiB/query cap** | ~$0.05/query ceiling |
| S3 (lake) | per GB-month | pennies at demo volume |
| EBS 30 GiB gp3 × N hosts | per GiB-month, **running or stopped** | $2.88/host/month |

**A 4-hour demo window, 20 days/month** (the documented shape): Airflow alone is $11.33/mo
including its EBS — 37.8% of the $30 budget (ADR-030). Add MSK and the source lab and the
window is what keeps this inside budget, not the instance sizes.

**Left running 24/7, Airflow alone is $79.97/mo — 267% of the entire budget.** That single
number is why stopping is scripted rather than remembered.

**No fixed total is promised here.** `docs/COST.md` is a worksheet — hours × rate,
GB × rate, TB scanned × rate. A hardcoded monthly figure would be stale within a quarter and
wrong in a way nobody would notice.

## 3. Expected idle cost

"Idle" has two meanings and only one of them is cheap:

| State | What bills | Roughly |
|---|---|---|
| **Stopped** (instances stopped, MSK kept) | EBS + MSK + S3 + EIPs | **MSK dominates — it does not stop** |
| **Stopped, MSK destroyed** | EBS + S3 | ~$2.88/host + storage |
| **Destroyed** (lake retained) | S3 + KMS CMKs | ~$0.35/mo at current volume |
| **Destroyed, lake deleted** | nothing | $0.00 |
| **Today** | 44 unrelated buckets | **~$0.35/mo** |

**Stop is cheaper, not free.** A stopped instance bills no compute, but its 30 GiB gp3
volume is billed per provisioned GiB-month regardless. Only *terminate* removes it. That is
the most common lab-cost surprise and it is why `show-cost-resources.sh` separates
"stopped-but-billing" into its own section.

**MSK is the exception that matters.** There is no "stop" for a managed MSK cluster — you
keep it or you delete it. Deleting loses in-flight CDC events (24h retention), so between
sessions the choice is: pay for MSK, or accept a Debezium re-snapshot on return.

## 4. Exact commands to stop expensive compute

```bash
cd /path/to/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1

# See what would stop, and what will keep billing afterwards
bash scripts/stop-ephemeral.sh

# Do it
bash scripts/stop-ephemeral.sh --execute
```

Individually, if you want finer control:

```bash
# Airflow k3s only
bash scripts/airflow-node.sh stop --execute

# Source lab (Oracle + SQL Server containers)
bash scripts/source-lab.sh stop --execute

# Any running EC2, by id
aws ec2 stop-instances --instance-ids <id> --profile my-aws-profile --region ap-southeast-1

# EMR Serverless application (jobs auto-stop; the application may sit STARTED)
aws emr-serverless stop-application --application-id <id> \
  --profile my-aws-profile --region ap-southeast-1
```

**Not included, on purpose:** any MSK action. Stopping MSK means deleting it, which is a
destroy decision with a 24h data-loss consequence — not part of a daily shutdown.

## 5. Exact commands to verify things stopped

```bash
# Gate-able: exits non-zero if anything is still running
bash scripts/verify-destroy.sh --stopped

# Full inventory, all three billing categories
bash scripts/show-cost-resources.sh

# Just what bills while you are NOT testing
bash scripts/show-cost-resources.sh --idle
```

Raw, if you prefer to see the API answer yourself:

```bash
aws ec2 describe-instances --filters Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name]' \
  --output table --profile my-aws-profile --region ap-southeast-1

aws emr-serverless list-applications --query 'applications[?state==`STARTED`]' \
  --profile my-aws-profile --region ap-southeast-1

aws kafka list-clusters-v2 --query 'ClusterInfoList[].[ClusterName,State]' \
  --profile my-aws-profile --region ap-southeast-1
```

**Verify against the service API, not `terraform state`.** Terraform reporting success means
it removed what was in its state — not that the resource is gone, not that asynchronous
deletion finished, and not that anything created outside Terraform was touched. Only the API
answer appears on the bill.

## 6. Safe destroy procedure (for later — NOT run)

**Nothing was destroyed in this session.** This is the documented path.

### Order matters — dependencies destroy inward

```bash
cd terraform/envs/dev
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1

aws sts get-caller-identity     # 1. ALWAYS confirm account, ARN, region first

# 2. Optional / leaf modules — safe individually, no dependents
terraform destroy -target=module.airflow_k3s
terraform destroy -target=module.cdc_runtime
terraform destroy -target=module.source_lab
terraform destroy -target=module.emr_serverless

# 3. Catalog and query layer
terraform destroy -target=module.athena
terraform destroy -target=module.glue_catalog

# 4. Platform — MSK, VPC, endpoints. Slowest; MSK deletion takes minutes.
terraform destroy -target=module.kafka_platform

# 5. Everything else
terraform destroy

# 6. VERIFY against the APIs, not the state file
bash ../../../scripts/verify-destroy.sh --destroyed
```

Or, without Terraform at all — flip the flags and apply:

```bash
terraform apply -var="enable_airflow=false" -var="enable_cdc_runtime=false" \
                -var="enable_source_lab=false" -var="enable_emr_serverless=false"
```

Every module has `enable_*` defaulting to `false` (CLAUDE.md §4.12), so this is a supported
path rather than a trick.

### What survives destroy — deliberately

| Retained | Why | To remove |
|---|---|---|
| **S3 lake bucket** | holds the data; `force_destroy=false` | empty it, then delete — a separate deliberate act |
| **S3 state backend** | holds Terraform state | destroying it **strands every other resource**; never in a destroy path |
| **KMS keys** | 7–30 day waiting period | `schedule-key-deletion`; they still list until the window elapses |
| **Glue `vannk-dev-oracle-db`** | 39 tables, unknown provenance (OPEN-20) | owner decision — see below |
| **CloudWatch Logs** | ingestion already paid | delete the log group |

**Confirmation is required.** The destroy scripts print `sts get-caller-identity` and demand
a typed phrase — not a y/n prompt, which gets answered reflexively. `confirm_destructive()`
also refuses outright when stdin is not a terminal, so no automation can satisfy it.

## 7. Daily lab shutdown procedure

### Before a session

```bash
bash scripts/show-cost-resources.sh            # know your starting point
terraform apply -var="enable_airflow=true" ...  # only the flags you need
```

### After a session — **the two commands that matter**

```bash
bash scripts/stop-ephemeral.sh --execute
bash scripts/verify-destroy.sh --stopped
```

The second is not optional. `stop-instances` returns immediately and the instance takes
~30s to actually stop; a shutdown that was never verified is a shutdown that sometimes
did not happen.

### Weekly

```bash
bash scripts/show-cost-resources.sh --idle     # catch orphaned EBS and unattached EIPs
aws budgets describe-budgets --account-id 111122223333 --profile my-aws-profile
```

Orphaned volumes and unattached Elastic IPs are what a stop-based workflow accumulates —
each individually cheap, none of it visible from "is anything running".

### If leaving the lab for more than a few days

Destroy MSK. It cannot be stopped, and it is the largest single line item. The cost of
returning is a Debezium re-snapshot, which the pipeline already supports (DR §2, scenario 11).

## 8. Guardrail verification

| Guardrail | Status |
|---|---|
| **AWS Budget** | **PRESENT** — "My Monthly Cost Budget" $30 + "My Zero-Spend Budget" $1 |
| Budget module in Terraform | `modules/budget_guardrails` (unapplied) |
| Resource tags | `Project`/`Environment`/`ManagedBy`/`Owner`/`CostCenter`/`AutoDestroyAfter` via provider `default_tags` |
| S3 lifecycle | athena-results 7d, logs, dq-results, quarantine, checkpoints — **no rule on `warehouse/`** (Iceberg manages its own) |
| CloudWatch retention | **GAP** — 3 existing log groups have none (§1) |
| EMR auto-stop | configured (`auto_stop_idle_minutes`) |
| Athena cutoff | 10 GiB/query, `enforce_workgroup_configuration=true` |
| Feature flags | every module `enable_*`, default false |
| Start/stop scripts | `airflow-node.sh`, `source-lab.sh`, `cdc-runtime.sh`, `stop-ephemeral.sh` |
| Cleanup scripts | `verify-destroy.sh`, per-module targeted destroy |
| **No NAT / EKS / RDS** | **verified 0 of each** |
| Redshift / Trino | `false`, and **0 workgroups exist** |

## 9. Optional infrastructure disables cleanly — verified

`terraform plan` with `enable_redshift_serverless=false`, `enable_trino=false`:

- **0** engine resources — no `aws_redshiftserverless_namespace`, no workgroup, no Trino
  cluster.
- **7** IAM objects named for them (roles, instance profile, attachments) which are **free**
  and exist so the engines can be enabled later without an IAM change.

"Zero Redshift/Trino resources" would be imprecise. **"Zero billable Redshift/Trino
resources"** is accurate, and the distinction is the kind that matters when reading a plan.
