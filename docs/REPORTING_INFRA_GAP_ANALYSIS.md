# Phase 12 — reporting infrastructure gap analysis

- Session: 31, 2026-08-20
- Status: **PLANNED, NOT APPLIED.** No AWS mutation. Two saved plans, both discarded after
  analysis.
- Related: ADR-036 (runtime state store), ADR-021/030 (backend), ADR-022 (egress),
  ADR-027 (ephemeral), ADR-041/045 (streaming)

## 1. AWS context, verified

| | |
|---|---|
| Identity | `arn:aws:iam::111122223333:user/my-aws-profile` |
| Account | `111122223333` — matches the `EXPECT_ACCOUNT` guard |
| Profile | `my-aws-profile` — pinned in `providers.tf`, not the default chain |
| Region | `ap-southeast-1` |
| Environment | `dev`, workspace `default` |
| Backend | `s3://aws-cdc-lakehouse-tfstate-111122223333/aws-cdc-lakehouse/dev/terraform.tfstate`, versioned, SSE-S3 (ADR-030), serial 14 |
| Budget | `$30/month`, tag-filtered on `Project=kafka-dev-lab`, email set |
| Tags | all six supplied by `default_tags`; **`AutoDestroyAfter` is `2026-08-16T18:00:00Z` — four days in the past.** ADR-027 requires a real future timestamp for the window being opened. It must be reset before any apply, or every resource is created already-expired to any cleanup sweep |

## 2. The `enable_kafka_platform` discrepancy — resolved

**The flag was honoured. The documents were stale.**

`terraform.tfvars:86` says `enable_kafka_platform = true`, and it was true at apply time:

```
CloudTrail CreateCluster   2026-08-16T09:21:31Z   user my-aws-profile
CloudTrail DeleteCluster   2026-08-16T10:58:14Z   user my-aws-profile
state object 587,338 B at 09:44   ->   16,226 B at 11:03
```

The full platform — MSK included — was applied on 2026-08-16 and destroyed the same
morning, about 1h37m later. The state file shrinking from 587 KB to 16 KB across that window
*is* the destroy.

`DECISION_LOG` O22-2 ("tfvars true, applied tier ran false") and `PROJECT_STATE` ("MSK never
created") describe the **Session 20 cheap tier of 2026-08-15**, which really did run with the
flag false. They were correct when written and were never updated after the 08-16 cycle. Both
are corrected in this session.

**There is no drift.** State and reality agree: 3 resources in state, and in AWS the two
buckets plus two KMS keys that survive a destroy by design.

Evidence: `artifacts/validation/session-31/kafka-platform-discrepancy.txt`.

### Orphans — in AWS, not in Terraform state

| Resource | State | Cost | Action |
|---|---|---|---|
| `emr-serverless/applications/00g81hc564122m25` | TERMINATED | $0.00 | none — a terminated application stays listed forever and bills nothing |
| KMS CMK `c449919b…` (lake) | PendingDeletion → 2026-08-23 | $1/mo until then | none — cancelling would not help; a plan creates a new key |
| KMS CMK `a3f3f553…` (MSK) | PendingDeletion → 2026-08-23 | $1/mo until then | none — let it expire |

`scripts/verify-destroy.sh --destroyed` currently reports **NOT CLEAN** on
`Project-tagged = 5`. All five are the rows above plus the two buckets. That is the check
being literal, not a leak; it is left strict rather than loosened.

## 3. Minimum required infrastructure

Checked against what already exists. Nothing below duplicates a current resource.

| Need | Status | Action |
|---|---|---|
| Glue `ops` database | **already configured** (`glue_databases.ops` → `kafka_dev_lab_dev_ops`), matches `reporting/layers.yaml` | none |
| Glue layer databases | already configured, all 7 | none |
| S3 lake bucket + policy/encryption/versioning | bucket **live**; its sub-resources are not in state and would be re-created | none new |
| S3 `checkpoints/` prefix + 14-day lifecycle | **already configured**, top-level sibling of `warehouse/` | none |
| S3 `ops/config/`, `artifacts/dbt/`, `checkpoints/reporting/` | **missing** | **added** to `lake_prefixes` (3 markers) |
| DynamoDB runtime state | **missing** | **added** — `modules/reporting_ops`, 4 tables |
| Reporting IAM | **missing**, and `lake_iam` cannot supply it | **added** — 1 role, 2 policies |
| EMR Serverless application | module exists, **unreachable** — see §5 | **not changed**; needs a decision |
| Airflow | module exists, `enable_airflow=false`, needs EMR + MSK | **not changed** |
| Logging / metrics | `logs/emr-serverless/`, `logs/airflow/` prefixes exist; alarms are in `budget_guardrails` and `kafka_platform` | none |

## 4. ADR-036 — the DynamoDB design, resolved

Four tables. `modules/reporting_ops/main.tf`.

| Table | Hash key | Range key | GSI | TTL |
|---|---|---|---|---|
| `kafka-dev-lab-dev-job-execution` | `execution_id` | — | `job_flow_index` on `job_flow`, projection ALL | `ttl`, 30 days |
| `kafka-dev-lab-dev-job-watermark-state` | `watermark_key` | — | — | none |
| `kafka-dev-lab-dev-summary-config` | `summary_key` | — | — | none |
| `kafka-dev-lab-dev-streaming-app-state` | `job_id` | `deployment_id` | — | none |

| Property | Decision |
|---|---|
| Billing | `PAY_PER_REQUEST` on all four — idle cost $0.00, which is what preserves ADR-027's "an unused window costs nothing" |
| Encryption | SSE with the **lake CMK**, so one key policy governs the state and the data it describes |
| PITR | **off** on all four. These are the working copy; the record is the Iceberg audit table in a versioned bucket. Accepted as `CKV_AWS_28` ×4 in `docs/SECURITY_SCAN_BASELINE.md` |
| TTL | on the execution table only, at 30 days, matching `EXECUTION_TTL_DAYS` in `dynamodb_state.py`. A watermark has no TTL — expiring current position silently resets a job to a cold start |
| IAM | `GetItem/PutItem/UpdateItem/Query/ConditionCheckItem/DescribeTable` on five named ARNs (four tables + the index). **No `Scan`, no `DeleteItem`, no wildcard.** Plus `kms:Decrypt/GenerateDataKey` conditioned on `kms:ViaService = dynamodb.ap-southeast-1.amazonaws.com` |
| Cost | **$0.00/month idle.** At lab volume (≤10 marts, a few metered hours a month) request charges are well under $0.01/month; storage is bytes |

### Two divergences between ADR-036 and the implementation

Built to the implementation, because it is what the tests exercise and what works. **ADR-036
needs amending; that is an operator decision and was not made here.**

1. **`summary_config_v1` sort key.** ADR-036 specifies SK `coordinator_run_id`. The adapter
   calls `get_item(Key={"summary_key": …})` with no sort key (`dynamodb_state.py:418`),
   which a composite-key table rejects with a `ValidationException`. Building the ADR's
   shape would break the first read. `coordinator_run_id` is written as a plain attribute,
   and the per-run history the SK implied lives in Iceberg `ops.summary_config_hist_v1`.
2. **Execution table GSIs.** ADR-036 names two (`job_flow_date`, `status`). The
   implementation queries exactly one (`job_flow_index`) and nothing reads a status index.
   Only the used index is created: an unused GSI is charged write units on every base-table
   write, so a second one would cost real money to serve no query.

`spark/tests/test_reporting_infra.py` (12 tests) pins the Terraform key schema against the
adapter's actual `Key=` names — the mismatch class that fails only against real AWS, because
every local test injects a fake table that accepts any key it is handed.

## 5. The finding that matters most: EMR is unreachable without MSK

```hcl
module "emr_serverless" { count = var.enable_emr_serverless && var.enable_kafka_platform ? 1 : 0 }
module "lake_iam"       { count = var.enable_kafka_platform && var.enable_athena       ? 1 : 0 }
module "airflow_k3s"    { count = var.enable_airflow && var.enable_emr_serverless && var.enable_kafka_platform && var.enable_athena ? 1 : 0 }
```

`lake_iam` also takes `module.kafka_platform[0].msk_cluster_arn` and the platform CMK as
**required** inputs, so the coupling is structural, not just a `count`.

The consequence: **there is no way to run a dbt-on-EMR reporting job without creating an MSK
cluster**, at 3 × `kafka.m7g.large` ≈ **$0.7650/hr** — the whole $30 budget in about 39
hours. Reporting reads Iceberg through Glue and never touches Kafka.

`modules/reporting_ops` is deliberately self-contained (its own role, its own policies, no
MSK inputs) so the state store does not inherit that coupling. **Decoupling EMR itself is an
architecture change and is NOT implemented here** — it needs an approved decision, because
the EMR security group currently references the MSK security group, and the roles in
`lake_iam` are shared with the L1/L2 streaming jobs.

Options, for the review this document is written for:

| Option | Effect | Cost |
|---|---|---|
| **A. Decouple** — gate EMR on `enable_emr_serverless` alone, make MSK inputs optional | reporting runs without Kafka | EMR: $0 idle, ~$0.30/hr per running job |
| **B. Accept** — run MSK whenever reporting compute is needed | no code change | +$0.7650/hr for a cluster nothing reads |
| **C. Defer** — apply the state store only; run dbt locally against the lake | proves the framework, not the platform | $0 |

## 6. Change report

Every change below is **CREATE**. There is **no UPDATE, no REPLACE and no DESTROY** in
either plan.

| # | Change | Type | Reason | Cost | Security | Depends on | Rollback |
|---|---|---|---|---|---|---|---|
| 1 | 4 DynamoDB tables | CREATE | ADR-036; the framework has no runtime store | **$0.00 idle** | New service class. SSE-CMK, no public path, item-level IAM | lake CMK | `enable_reporting_framework=false`; Iceberg audit survives |
| 2 | `…-reporting-runtime-state` IAM policy | CREATE | scope the tables to four verbs | $0 | No `Scan`, no `DeleteItem`, no wildcard | tables | destroyed with the module |
| 3 | `…-reporting-lake` IAM policy | CREATE | S3 + Glue for reporting jobs | $0 | Writes only `mart/`, `curated/`, `ops/`, `artifacts/dbt/`, `checkpoints/reporting/`; **cannot rewrite the CDC layers** | lake bucket, Glue DBs | destroyed with the module |
| 4 | `…-reporting` IAM role | CREATE | EMR Serverless job execution identity | $0 | Trust limited to `emr-serverless.amazonaws.com` with `aws:SourceAccount` confused-deputy guard | — | destroyed with the module |
| 5 | 3 S3 prefix markers | CREATE | `ops/config/`, `artifacts/dbt/`, `checkpoints/reporting/` | bytes | inherit bucket policy, BPA, SSE | lake bucket | removed with the bucket |
| 6 | `enable_reporting_framework` variable | CREATE | ADR-036 requires operator sign-off | — | default **false** | — | n/a |

Supporting changes: `verify-destroy.sh` gained a DynamoDB check (ADR-036 validation item),
`validate-docs.py` check 14 now covers `streaming-reset.sh`, and the checkov baseline gained
`CKV_AWS_28`.

### Explicit answers

| Question | Answer |
|---|---|
| **Unexpected destruction** | **NONE.** Both plans are `0 to destroy, 0 to change`. |
| **MSK creation** | **YES in Plan A** (1 cluster, $0.7650/hr). **NO in Plan B.** Plan A is what `bash scripts/tf.sh plan` produces today — the trap this analysis exists to surface. |
| **EMR creation** | **YES in Plan A** (1 application, $0 idle). **NO in Plan B** — and that is the §5 gap, not a choice. |
| **Airflow creation** | **NO in either.** `enable_airflow=false`, and it needs EMR + MSK + Athena. |
| **DynamoDB creation** | **NO in Plan A** (the flag defaults false). **YES in Plan B** — 4 tables, $0.00 idle. |

## 7. Plans

| | Plan A — current tfvars | Plan B — reporting-only |
|---|---|---|
| Command | `-var-file=terraform.tfvars` | `+ -var-file=reporting-only.tfvars` |
| Resources | **164 to add**, 0 change, 0 destroy | **42 to add**, 0 change, 0 destroy |
| MSK / EC2 / VPC endpoints | 1 / 3 / 3 | 0 / 0 / 0 |
| DynamoDB | 0 | 4 |
| Hourly | **$1.1244/hr** | **$0.0000/hr** |
| 6-hour window | $6.75 | $0.00 |
| Monthly floor | $2.00 (2 CMKs) | $1.00 (1 CMK) |

Neither was applied. Evidence: `artifacts/validation/session-31/plan-comparison.txt`.

## 8. Checks run

```
terraform fmt -recursive -check    all formatted
terraform validate                 Success, 0 warnings (GSI migrated to key_schema)
terraform plan x2                  saved, reviewed, not applied
pytest spark/tests airflow/tests   875 passed, 0 failed
scripts/validate-docs.py           14 passed
make lint-shell                    shellcheck 0.11.0, no errors or warnings
checkov -d modules/reporting_ops   51 passed, 4 failed (CKV_AWS_28, accepted in the baseline)
```

## 9. What must be decided before an apply

1. **Reset `auto_destroy_after`** — it is in the past.
2. **Sign off ADR-036** — it is marked "requires operator sign-off before `terraform apply`".
3. **Amend ADR-036** for the two divergences in §4.
4. **Choose A, B or C** in §5 — this is the one that decides whether reporting can run at all.
5. **Never apply Plan A unreviewed.** It creates MSK at $0.7650/hr and does not create the
   reporting tables. If the intent is reporting only, the flags must change first.
