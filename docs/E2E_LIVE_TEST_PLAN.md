# End-to-End Live Test Plan — cost/safety gate + execution order

- **Date:** 2026-09-03
- **Status:** **GO, conditional on three operator actions in §3.** Nothing applied.
- **Saved plan:** `terraform/envs/dev/tfplan` (copy: `artifacts/validation/final-e2e/tfplan.saved`)
- **Evidence:** `artifacts/validation/final-e2e/`
- **Spend to produce this plan:** **$0.00** — `fmt`, `validate`, `plan`, and read-only
  `describe`/`list` calls only. No `apply`, no resource created or started.

---

## 1. Gate: environment and backend

All verified, none assumed. Full output: `artifacts/validation/final-e2e/00-gate-identity-and-backend.txt`.

| Check | Required | Actual | |
|---|---|---|---|
| `AWS_PROFILE` | `my-aws-profile` | `my-aws-profile` | PASS |
| `AWS_REGION` | `ap-southeast-1` | `ap-southeast-1` | PASS |
| `ENVIRONMENT` | `dev` | `dev` | PASS |
| `DEPLOYMENT_PROFILE` | `lab_low_cost` | `lab_low_cost` | PASS |
| Account | `111122223333` | `111122223333` | PASS |
| Caller ARN | lab user | `arn:aws:iam::111122223333:user/my-aws-profile` | PASS |
| Terraform | `~> 1.15.0` | `1.15.8` | PASS |
| AWS provider | exact `6.56.0` | `6.56.0` | PASS |
| Workspace | `default` | `default` | PASS |
| Backend | S3, encrypted, locked | `aws-cdc-lakehouse-tfstate-…`, `encrypt=true`, `use_lockfile=true` | PASS |
| **Backend profile pinned** | required (OPEN-13) | `profile = "my-aws-profile"` present | PASS |
| Provider profile pinned | required (§2) | `profile = var.aws_profile` present | PASS |
| `terraform fmt -check -recursive` | clean | exit 0 | PASS |
| `terraform validate` | success | `Success! The configuration is valid.` | PASS |

State before apply — **3 resources**, matching reality exactly (no drift):

```text
data.aws_caller_identity.current
module.budget_guardrails.aws_budgets_budget.project_monthly
module.data_lake.aws_s3_bucket.lake
```

The backend profile pin matters more than it looks: without it the S3 backend resolves
credentials through the default chain, which on this workstation is account
`444455556666`. It is present and correct.

## 2. Gate: what is billable right now

`bash scripts/tf.sh verify` and `bash scripts/show-cost-resources.sh --idle`
(`01-billable-now.txt`, `02-idle-cost-inventory.txt`):

```text
MSK clusters   : 0        EC2 (running) : 0        EBS volumes : 0
NAT gateways   : 0  <- must be 0        VPC endpoints : 0
Month-to-date spend: $0.31
```

Nothing bills by the hour today. The standing floor is **8 customer-managed KMS keys**
(~$8/month), of which 4 are `PendingDeletion` and drain to ~$4/month by 2026-09-05.

## 3. Verdict: **GO**, conditional

The plan is structurally clean — **248 create, 0 change, 0 destroy, 0 replace** — and every
NO-GO tripwire is clear. Three conditions must be met **before** `apply`, because two of
them change the plan and the third changes what the plan means.

| # | Condition | Why it blocks |
|---|---|---|
| **C1** | Set `auto_destroy_after` to a future timestamp, then **re-plan** | It is `2026-08-25T00:00:00Z` — **9 days expired**. Its `validation` block only checks ISO-8601 *format*, not recency, so it passes silently and tags every one of the 248 resources already-expired, defeating ADR-027. It feeds provider `default_tags`, so changing it changes the plan and the saved plan must be regenerated. **Right now this is free** — with only 3 resources in state there is nothing to replace. The moment the stack exists, changing this value replaces all 6 subnets and cascades into MSK, EMR and every EC2 instance (OPEN-34). This is the cheapest it will ever be. |
| **C2** | Decide `monthly_budget_usd` | The plan raises `Warning: Check block assertion failed` — `monthly_budget_usd` is **100**, against a **$50** budget of record (`main.tf:95`, ADR-030 as amended 2026-08-23). The live AWS budget is also $100, so the alarm will not fire until 2× the budget of record. Either lower it to 50 or record the amendment. Note ADR-030's own document still reads "$30/month" in its title and body — the $50 amendment exists **only in the check block**, which is doc/code drift worth closing while you are here. |
| **C3** | Regenerate the plan immediately before applying | A saved plan is applied as-is without re-reading the configuration. This one was generated 2026-09-03T08:24Z against a 3-resource state. If anything changes — including C1 — apply the *new* plan, not this one. |

`terraform.tfvars` is gitignored and operator-owned; C1 and C2 are deliberately left as
operator edits rather than made here.

### NO-GO tripwires — all clear

Machine-checked against `tfplan.json` (`plan-diff-analysis.txt`):

```text
DESTROY / REPLACE entries         0     <- any non-zero is NO-GO
aws_vpc                           1     <- exactly one, no second VPC
aws_msk_cluster                   1     <- exactly one, no second MSK
aws_msk_serverless_cluster        0
aws_nat_gateway                   0     <- CLAUDE.md 4.2
aws_eks_cluster                   0     <- ADR-011, k3s instead
aws_redshiftserverless_workgroup  0     <- ADR-013 flag off
aws_redshiftserverless_namespace  0
aws_opensearchserverless_collection 0   <- ADR-049, vector index off
aws_sagemaker_endpoint            0     <- no always-on ML endpoint
aws_eip                           0     <- an unattached EIP bills
aws_db_instance                   0     <- ADR-032, no RDS Oracle/SQL Server
aws_secretsmanager_secret         0     <- ADR-031, SSM SecureString instead
aws_lb                            0     <- no Trino/ALB
```

Action census across the whole plan: **248 `create`, 2 `no-op`, 19 `read`, 0 `delete`.**
The two no-ops are the resources that already exist and are left untouched:

```text
module.budget_guardrails.aws_budgets_budget.project_monthly
module.data_lake.aws_s3_bucket.lake       <- holds all 1,585 lake objects
```

**The existing lake bucket is not recreated and not touched.** That is the single most
important line in this plan: 660 durable objects live in it.

## 4. Minimum live-test profile

The flags in `terraform.tfvars` already describe the minimum profile for the requested
scope. Nothing needs enabling; nothing expensive needs disabling. Confirmed against the
plan, not just the tfvars.

| Flag | Value | Required for | Cost |
|---|---|---|---|
| `enable_kafka_platform` | **true** | MSK, VPC, toolbox — and it gates EMR, `lake_iam` and Airflow | $0.7650/hr |
| `enable_source_lab` | **true** | Oracle + SQL Server containers | $0.1888/hr |
| `enable_cdc_runtime` | **true** | Debezium Connect + Apicurio | $0.1056/hr |
| `enable_emr_serverless` | **true** | every Spark job; also gates the Glue endpoint | metered |
| `enable_airflow` | **true** | coordinator DAGs — the part Phase 14 never proved | $0.1056/hr |
| `enable_athena` | **true** | serving, DQ, reconciliation, AI tools | $5/TB |
| `enable_reporting_framework` | **true** | 4 DynamoDB state tables | ~$0 |
| `enable_observability_ext` | **true** | 9 CloudWatch alarms | ~$0 |
| `enable_toolbox` | **true** | SSM entry point | $0.0264/hr |
| `enable_ai_rag` | **true** | AI IAM role + policy only | **$0.00/hr** |
| `enable_nat_gateway` | false | — | avoided |
| `enable_redshift_serverless` | false | ADR-013 | avoided |
| `enable_trino` | false | ADR-014 | avoided |
| `enable_eks` | false | ADR-011 | avoided |
| `enable_ai_vector_index` | false | ADR-049 — BM25 is the V1 retriever | avoided |
| `enable_ai_feature_store` / `_online_` | false | offline features are computed by Spark | avoided |
| `enable_ai_agent_runtime` | false | ADR-056 `DEFERRED_LAMBDA` | avoided |
| `enable_governance` / `marquez` / `lake_formation` | false | optional | avoided |

Cost-shape settings confirmed **in the plan**, not just in config:

```text
EMR Serverless   ARM64, emr-7.2.0, auto_stop enabled @ 15 min idle,
                 max 16 vCPU / 64 GB, initial_capacity = []   <- no pre-init (CLAUDE.md 4.5)
Athena workgroup bytes_scanned_cutoff = 10 GiB per query      <- CLAUDE.md 4.7
DynamoDB         4 tables, all PAY_PER_REQUEST
VPC endpoints    S3 + DynamoDB Gateway (free), Glue Interface (billed)
MSK              3 x kafka.m7g.large, 3.9.x.kraft, 20 GiB/broker, autoscaling OFF
EC2              t3.large airflow, t3.large cdc-runtime, t3a.xlarge source-lab, t3.small toolbox
```

`broker_ebs_gib = 20` and `enable_storage_autoscaling = false` both pass their check
assertions. MSK storage is a one-way ratchet — it cannot be reduced after creation.

## 5. Cost envelope

Unit prices: `docs/PRICE_REFERENCE.md`, `ap-southeast-1`, collected **2026-08-12** (22 days
old, inside the 90-day re-collection rule). Quantities from the saved plan.
Full arithmetic: `artifacts/validation/final-e2e/05-cost-envelope.txt`.

### Hourly baseline — everything up, no Spark job running

| Component | $/hr | Share |
|---|---|---|
| MSK 3 × `kafka.m7g.large` | 0.7650 | **62.0%** |
| EC2 × 4 | 0.4264 | 34.6% |
| EBS 150 GiB gp3 | 0.0197 | bills while **stopped** too |
| MSK storage 60 GiB | 0.0099 | one-way ratchet |
| Glue interface endpoint | 0.0130 | per AZ; 3 AZ would be 0.0390 |
| NAT gateway | 0.0000 | none planned |
| **Baseline** | **1.2340** | |

Metered on top, only while working: **EMR Serverless ARM** $1.2091/hr at max capacity
(16 vCPU / 64 GB), $0.6046/hr at half; **Athena** $0.0488 per query at the 10 GiB
workgroup ceiling.

### Window bounds

| Window | Expected | **Hard cap** | **Abort threshold** |
|---|---|---|---|
| **2 h** | ~$4.07 | $7.34 | **$8.00** |
| **4 h** | ~$7.95 | $14.18 | **$15.00** |

The hard cap assumes the pessimal case throughout: baseline for the full window, EMR
Serverless pinned at maximum capacity for every minute of it, and every Athena query
scanning the full 10 GiB cutoff. Reaching it means something is wrong — treat the abort
threshold as an alarm, not a budget.

**Set a wall-clock alarm.** The tag-filtered budget reports hours late and will still read
near $0 when the window is half spent; it cannot stop anything. MSK cannot be stopped, only
destroyed, and it is 62% of the burn.

### Retained after the window

| Item | $/month | Action |
|---|---|---|
| 2 new KMS CMKs from this plan | 2.00 | keep — they encrypt the lake |
| 4 lake CMKs already Enabled | 4.00 | consolidate via `reencrypt-lake-cmk.sh`, then schedule deletion |
| S3 lake (85.6 MB + new) | ~0.01 | **retain — this is the deliverable** |
| 4 CMKs `PendingDeletion` | 4.00 → 0 | drains by 2026-09-05, no action |

## 6. STOP vs DESTROY vs RETAIN

| Resource | After test | Why |
|---|---|---|
| **MSK** | **DESTROY** | Cannot be stopped. 62% of burn. Topics are replayable from the source lab. |
| source-lab EC2 | **DESTROY** | Seed is deterministic and re-runnable. |
| cdc-runtime EC2 | **DESTROY** | Connect state rebuilds from the connector JSON. |
| airflow k3s EC2 | **STOP**, then destroy at window end | `airflow-node.sh stop` preserves the metadata DB across a resume; EBS keeps billing at $0.0039/hr. |
| toolbox EC2 | **STOP** | The SSM entry point; cheap and useful during teardown verification. |
| EMR Serverless app | **RETAIN** | $0 when idle, auto-stops after 15 min. Destroying it buys nothing. |
| DynamoDB × 4 | **RETAIN** | PAY_PER_REQUEST, ~$0 idle. **They hold the execution/watermark evidence** — destroying them is what erased Phase 14's proof last time. |
| Glue databases | **RETAIN** | Metadata only, free, and points at retained S3 data. |
| Athena workgroup | **RETAIN** | Free; the cutoff guard is worth keeping in place. |
| **S3 lake** | **RETAIN** | The deliverable. |
| KMS CMKs | **RETAIN** | Deleting orphans the lake. Consolidate first. |
| Glue interface endpoint | **DESTROY** with the platform | $0.0130/hr per AZ for nothing when idle. |
| VPC / subnets / SGs | **DESTROY** | Free, but leaving them invites a second VPC on the next apply. |

The distinction that costs money: **STOP ends compute billing only.** EBS, EIPs, endpoints,
KMS and S3 keep billing until destroyed. `scripts/stop-ephemeral.sh` only ever stops;
`scripts/verify-destroy.sh --destroyed` is what proves a teardown actually happened, since
`terraform destroy` reporting success only means Terraform removed what was in *its* state.

## 7. Start order

Each step is a gate. **Do not proceed on a non-zero exit.** Billing starts the moment MSK
reaches ACTIVE, so the expensive mistake is testing against a half-booted stack and burning
the window debugging a readiness problem.

| # | Command | Phrase | Gate |
|---|---|---|---|
| 0 | *(operator)* set `auto_destroy_after`, decide `monthly_budget_usd` | — | C1, C2 |
| 1 | `bash scripts/tf.sh plan` | — | re-plan (C3). Confirm `0 to change, 0 to destroy` |
| 2 | `bash scripts/tf.sh apply --execute` | `APPLY THE SAVED PLAN` | **billing starts.** Note the wall-clock time |
| 3 | `bash scripts/cdc-window-start.sh` | — | blocks until every component is healthy; **exit 0 required** |
| 4 | `bash scripts/source-lab.sh enable-cdc --execute` | `ENABLE CDC` | ARCHIVELOG + supplemental logging + `sp_cdc_enable_*` |
| 5 | `bash scripts/source-lab.sh verify-cdc` | — | without this, Debezium runs HEALTHY and produces nothing (R15) |
| 6 | `bash scripts/source-lab.sh seed --execute` | `SEED SOURCE LAB` | deterministic seed |
| 7 | `bash scripts/cdc-runtime.sh record-checksums --execute` | `RECORD CHECKSUMS` | pins artifact SHA256s |
| 8 | `bash scripts/cdc-runtime.sh create-topics --execute` | `CREATE TOPICS` | fixed partition counts; connectors must not auto-create |
| 9 | `bash scripts/register-connectors.sh register --execute` | `REGISTER CONNECTORS` | |
| 10 | `bash scripts/cdc-runtime.sh status` + `topics` | — | both connectors `RUNNING`, task 0 `RUNNING`, 8 CDC topics |

## 8. The tests

Every test names the command, the assertion, and what a failure means. Tests marked
**FIRST-EVER** have never run live; they are the reason for the window.

### T1 — source → Debezium → Kafka
```bash
bash scripts/source-lab.sh workload 1 --execute      # phrase: RUN WORKLOAD 1
bash scripts/cdc-runtime.sh correctness all
```
- Topic counts match the seed **exactly** (Oracle 2000/320/200/4, SQL Server 3000/150/50/4).
- `commit_scn` non-decreasing; same PK always lands in the same partition (§5.1).
- Registry **refuses** a narrowing schema — a negative test; if it *accepts*, BACKWARD
  compatibility is not enforced and that is a fail.
- **Watch for:** a poison record. With `errors.tolerance: all` on a *source* connector the
  DLQ settings are inert and the record is dropped **silently** (G-P1-3). Compare topic
  counts against the seed; that difference is the only signal you get.

### T2 — normalizer / envelope contract
```bash
bash scripts/cdc-runtime.sh correctness 3
```
- Envelope preserves all 30 L1 contract columns; `before`/`after` stay JSON strings.
- `event_id` deterministic; Oracle SCN zero-padded, SQL Server LSN validated as hex.

### T3 — canonical FULL_CDC — **UTC FIX UNDER TEST**
```bash
aws emr-serverless start-job-run ...  # spark/jobs/full_cdc/job.py
```
- `FULL_CDC_INGESTED` = topic total; rerun is a **no-op** (MERGE on `dv_event_id`).
- **Assert `event_date` partitions match `source_commit_ts` in UTC.** This is the first live
  exercise of the G-P0-1 fix; a one-day-off partition means the fix did not take.
- `tombstones=N` reported and excluded from decode.
- **Known gap (G-P1-1):** there is **no quarantine path** on this job. One undecodable
  record fails the run. If it fails on `from_avro`, that is the gap, not a regression.

### T4 — REALTIME window
```bash
aws emr-serverless start-job-run ...  # spark/jobs/realtime/job.py --window-hours 72
```
- `REALTIME_WINDOW cutoff` is UTC; `REALTIME_ROWS` ≤ FULL_CDC rows.
- Run twice → identical row count (full refresh is idempotent).
- Empty window must **abort**, not publish.

### T5 — EOD / snapshot
```bash
aws emr-serverless start-job-run ...  # spark/jobs/eod/job.py --business-date <D>
bash scripts/snapshot-rebuild.sh --entity banking.customer --date <D> --execute
```
- One row per PK; deletes excluded; `EOD_DISTINCT_DATES` = 1 for a scoped run.
- Rebuild at the same cutoff → **identical checksum** (determinism).
- `ops.eod_watermark` row written **and** Iceberg tag `EOD_<date>` created — the gate needs
  both, and until Session 34 nothing wrote either.
- **New guard (G-P1-4):** a non-numeric source position must **refuse loudly**. It should
  not fire on this Oracle-scoped run; if it does, the filter has been widened.

### T6 — the five reporting modes — **FIRST-EVER for four of them**
```bash
python3 scripts/reporting-live-run.py --flow EOD          --date <D>
python3 scripts/reporting-live-run.py --flow STREAM_BATCH --date <D>
python3 scripts/reporting-live-run.py --flow AUTO_CORRECT --date <D>
python3 scripts/reporting-live-run.py --flow FULFILL      --date <D-3>
python3 scripts/reporting-live-run.py --flow STREAMING_RT --date <D>
```
Phase 14 called the coordinator and then did the MERGE **in the driver**, so none of the
five flow modules has ever executed. `reporting-live-run.py` exists precisely to call the
flow functions themselves.
- EOD stamps `CERTIFIED`; STREAM_BATCH `PROVISIONAL_NRT`; AUTO_CORRECT `PROVISIONAL_CORRECTED`.
- **A correction must LAND** — every prior run proved only the refusal path (G-P1-3 in the
  reporting review, gap G3).
- STREAM_BATCH: `watermark_ts` **non-null**, and the `[wm − overlap, frozen_upper)` window
  computed once and recorded (gap G2).
- STREAMING_RT: **process ≥1 event and write a checkpoint** — it has processed zero, ever
  (gap G5). Checkpoint must be under `checkpoints/`, never `warehouse/`.
- Downgrade refusal against **live** Iceberg data, through
  `dbt/macros/reporting/reporting_mart.sql` — not a driver copy.

### T7 — dbt on EMR — **FIRST-EVER, the biggest unknown**
```bash
bash scripts/dbt-verify.sh          # local, free, run BEFORE the window
# then, in-window, via the EOD flow's dbt entrypoint
```
`dbt build` has never run anywhere — not locally against Glue, not on EMR. dbt-core and
dbt-spark are absent from the EMR image and the private subnets cannot reach PyPI, so
**package dbt into the image before the window opens.**
- `dbt build` completes; `dbtRunner` with `method: session`; models materialise as Iceberg.
- Manifest dependencies resolve into the same topological turns
  `make reporting-graph` prints offline.

### T8 — Airflow coordinator — **FIRST-EVER**
```bash
bash scripts/airflow-node.sh ui        # SSM port-forward; ~15s warm-up before READY
bash scripts/airflow-node.sh credentials
```
- `datamart_eod` and `datamart_stream_batch` run end to end with dynamic mapping and pools.
- **Expect `NotImplementedError` for AUTO_CORRECT and FULFILL** (`reporting_common.py:375`,
  OPEN-28). That is the known state, not a discovery — record it and move on.
- `catchup=False` honoured; a backfill is FULFILL, not catchup.

### T9 — datamart → Athena
```bash
bash scripts/athena-smoke.sh --execute
bash scripts/athena-benchmark.sh
```
- Queries use the **prefixed** databases (`kafka_dev_lab_dev_mart`); unprefixed names return
  `SCHEMA_NOT_FOUND` — defect F2.
- The 10 GiB cutoff is **enforced by the workgroup**: confirm a deliberately oversized query
  is cancelled, not merely warned about.

### T10 — DQ, reconciliation, lineage
```bash
bash scripts/dq-check.sh --date <D>            # exit 2 = DQ blocks publish, and that is correct
aws emr-serverless start-job-run ...           # spark/ops/reconcile_job.py
```
- Exit 0 publishes; **exit 2 must block** the certified partition. Exit 2 is the gate
  working, not a crash.
- FULL_CDC → EOD counts reconcile within tolerance, using **source position**, not
  `kafka_offset`.
- Freshness, completeness, uniqueness, referential integrity all evaluated — a
  `NOT_EVALUATED` ERROR check must block too.
- OpenLineage events emitted per `governance/lineage/openlineage.yml`.

### T11 — RAG, KPI, drivers, anomaly, forecast
```bash
make ai-eval-rag-gate
make business-ai-eval
make ai-ask Q="what was the closing balance trend last week"
```
- Retrieval gate passes against the recorded baseline (passes offline today).
- **The real test is arithmetic against live marts**, which has never happened: every metric
  value so far is proven against a fixture. Cross-check each KPI with an independent Athena
  `SELECT`.
- Drivers/anomaly/forecast: expect **`None`, not a fabricated value**, wherever history is
  insufficient — there are only 4 business dates with a hole.
- Dimensions `product_code`, `branch_id`, `segment_code`, `currency` must **refuse at compile
  time** while `dim_account`/`dim_customer` are unmaterialised. A silently dropped dimension
  answers a different question than the one asked.

### T12 — features / ML
```bash
make ai-features && make ai-ml-pilot && make ai-ml-train
```
- PIT join on live data: `feature_event_time <= label_event_time`, and the horizon guard
  rejects a feature computed inside `(t, t+horizon]`.
- Processing-time columns rejected as join keys.
- **The pilot label is synthetic (AUC 1.0)** — this proves plumbing, not predictive power.
  Do not report the AUC as a result.

### T13 — agent, tool safety, injection — **guard fixes under test**
```bash
make ai-eval-agent-gate
make ai-tools
python3 ai/eval/drills_p15.py
```
- All 8 tools resolve against live backends; the agent gate passes (it fails today only
  because there is no backend).
- **G-P0-2 regression, live:** `SELECT ... FROM <db>_mart.t JOIN raw_pii p ON ...` must be
  **refused** for the unqualified reference. Also verify IAM independently denies
  `_stream` / `_full_cdc` — the allow-list is the *second* control.
- 20/20 drills, 0 P0. Injected instructions inside retrieved chunks must not produce a
  mutating statement.
- **Generation is expected to stay unavailable** (Bedrock `INVALID_PAYMENT_INSTRUMENT`). If
  it becomes available, the G-P1-2 redaction fix gets its first live exercise.

### T14 — observability
```bash
bash scripts/show-cost-resources.sh --compute
```
- 9 CloudWatch alarms present and in `OK`/`INSUFFICIENT_DATA`, not `ALARM`.
- `cdc_lakehouse_freshness_seconds` and the SLO set published; pushed metrics are sticky, so
  confirm the timestamp advances rather than trusting the value.

### T15 — recovery drills
```bash
bash scripts/failure-drill.sh list
bash scripts/failure-drill.sh run <id> --execute
```
- Connector restart resumes from the committed offset with no gap and no duplicate
  (`event_id` idempotency absorbs replay).
- A mid-batch EMR failure replays the whole batch; FULL_CDC row count is unchanged.
- No destructive broker action against managed MSK — ever.

### T16 — cleanup
See §9.

## 9. Stop order and rollback

```bash
# 1. capture evidence FIRST — this is what survives the teardown
bash scripts/cdc-runtime.sh status    > artifacts/validation/final-e2e/live-connectors.txt
bash scripts/dq-check.sh --date <D>   > artifacts/validation/final-e2e/live-dq.txt
aws dynamodb scan --table-name kafka-dev-lab-dev-job-execution > .../live-executions.json

# 2. stop compute (reversible; EBS keeps billing)
bash scripts/stop-ephemeral.sh --execute
bash scripts/verify-destroy.sh --stopped

# 3. consolidate CMKs BEFORE destroying — order matters
bash scripts/reencrypt-lake-cmk.sh audit
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute

# 4. destroy
bash scripts/tf.sh destroy --execute        # phrase: DESTROY THE DEV LAB
bash scripts/verify-destroy.sh --destroyed  # non-zero if anything survives
```

**Rollback at any point is `tf.sh destroy --execute`.** There is no partial-apply state that
needs unwinding: the plan creates 248 resources and destroys none, so aborting mid-apply
leaves a subset that `destroy` removes cleanly. The S3 lake is `prevent_destroy`-guarded by
`allow_destroy_with_data = false`.

**Two ordering traps, both previously hit:**

1. **Re-encrypt before destroy, not after.** `terraform destroy` schedules the lake CMK for
   deletion and the next apply creates a *new* one; objects keep the old key. The bucket has
   already accumulated five CMKs this way, and 161 objects now reference a key that no
   longer exists (harmless — they are all regenerable `query-results/` and `logs/`).
2. **Do not destroy the DynamoDB tables.** They hold the execution and watermark evidence.
   Destroying them is exactly what erased Phase 14's durable proof.

## 10. Residual risks accepted for this window

| Risk | Why it is acceptable |
|---|---|
| No quarantine on the canonical ingest (G-P1-1) | Fails loudly; does not corrupt |
| Source-connector DLQ inert (G-P1-3) | Detected by comparing topic counts to the seed |
| AUTO_CORRECT / FULFILL not orchestrated (OPEN-28) | Covered via `reporting-live-run.py` |
| `dbt build` unproven (G-P1-7) | It is the point of T7 |
| 4 business dates with a hole | Forecast/anomaly report `None`, by design |
| Two TF resources own `logs/airflow/` (G-P3-3) | Cosmetic 1-resource diff; never converges |

---

## 11. Live-approval prompt

Paste this only when you are ready to spend money. It is the operator's decision; no agent
can satisfy the typed phrases, and `confirm_destructive()` refuses outright when stdin is
not a terminal.

> **APPROVE LIVE E2E WINDOW**
>
> I have:
> 1. set `auto_destroy_after` in `terraform/envs/dev/terraform.tfvars` to
>    `<UTC timestamp = window end + 2h margin>`;
> 2. set `monthly_budget_usd` to `50`, or recorded an amendment to ADR-030 for `100`;
> 3. re-run `bash scripts/tf.sh plan` and confirmed **`0 to change, 0 to destroy`**,
>    exactly **1** `aws_vpc`, **1** `aws_msk_cluster`, and **0** NAT / EKS / Redshift /
>    OpenSearch / SageMaker endpoint / EIP / RDS / Secrets Manager;
> 4. packaged dbt-core and dbt-spark into the EMR image (T7 fails without it);
> 5. set a wall-clock alarm for `<2h or 4h>` and accepted the hard cap of
>    **$8.00 (2h) / $15.00 (4h)**;
> 6. read §9 and will re-encrypt the lake CMK **before** destroying.
>
> Window: `<2h|4h>` starting `<UTC time>`. Proceed with
> `bash scripts/tf.sh apply --execute` and the phrase `APPLY THE SAVED PLAN`.

---

*Plan produced read-only. `terraform fmt`, `terraform validate`, `terraform plan -out=tfplan`
and read-only AWS describes only. No AWS resource was created, started, modified or
destroyed, and no `apply` was run.*
