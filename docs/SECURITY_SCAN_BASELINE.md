# Security scan baseline — checkov

- Tool: `checkov` **3.3.10** (`docs/VERSIONS.md`)
- Command: `checkov -d terraform --compact --quiet --framework terraform`
- Run: 2026-08-13, Session 02 Stage B
- Result: **314 passed, 19 failed**
- Evidence: `artifacts/validation/session-02/stage-b/checkov.txt`

`sessions/02_terraform_foundation_data_lake.md` accepts "scan passes **or** the real
limitation is recorded". This file is that record. **Nothing is suppressed** — no
`skip_check`, no baseline file. Every finding still fires on every run; each is listed
below with the reason it is accepted, so a future reader can re-litigate it rather than
inherit a silent exception.

## Fixed rather than accepted

Two findings were real, free to fix, and were fixed in this session:

| Check | What it caught | Fix |
|---|---|---|
| **CKV2_AWS_64** | The lake CMK had no explicit key policy, so it fell back to the default | Added an explicit policy (`modules/data_lake/main.tf`). S01-11's whole argument for a *separate* lake key was that key policies differ — without one, that was true only in principle |
| **CKV2_AWS_12** | The VPC's default security group carried its AWS-created allow-all-from-self rule | `aws_default_security_group` with no ingress and no egress (`modules/kafka_platform/network.tf`). Nothing uses it, but anything launched without an explicit SG lands in it |

## Accepted — architectural decisions, each with an ADR

| Check | Finding | Why accepted |
|---|---|---|
| **CKV_AWS_130** ×3 | VPC subnets assign public IP by default | **ADR-022.** This is the design, not an oversight. Public subnet + public IP + a security group with **zero inbound rules** gives free IGW egress. The alternative — 12 interface endpoints × 3 AZ — is $341.64/month, **11× the entire budget**. `CLAUDE.md` §3.3 forbids inbound `0.0.0.0/0`; it does not forbid a public IP with no inbound path. Verified: the plan contains **zero** `0.0.0.0/0` ingress rules |
| **CKV_AWS_88** | EC2 instance has a public IP | Same as above. The toolbox is repo A's audited pattern, unchanged |
| **CKV_AWS_382** ×2 | Security group allows egress to `0.0.0.0/0` | The egress side of ADR-022. Outbound is how these workloads reach S3, Glue and ECR without a NAT Gateway. Egress restriction would require the endpoint set this design exists to avoid |
| **CKV_AWS_338** | CloudWatch log groups retain < 1 year | **ADR-030** cut retention to 3 days as part of the floor reduction. One year of retention on a lab that runs ~18 hours a month is storage for logs nobody will read. Explicitly not a production setting |
| **CKV_AWS_28** ×4 | DynamoDB point-in-time recovery disabled on the four reporting runtime-state tables | **ADR-036.** These tables are the WORKING COPY; the record is the Iceberg audit table, written once per terminal execution and held in a versioned S3 bucket. PITR bills continuously to protect state that is rebuilt by re-running the coordinator, and the one row that could not be rebuilt — the watermark — is reconstructible from the audit table's last SUCCEEDED execution. Revisit if these tables ever become the system of record, which ADR-036 forbids |
| **CKV_AWS_144** | S3 bucket has no cross-region replication | Doubles storage cost and egress for a lab whose data is regenerable from the source databases by re-running CDC. The lake is not the system of record |

## Accepted — inherited from repo A, audited in Session 00

| Check | Finding | Why accepted |
|---|---|---|
| **CKV_AWS_109 / 111 / 356** ×2 each | IAM policy allows actions without constraints | All six land on **one resource**: `modules/kafka_platform/aws_iam_policy_document.kms`, the KMS key policy's root statement (`kms:*` on `*` for the account root). **This is the AWS-documented required pattern** — omit it and the key can become permanently unmanageable, because there is no break-glass path back into a KMS key policy. Checkov flags the shape without recognising the resource type. Confirmed not present in any *workload* policy: `modules/lake_iam` scopes every statement to a bucket, key, database or workgroup ARN |
| **CKV_AWS_126** | Detailed EC2 monitoring disabled | $2.10/instance/month for 1-minute metrics on a toolbox that exists a few hours per window. 5-minute basic monitoring is sufficient at this cadence |
| **CKV_AWS_135** | EC2 not EBS-optimized | `t3` instances are EBS-optimized by default and the attribute is not settable. Effectively a false positive for this family |

## Accepted — deliberate scope decisions

| Check | Finding | Why accepted |
|---|---|---|
| **CKV2_AWS_11** | VPC flow logging disabled | Genuine gap, deferred rather than dismissed. Flow logs to S3 are cheap but not free, and the analysis value is low for a VPC that exists ~18 hours a month. **Revisit in Session 15** alongside the rest of observability; recorded there rather than lost here |
| **CKV_AWS_18** | S3 access logging disabled | Needs a second bucket and its own lifecycle. CloudTrail data events already cover the audit question for the lake. Revisit with CKV2_AWS_11 in Session 15 |
| **CKV2_AWS_62** | S3 event notifications not configured | Not applicable. Nothing in this architecture is event-driven off S3 — Spark reads on a schedule and Iceberg tracks its own state. Configuring notifications nothing consumes would be noise |

## What the scan does NOT check, and was verified separately

`checkov` reasons about configuration, not about the plan. These were asserted against
the saved plan directly (`artifacts/validation/session-02/stage-b/plan-assertions.txt`):

| Assertion | Requirement | Result |
|---|---|---|
| `0.0.0.0/0` **ingress** rules | `CLAUDE.md` §3.3 — zero | **0** |
| NAT gateways | `CLAUDE.md` §4.2 — zero | **0** |
| Secrets Manager secrets | ADR-031 — zero | **0** |
| EKS resources | session criterion — zero | **0** |
| RDS resources | session criterion — zero | **0** |
| Redshift resources | flag off — zero | **0** |
| Plaintext secrets in plan JSON | none | none — 140 `after_sensitive` markers |
| Resolved AWS account | `111122223333` | **matches** — and a plan-time precondition now enforces it |

That last row is not routine. See `DECISION_LOG.md` S02B-1: the first saved plan of this
session was generated against **the wrong AWS account**.
