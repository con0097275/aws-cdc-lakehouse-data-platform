<!-- PROVENANCE: copied verbatim from
     /path/to/aws-cdc-lakehouse-claude-guide-v2/docs/SOURCE_REPOSITORY_DECISION.md
     (Session 00 output, guide package v2). Copied 2026-08-12 by Session 01 under ADR-025.
     The original is read-only and checksummed by the guide package MANIFEST.json/SHA256SUMS.
     Corrections must be made HERE, not upstream. -->

# SOURCE REPOSITORY DECISION

- Session: 00 — Audit existing repository
- Date: 2026-08-07
- Author: `/senior-architect` pass
- AWS account: `111122223333` / region `ap-southeast-1` / profile `my-aws-profile`
- Status: **DECIDED** (not BLOCKED — evidence was sufficient)

---

## 1. Decision

**Repository A — `~/terraform-kafka-kraft/kafka-kraft-aws/kafka-aws-production-lab` — is the authoritative Kafka platform codebase.**

Qualified: it is authoritative **by code and by deployment history**, but its infrastructure is **NOT currently deployed**. It was applied and then destroyed on 2026-07-26. Nothing from it can be consumed at runtime today.

**Repository B — `~/terraform-kafka-kraft/terraform-kafka-main` — is REJECTED** as a platform source. It is unrelated third-party tutorial code that has never been applied to this account and violates four security invariants in `CLAUDE.md`.

---

## 2. Relationship determination

`sessions/00_audit_existing_repo.md` requires deciding whether the two repositories are duplicates, parent/child, separate environments, or one is stale. All four hypotheses were tested:

| Hypothesis | Verdict | Evidence |
|---|---|---|
| **Duplicates** | **Rejected** | Repo A provisions AWS-managed MSK (`terraform/msk.tf:35` `resource "aws_msk_cluster" "this"`). Repo B installs Apache Kafka onto raw EC2 via `null_resource` + SSH `remote-exec` provisioners (`main.tf`). No shared resource, variable name, module, or template between them. |
| **Parent / child** | **Rejected** | No `module` block in either repo references the other. No shared state lineage. No common ancestor file or copied module with provenance. |
| **Separate environments of one system** | **Rejected** | Different regions (`ap-southeast-1` vs `us-east-1`), different Kafka delivery models (managed vs self-managed), different auth models (IAM SASL vs PLAINTEXT), different tagging schemes (six-key `default_tags` vs ad-hoc two-key). Environments of one system would share at least a naming convention; these share none. |
| **One is stale** | **Partially — but this is not the operative relationship** | Repo A's own `IMPLEMENTATION_REPORT.md` / `VALIDATION_REPORT.md` are stale (see §4). Repo B is not a stale version of Repo A; it is a different lineage entirely. |

**Operative relationship: the two repositories are unrelated codebases that solve the same problem by different means.** Repo A is a purpose-built, security-hardened lab for this account. Repo B is upstream tutorial material retained as reference.

---

## 3. Evidence for Repo A being authoritative

### 3.1 It is the only configuration ever applied to this account

State lineage is decisive. Both state files in `kafka-aws-production-lab/terraform/` share lineage `bd2a46c8-9ae6-abd3-d27b-1c18076b9d75`:

| File | Serial | Resources | Outputs | Terraform |
|---|---:|---:|---:|---|
| `terraform.tfstate.backup` | 52 | **43** | 10 | 1.15.8 |
| `terraform.tfstate` | 106 | **0** | 0 | 1.15.8 |

A same-lineage progression from serial 52 (43 resources) to serial 106 (0 resources) is an apply followed by a destroy on one workspace. It is not a fresh `init`, not a state loss, and not a second workspace.

Verify:

```bash
python3 -c "import json;d=json.load(open('~/terraform-kafka-kraft/kafka-kraft-aws/kafka-aws-production-lab/terraform/terraform.tfstate'));print(d['lineage'],d['serial'],len(d['resources']))"
# bd2a46c8-9ae6-abd3-d27b-1c18076b9d75 106 0
```

### 3.2 Real AWS resource IDs in the backup state

State serial 52 records live identifiers from this account, e.g.:

- MSK cluster `arn:aws:kafka:ap-southeast-1:111122223333:cluster/kafka-prod-lab-lab/39ba0b33-b423-4873-b7a0-4eacdf1310c0-6`
- VPC `vpc-0345fd1e6238b81ee` (CIDR `10.42.0.0/16`)
- Toolbox EC2 `i-03e1daf813522ba7b`
- KMS CMK `8d13d369-dc5c-4ae5-af5d-bc667b2f0745`, alias `alias/kafka-prod-lab-lab`
- Bootstrap brokers `b-{1,2,3}.kafkaprodlablab.8dvhy9.c6.kafka.ap-southeast-1.amazonaws.com:9098`

These are real, account-scoped, region-correct identifiers. Repo B's state does not exist.

### 3.3 CloudTrail corroborates the destroy

```bash
aws cloudtrail lookup-events --region ap-southeast-1 \
  --lookup-attributes AttributeKey=EventName,AttributeValue=DeleteCluster --max-results 5
```

Returns exactly one event: `DeleteCluster` by user `my-aws-profile` at **2026-07-26T19:09:46Z**. This matches the state-file transition and the file mtimes.

### 3.4 Repo A alone satisfies the project's security invariants

`CLAUDE.md` §3 invariants, checked against Repo A:

| Invariant | Repo A |
|---|---|
| No inbound `0.0.0.0/0` for Kafka/SSH (`CLAUDE.md:33`) | ✅ MSK SG ingress on 9098/11001/11002 sourced from the toolbox SG only (`network.tf:128-148`); toolbox SG has zero inbound rules |
| No SSH / key pair (`CLAUDE.md:36`) | ✅ SSM Session Manager only; no `key_name` on the instance |
| KMS encryption (`CLAUDE.md:37`) | ✅ CMK at rest, TLS in transit, `public_access { type = "DISABLED" }` (`msk.tf:47-48`) |
| Pin versions, no `latest` (`CLAUDE.md:41`) | ✅ `required_version = "~> 1.15.0"`, `aws = 6.56.0`, `random = 3.9.0` (`versions.tf`) |
| Required tags (`CLAUDE.md:76`) | ✅ all six keys via provider `default_tags` |

### 3.5 Git history is unavailable and was not used

None of the three directories is a Git repository:

```bash
git -C ~/terraform-kafka-kraft/kafka-kraft-aws rev-parse --is-inside-work-tree
# fatal: not a git repository
```

The same is true of `terraform-kafka-main` and of the guide package itself. `sessions/00_audit_existing_repo.md` lists Git history as an evidence source; **it is not available here and no conclusion in this document relies on it.** Repo A does carry a `.gitignore` and `.github/workflows/ci.yml`, indicating it was version-controlled upstream, but the `.git` directory is absent locally.

---

## 4. Correction: Repo A's own status documents are stale

`kafka-aws-production-lab/IMPLEMENTATION_REPORT.md` and `VALIDATION_REPORT.md` state that **no `terraform apply` was run**, as of 2026-07-26. The state files and CloudTrail prove an apply did occur and was followed by a destroy later that same day.

**Treat the state files and AWS API responses as ground truth; treat Repo A's markdown status claims as superseded.** This is recorded so a later session does not re-derive a false "never deployed" conclusion from those documents.

---

## 5. Repo B rejection — security appendix

Recorded in detail so this decision is not revisited.

| # | Disqualifier | Evidence | Invariant violated |
|---|---|---|---|
| 1 | Security group opens **22, 80, 443, 3000, 8080, 7071, 9090, 9092, 9097** to `0.0.0.0/0` | `main.tf:10` `allowed_ports`, `main.tf:18-26` dynamic ingress with `cidr_blocks = ["0.0.0.0/0"]` | `CLAUDE.md:33` — no inbound `0.0.0.0/0` for SSH, Kafka, Grafana, Prometheus |
| 2 | Kafka listener is **PLAINTEXT**, no TLS, no SASL, no IAM auth | `resources_00_tmp/scripts/setupKafkaService.sh` writes `PLAINTEXT://<public-ip>:9092` | `CLAUDE.md:37` — KMS/TLS encryption required |
| 3 | Access by **SSH key pair**; instances in the **default VPC's public subnets** | `terraform.tfvars:5-6` (`pem_file`, `pem_key_name`); `data.aws_vpc.default` in `main.tf` | `CLAUDE.md:36` — SSM Session Manager only |
| 4 | **No version pinning at all** — no `terraform {}` block, no `required_version`, no `required_providers` | `grep -rn "required_version\|required_providers" *.tf` returns nothing | `CLAUDE.md:41` — pin version/provider/image; no `latest` |
| 5 | Not executable on this machine | `terraform.tfvars:5` sets `pem_file = "/Users/sachin/work/keys/aws/sjlearning_2024/sachin-kp-us-east-1.pem"` — an upstream author's macOS path. `file()` fails at plan time. | — |
| 6 | Region-locked to the wrong region | `terraform.tfvars:1` `aws_region="us-east-1"`; `ami-027979a480e532c35` is a `us-east-1` AMI | Project region is `ap-southeast-1` (`CLAUDE.md:13`) |
| 7 | `null_resource` with `triggers.always_run = timestamp()` | `main.tf` — re-executes SSH provisioners on **every** plan/apply | Non-idempotent; conflicts with `CLAUDE.md:71` |
| 8 | No IAM roles, no instance profile, no KMS, no SSM, no CloudWatch | absent from `main.tf` | `CLAUDE.md:32`, `:37` |

**Permitted residual use of Repo B:** read-only reference for KRaft configuration and the JMX exporter rule set at `resources_00_tmp/config/kafka_kraft.yml` (which includes `raft-metrics` and `broker-metadata-metrics` patterns). Nothing else. No file may be copied into the target repository without a documented provenance note and a security review.

---

## 6. Naming discrepancy — flagged for Session 01

`CLAUDE.md:12` describes the existing project as **`kafka-dev-lab`**. Repo A actually builds **`kafka-prod-lab-lab`**:

```hcl
# kafka-aws-production-lab/terraform/terraform.tfvars:1-2
project_name = "kafka-prod-lab"
environment  = "lab"
```

No resource named `kafka-dev-lab` has ever existed in account `111122223333`. Additionally, the guide's environment is `dev` (`LOCAL_PROJECT_CONTEXT.md:24`) while Repo A's is `lab`.

This matters beyond cosmetics: Repo A's AWS Budget cost filter keys on `user:Project$kafka-prod-lab` (`budget.tf`), so the `Project` tag value is load-bearing for cost attribution. Renaming forces a budget-filter change and a full resource replacement.

**Action for Session 01:** decide whether to (a) correct `CLAUDE.md` to match reality, or (b) rename the platform to `kafka-dev-lab` with `environment = "dev"`. Recorded as an OPEN decision in `DECISION_LOG.md`.

---

## 7. Consequences of this decision

1. Repo A remains **read-only** under `LOCAL_PROJECT_CONTEXT.md:15` until an explicit exception is granted. This session made no modification to it.
2. Because Repo A's infrastructure is destroyed, **the "reuse the existing platform" premise cannot be satisfied at runtime.** Sessions 04, 05, 06, 12 and 15 hard-depend on a running MSK and are blocked until it is re-provisioned. See `docs/GAP_ANALYSIS.md` Gap 0.
3. Repo A's backend is **local state** with no `backend` block, so `terraform_remote_state` consumption is **not possible today**. See `docs/GAP_ANALYSIS.md` Gap 1.
4. Repo A exports only 4 of the ~12 outputs the lakehouse needs. See the integration contract in `docs/GAP_ANALYSIS.md` §4.
5. The re-provisioning cost conflicts with the account budget. See `docs/COST.md`.

---

## 8. Verification commands

```bash
# Identity guard
aws sts get-caller-identity --profile my-aws-profile --region ap-southeast-1

# Platform absent
aws kafka list-clusters-v2 --profile my-aws-profile --region ap-southeast-1 --query 'length(ClusterInfoList)'
aws ec2 describe-vpcs --profile my-aws-profile --region ap-southeast-1 --query 'Vpcs[?IsDefault==`false`].VpcId'

# State lineage
python3 -c "import json;[print(f,json.load(open(f))['lineage'],json.load(open(f))['serial'],len(json.load(open(f))['resources'])) for f in ['~/terraform-kafka-kraft/kafka-kraft-aws/kafka-aws-production-lab/terraform/terraform.tfstate','~/terraform-kafka-kraft/kafka-kraft-aws/kafka-aws-production-lab/terraform/terraform.tfstate.backup']]"

# Destroy event
aws cloudtrail lookup-events --profile my-aws-profile --region ap-southeast-1 \
  --lookup-attributes AttributeKey=EventName,AttributeValue=DeleteCluster --max-results 5

# Repo B disqualifiers
grep -n "allowed_ports\|0.0.0.0/0" ~/terraform-kafka-kraft/terraform-kafka-main/main.tf
grep -rn "required_version\|required_providers" ~/terraform-kafka-kraft/terraform-kafka-main/*.tf   # expect no output
```

All commands above are read-only. No AWS write, no Terraform state operation, and no modification to either reference repository was performed in this session.
