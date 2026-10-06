# GATE 2 PACKAGE — Session 02 first apply

- Session: 02 Stage B
- Status: **PREPARED — awaiting human approval. Nothing has been applied.**
- Gate: `docs/APPROVAL_GATES.md`
- Saved plan: `terraform/envs/dev/tfplan`
- Evidence: `artifacts/validation/session-02/stage-b/`

---

## 1. What this applies

**113 to add, 0 to change, 0 to destroy.**

| Module | Resources | Billable? |
|---|---:|---|
| `kafka_platform` | 52 | **yes** — MSK cluster, toolbox EC2, platform CMK, 6 alarms |
| `lake_iam` | 43 | no |
| `data_lake` | 23 | **yes** — lake CMK ($1/mo); bucket empty at first apply |
| `glue_catalog` | 7 | no — first 1M objects free |
| `vpc_endpoints` | 2 | **no** — S3 + DynamoDB **gateway** endpoints are free |
| `athena` | 1 | no — bytes scanned only |
| `budget_guardrails` | 1 | no — first two budgets free |
| root | 1 | no — `terraform_data` invariants |

## 2. Cost — read this before approving

| | |
|---|---:|
| **While running** | **$0.8052/hr** |
| 4-hour window | $3.22 |
| 6-hour window | $4.83 |
| **24/7 for a month** | **$587.80 — 19.6× the $30 budget** |
| **Always-on floor after destroy** | **$2.00/month** (2 KMS CMKs) |

**The entire monthly budget is gone in 37 hours of runtime.** MSK is $0.7749 of the
$0.8052 — 96% of it.

Two things that are not obvious:

- **KMS CMKs keep billing after destroy.** They enter a 7-day deletion window, so
  `verify` run the same day still shows $2.00 of floor. That is correct, not a leak.
- **MSK broker storage cannot be reduced after creation.** `broker_ebs_gib = 20` is right
  in this plan. If it were wrong, the only remedy would be destroying and recreating the
  cluster.

## 3. Blockers — this gate cannot be approved yet

### 3.1 The state backend does not exist, and its script is wrong

`terraform/envs/dev/versions.tf` has the `backend "s3"` block **commented out**, so this
plan was produced against **local state**. Applying it would write
`terraform.tfstate` to the working directory — which `.gitignore` excludes, but which
also means no locking, no versioning and no recovery.

`docs/gates/GATE-1-state-backend-bootstrap.md` is itself **BLOCKED**: the bootstrap
script still creates a KMS CMK that ADR-030 replaced with SSE-S3, and bucket encryption
cannot be changed retroactively for objects already written.

**Order of operations:** fix the bootstrap script → approve Gate 1 → run it → uncomment
the backend block → `terraform init -backend-config=backend.hcl` → **re-plan** → then
this gate.

### 3.2 OPEN-04 is unresolved

ADR-022 places four workloads in public subnets with public IPs and zero-inbound security
groups. The plan contains **zero `0.0.0.0/0` ingress rules** and this is repo A's audited
pattern — but extending it from one toolbox to four workloads has not had explicit
security sign-off. The alternative (A-min, 8 interface endpoints) costs **+$0.1040/hr**,
which is +$0.62 per 6-hour window.

## 4. Verification already performed

All read-only. Transcripts in `artifacts/validation/session-02/stage-b/`.

```
terraform fmt -recursive -check    clean
terraform validate                 Success
checkov -d terraform               314 passed, 19 failed (all triaged: docs/SECURITY_SCAN_BASELINE.md)
tflint                             ABSENT — OPEN-06, not claimed as passing
```

**12 plan assertions, all pass** (`plan-assertions.txt`): zero `0.0.0.0/0` ingress, zero
NAT, zero Secrets Manager, zero EKS, zero RDS, zero Redshift, 7 Glue databases, 2 KMS
keys, 1 MSK cluster, 1 S3 bucket, 2 gateway endpoints, 8 IAM roles.

**9 negative tests, all fire** (`negative-tests.txt`) — every guard proven by execution
rather than asserted: ADR-026 mutual exclusion, `kafka.t3.small`, NAT, `auto_destroy_after
= "manual"`, empty `budget_email`, `az_count = 2`, wrong AWS account, `broker_ebs_gib =
100` (warns), and `enable_cdc_runtime` without `enable_source_lab`.

## 5. What went wrong during this session, and what fixed it

**The first saved plan targeted the wrong AWS account.**

`providers.tf` had no `profile`, so Terraform used the default credential chain. On this
workstation `[default]` is account **444455556666**, not the lab's `111122223333`. The
shell identity guard printed the correct account throughout — **it does not constrain
Terraform's provider.**

The credentials file also holds `vannk-prod`, `vannk_prod_msm`, `tramntb_prod` and
`prod`. An unpinned provider is one misconfigured default away from planning against
production.

Two fixes, both in this plan:

1. `profile = var.aws_profile` in `providers.tf`, with a variable validation pinning it
   to `my-aws-profile`.
2. A plan-time **precondition** comparing `data.aws_caller_identity.current.account_id`
   against `expected_account_id`. Proven to fire (negative test T7).

The wrong-account plan was discarded, not amended.

## 6. Approval

To approve, confirm each:

- [ ] Gate 1 (state backend) fixed, approved and executed; the S3 backend is initialised
- [ ] The plan has been **re-generated against the S3 backend** after that
- [ ] OPEN-04 resolved — ADR-022's public-subnet placement accepted, or A-min taken
- [ ] `terraform.tfvars` exists with a **real `auto_destroy_after`** for this window
- [ ] You accept **$0.8052/hr** and will destroy at the end of the window
- [ ] You have read §2's note that MSK storage is unreducible after creation

Then:

```bash
bash scripts/tf.sh plan               # re-plan; review it
bash scripts/tf.sh apply --execute    # types a confirmation phrase
```

**Do not run `terraform apply` directly.** The wrapper prints the identity guard and the
cost summary first, and refuses to run non-interactively.
