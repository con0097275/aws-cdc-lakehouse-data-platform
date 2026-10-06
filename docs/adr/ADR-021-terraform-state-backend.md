# ADR-021 — Terraform state backend: S3 with native locking

> **AMENDED 2026-08-13 by [ADR-030](ADR-030-budget-of-record-and-release-schedule.md):
> the state bucket uses SSE-S3, not a dedicated CMK.** The reasoning below — that state
> and lake data have different audiences, so a shared key makes least-privilege key
> policies unwritable — remains correct in principle. It was priced against an $80 budget.
> Against the real **$30**, a dedicated state CMK costs **$1.00/month, 3.3 % of the entire
> budget**, for a bucket whose access is already restricted by IAM and a bucket policy and
> whose contents have exactly one audience: the operator. The bucket stays encrypted,
> private, versioned and TLS-only. **Platform and lake CMKs remain separate** (S01-11) —
> those audiences genuinely differ. Decide before running
> `scripts/bootstrap-state-backend.sh`: changing bucket encryption later does **not**
> re-encrypt existing objects.

- Status: **ACCEPTED** (Session 01)
- Closes: Gap 1, `CLAUDE.md` §3.8
- Related: ADR-001, risk R8

## Context

Repo A has **no `backend` block**. State is local files in `terraform/`. Session 00
found `terraform.tfstate` (serial 106) and `terraform.tfstate.backup` (serial 52)
sitting next to the code.

This violates `CLAUDE.md` §3.8 — state must be encrypted, locked and IAM-restricted —
on all three counts. And it is not a theoretical exposure: the backup state contains
the MSK cluster ARN, VPC and subnet IDs, the KMS key ID and the Grafana SSM parameter
name.

Repo A ships `backend.hcl.example` whose first line is already the right instruction:
*"Create the backend separately. Do not let the disposable lab delete its own state
backend."* Its bucket value is an unresolved placeholder.

## Options

| Option | Locking | Verdict |
|---|---|---|
| **S3 + `use_lockfile = true`** | S3-native conditional writes | **CHOSEN** |
| S3 + DynamoDB table | a DynamoDB table to create and pay for | Rejected — superseded |
| Terraform Cloud | free tier, external dependency | Rejected |
| Keep local state | none | Rejected — violates §3.8 |

## Decision

S3 backend with `use_lockfile = true`, CMK encryption, versioning, and a bucket
**created out of band and never managed by the stack that stores state in it**.

```hcl
# terraform/envs/dev/backend.hcl  (gitignored; backend.hcl.example is tracked)
bucket       = "<state-bucket>"
key          = "aws-cdc-lakehouse/dev/terraform.tfstate"
region       = "ap-southeast-1"
encrypt      = true
kms_key_id   = "<state CMK ARN>"
use_lockfile = true
```

S3-native locking rather than DynamoDB: it removes a resource, removes a cost, and
removes the classic failure where the lock table is destroyed with the stack and
leaves state unlockable.

**The bucket must not be in this stack's state.** If it were, `terraform destroy`
would delete the bucket holding the state describing what it is deleting — mid-destroy.
Repo A's comment already warns of this; ADR-021 makes it a rule.

## Consequences

- A one-time out-of-band bootstrap (CLI or a tiny separate stack) before the first
  apply. Session 02 step 3.
- Versioning is the recovery path for R8: state loss means orphaned billable
  resources invisible to Terraform, which is precisely the failure this budget
  cannot absorb.
- Bucket policy restricts access to the operator principal; BPA on; TLS-only.
- The bucket outlives every window — deliberately. Its cost is ~$0.01/month.

## Cost

Effectively zero: a few hundred KB of versioned state at $0.025/GB-Mo, plus the state
CMK. If the state CMK is shared with the lake CMK the marginal cost is $0; a separate
key adds $1.00/month and is the cleaner choice, since state and data have different
audiences.

## Security

Encrypted at rest with a CMK, TLS in transit, versioned, IAM-restricted, and never
output in plaintext. Satisfies `CLAUDE.md` §3.8 fully. Note that state remains
sensitive in content regardless of encryption — `terraform show` of a plan file must
not be pasted into logs or issues.

## Rollback

`terraform init -migrate-state` moves state between backends in either direction.
Migrating **to** S3 from the current local state is the forward step; the reverse
works if ever needed.

## Validation

- `terraform init` reports the S3 backend, not local.
- Two concurrent `terraform plan` runs: the second is blocked by the lock.
- The bucket has versioning enabled and BPA on.
- `terraform state list` does **not** contain the state bucket itself.
