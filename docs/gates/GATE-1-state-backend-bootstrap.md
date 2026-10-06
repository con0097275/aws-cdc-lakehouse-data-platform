# GATE 1 PACKAGE — Terraform state backend bootstrap (OPEN-01)

- Session: 02 Stage A
- Status: **UNBLOCKED 2026-08-15 (Session 20) — the script now complies with ADR-030.**
  Awaiting the operator's typed confirmation phrase; that is the only thing outstanding.
- Gate: `docs/APPROVAL_GATES.md` §1
- Script: `scripts/bootstrap-state-backend.sh`
- Decision: [ADR-021](../adr/ADR-021-terraform-state-backend.md), **amended by
  [ADR-030](../adr/ADR-030-budget-of-record-and-release-schedule.md)**. Closes OPEN-01, Gap 1, risk R8

> ## ✅ BLOCKER CLEARED 2026-08-15 (Session 20)
>
> The remediation below had **already been implemented** in the script; only this document
> was never refreshed, so the gate has been reading `BLOCKED` while actually being
> satisfied. Verified against `scripts/bootstrap-state-backend.sh` on 2026-08-15:
>
> | Requirement | State |
> |---|---|
> | Drop the CMK step (was 1/5) | **Done** — the script has **4** steps and creates no KMS key, alias or rotation |
> | `--server-side-encryption-configuration` = `AES256` | **Done** — `SSEAlgorithm: AES256`, `BucketKeyEnabled: true` |
> | Re-run the dry run and refresh the transcript | **Done** — `artifacts/validation/session-20/gate1-dry-run.txt` |
>
> The dry run makes exactly five mutating calls, all `s3api`, none KMS:
> `create-bucket`, `put-bucket-encryption`, `put-bucket-lifecycle-configuration`,
> `put-bucket-policy`, `put-bucket-tagging`.
>
> **Also fixed in Session 20:** the script declared six tag values (`TAG_PROJECT`,
> `TAG_ENVIRONMENT`, `TAG_OWNER`, `TAG_COST_CENTER`, `TAG_AUTO_DESTROY_AFTER`) and applied
> **none** of them — surfaced by shellcheck as unused once OPEN-06 closed. The bucket would
> have been created untagged and therefore invisible to `budget_guardrails`, which filters
> on `user:Project$kafka-dev-lab`. `put-bucket-tagging` is now step 2/4's final action.
>
> **What remains:** the typed confirmation phrase `BOOTSTRAP STATE BACKEND`.
> `confirm_destructive()` (`lib.sh:161`) refuses a non-TTY stdin by design, and
> `scripts/validate-docs.py:515` asserts that refusal exists. **It must be run from a real
> interactive terminal** — not from a CI job, not from an agent, and not through a
> non-interactive shell wrapper.
>
> ---
>
> <details><summary>Original blocker text, retained for the audit trail</summary>
>
> **ADR-030 amended ADR-021: the state bucket must use SSE-S3, not a dedicated KMS CMK.**
> A dedicated state CMK costs **$1.00/month — 3.3 % of the real $30 budget** — for a
> bucket already restricted by IAM and a bucket policy, whose contents have exactly one
> audience.
>
> **`scripts/bootstrap-state-backend.sh` still implements the old decision.** Step 1/5
> creates a CMK with an alias and rotation, and step 3/5 applies SSE-KMS against it.
> Running it as written creates a resource ADR-030 says should not exist.
>
> **This matters more than $1.00/month, because it is not reversible in place:**
> changing a bucket's default encryption **does not re-encrypt objects already written**.
> Getting it wrong means either living with it or recreating the backend and migrating
> state — exactly the operation ADR-021 exists to avoid.
>
> **Required before this gate can be approved:** Session 02 must update the script to drop
> step 1/5 and set `--server-side-encryption-configuration` to `AES256`, then re-run the
> dry run and refresh the transcript in §4 of this document. Session 01 deliberately did
> not edit the script — that is Session 02 implementation work.
>
> </details>

---

## 1. Why this is a shell script and not Terraform

The bucket that holds the state must never be managed by the stack whose state it holds.
If it were, `terraform destroy` would delete the bucket containing the state describing
what it is deleting — mid-destroy, leaving orphaned billable resources that Terraform can
no longer see.

Repo A's `backend.hcl.example:1` already carried the right instruction — *"Create the
backend separately. Do not let the disposable lab delete its own state backend."* —
and ADR-021 promotes it from a comment to a rule. Under ADR-027 the whole lab is
destroyed between every metered window, so this is not a theoretical edge case; it is
what would happen roughly eight times a month.

Repo A's state today is **local files** (`terraform.tfstate` serial 106 next to the
code), which violates `CLAUDE.md` §3.8 on all three counts — not encrypted, not locked,
not IAM-restricted. The backup state file contains the MSK cluster ARN, VPC and subnet
IDs, the KMS key ID and an SSM parameter name.

## 2. What will be created

| # | Resource | Configuration | Monthly cost |
|---|---|---|---:|
| 1 | KMS CMK | symmetric, rotation enabled, alias `alias/aws-cdc-lakehouse-tfstate` | **$1.00** |
| 2 | S3 bucket | `aws-cdc-lakehouse-tfstate-111122223333` | ~$0.01 |
| 3 | Versioning | `Enabled` — the recovery path for risk R8 | — |
| 4 | Block Public Access | all four settings `true` (`CLAUDE.md` §3.5) | — |
| 5 | Ownership controls | `BucketOwnerEnforced` — ACLs disabled | — |
| 6 | Default encryption | SSE-KMS on CMK #1, bucket key enabled | — |
| 7 | Lifecycle | noncurrent state versions expire after 90 days; incomplete MPUs after 7 | — |
| 8 | Bucket policy | deny non-TLS (`aws:SecureTransport=false`); deny non-KMS `PutObject` | — |
| 9 | `terraform/envs/dev/backend.hcl` | generated locally, gitignored | — |

The account ID is in the bucket name because S3 bucket names are globally unique.

**All six required tags** (`CLAUDE.md` §4.11) are applied to the CMK:
`Project=kafka-dev-lab`, `Environment=dev`, `ManagedBy=bootstrap-script`,
`Owner=my-aws-profile`, `CostCenter=learning`, `AutoDestroyAfter=never`.

`AutoDestroyAfter=never` is deliberate and is the one place in this project where it is
correct. The state backend is **exempt from the ephemeral lifecycle** — it outlives
every window by design, so `verify-destroy` must assert it still **exists** rather than
that it is gone.

## 3. Cost impact

This adds a **third** always-on CMK. `docs/COST.md` §3.2 previously listed two (platform
and lake):

```
floor                          4.60  ->  5.61 USD/month
available for metered windows 75.40  -> 74.39 USD/month
affordable 6-hour windows       7.98  ->  7.87
```

The envelope conclusion — "~8 windows per month" — does not change. `docs/COST.md` §3.2
and §3.3 have been updated so the line item is not invisible.

ADR-021 notes that SSE-S3 would make this free and explicitly chose a CMK anyway: state
and lake data have different audiences, and a shared key makes least-privilege key
policies impossible to write. That decision is being followed, not reopened.

## 4. Gate 1 checklist (`docs/APPROVAL_GATES.md` §1)

| # | Requirement | Status |
|---|---|---|
| 0.1–0.4 | Identity, profile, region, environment | `require_identity` refuses on any mismatch. Verified account `111122223333`, profile `my-aws-profile`, region `ap-southeast-1` |
| 0.5 | Working tree committed | branch `session-02-prerequisites`; commit before executing |
| 0.6 | Static checks pass | `make validate-docs`; `bash -n` clean; `checkov` 3.3.10 present. **`tflint` absent — OPEN-06** |
| **0.7** | **Budget notifies someone** | **PASSES** — an account-wide $30 budget already notifies `owner@example.com`. Verified live; see §6 |
| 1 | Saved plan | n/a — not Terraform. Every API call is printed by the dry run, saved at `artifacts/validation/session-02/open-01/dry-run.txt` |
| 2 | Resource action counts | **2 created** (1 CMK, 1 bucket), 0 changed, 0 destroyed, 0 replaced |
| 3 | Cost delta | +$1.01/month always-on. Month-to-date $0.00 → $1.01 against $80 |
| 4 | Security assertions | no security group, no ingress, no key pair, no secret. BPA on, TLS-only, KMS-only uploads, ACLs disabled |
| 5 | Destroy plan | §5 below. **Not routine** — this resource is meant to survive |
| 6 | `auto_destroy_after` | `never`, deliberately — §2 |

## 5. Reversal

Not a routine action. Destroying the backend while a stack depends on it orphans every
resource that stack manages.

```bash
# 1. Empty the bucket, including every version (versioning is on).
aws s3api delete-objects --bucket aws-cdc-lakehouse-tfstate-111122223333 \
  --delete "$(aws s3api list-object-versions \
      --bucket aws-cdc-lakehouse-tfstate-111122223333 \
      --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
      --profile my-aws-profile --region ap-southeast-1)" \
  --profile my-aws-profile --region ap-southeast-1

# 2. Delete the bucket.
aws s3api delete-bucket --bucket aws-cdc-lakehouse-tfstate-111122223333 \
  --profile my-aws-profile --region ap-southeast-1

# 3. Schedule the CMK for deletion. Gate 5 requires this be approved SEPARATELY
#    from the destroy itself — it is irreversible after the pending window, and the
#    key keeps billing $1.00/month throughout it.
aws kms schedule-key-deletion --key-id alias/aws-cdc-lakehouse-tfstate \
  --pending-window-in-days 7 --profile my-aws-profile --region ap-southeast-1
```

Migrating away instead of deleting is usually the right move:
`terraform init -migrate-state` moves state between backends in either direction.

## 6. Gate 0.7 — resolved, and it is not what Session 01 recorded

Session 01 marked precondition 0.7 **FAILING** because repo A sets `budget_email = ""`.
That inspected *code*, not the account. Verified read-only on 2026-08-12
(`artifacts/validation/session-02/aws/gate-0.7-budgets.txt`):

| Budget | Limit | Filter | Notifications | Subscriber |
|---|---:|---|---|---|
| `My Monthly Cost Budget` | $30.00 | **none — account-wide** | ACTUAL >85 %, ACTUAL >100 %, FORECASTED >100 % | **owner@example.com** |
| `My Zero-Spend Budget` | $1.00 | none — account-wide | ACTUAL > $0.01 (currently in `ALARM`) | — |

**Gate 0.7 passes**, so this bootstrap is not blocked. It also dissolves what would
otherwise have been a genuine circular dependency: the budget is Terraform,
Terraform needs a backend, and the backend cannot wait for the budget. The account-level
budget already covers the gap, and it does so *better* than the tag-filtered budget
Stage B will add, because an unfiltered budget cannot miss an untagged resource.

Two things still follow:

1. **OPEN-09** — the live budget is **$30** while `docs/COST.md` sizes the envelope
   against **$80**. At $30 the affordable window count is ~2.6, not ~7.9. The operator
   must reconcile these before Stage B applies anything hourly. This bootstrap's
   $1.01/month is inside either figure.
2. `modules/budget_guardrails` should still be the **first** thing applied in Stage B,
   so the project gets its own tag-scoped attribution on top of the account-wide net.

## 7. Approval

```bash
cd /path/to/aws-cdc-lakehouse

# review — prints every API call, makes no writes
bash scripts/bootstrap-state-backend.sh

# execute
bash scripts/bootstrap-state-backend.sh --execute
```

A typed confirmation phrase is required, and the script refuses to run
non-interactively. It is idempotent: every step checks for the existing resource first,
so a re-run after a partial failure is safe.

## 8. Post-execution verification

```bash
B=aws-cdc-lakehouse-tfstate-111122223333
P="--profile my-aws-profile --region ap-southeast-1"

aws s3api get-bucket-versioning     --bucket $B $P    # Status: Enabled
aws s3api get-public-access-block   --bucket $B $P    # all four true
aws s3api get-bucket-encryption     --bucket $B $P    # aws:kms
aws s3api get-bucket-policy         --bucket $B $P    # DenyInsecureTransport present
curl -s -o /dev/null -w '%{http_code}\n' "http://$B.s3.ap-southeast-1.amazonaws.com/"   # 403
```

And after the first Stage B apply, the assertion that matters most:

```bash
terraform -chdir=terraform/envs/dev state list | grep -c aws-cdc-lakehouse-tfstate    # 0
```

A non-zero count means the stack manages its own state bucket — the exact failure
ADR-021 exists to prevent. Fix it before any destroy.

Then record the bucket, the CMK ARN and the $1.01/month in `PROJECT_STATE.md` under
*Live resources / cost drivers*. Gate 1's post-apply step is mandatory: an unrecorded
live resource is an unbounded cost.
