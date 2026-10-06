# ADR-031 — Secrets live in SSM Parameter Store SecureString, not Secrets Manager

- Status: **ACCEPTED** (Session 01 re-derivation, 2026-08-13)
- Related: ADR-030 (this is its single largest floor saving), ADR-027

## Context

`docs/COST.md` §3.2 budgeted **4 Secrets Manager secrets** — Oracle, SQL Server, registry
and Grafana credentials — at $0.40/secret-month, **$1.60/month**. That is **5.3 % of the
$30 budget**, billed continuously whether or not any lab session runs, because secrets
survive `terraform destroy` of the compute stack.

`CLAUDE.md` §3.6 does not mandate Secrets Manager. It says secrets must be fetched at
runtime from **"Secrets Manager/SSM SecureString"** — either store satisfies the
invariant. The choice was never actually made; Secrets Manager was assumed.

Repo A, the audited and proven design in this account, **already uses SSM SecureString**:
it generates the Grafana password at apply time, writes it to an SSM SecureString
parameter, and never exposes it in a Terraform output
(`docs/EXISTING_PLATFORM_AUDIT.md`; `GAP_ANALYSIS.md` §6 lists this as a pattern to copy).
So the project was going to pay $1.60/month to introduce a *second* secret store
alongside one that already worked.

## Options

| Option | Mechanism | Monthly | Verdict |
|---|---|---:|---|
| **A** | 4 Secrets Manager secrets | $1.600 | Rejected — pays for rotation this lab never uses |
| **B** | **4 SSM Parameter Store SecureString parameters (standard tier)** | **$0.000** | **CHOSEN** |
| C | Advanced-tier SSM parameters | $0.20 | Rejected — advanced tier buys size and policies, neither needed |
| D | Plaintext in tfvars / user-data | $0.000 | **Forbidden** — `CLAUDE.md` §3.1 |

## Decision

**Use SSM Parameter Store SecureString, standard tier, encrypted with the platform CMK.**

Standard-tier parameters are free: no per-parameter charge and no API charge below the
standard throughput limit. The only cost is KMS API usage for encrypt/decrypt, billed at
$0.03 per 10,000 requests — for a lab fetching four secrets a handful of times per window,
that rounds to zero.

**What is given up.** Secrets Manager offers automatic rotation, cross-region replication
and a resource policy per secret. This project needs none of them: the Oracle and SQL
Server credentials belong to containers that are destroyed at the end of every window, and
the registry and Grafana credentials are regenerated at apply time. **Rotation of a
credential whose owner is destroyed every six hours is not a feature.**

If a later session introduces a long-lived credential that genuinely needs rotation —
a production RDS metadata database, say — that single secret moves to Secrets Manager and
pays its $0.40. The decision is per-secret, not global.

## Consequences

**Accepted:**

- Session 02's `lake_iam` roles grant `ssm:GetParameter` / `ssm:GetParameters` on
  `/kafka-dev-lab/dev/*` plus `kms:Decrypt` on the platform CMK — **not**
  `secretsmanager:GetSecretValue`. A role granted only one of the two fails closed, which
  is the desired behaviour and must be tested.
- Parameter naming is fixed at `/kafka-dev-lab/dev/<workload>/<key>`. Nothing sensitive
  may appear in a parameter *name*, because names are broadly listable even when values
  are not.
- Sessions 03, 04 and 05 fetch credentials from SSM at container start. Any Debezium or
  Apicurio configuration example that references Secrets Manager is now wrong.
- `docs/COST.md` §3.2 drops the Secrets Manager line to zero. This is the single largest
  item in ADR-030's floor reduction.

**Deliberately kept open:** the decision is **per-secret, not global**. If a later session
introduces a genuinely long-lived credential needing rotation — a production RDS metadata
database for Airflow, say — that one secret moves to Secrets Manager and pays $0.40. This
ADR does not forbid Secrets Manager; it removes it as the unexamined default.

**Risk introduced:** SSM standard-tier parameters have a 4 KB value limit and a shared
throughput limit. Neither binds for four short credentials, but a future session that
tries to store a certificate chain or a large JSON keyfile will hit the 4 KB ceiling and
must use advanced tier ($0.05/parameter-month) or S3+KMS rather than silently truncating.

## Cost

| | Secrets Manager | SSM SecureString |
|---|---:|---:|
| 4 secrets, monthly | **$1.600** | **$0.000** |
| Share of the $30 budget | **5.3 %** | 0 % |
| KMS API (≈2,000 req/month) | included | ~$0.006 |

**Saves $1.60/month — the single largest item in ADR-030's floor reduction**, and it is
recovered with no design compromise whatsoever. In lab-time terms, $1.60/month buys
roughly **1 additional hour of full-stack lab per month** at $1.5323/hr.

## Security

**This is not a downgrade.** SSM SecureString and Secrets Manager offer the same
protection for the property that matters here:

- Encrypted at rest with a customer-managed KMS key; access requires both the SSM
  permission **and** `kms:Decrypt` on the key — the same two-key structure Secrets Manager
  uses.
- Never rendered in a Terraform output (`CLAUDE.md` §3.6), never written to Git, never
  placed in container environment variables in plaintext (§3.1).
- Fetched at runtime by the workload role via the default credential chain (§3.2).
- Read access is auditable in CloudTrail identically for both services.

Two genuine differences, both accepted deliberately:

1. **No automatic rotation.** Addressed above — the credential holders are ephemeral.
2. **Parameter *values* are hidden but parameter *names* are broadly listable.** Do not
   encode anything sensitive in a parameter name. Naming follows
   `/kafka-dev-lab/dev/<workload>/<key>`, which reveals only structure.

`terraform state` will still contain the secret value for any parameter Terraform
creates — that is a property of Terraform, not of the store, and it is exactly why
ADR-021 requires the state backend to be encrypted and IAM-restricted.

## Rollback

No infrastructure exists yet, so rollback is a document change:

```bash
git checkout docs/COST.md
rm docs/adr/ADR-031-secret-store.md
```

If SSM proves inadequate after implementation, migrating a parameter to Secrets Manager is
a create-and-cutover, not a data migration: write the value into a new secret, repoint the
workload's fetch, delete the parameter. Cost returns to $0.40/secret-month. No data is
lost in either direction because the values are regenerated at apply time.

## Validation

- `python3 scripts/derive-cost-envelope.py` prices `ssm_secure_params` at zero and shows
  the floor moving from $5.60 to $2.28; the $1.60 delta is this ADR's contribution.
- Session 02 must assert, as a plan-time check, that **no `aws_secretsmanager_secret`
  resource is created by default** — the flag exists but the count is zero.
- Session 04/05 must verify at runtime that the Connect and source-lab roles can
  `ssm:GetParameter` **and** `kms:Decrypt`, and that a role missing either one fails
  closed. A secret store that silently returns nothing is worse than one that errors.
- Status: **`static-validated`**. Pricing is arithmetic; the runtime behaviour is
  `planned` until Session 04 tests it.
