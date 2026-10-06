# ADR-026 — Query-engine feature flags and mutual exclusion

- Status: **ACCEPTED** (Session 01)
- Required by: `DECISIONS.md:34`. Closes defect D17.
- Related: ADR-012, ADR-013, ADR-014

## Context

Three optional query engines, one small budget. The rule that Redshift Serverless and
Trino must not both be enabled in the lab is stated in **three places with three
different strengths**:

| Source | Form | Machine-checkable |
|---|---|---|
| `ARCHITECTURE.md:199` | "Không bật cùng Redshift trong lab trừ khi session benchmark yêu cầu" | no |
| `reference/SERVING_LAYER_STRATEGY.md:103` | prose guidance | no |
| `CLAUDE.md` §8 | "Không bật đồng thời … nếu chưa explicit allow/cost approval" | no |
| `reference/TERRAFORM_MODULE_MAP.md:39` | `allow_multiple_optional_query_engines = false` | **yes** |

That was defect D17. A rule expressed only in prose is enforced only by whoever
happens to read it.

## Options

| Option | Verdict |
|---|---|
| **Terraform `precondition` on the root module** | **CHOSEN** |
| Documentation only | Rejected — the status quo that produced D17 |
| Separate workspaces per engine | Rejected — heavier, and hides the cost interaction |

## Decision

`allow_multiple_optional_query_engines = false`, enforced by a `precondition` that
**fails at plan time**:

```hcl
lifecycle {
  precondition {
    condition = var.allow_multiple_optional_query_engines || !(
      var.enable_redshift_serverless && var.enable_trino
    )
    error_message = <<-EOT
      enable_redshift_serverless and enable_trino cannot both be true unless
      allow_multiple_optional_query_engines = true. Combined cost is ~$4.02/hr
      (docs/COST.md sections 4 and 5) — over a quarter of the monthly budget per
      session. Set the override deliberately and record the reason in
      DECISION_LOG.md.
    EOT
  }
}
```

Plan time, not apply time, so the failure costs nothing and arrives while the operator
is still reading. The error message carries the **cost** and the required
**record-keeping action**, not just a prohibition — an error that only says "not
allowed" invites someone to flip the override without understanding why it exists.

Full flag matrix: `docs/TARGET_ARCHITECTURE.md` §8. Defaults: `enable_athena = true`,
every other optional flag `false`, per `CLAUDE.md` §4.10.

## Consequences

- Running both engines requires an explicit override plus a `DECISION_LOG.md` entry.
- Combined cost is ~$4.02/hr — $12.07 for a 3-hour session on top of the lab stack,
  a quarter of the monthly budget.
- Every optional module still needs its own destroy verification; the flag prevents
  co-enablement, not abandonment.
- The precondition lives on a root-module resource, so it evaluates on every plan
  regardless of which modules are enabled.

## Cost

Zero to implement. Prevents an unbudgeted ~$4.02/hr combination.

## Security

Neutral. Fewer enabled engines is a smaller attack surface, incidentally.

## Rollback

Set the override to `true`. Deliberately a one-line, visible, reviewable change.

## Validation

- `enable_redshift_serverless = true` **and** `enable_trino = true` with the override
  `false` → **plan fails** with that message. Tested, not assumed.
- The same with the override `true` → plan succeeds.
- Defaults produce zero optional-engine resources in the plan.
- `scripts/validate-docs.py` confirms every `enable_*` flag in `docs/COST.md` appears
  in the flag matrix and vice versa.
