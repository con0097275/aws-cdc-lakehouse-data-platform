# artifacts/validation/final-e2e

Evidence for the 2026-09-03 cost/safety gate and live E2E test plan.
Plan document: `docs/E2E_LIVE_TEST_PLAN.md`.

Produced read-only: `terraform fmt`, `validate`, `plan -out=tfplan`, and AWS
`describe`/`list`/`head` calls. **No `apply`. No resource created or started. $0.00.**

| File | What |
|---|---|
| `00-gate-identity-and-backend.txt` | profile/region/env/account, STS, Terraform + provider versions, backend pin, pre-apply state, fmt/validate |
| `01-billable-now.txt` | `scripts/tf.sh verify` — what bills right now |
| `02-idle-cost-inventory.txt` | `scripts/show-cost-resources.sh --idle` |
| `03-compute-inventory.txt` | `scripts/show-cost-resources.sh --compute` |
| `04-plan-human.txt` | `terraform show tfplan` (8,837 lines) |
| `05-cost-envelope.txt` | hourly baseline, 2h/4h expected + hard cap, retained cost |
| `plan-diff-analysis.txt` | action census, NO-GO tripwire counts, cost-driver detail |
| `tfplan.saved` | byte copy of the saved plan |
| `tfplan.json` | `terraform show -json tfplan` |
| `tf-plan-run.txt` | full stdout of `scripts/tf.sh plan` |

## Headline

```text
248 to add, 0 to change, 0 to destroy, 0 to replace
1 VPC   1 MSK   0 NAT   0 EKS   0 Redshift   0 OpenSearch   0 SageMaker endpoint
0 EIP   0 RDS   0 Secrets Manager
baseline $1.2340/hr  (MSK = 62%)
2h hard cap $8.00    4h hard cap $15.00
```

**The saved plan is NOT approved for apply as-is** — `auto_destroy_after` is expired and
`monthly_budget_usd` fails its check assertion. See §3 of the plan document.
