# FINAL COST REPORT — 2026-09-03

## This window
```
start (true)     2026-09-03T09:06:04Z
elapsed          151 min
platform         ~$3.11   at $1.2340/hr measured baseline
EMR Serverless   ~$0.60      across ~20 job runs (incl. 6 deliberate/defect failures)
Athena           <$0.01      every query in the low kilobytes, 10 GiB cutoff enforced
TOTAL            ~$3.72
```

Against the plan's hard caps: **$8.00 (2 h) / $15.00 (4 h)**. Actual is well inside.

## Hourly baseline, measured not estimated

| Component | \$/hr | Share |
|---|---|---|
| MSK 3 x kafka.m7g.large | 0.7650 | **62.0%** — cannot be stopped, only destroyed |
| EC2 x4 | 0.4264 | 34.6% |
| EBS 150 GiB + MSK storage 60 GiB | 0.0296 | bills while STOPPED too |
| Glue interface endpoint | 0.0130 | per AZ |
| NAT | 0.0000 | none |
| **Baseline** | **1.2340** | |

## Retained after teardown

| Item | \$/month |
|---|---|
| lake CMK 2fd510a7 + MSK CMK b43ae6ae | 2.00 |
| 6 further lake CMKs from earlier cycles | 6.00 — consolidate then schedule deletion |
| S3 lake (~90 MB) | ~0.01 |

## Governance note

`monthly_budget_usd = 100` still fails the `<= 50` check assertion (main.tf:95, ADR-030
as amended). ADR-030's own document still reads "\$30/month" — the \$50 amendment lives
only in the check block. **`auto_destroy_after` is expired**, so no automatic teardown
exists; a wall-clock alarm is the only control.
