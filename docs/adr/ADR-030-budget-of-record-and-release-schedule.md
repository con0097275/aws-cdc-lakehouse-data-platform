# ADR-030 — Budget of record is $30/month; the core release spans two months

- Status: **ACCEPTED** (Session 01 re-derivation, 2026-08-13)
- Supersedes: the `$80` envelope throughout `docs/COST.md` and `docs/SESSION_DEPENDENCY_GRAPH.md` §5
- Related: ADR-021 (amended here), ADR-027, ADR-031

## Context

`docs/COST.md` sized this entire project against **$80/month**. Session 00's closeout
found that number was never decided — it is `monthly_budget_usd = 80` in
`kafka-aws-production-lab/terraform/terraform.tfvars:26`, inherited verbatim from a
repository tagged `kafka-prod-lab` and carried into planning unexamined. The account's
actual budget is **$30** (`aws budgets describe-budgets`, `My Monthly Cost Budget`).
The operator confirmed $30 is authoritative on 2026-08-13.

At $30 the previous plan does not fit. `docs/SESSION_DEPENDENCY_GRAPH.md` §5 concluded
the core release costs ≈ $48.52 and "fits inside one month's $80 budget with ~$31 of
headroom". Against $30 that is a **62 % overrun**, not headroom.

Two structural facts shape every option:

1. **MSK is 50.6 % of the full-stack hourly burn** — `$0.7749` of `$1.5323`/hr. Nothing
   else comes close; the next largest is EMR Serverless NRT at 19.7 %.
2. **The always-on floor was 18.7 % of the budget** ($5.60 of $30) before this ADR —
   money spent whether or not any lab session runs.

## Options

| Option | Mechanism | Core release | Verdict |
|---|---|---:|---|
| **A** | Floor + window optimisation, 3 brokers, `t3.xlarge` source lab | $30.25 | Rejected — **over budget**, zero re-run headroom |
| **A-** | As A, source lab shrunk to `t3.large` | $28.25 | **CHOSEN**, with the schedule change below |
| **B** | As A-, MSK reduced to **2 brokers / 2 AZ** | $23.34 | Rejected as default; retained as a costed fallback |
| **C** | Keep the design, spend two months | — | **CHOSEN**, combined with A- |

## Decision

**Take A-, and stop requiring the core release to fit in one calendar month.**

The instinct at a reduced budget is to degrade the architecture. That is the wrong
instinct here, because **the schedule is free and the architecture is not.** The
always-on floor after optimisation is $2.28/month, so carrying the project across a
second month costs $2.28 — far less than what option B's durability reduction would
"save", and it costs nothing in design quality.

### Split

| Month | Windows | Sessions | Windows | Floor | Total | Headroom |
|---|---|---|---:|---:|---:|---:|
| **1** | W1, W2, W3 | 03→07, 09 — the Kafka-dependent chain | $15.74 | $2.28 | **$18.02** | **$11.98** |
| **2** | W4, W5 | 08, 10–15, 17–19 | $10.23 | $2.28 | **$12.51** | **$17.49** |

Month 1 carries the sessions most likely to need a second attempt (W2 builds L1 and L2)
and gives them **$11.98 of headroom — two full W2 re-runs**. The single-month plan gave
$1.75, which would not have covered even one.

### What is NOT degraded

- **3 brokers, 3 AZ, RF=3** stand. `CLAUDE.md` §2 mandates 3 AZ, and replication factor 3
  with `min.insync.replicas=2` is the property that makes this a production-grade Kafka
  design rather than a demo. Degrading the centrepiece of a portfolio project to save
  $4.91/month is a bad trade.
- **`kafka.m7g.large` stands** — it is forced, not chosen (OPEN-03), and is already the
  cheapest broker offered in `ap-southeast-1`.
- No `enable_*` flag defaults change. No optional engine is enabled.

### Option B is retained as a costed fallback, not a plan

If month 1 overruns despite the headroom, dropping to **2 brokers / 2 AZ** saves
**$4.91** across the sequence. It requires an explicit amendment to `CLAUDE.md` §2 and
reduces RF to 2 / `min.insync.replicas` to 1 — a single broker failure then risks losing
unacknowledged writes. Recorded with the number attached so the trade can be made
deliberately under pressure rather than improvised.

## Consequences

**Accepted:**

- The core release takes **two calendar months**, not one. Any plan, README or portfolio
  narrative claiming a one-month build is now wrong and must be corrected.
- **ADR-021 is amended**: the Terraform state bucket uses SSE-S3, not a dedicated CMK.
  This must be settled *before* `scripts/bootstrap-state-backend.sh` runs, because
  changing bucket encryption afterwards does not re-encrypt existing objects.
- **ADR-031 is created** to carry the Secrets Manager → SSM decision, which is large
  enough to need its own security analysis.
- CloudWatch alarms move inside the platform module's `enable_*` flag. Session 15 must not
  assume alarms exist between windows — **there is no monitoring when the lab is down**,
  which is correct, because there is nothing to monitor.
- Session 02 must implement `monthly_budget_usd = 30` and a 3-day log retention default.
- One optional engine session (13B at $20.25) now consumes **68 %** of a month. Both
  optional engines in one month is impossible.

**Rejected, and why it matters later:** option B (2 brokers) is not merely deferred — it is
recorded with its price so that if month 1 overruns, the fallback is a *decision with a
number*, not a panic. Anyone taking it must amend `CLAUDE.md` §2 and accept RF=2.

**Risk introduced:** a two-month schedule means the always-on floor is paid twice, and an
abandoned project leaks $2.28/month indefinitely. `ADR-027`'s `AutoDestroyAfter` tag and
`verify-destroy` are the controls; Session 17 must test that the floor really is $2.28 and
not something larger that nobody noticed.

## Cost

Floor, before and after (`python3 scripts/derive-cost-envelope.py`):

| Item | Baseline | Optimised | Change |
|---|---:|---:|---|
| S3 lake storage (10 GiB) | 0.250 | 0.250 | — |
| KMS CMKs | 3.000 | **2.000** | state bucket → SSE-S3; **amends ADR-021** |
| Secrets Manager (4 secrets) | 1.600 | **0.000** | → SSM SecureString (ADR-031) |
| CloudWatch alarms (6) | 0.600 | **0.000** | alarms become ephemeral with the cluster |
| CloudWatch Logs | 0.150 | **0.030** | 5 GiB/7 d → 1 GiB/3 d |
| State bucket | 0.010 | 0.010 | — |
| **Floor** | **5.60** | **2.28** | **−$3.32/month, −59 %** |

$3.32/month is **11.1 % of the entire budget**, recovered without touching a single
design decision. That is why the floor was attacked first.

**Alarms become ephemeral** because CloudWatch alarms watching an MSK cluster that exists
~15 hours a month are not observability — they are a subscription. They move into the
`enable_*` flag of the platform module and are destroyed with it.

**OPEN-12 closed, negative.** `express.m7g.large` is **$0.5100/hr against
`kafka.m7g.large`'s $0.2550** — exactly 2×. It bundles broker storage, which is worth
$0.0033/hr at 20 GiB. Paying $0.2550 to save $0.0033 is a 77× loss. MSK Express is for
high-throughput production, not a bounded lab.

## Security

Nothing in this ADR weakens a security control.

- **The state CMK → SSE-S3 change is the one that touches security posture**, and it is
  narrow: the state bucket remains encrypted at rest, private, versioned, TLS-only and
  IAM-restricted. What is lost is a *separate key policy* as a second access boundary.
  ADR-021 chose a CMK so that state and lake data could have different key audiences —
  sound reasoning, but at $30 that boundary costs 3.3 % of the entire budget, and this
  project's state has exactly one audience: the operator.
- **Platform and lake CMKs stay separate** (S01-11 stands). Those genuinely do have
  different audiences — Kafka operations versus data consumers — and merging them would
  make least-privilege key policies unwritable.
- Reducing CloudWatch Logs retention to 3 days shortens the forensic window. Acceptable
  for a lab that runs in supervised windows; it would not be acceptable in production.

## Rollback

Every element is a variable or a document, and nothing here has been applied:

```bash
git checkout docs/COST.md docs/SESSION_DEPENDENCY_GRAPH.md docs/adr/ADR-021-terraform-state-backend.md
rm docs/adr/ADR-030-budget-of-record-and-release-schedule.md
```

To reverse only the state-bucket key decision, restore `kms_key_arn` on the state bucket
in `scripts/bootstrap-state-backend.sh` and re-run it — the script is idempotent, but note
that **changing encryption on an existing bucket does not re-encrypt existing objects**.
Decide before the bucket is created, not after.

## Validation

- `python3 scripts/derive-cost-envelope.py --budget 30` reproduces every figure above
  from the unit prices in `docs/PRICE_REFERENCE.md`. No total is hard-coded.
- `make validate-docs` — check 11 asserts every price in `docs/COST.md` traces to
  `docs/PRICE_REFERENCE.md`.
- Status: **`static-validated`**. Arithmetic over a dated price list. Nothing here has met
  a real invoice, and the model is not confirmed until `aws ce get-cost-and-usage` is run
  after the first metered window and the delta recorded (`docs/COST.md` §7).

---

## Amendments

This ADR's number has been raised twice, both on the operator's explicit instruction. The
guard in `terraform/envs/dev/main.tf` is amended to match each time rather than removed —
its purpose is to make the NEXT change deliberate, not to freeze a figure.

| Date | Budget | Why |
|---|---|---|
| 2026-08-13 | **$30** | The account's real budget, confirmed against `aws budgets describe-budgets`. Replaced an undecided `monthly_budget_usd = 80` inherited from repo A |
| 2026-08-23 | **$50** | $30 was reached ($32.64 actual) before P0-1 — proving scheduled execution — had been attempted. An Airflow deployment does not fit inside $30 |
| **2026-09-06** | **$100** | Operator-confirmed. The ACCOUNT already carried it: both `My Monthly Cost Budget` and `kafka-dev-lab-dev-monthly` read 100.0 USD. Only this document and the assert were stale, so every plan since 2026-08-23 emitted a budget warning |

**The lesson of the second gap is worth more than the number.** For two weeks the code
asserted `<= 50` while tfvars carried `100` and the account carried `100`. Every `terraform
plan` printed a budget warning that was correct about the disagreement and wrong about the
budget. A check that always fires is a check operators learn to scroll past — so when a
guard and reality disagree, fix the guard or fix reality, but never leave them arguing.

### What $100/month actually buys

At the measured baseline of **$1.1244/hr** (MSK is ~68% of it and cannot be stopped, only
destroyed), excluding metered EMR:

| | hours | note |
|---|---|---|
| Full month at $100 | ~89 h | if nothing else is spent |
| A 6-hour window | ~$6.75 | ~13 windows/month |
| Idle between windows | ~$0.30/day | KMS CMKs + S3 only, after `destroy` |

The dominant risk is not the hourly rate — it is **leaving the platform up**. 24/7 for a
month is ~$810, eight times the budget. `auto_destroy_after` and the wall-clock alarm exist
for exactly that.
