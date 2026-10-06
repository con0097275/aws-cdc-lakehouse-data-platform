# Data Platform Copilot — User & Test Guide

How to use the AI layer on your AWS CDC lakehouse, what it can and cannot do, and how to
prove every answer it gives you.

Verified live against account `111122223333` on 2026-08-29.

---

## 1. What it is

A **read-only business copilot** over your governed mart. It answers business questions with
real numbers from Athena, and it is deliberately **not** a chatbot:

| The copilot does | The copilot never does |
|---|---|
| resolve a business phrase to ONE governed metric | invent a metric definition |
| compile bounded, reviewable SQL | write free-form SQL from your text |
| compute deltas, drivers, anomalies, forecasts | do arithmetic in a language model |
| say when evidence is insufficient | fill the gap with a plausible number |
| recommend what to **investigate** | recommend an action on a customer or account |

**No model is called.** Bedrock is not invokable on this account, so answers are assembled
deterministically from real query results. Every number is reproducible and carries an
Athena query id.

---

## 2. Start it

```bash
cd /path/to/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile AWS_DEFAULT_REGION=ap-southeast-1   # REQUIRED

python3 scripts/ai-ui.py            # LIVE  -> http://127.0.0.1:8501
python3 scripts/ai-ui.py --demo     # fixtures, no AWS, $0
```

**If you skip the `export`, the CLI silently uses different credentials.** That has produced
`ParameterNotFound` on secrets and a `KMS key does not exist` error against the wrong
account. When something looks impossibly broken, check `aws configure list` first.

Demo mode runs the same engines on an in-memory fixture, so the UI is usable while the
platform is destroyed for cost reasons. Every panel labels which mode it is in.

---

## 3. The five tabs

| Tab | Ask it | Backed by |
|---|---|---|
| **Ask** | any business question; it routes itself | the full LangGraph copilot |
| **Diagnose** | why a metric moved on a date | completeness → baseline → contribution |
| **Forecast** | next value of a metric | 6 methods, chosen by walk-forward backtest |
| **Governance** | what to check when the mart looks wrong | freshness · completeness · watermark · volume |
| **Metrics** | which metrics exist and their rules | the governed registry |

---

## 4. Questions that work

Verified live:

```
What is total deposits on 2026-08-22?
  -> Total closing balance was 2,031,880,160 VND versus 2,031,881,160 VND
     (previous day), a change of -999.99 VND (-0.00%). Data status: CERTIFIED.

Compare total deposits with the previous day
  -> on a mart with no prior date: "No comparison against the previous day: that period
     returned no rows, so the change cannot be computed." — never a bare value.

Top 5 account_sk by transaction count
Which account contributed most to the change?   <- needs no metric named; defaults and says so
Is it abnormal?                                 <- z-score vs baseline, labelled an observation
What should I investigate?                      <- accounts ranked, with the date scored as of
What does total deposits mean?          <- definition, no AWS call
Is total deposits certified?            <- tier + data-quality triage
What feeds total deposits?              <- lineage from the dbt manifest
```

Seven governed metrics: `total_closing_balance` · `total_debit_amount` ·
`total_credit_amount` · `net_cash_flow` · `total_txn_count` · `active_account_count` ·
`avg_balance_per_account`. Synonyms resolve — *"total deposits"*, *"deposit balance"* and
*"deposit amount"* all reach the same definition.

---

## 5. Answers that are refusals, and why that is correct

| You ask | You get | Why |
|---|---|---|
| *"What is gross margin?"* | `no governed metric matches` | undefined metrics are refused, never improvised |
| *"Break it down by product_code"* | `NOT QUERYABLE: dim_account is not materialised` | answering without the requested breakdown answers a different question |
| *"Forecast the next 7 days"* | `NOT produced: N points, 14 required` | a forecast from two weekly cycles is a guess with a band drawn round it |
| *"Is this abnormal?"* on short history | `anomaly inconclusive` | a z-score over 3 points is arithmetic, not evidence |
| *"Drop the source table"* | refuses, **0 tool calls** | there is no write tool to reach |

A refusal here is the product working. The failure mode this design exists to prevent is a
confident number nobody can check.

---

## 6. Prove any answer

Every answer carries an **Athena query id**. Re-run it yourself:

```bash
aws athena get-query-execution --query-execution-id <id> \
  --query 'QueryExecution.[Query,Statistics.DataScannedInBytes]' --output text
```

Or check the whole number independently:

```bash
aws athena start-query-execution \
  --query-string "SELECT SUM(closing_balance) FROM kafka_dev_lab_dev_mart.mart_account_balance_daily WHERE business_date = DATE '2026-08-22'" \
  --work-group kafka-dev-lab-dev-wg --query QueryExecutionId --output text
```

The copilot's number and the direct query must match. That cross-check is how AI-P16 was
closed, and it is the only evidence worth trusting.

---

## 7. Test it yourself

All $0 unless marked:

```bash
# the whole AI layer
python3 -m pytest spark/tests/test_ai_*.py -q            # 564 passed
python3 -m pytest spark/tests/test_business_*.py -q      # 220 passed

# end-to-end scenarios and safety drills
python3 ai/eval/e2e_p14.py                               # 10 PASS / 0 FAIL / 0 BLOCKED
python3 ai/eval/drills_p15.py                            # 20/20, 0 P0 violations

# quality gates against recorded baselines
make ai-eval-rag-gate
make ai-eval-agent
make business-ai-eval                                    # 25/25, P0=0 P1=0

# safety, by hand
python3 - <<'PY'
import sys; sys.path.insert(0, "ai")
from guards import assert_read_only_sql, GuardViolation
for sql in ["DROP TABLE mart.x", "SELECT 1; DELETE FROM mart.x",
            "/* SELECT */ UPDATE mart.x SET a=1", "MSCK REPAIR TABLE mart.x"]:
    try:
        assert_read_only_sql(sql); print("NOT BLOCKED (bad):", sql)
    except GuardViolation:
        print("blocked  :", sql)
PY
```

All four must print `blocked`, including the comment-obfuscated and multi-statement forms.

---

## 8. Command reference

```bash
make ai-ui                       # the web UI
make ai-retrieve Q="..."         # raw retrieval, no AWS
make ai-ask Q="..."              # CLI copilot
make ai-tools                    # tool catalog + what is deliberately absent
make ai-contracts                # validate configs, print the plan hash
make ai-governance               # cost envelope; always-on components
make business-ai-eval            # the business golden set
```

---

## 9. Known limitations

1. **No generated prose.** Bedrock returns `AccessDenied`
   (`INVALID_PAYMENT_INSTRUMENT`). Answers are deterministic. Fix at Billing → Payment
   preferences, then Bedrock → Model access.
2. **~6–9 s per live question** — Athena cold start, not the agent (which spends ~80 ms).
3. **Business-dimension drivers are blocked.** `curated.dim_account` / `dim_customer` exist
   in Glue but are not populated, so breakdowns work on surrogate keys only.
4. **Short history — this is the live limitation that matters.** The mart holds **1 business
   date** (2026-09-23). A value question and a certification check work; a day-on-day change
   needs 2 dates and a forecast or anomaly score needs 14. The header states coverage, and
   those questions refuse explicitly rather than returning a bare number that reads like a
   complete answer. **Use `--demo` for a 30-date fixture that exercises every panel at $0.**
5. **The pilot model has a synthetic label** and no predictive meaning; it proves plumbing.

---

## 10. Two traps that have bitten this project

**Forgetting `AWS_PROFILE`.** Commands then run against a different account and fail with
messages that look like missing resources. It once reported that a KMS key "does not exist"
when the key was fine — the shell was simply on the wrong account.

**`terraform destroy` schedules the lake CMK for deletion.** The next apply creates a NEW
key while existing objects keep the old one, and Spark then fails with `kms:Decrypt` denied
— which sends you debugging IAM instead of KMS. After **every** rebuild:

```bash
bash scripts/reencrypt-lake-cmk.sh audit                 # expect ONE durable key
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute   # if it shows more
```

---

## 11. Cost

| Action | Cost |
|---|---|
| Demo mode, retrieval, all test suites | **$0.00** |
| One live business question | 1–2 Athena scans, capped at 10 GiB by the workgroup |
| Model calls | **none** |

Platform while running is ~$1.40/day for MSK plus EC2. `bash scripts/stop-ephemeral.sh
--execute` stops the instances; **MSK cannot be stopped, only destroyed.**

---

## 12. Where to go next

| Document | For |
|---|---|
| `docs/END_TO_END_WALKTHROUGH.md` | CDC → S3 → mart → AI, runnable |
| `docs/BUSINESS_METRIC_LAYER.md` | defining a metric, aliases, certification |
| `docs/AI_V2_TEST_GUIDE.md` | diagnosis / forecast / governance engines |
| `docs/AI_FAILURE_MATRIX.md` | the 20 failure drills |
| `docs/runbooks/ai-platform-operations.md` | what to check when a drill fails |
| `docs/AI_COPILOT_TRANSCRIPTS.md` | real captured sessions from this copilot **and** the reliability copilot |
| `docs/LIVE_MODE_SETUP.md` | live mode, measured coverage, and how to build more business dates |
