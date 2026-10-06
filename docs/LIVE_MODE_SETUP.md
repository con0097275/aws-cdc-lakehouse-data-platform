# Running the copilot in LIVE mode

```bash
export AWS_PROFILE=my-aws-profile AWS_DEFAULT_REGION=ap-southeast-1   # REQUIRED
aws sts get-caller-identity          # confirm the account before anything else
python3 scripts/ai-ui.py             # LIVE -> http://127.0.0.1:8501
```

**Skipping the export is the most common failure.** Commands then run against a different
account and fail with messages that read like missing resources — a KMS key "does not exist"
when the key is fine, because the shell was on the wrong account.

---

## Live mode already works. Here is exactly what it can and cannot answer.

Verified **2026-09-30** against account `111122223333`.

| Question | Live | Why |
|---|---|---|
| *What is total deposits?* | ✅ | one business date is enough for a value |
| *What does total deposits mean?* | ✅ | the semantic layer, no warehouse call |
| *Is it certified?* | ✅ | certification ladder + DQ triage |
| *What feeds total deposits?* | ✅ | lineage from the dbt manifest |
| *What is gross margin?* | ✅ refuses | undefined metrics are never improvised |
| *Drop the mart table* | ✅ refuses | no write tool exists |
| Governance tab | ✅ | freshness · completeness · watermark · volume |
| *Compare with the previous day* | ⚠️ says it cannot | needs **2** business dates |
| *Which account contributed most?* | ⚠️ | needs a change to attribute |
| *Forecast total deposits* | ❌ refuses | needs **14** business dates |
| *Is it abnormal?* | ❌ | a z-score over 1 point is arithmetic, not evidence |

The header states this up front:

> **Data coverage: 1 business date** (2026-09-23). Not enough for: a day-on-day change; a
> forecast or an anomaly score.

**Nothing is broken.** The mart holds one partition, and the copilot refuses rather than
estimating. That is the product working.

---

## Why there is only one date

Measured from the lake, not assumed:

| Layer | rows | business dates | range |
|---|---|---|---|
| `mart.mart_account_balance_daily` | 320 | **1** | 2026-09-23 |
| `snapshot.eod_oracle_coredb_corebank_account` | 320 | **1** | 2026-09-20 |
| `full_cdc.cdc_oracle_coredb_corebank_account` | 670 | 2 commit days | 2026-09-20, 2026-09-29 |

And the CDC events themselves:

| commit day | op | rows |
|---|---|---|
| 2026-09-20 | `r` (snapshot load) | 320 |
| 2026-09-20 | `u` (update) | **30** |
| 2026-09-29 | `r` (snapshot load) | 320 |

**The whole change history of this platform is 30 updates on one day.** That is not a
pipeline fault — the source lab was seeded, a workload was run once, and CDC captured it
exactly. There is simply no history to show.

**No backfill can invent history the source never had.** This is worth being blunt about,
because the tempting shortcut — writing synthetic rows straight into the mart — would make
`processing_status = CERTIFIED` a lie about data no EOD run produced, and the certification
badge is the one thing this project exists to make trustworthy.

---

## Getting more dates, the real way

### Step 1 — build the dates that already exist

`eod_engine --fulfill` rebuilds a historical COB date from FULL_CDC. It is a first-class,
designed path, not a workaround. Dry-run by default:

```bash
scripts/backfill-business-dates.sh --from 2026-09-20 --to 2026-09-29
scripts/backfill-business-dates.sh --from 2026-09-20 --to 2026-09-29 --execute
```

One EOD job per date, then **one** FULFILL run covering the whole span for the marts. EOD
first for every date, then the marts: a mart built before its EOD partition exists reads
nothing and writes a zero, which looks exactly like a business collapse.

This gets you to **10 business dates** — enough that *"Compare with the previous day"* starts
answering. It will not reach 14, and the series will be **flat** between 2026-09-20 and
2026-09-29, because nothing happened at the source in between. Those snapshots are real and
identical, which is the correct answer to "what was the state on 2026-09-22".

### Step 2 — create actual movement

A forecast needs variation, and variation comes from source activity:

```bash
scripts/source-lab.sh workload 1 --execute      # operator-gated: you type the phrase
```

Run a scenario, let CDC flow through to FULL_CDC, close that COB date, build the mart.
Repeat per business date you want. Scenarios 1–6 exercise different shapes (inserts,
updates, deletes, late events, out-of-order).

**This is a pipeline exercise, not a configuration change.** Budget a run per date.

### Step 3 — confirm it landed

```bash
aws athena start-query-execution --work-group kafka-dev-lab-dev-wg \
  --query-string "SELECT count(DISTINCT business_date) FROM kafka_dev_lab_dev_mart.mart_account_balance_daily" \
  --query QueryExecutionId --output text
```

Re-open the UI — the coverage banner should have changed. At 2 dates the comparison starts
working; at 14 the forecast and anomaly stop refusing.

---

## What it costs to leave running

~$1.40/day for MSK plus EC2, plus EMR Serverless per vCPU-second while a job runs.

```bash
bash scripts/stop-ephemeral.sh --execute      # stops the EC2 instances
```

**MSK cannot be stopped, only destroyed.** After any rebuild, re-check the lake CMK — a
`terraform destroy` schedules the old key for deletion and the next apply creates a new one,
leaving existing objects unreadable and Spark failing on `kms:Decrypt`:

```bash
bash scripts/reencrypt-lake-cmk.sh audit                 # expect ONE durable key
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute   # only if it shows more
```

---

## Three ways to run it

```bash
python3 scripts/ai-ui.py            # the real certified mart — 1 business date
python3 scripts/ai-ui.py --seeded   # live Athena, 90 SEEDED business dates
python3 scripts/ai-ui.py --demo     # in-memory fixture, 30 dates, no AWS, $0
```

`--seeded` is the one to use when you want every panel working against real Athena. It reads
`mart_account_balance_daily_demo` (built once by `scripts/seed-demo-mart.py`), shows a
standing banner naming the relation, and never touches the certified mart — see
[`SEEDED_DEMO_DATA.md`](SEEDED_DEMO_DATA.md).

30 business dates, the **real** engines, **$0**, no AWS account needed — so anyone reading
your repository can run it. Live mode proves the numbers are real and re-checkable; demo mode
proves the analysis works. Capture both:

* **live** — screenshots 1, 3, 4, 9, 10 and the coverage banner from
  [`DEMO_CAPTURE_GUIDE.md`](DEMO_CAPTURE_GUIDE.md); these carry Athena query ids a reviewer
  can re-run
* **demo** — the remaining 18, which need history

| Also see | |
|---|---|
| [`DEMO.md`](DEMO.md) | the three tiers and what each costs |
| [`DEMO_CAPTURE_GUIDE.md`](DEMO_CAPTURE_GUIDE.md) | 24 questions covering every capability |
| [`SEEDED_DEMO_DATA.md`](SEEDED_DEMO_DATA.md) | 90 seeded business dates on live Athena |
| [`AI_COPILOT_USER_GUIDE.md`](AI_COPILOT_USER_GUIDE.md) | the business copilot in full |
