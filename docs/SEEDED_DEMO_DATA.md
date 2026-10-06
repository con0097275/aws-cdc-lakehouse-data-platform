# Seeded demo data — 90 business dates in the live mart

The lakehouse holds **one** real business date, because the whole change history of the
source is 30 updates on one day (see [`LIVE_MODE_SETUP.md`](LIVE_MODE_SETUP.md)). That is
enough for a value question and nothing else: a day-on-day change needs 2 dates, a forecast
or an anomaly score needs 14.

`scripts/seed-demo-mart.py` writes **90 business dates** into a separate relation so every
panel has something true to work on, while the certified mart stays exactly as the pipeline
left it.

```bash
scripts/seed-demo-mart.py                              # dry-run: the plan and the SQL
scripts/seed-demo-mart.py --execute --drop             # build it (~90 Athena INSERTs)

python3 scripts/ai-ui.py --seeded                      # → 127.0.0.1:8501
```

Drop `--seeded` to go back to the real mart. Nothing is overwritten either way.
(`--seeded` just sets `AI_MART_RELATION`; the variable still works if you prefer it.)

---

## Why a separate table, and not just more rows in the mart

Every value of `processing_status` is a **claim about a process that ran**. `CERTIFIED`
asserts an EOD close and a certification gate; `RECONCILED` asserts a reconciliation.
Synthetic rows can honestly claim none of them, and there is no tier meaning *"made up for a
demo"*.

So the rows move instead of the label:

* they live in `mart_account_balance_daily_demo`, never in the certified mart
* every row carries `source_flow_mode = 'SEED_DEMO'` and `run_id = 'seed-<date>'`, so the
  provenance is legible to anyone who queries the table directly
* `metric_version` changes with the relation — a metric read from a different table **is** a
  different metric, and the evidence pack says so
* the UI shows a standing banner naming the relation, because the seeded rows do carry
  `processing_status = 'CERTIFIED'` (a metric refuses a window weaker than its
  `minimum_certification`, so without it the gate would block every question rather than
  answer it), and the only thing keeping that honest is saying so **where the numbers are
  read**

> **SEEDED DATA** — reading `kafka_dev_lab_dev_mart.mart_account_balance_daily_demo`, not the
> certified mart. Written by `scripts/seed-demo-mart.py`; every row carries
> `source_flow_mode = 'SEED_DEMO'`. The engines, the SQL and the Athena query ids are real;
> the **numbers are synthetic**.

**The analysis is not faked.** Athena runs the real compiled SQL, the diagnosis and forecast
engines are the deployed ones, and every answer still carries a query id you can re-run.

---

## What is in the data, and why

10,720 rows · 90 dates (2026-06-25 → 2026-09-22) · 120 accounts. Deterministic: the
generator is seeded, so a rebuild is byte-identical.

| Shape | Why it is there |
|---|---|
| weekly cycle + mild upward drift | a forecast needs a pattern to backtest against |
| **short partition on 2026-09-13** — only 40 of 120 accounts land | so Diagnose reports a **pipeline** fault, not a business drop |
| **balance spike on 2026-09-18** — +18% | so the anomaly detector has a real outlier |
| real `txn_count`, `debit_amount`, `credit_amount` | the live mart has `0.00` in these, and a metric that is always zero demonstrates nothing |

The range ends at **2026-09-22**, the day before the real mart's only date, so the seeded and
certified data never overlap.

### The short day is the point

**Diagnose · `total_account_balance` · 2026-09-13**

> **DATA_INCOMPLETE** · 258,849,951 vs baseline 758,121,548 · delta −499,271,597 (**−65.9%**)
> · completeness: **40 rows vs typical 120.0 (33%)**
>
> only 40 rows vs a typical 120 (33%). **Treat this as a PIPELINE problem first:** any
> movement in the measure is unreliable while the partition is short. next: check the EOD job
> watermark and the source connector, not the business

A two-thirds fall in a KPI that is really a third-loaded partition is the most expensive
wrong answer this layer can give. Completeness is checked **before** contribution, always.

Compare with the spike, five days later:

**Diagnose · `total_account_balance` · 2026-09-18**

> **MOVED** · 910,166,019 vs baseline 689,079,333 · delta +221,086,686 (+32.1%) ·
> completeness: 120 rows vs typical 108.6 (110%)

Same engine, same magnitude of movement, opposite verdict — because one partition was short
and the other was not.

---

## What it looks like

![Ask tab answering a day-on-day comparison](images/copilot-ask-day-on-day.png)

The seeded banner is visible above the answer, and the answer still carries real Athena query
ids — the relation is synthetic, the execution is not.

![Forecast tab backtesting six methods](images/copilot-forecast-backtest.png)

90 dates is what makes this panel possible at all: the forecast refuses below 14, and the
backtest here runs over 21 walk-forward points.

## Every panel, verified on this data

Run on 2026-09-30 against `..._demo` through live Athena.

| Ask | Answer |
|---|---|
| *What is total deposits?* | 757,039,147 VND versus 761,989,193 (day before), **−4,950,046 (−0.65%)** |
| *Compare with the previous day* | same, with the delta |
| *Forecast total deposits* | 766,879,683 VND (673,090,298 – 860,669,068), method **ses**, backtested error 12.2% |
| *Is it abnormal?* | against a baseline of 771,097,055 — **NORMAL** (score −0.18 via ewma) |
| *Which account contributed most?* | 1000062 contributed 3.0%; 1000029 contributed 2.8% |
| *What should I investigate?* | the movement, plus **3 accounts worth investigating first** |

| Tab | Result |
|---|---|
| **Forecast** | 6 methods backtested over 22 walk-forward points — naive 14.04%, seasonal_naive 13.04%, moving_average 13.02%, drift 14.25%, **ses 12.76%**, holt 13.37% |
| **Diagnose** | `DATA_INCOMPLETE` on 2026-09-13, `MOVED` on 2026-09-18 |
| **Governance** | `HEALTHY` — completeness 120/120, freshness 0d behind, volume_stability flat |

---

## Cost, and removing it

Roughly 90 small Athena `INSERT` statements to build, and 10,720 rows of Parquet in
`s3://<lake>/warehouse/demo/`. Both are negligible, and neither touches the certified mart.

```bash
# remove it entirely
aws athena start-query-execution --profile my-aws-profile --region ap-southeast-1 \
  --work-group kafka-dev-lab-dev-wg \
  --query-string "DROP TABLE kafka_dev_lab_dev_mart.mart_account_balance_daily_demo" \
  --query QueryExecutionId --output text
```

---

## Which to use for what

| | Use |
|---|---|
| a reviewer with no AWS | `python3 scripts/ai-ui.py --demo` — 30 dates, in-memory, **$0** |
| showing real Athena integration with a full series | `python3 scripts/ai-ui.py --seeded` |
| proving the numbers are the pipeline's own | plain `python3 scripts/ai-ui.py` — one real certified date |

All three run the same engines. They differ only in where the numbers come from, and each
says which on every page.

| Also see | |
|---|---|
| [`LIVE_MODE_SETUP.md`](LIVE_MODE_SETUP.md) | why the real mart is thin, and the backfill path |
| [`DEMO_CAPTURE_GUIDE.md`](DEMO_CAPTURE_GUIDE.md) | 24 questions covering every capability |
| [`DEMO.md`](DEMO.md) | the three tiers and what each costs |
