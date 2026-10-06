# AI v2 — Conversational Analytics & Data Governance: Guide and Test Plan

What was added, why it is built this way, and how to prove it works — **entirely at $0, with
your AWS platform destroyed.**

---

## 1. What you asked for, and what actually ships

You asked to chat with the mart (*"why is the total account balance so small today"*,
*"foresee tomorrow"*) and to use AI for data governance with a UI.

The instinct is natural-language-to-SQL plus a chatbot. That is **not** what this builds, for
two reasons — the second matters more than the first:

1. **Bedrock is not invokable on your account** (`INVALID_PAYMENT_INSTRUMENT`). An LLM-first
   design would ship nothing.
2. **None of your three questions is a generation problem.**

| Your question | What it actually is | Needs an LLM? |
|---|---|---|
| "Why is the balance so small today?" | contribution analysis over declared dimensions | **no** |
| "What will it be tomorrow?" | forecast with a backtested error | **no** |
| "What should I check, the pipeline looks wrong?" | ranked DQ triage with evidence | **no** |

So the design splits **analysis** (deterministic, auditable, mandatory) from **narration**
(LLM, optional, currently off). The analysis works today. Narration becomes a thin layer that
rephrases a number it was handed — it never chooses the number. That is [ADR-061](adr/ADR-061-deterministic-analytics-before-narration.md).

The practical consequence: **every answer below is reproducible and you can check the
arithmetic.** An LLM answer to the same question would be fluent, unverifiable, and would
fail silently when the data is incomplete.

---

## 2. Architecture

```
                    ai/analytics/            <- deterministic core, no model call
                    ├── registry.py          metric contract: measure, grain, dimensions,
                    │                        owner, and whether it is ADDITIVE
                    ├── datasource.py        AthenaSource (governed) | FrameSource (fixtures)
                    ├── diagnose.py          completeness -> baseline -> contribution
                    ├── forecast.py          3 methods, chosen by walk-forward backtest
                    └── governance.py        ranked findings + PROPOSED fix (never applied)
                             │
                             v
        ai/agent_tools/catalog.py    4 new read-only tools, audited like every other
                             │       diagnose_metric  forecast_metric
                             │       governance_review  list_metrics
                             v
        ai/agent/router.py           3 new intents: DIAGNOSIS  FORECAST  GOVERNANCE
                             │
                             v
        scripts/ai-ui.py             5 panels: Ask · Diagnose · Forecast · Governance · Metrics
```

Nothing else changed. `WRITE_TOOLS` is still `{}`, no Terraform resource was added, no IAM
was widened, and `ai/` remains deletable.

### The three judgements the design turns on

**Completeness is checked before the business is blamed.** A balance that "dropped 60%"
because half the partition is missing is a **pipeline incident**. `diagnose()` returns
`DATA_INCOMPLETE` and *refuses to decompose*. Answering it as a business event sends someone
to the wrong team while the real fault keeps running. This is the single most valuable
behaviour here and §4.2 tests it explicitly.

**Non-additive measures refuse decomposition.** `AVG` and `COUNT(DISTINCT)` do not sum across
segments. A contribution table whose parts do not sum to the whole is worse than no table,
because it looks like arithmetic. `MetricSpec.additive` is declared per metric.

**A forecast without a measured error is an opinion.** Method chosen by walk-forward
backtest; interval derived from that method's own error on that series; fewer than two
weekly cycles produces a **refusal**, not a number.

---

## 3. Run it — 60 seconds, $0, no AWS

```bash
cd /path/to/aws-cdc-lakehouse
python3 scripts/ai-ui.py --demo        # open http://127.0.0.1:8501
```

Demo mode feeds the **real engines** from an in-memory fixture: 30 days of daily balances
with a weekly cycle, a mild trend, and **one deliberately broken day (2026-08-21)** where
only `CURRENT` lands. Every panel says `DEMO MODE` so you can never mistake fixture numbers
for live ones.

Drop `--demo` once your platform is rebuilt and the same panels read the governed mart
through the read-only Athena tool.

---

## 4. Test plan

Each test states what to do, what you should see, and **what it would mean if you saw
something else** — that last column is the point.

### 4.1 The metric contract

```bash
python3 -c "
import sys; sys.path.insert(0,'ai')
from agent_tools.catalog import list_metrics
for m in list_metrics()['metrics']:
    print(f\"{m['name']:24s} additive={str(m['additive']):5s} {m['measure']}\")"
```

**Expect**

```
total_account_balance    additive=True  SUM(closing_balance)
active_account_count     additive=False COUNT(DISTINCT account_id)
avg_account_balance      additive=False AVG(closing_balance)
```

`additive=False` on the last two is the safety property. If a future edit marks `AVG` as
additive, the diagnosis engine will happily produce a decomposition whose parts do not sum to
the whole — the exact class of number that survives review because it looks like arithmetic.

### 4.2 Diagnosis — the case that matters

UI → **Diagnose**, metric `total_account_balance`, date **`2026-08-21`**, dimension
`product_code`.

**Expect** `DATA_INCOMPLETE`, a red banner, **no contribution table**, and:

> only 4 rows vs a typical 12 (33%). Treat this as a PIPELINE problem first…
> next: check the EOD job watermark and the source connector, not the business

**If instead you see a contribution table blaming SAVINGS**, the completeness guard has
regressed and the tool is now inventing business explanations for pipeline faults. That is
the failure this feature exists to prevent.

Now a **real** business move — same metric, date `2026-08-20`:

```bash
python3 -c "
import sys; sys.path.insert(0,'ai'); sys.path.insert(0,'.')
import importlib.util as u
spec=u.spec_from_file_location('uidemo','scripts/ai-ui.py')
" 2>/dev/null
python3 - <<'PY'
import sys; sys.path.insert(0,'ai')
from analytics.registry import get_metric
from analytics.datasource import FrameSource
from analytics.diagnose import diagnose
rows=[]
for i in range(14,21):
    for p,v in (('CURRENT',1000),('SAVINGS',500),('LOAN',200)):
        rows.append({'business_date':f'2026-08-{i:02d}','value':v,'product_code':p})
for p,v in (('CURRENT',1000),('SAVINGS',50),('LOAN',200)):
    rows.append({'business_date':'2026-08-21','value':v,'product_code':p})
d=diagnose(get_metric('total_account_balance'),FrameSource(rows),'2026-08-21',
           dimension='product_code')
print('verdict:',d.verdict,'| rel change:',f'{d.rel_change:+.1%}')
for c in d.contributions:
    print(f'  {c.segment:8s} {c.baseline:7.0f} -> {c.actual:7.0f}  share {c.share_of_delta:6.1%}  {c.status}')
PY
```

**Expect** `MOVED`, `-26.5%`, and **SAVINGS carrying 100.0% of the delta**. The shares must
sum to 100%; if they do not, the dimension does not partition the metric and the engine emits
a warning saying so.

### 4.3 Forecast — and its refusal

UI → **Forecast**, `total_account_balance`, as-of `2026-08-23`.

**Expect** a point value, a range, `method seasonal_naive`, and a stated historical error
(~5%) with a comparison table of all three candidate methods.

The number to read is **not** the forecast — it is the MAPE. A 5% historical error makes the
point usable; the UI raises a warning above 25% telling you not to use it for a threshold
decision.

Now prove it refuses:

```bash
python3 - <<'PY'
import sys; sys.path.insert(0,'ai')
from analytics.registry import get_metric
from analytics.datasource import FrameSource
from analytics.forecast import forecast
rows=[{'business_date':f'2026-08-{d:02d}','value':100,'product_code':'C'} for d in range(1,9)]
f=forecast(get_metric('total_account_balance'),FrameSource(rows),as_of='2026-08-08')
print('point   :',f.point)
print('refused :',f.refused)
PY
```

**Expect** `point: None` and *"only 8 daily points; 14 are required"*. A system that always
produces a number is not more useful — it is less honest, and you cannot tell its confident
answers from its guesses.

### 4.4 Governance — propose, never apply

UI → **Governance**, `total_account_balance`, as-of `2026-08-21`, watermark `2026-08-21`.

**Expect**, worst-first:

| severity | check | meaning |
|---|---|---|
| `CRITICAL` | completeness | 4 rows vs typical 12 |
| `HIGH` | volume_stability | flat baseline, today is 67% away |
| `PASS` | freshness | partition exists for the date |
| `PASS` | watermark_vs_reality | claim matches the data |

Each finding shows evidence, what to check, and a **proposed fix you copy and run yourself**.
The page ends with:

> proposed fixes are NOT executed — this module has no write path (ADR-057)

Verify that claim rather than believing it:

```bash
grep -nE "subprocess|os\.system|popen|delete_object|run_query\(" ai/analytics/governance.py \
  || echo "no execution path in governance.py"
```

**Expect** `no execution path in governance.py`. A test asserts this too, so it cannot be
quietly added later.

Also worth seeing — the "job says SUCCEEDED but produced nothing" case, which is the worst
failure in the platform because every downstream gate believes the watermark:

```bash
python3 - <<'PY'
import sys; sys.path.insert(0,'ai')
from analytics.registry import get_metric
from analytics.datasource import FrameSource
from analytics.governance import review
rows=[{'business_date':f'2026-08-{d:02d}','value':100,'product_code':'C'}
      for d in range(14,21) for _ in range(10)]           # nothing on the 21st
r=review(get_metric('total_account_balance'),FrameSource(rows),
         as_of='2026-08-21',watermark='2026-08-21')
for f in r.findings:
    if f.check=='watermark_vs_reality':
        print(f.severity,'-',f.summary); print('  fix:',f.proposed_fix)
PY
```

**Expect** `CRITICAL - watermark claims 2026-08-21 but 2026-08-21 has no rows`.

### 4.5 Routing

```bash
python3 - <<'PY'
import sys; sys.path.insert(0,'ai')
from agent.router import route
for q in ["why is the total account balance so small today",
          "what will the total balance be tomorrow",
          "what should i check, the mart looks wrong",
          "how many rows are in the mart",
          "Why is EOD stale?",
          "drop table mart.dim_customer"]:
    print(f"{route(q).value:16s} <- {q}")
PY
```

**Expect**

```
DIAGNOSIS        <- why is the total account balance so small today
FORECAST         <- what will the total balance be tomorrow
GOVERNANCE       <- what should i check, the mart looks wrong
STRUCTURED_DATA  <- how many rows are in the mart
PIPELINE_OPS     <- Why is EOD stale?
UNSAFE           <- drop table mart.dim_customer
```

Two of these encode decisions worth understanding:

- *"why is the **total** balance…"* contains `total`, which also matches `STRUCTURED_DATA`.
  The **interrogative** decides the intent, not the measure word — otherwise the answer is a
  bare `SUM`, which restates the question instead of explaining it.
- *"Why is EOD **stale**?"* is a `why` question whose subject is a **pipeline**, so
  `PIPELINE_OPS` wins over `DIAGNOSIS`. Diagnosing a metric while the job is broken answers
  the wrong question. An earlier version of this precedence got it wrong and the existing
  routing test caught it.

### 4.6 Safety has not moved

```bash
python3 -m pytest spark/tests/test_ai_analytics.py -q      # 22 passed
python3 -m pytest spark/tests/test_ai_*.py -q              # 548 passed
python3 ai/eval/drills_p15.py                              # 20/20, 0 P0
python3 -c "
import sys; sys.path.insert(0,'ai')
from agent_tools.catalog import CATALOG, WRITE_TOOLS
print('tools:', len(CATALOG), '| WRITE_TOOLS:', WRITE_TOOLS)"
```

**Expect** `tools: 12 | WRITE_TOOLS: {}`. Four tools were added and the write surface is
still empty.

---

## 5. Cost

| Action | Cost |
|---|---|
| Everything in §4 with `--demo` | **$0.00** — no AWS call |
| One diagnosis against live Athena | 2 scans, bounded by the workgroup's 10 GiB cutoff |
| One forecast | 1 scan over `history_days` |
| Model calls | **none** — no model is invoked anywhere |

At the time of writing you were at **$69.75 of $100** with the platform destroyed, which is
why demo mode exists: this feature is developable and demonstrable without rebuilding.

---

## 6. Honest limitations

1. **This is not a chatbot.** It answers three shapes of question well and refuses the rest.
   Free-form conversation needs generation, and generation needs Bedrock.
2. **The registry has three metrics.** Adding one is deliberate — a name, an owner, a grain,
   and an additivity decision. That friction is the feature.
3. **Forecasting is intentionally simple.** Three methods, chosen by backtest. A few weeks of
   daily points cannot justify anything more without overfitting, and a complicated forecast
   on a short series is a confident wrong answer.
4. **Governance covers four checks** — freshness, completeness, watermark-vs-reality, volume.
   Uniqueness, null-rate and cross-layer reconciliation are not implemented; they are listed
   here rather than stubbed, because a check that returns `PASS` without running is worse
   than an absent one.
5. **Demo numbers are fixtures.** Nothing in demo mode says anything about your real data.

---

## 7. When you rebuild

```bash
bash scripts/tf.sh plan && bash scripts/tf.sh apply --execute   # type: APPLY THE SAVED PLAN
# re-register the Iceberg tables (docs/runbooks/rebuild-from-scratch.md §5)
bash scripts/reencrypt-lake-cmk.sh audit     # expect ONE key
python3 scripts/ai-ui.py                     # LIVE mode
```

**Check the CMK first.** `terraform destroy` schedules the lake key for deletion; if it lapses
while your data is on it, the lake is unreadable permanently.
