# AWS CDC Lakehouse + AI — Architecture & Operating Guide

The single entry point. What the platform is, how the AI layer sits on it, how to run and
test both, and the traps that have actually bitten this project.

Verified live against account `111122223333` · `ap-southeast-1` · 2026-08-29.

---

## 1. Before any command

```bash
cd /path/to/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile AWS_DEFAULT_REGION=ap-southeast-1
```

**Skipping this is the most common failure in the project.** The CLI silently falls back to
different credentials, and the errors look like missing resources: `ParameterNotFound` on a
secret that exists, and once `KMS key does not exist` for a key that was perfectly healthy —
the shell was simply on another account. When something looks impossibly broken, run
`aws configure list` before debugging anything else.

---

## 2. Architecture

```
Oracle 23ai ─┐
             ├─ Debezium 2.7.3 ─> MSK ─> Spark ─> Iceberg on S3 ─> Glue ─> Athena ─> Power BI
SQL Server ──┘                                        │
                                                      └──> AI layer  (read-only, adjacent)
```

### The layer model is a FAN-OUT, not a chain

| Layer | Meaning | Grain |
|---|---|---|
| **L1 FULL_CDC** | canonical truth; every I/U/D, append-only, straight from Kafka | one row per change event |
| **L2 STREAM** | derived low-latency view | one row per change event |
| **L3 SNAPSHOT** | as-of state at a cutoff, deduped by PK | one row per key per day |
| **MART** | business-facing model | one row per business grain |

`STREAM` is **derived from** FULL_CDC, not a landing zone before it. FULL_CDC is the only
thing entitled to be called canonical.

### The certification ladder

`REALTIME` < `PROVISIONAL_NRT` < `PROVISIONAL_CORRECTED` < `RECONCILED` < `CERTIFIED`

Carried on every mart row as `processing_status`. `mart_customer_360_daily` propagates the
**weakest** status across joined facts. This is the platform's most distinctive property:
it can answer *"is this number certified?"* from a column that already exists.

### Where the AI sits

**Downstream and adjacent.** It reads the mart and the docs; it writes nothing, owns no
truth, and no pipeline code imports it:

```bash
grep -rn 'import ai\.\|from ai\.' spark/jobs spark/reporting airflow/dags | wc -l   # 0
```

That `0` is enforced by a test. Delete the AI layer and every number the platform produces
is unchanged.

---

## 3. The AI layer

```
question
   ↓
LangGraph copilot          ai/business_agent/    bounded: 12 steps, 6 tool calls, no ReAct
   ↓
metric registry            aiplatform/metrics/   7 governed metrics, 40 aliases
   ↓
query compiler             ai/analytics/         one bounded SELECT, identifiers validated
   ↓
Athena (read-only)         10 GiB workgroup cutoff, SELECT-only, allow-listed schemas
   ↓
analytics engines          drivers · anomaly · forecast · governance   (deterministic)
   ↓
EvidencePack               query ids · sql hashes · metric version · certification
   ↓
answer                     assembled from real numbers; narration optional and OFF
```

### The design decision that matters (ADR-061)

**Analysis is deterministic; narration by a language model is optional.**

*"Why did the balance drop?"* is a contribution analysis. *"What will it be tomorrow?"* is a
forecast with a backtested error. *"What should I check?"* is ranked DQ triage. None is a
generation problem. Building the sentence first produces something that sounds confident and
cannot be audited.

Consequence: **every number is reproducible and carries an Athena query id.**

### Components

| Path | Role |
|---|---|
| `aiplatform/metrics/business_metrics.yaml` | the governed metric definitions |
| `ai/analytics/semantic.py · resolver.py · compiler.py` | phrase → metric → bounded SQL |
| `ai/analytics/drivers · anomaly · forecast · governance` | the deterministic engines |
| `ai/business_agent/` | LangGraph copilot, 14 tools, evidence gate |
| `ai/insights/` + `airflow/dags/business_insights.py` | proactive daily insights, ONE DAG for all KPIs |
| `ai/business_ml/` | offline features, PIT joins, 2 unsupervised pilots |
| `scripts/ai-ui.py` | local web UI, 5 tabs |

---

## 4. Running the platform

```bash
bash scripts/tf.sh plan
bash scripts/tf.sh apply --execute            # type: APPLY THE SAVED PLAN
bash scripts/reencrypt-lake-cmk.sh audit      # ALWAYS after a rebuild — see §8

bash scripts/source-lab.sh status             # Oracle + SQL Server CDC preconditions
bash scripts/cdc-runtime.sh status            # connectors RUNNING
bash scripts/cdc-runtime.sh topics
bash scripts/source-lab.sh workload 1 --execute
bash scripts/cdc-runtime.sh correctness all

bash scripts/airflow-node.sh ui               # wait for READY
bash scripts/airflow-node.sh credentials      # user: admin
```

**Stopping:**

```bash
bash scripts/stop-ephemeral.sh --execute      # stops EC2; state survives
bash scripts/show-cost-resources.sh
```

**MSK cannot be stopped, only destroyed.** It is the majority of the bill, so stopping saves
roughly a third of the overnight burn — not all of it.

---

## 5. Running the AI

```bash
python3 scripts/ai-ui.py            # LIVE   -> http://127.0.0.1:8501
python3 scripts/ai-ui.py --demo     # fixtures, no AWS, $0
```

Five tabs: **Ask · Diagnose · Forecast · Governance · Metrics**. The header shows the latest
business date the mart actually holds — questions are anchored to it, not to the wall clock.

Questions verified live:

```
What is total deposits?                 -> 2,031,880,160 VND, CERTIFIED
Compare with the previous day           -> delta + %, both periods
Top 5 account_sk by transaction count   -> ranked segments
Is total deposits abnormal?             -> NORMAL (score -0.42 via zscore)
Forecast total deposits                 -> point + interval + backtested error
What should I investigate?              -> ranked accounts, advisory only
what is FULL_CDC                        -> knowledge corpus, cited
Is total deposits certified?            -> tier + data-quality triage
Drop the source table                   -> refused, 0 tool calls
```

CLI equivalents: `make ai-ask Q="..."`, `make ai-retrieve Q="..."`, `make ai-tools`.

### Refusals are the product working

| Ask | Response | Why |
|---|---|---|
| an undefined metric with a date | refuses | asking for a number we cannot define |
| breakdown by `product_code` | refuses, names the blocked join | answering without it answers a different question |
| forecast on short history | refuses | a forecast from <2 seasonal cycles is a guess with a band drawn round it |
| anomaly on short history | inconclusive | a z-score over 3 points is arithmetic, not evidence |
| a mutation | refuses, 0 tool calls | there is no write tool to reach |

### Production behaviour (added 2026-08-29)

**Cross-request query cache** — `ai/business_agent/cache.py`. A mart partition for a
**closed** business date is immutable, so re-querying it bills twice for a byte-identical
answer. Closed dates are cached indefinitely (size-capped, oldest evicted); the **current**
date gets a 5-minute TTL only, because it is still being written and a cached "today" is how
a live dashboard silently shows an hour-old number. Certification tier is part of the cache
key — a row cached as `PROVISIONAL_NRT` must never be served later as `CERTIFIED`.

Measured: three identical questions on a closed date cost **one** Athena call.

**Follow-up memory** — the copilot carries the last resolved metric, so *"and compare with
the previous day"* works without naming it again. It inherits only on a genuine
continuation (`and`, `also`, `what about`, `compare`, `top 5`…) or a subjectless
operational question. A question that NAMES an unknown subject still refuses:

```
"What is gross margin yesterday?"      -> REFUSED   (names an unknown metric)
"and compare with the previous day"    -> inherits the previous metric
"What should I investigate?"           -> defaults, and says so
```

Answering the first with the closing balance would be invisible in the reply — which is the
failure this whole layer exists to prevent.

Cache statistics are returned on every answer (`hits`, `hit_rate`, `athena_bytes_saved`).

---

## 6. Testing

```bash
# platform
make test                      # full suite (needs local Iceberg jars)
make validate-docs             # 14 checks
make lint-shell

# AI
python3 -m pytest spark/tests/test_ai_*.py spark/tests/test_business_*.py -q   # 784
python3 ai/eval/e2e_p14.py        # 10 PASS / 0 FAIL / 0 BLOCKED
python3 ai/eval/drills_p15.py     # 20/20, 0 P0 violations
make business-ai-eval             # 25/25, P0=0 P1=0
make ai-eval-rag-gate && make ai-eval-agent

# safety, by hand
python3 - <<'PY'
import sys; sys.path.insert(0, "ai")
from guards import assert_read_only_sql, GuardViolation
for sql in ["DROP TABLE mart.x", "SELECT 1; DELETE FROM mart.x",
            "/* SELECT */ UPDATE mart.x SET a=1", "MSCK REPAIR TABLE mart.x"]:
    try: assert_read_only_sql(sql); print("NOT BLOCKED (bad):", sql)
    except GuardViolation: print("blocked  :", sql)
PY
```

All four must print `blocked`, including the comment-obfuscated and multi-statement forms.

**Verify any answer yourself.** Every answer carries an Athena query id:

```bash
aws athena get-query-execution --query-execution-id <id> \
  --query 'QueryExecution.[Query,Statistics.DataScannedInBytes]' --output text
```

---

## 7. Security posture

| Control | Where |
|---|---|
| No write tool exists | `WRITE_TOOLS == {}`, asserted by test (ADR-057) |
| SELECT-only, single statement | `assert_read_only_sql` + Athena tool |
| Raw CDC denied to the AI role | explicit `Deny` in the KB IAM policy |
| Bytes cap | 10 GiB, `enforce_workgroup_configuration = true` |
| No public inbound | SSM Session Manager is the only path; UI binds `127.0.0.1` |
| Secrets | SSM SecureString at runtime; redaction on output |
| Prompt injection | tested in user text, retrieved documents and tool results |

Refusal is enforced **twice** — the router refuses and `assert_read_only_sql` refuses — so a
routing miss alone cannot mutate anything.

---

## 8. The four traps

**1. Forgetting `AWS_PROFILE`.** §1. Wrong account, misleading errors.

**2. `terraform destroy` schedules the lake CMK for deletion.** The next apply creates a NEW
key while existing objects keep the old one. Spark then fails with `kms:Decrypt` denied,
which sends you debugging IAM instead of KMS. This has fired three times; once the key was
5 days from making the entire lake unreadable. **After every rebuild:**

```bash
bash scripts/reencrypt-lake-cmk.sh audit                 # expect ONE durable key
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute   # if it shows more
```

**3. `terraform destroy` also drops the Glue tables.** The S3 data and every Iceberg
`metadata.json` survive; only the pointers go. Athena then says `Table not found`, which
reads as data loss and is not. Recovery is one catalog write per table —
`spark/ops/register_tables.py` submitted to EMR Serverless
(`docs/runbooks/rebuild-from-scratch.md` §5).

**4. A watermark is a claim, not a fact.** A job can report `SUCCEEDED` and produce nothing.
Always cross-check that the target table exists and has rows.

---

## 9. Cost

| | |
|---|---|
| Demo mode, retrieval, all test suites | **$0.00** |
| One live business question | 1–2 Athena scans, capped at 10 GiB |
| A repeated question on a closed date | **$0** — served from cache |
| Model calls | **none** — Bedrock is not invokable |
| Platform running | ~$1.40/day MSK + EC2 |
| Guardrails | 3 budgets, enforced workgroup cutoff, no NAT/EKS/OpenSearch/SageMaker |

---

## 10. Known limitations

1. **No generated prose.** Bedrock returns `AccessDenied` (`INVALID_PAYMENT_INSTRUMENT`).
   Answers are deterministic — arguably better for reporting until billing is fixed.
2. **~6–9 s per live question** — Athena cold start, not the agent (~80 ms).
3. **Business-dimension drivers blocked.** `dim_account` / `dim_customer` are not
   materialised, so breakdowns work on surrogate keys only.
4. **The mart currently holds generated test data** tagged `run_id = 'bai-testdata'`
   (30 dates, 115,990 transactions). It exercises every engine; it is not real business data,
   and the forecast's near-zero backtest error reflects that smoothness.
5. **The pilot ML model is unsupervised by choice** — no honest label exists in this history.

---

## 11. Document index

| Document | For |
|---|---|
| `docs/END_TO_END_WALKTHROUGH.md` | run the whole platform, CDC → mart → AI |
| `docs/AI_COPILOT_USER_GUIDE.md` | using and testing the copilot |
| `docs/BUSINESS_METRIC_LAYER.md` | defining metrics, aliases, certification |
| `docs/AI_V2_TEST_GUIDE.md` | diagnosis / forecast / governance engines |
| `docs/AI_E2E_VALIDATION.md` · `docs/AI_FAILURE_MATRIX.md` | evidence for the 10 E2E and 20 drills |
| `docs/runbooks/rebuild-from-scratch.md` | rebuilding, and the benign errors not to chase |
| `docs/runbooks/ai-platform-operations.md` | AI failure modes, drill triage |
| `docs/BAI_P0_DISCOVERY.md` | what the marts actually contain |
| `DECISIONS.md` · `docs/adr/` | why any of this is the way it is |
| `AI_PLATFORM_STATE.md` · `docs/claude/BUSINESS_AI_IMPLEMENTATION_STATE.md` | current state |
