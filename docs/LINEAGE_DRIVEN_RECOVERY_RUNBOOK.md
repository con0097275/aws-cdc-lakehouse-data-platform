# LINEAGE-DRIVEN RECOVERY — RUNBOOK

- Audience: the operator who has a failed check and has to decide what to rerun.
- Status: **the loop below is designed and partly built.** §1–§4 (check the data, find the
  blast radius by hand, rerun a bounded scope) work **today** with the commands shown.
  §5–§7 (automatic impact, policy gate, one-command recovery) arrive with DRP8/DRP9 and are
  marked as such. Nothing here claims to be automated that is not.
- Companions: `DATA_RELIABILITY_OVERVIEW.md`, `DATA_INCIDENT_RECOVERY_MODEL.md`,
  `PLATFORM_RESOURCE_INVENTORY.md`, `REALTIME_EOD_REBASE_RUNBOOK.md`.

---

## 0. The loop

```text
DQ / Reconciliation FAIL
          │
          ▼
     Incident Open
          │
          ▼
DataHub downstream lineage
          │
          ▼
   Blast Radius
          │
          ▼
intersect with REGISTERED EXECUTABLE JOBS
          │
          ▼
existing dbt + job_dependency graph
          │
          ▼
topological recovery plan
          │
          ▼
     policy gate
      /       \
     /         \
 AUTO          APPROVAL
     │
     ▼
 repair root dataset
     │
     ▼
DQ + Reconciliation
     │
     ▼
root CERTIFIED
     │
     ▼
rerun affected descendants only
     │
     ▼
DQ + Reconciliation
     │
     ▼
Re-certify
     │
     ▼
Incident Closed
```

### The three ideas that make this safe

**Lineage says WHAT is affected; the job graph says HOW and IN WHAT ORDER.** DataHub is
never allowed to execute anything. Its answer is intersected with the set of *registered
executable jobs* — the dbt manifest and the job dependency graph — and anything that does
not resolve to a registered job becomes an operator decision, never a silent omission.
A dashboard is not rerun as a Spark job.

**The root is repaired and validated BEFORE any descendant runs.** Rerunning a mart on top
of a still-broken EOD snapshot produces a second wrong number and burns the evidence.

**Only affected descendants rerun.** Not the whole warehouse, not the whole history. See
§4 for how the scope is bounded.

---

## 1. Check the data — before you change anything

Start read-only. Every command here is safe and costs cents at most.

### 1a. Is the platform even current?

```bash
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1
aws sts get-caller-identity --output table

# 39 read-only checks across the whole chain
python3 scripts/cdc-e2e-verify.py
```

### 1b. What does the control plane say about the last runs?

The OPS tables are the operational truth and they are queryable from Athena.

```sql
-- Did the EOD close actually certify, and on what?
SELECT cob_date, table_id, status, rows_written, certified, run_id, finished_at
FROM   kafka_dev_lab_dev_ops.eod_run
WHERE  cob_date = DATE '2026-09-28'
ORDER  BY table_id;

-- Did REALTIME read a real delta, or no-op?
SELECT table_id, operation, read_mode, fallback_reason,
       source_snapshot_before, input_rows, rows_written, attempt, finished_at
FROM   kafka_dev_lab_dev_ops.realtime_run
WHERE  finished_at > current_timestamp - interval '1' day
ORDER  BY finished_at DESC;

-- Did ingest actually move rows? `batches = 0` with a green exit is the trap.
SELECT app_id, batches, rows_written, started_at, finished_at
FROM   kafka_dev_lab_dev_ops.streaming_batch_ledger
ORDER  BY finished_at DESC
LIMIT  20;
```

> **The failure to look for first.** A `SUCCESS` with `batches: 0, rows: 0` is not a quiet
> day — on 2026-09-29 it was a Structured Streaming checkpoint that had outlived its MSK
> cluster. Green exit code, no data, no warning. Only a before/after row count reveals it.
> See `PLATFORM_RESOURCE_INVENTORY.md` §0c.

### 1c. Count the rows yourself

```sql
-- One active row per key is the EOD invariant.
SELECT dv_pk_hash, count(*) AS n
FROM   kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account
WHERE  cob_date = DATE '2026-09-28'
GROUP  BY dv_pk_hash HAVING count(*) > 1
LIMIT  20;

-- Layer-to-layer agreement for one COB.
SELECT 'full_cdc' AS layer, count(*) AS n
FROM   kafka_dev_lab_dev_full_cdc.cdc_oracle_coredb_corebank_account
WHERE  event_date = DATE '2026-09-28'
UNION ALL
SELECT 'eod', count(*)
FROM   kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account
WHERE  cob_date = DATE '2026-09-28';
```

### 1d. Which Iceberg snapshot am I looking at?

Snapshots are how a rerun is made reproducible, and how you prove which version a figure
came from.

```sql
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM   kafka_dev_lab_dev_snapshot."eod_oracle_coredb_corebank_account$snapshots"
ORDER  BY committed_at DESC
LIMIT  10;
```

### 1e. Governance and quality metadata

```bash
# Owner, domain, classification, PII, policies for every asset — offline, $0
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.governance_plan import compile_inventory; i = compile_inventory(); \
  print(i.config_version(), i.coverage()); [print(' ', f) for f in i.findings]"
```

---

## 2. Open the incident before you touch anything

An incident is not paperwork — it is what bounds the retries and stops the
DQ → rerun → DQ loop. Record, at minimum:

| Field | Why |
|---|---|
| `dataset_id` (an `AssetId`, e.g. `eod:oracle_coredb_corebank_account`) | the root |
| `cob_date` / interval | what has to be repaired, and nothing more |
| `failed check` / `recon_run_id` | what is actually wrong |
| `source_run_id`, Iceberg snapshot | which run and which version produced it |
| `failure_class` | decides whether a rerun can help at all |
| `root_cause_status` | `unknown` blocks automatic recovery |

**A rerun cannot fix every failure class.** `source_defect` and `infra` are outside
`REPAIRABLE_BY_RERUN`: rerunning a pipeline over bad source data reproduces the same bad
answer, more expensively. If the source value itself is wrong the incident goes to
`WAITING_SOURCE_CORRECTION` — **never write back to Oracle or SQL Server** — and recovery is
planned only after the corrected CDC arrives.

---

## 3. Find the blast radius

### Today (DRP2), by hand

Three sources, in descending order of trustworthiness:

```bash
# 1. dbt — authoritative for model-to-model dependencies
cd dbt && dbt ls --select 'source:curated.fact_account_daily_snapshot+' --resource-type model

# 2. The reporting job graph — turns, closure, cycle detection
python3 -c "import sys; sys.path.insert(0,'reporting'); import compile as c; print(c.__doc__)"

# 3. ADR-086 — which reporting jobs declare this CDC table as a source
grep -rn 'source_tables' reporting/jobs/*.yaml
```

Then intersect by hand with what is actually runnable. The sibling rule helps: the three
lake layers of one table share a `lineage_key`, so from `eod:X` you can always find
`realtime:X` and `full_cdc:X`.

### From DRP8 onwards

`LineageImpactService` queries DataHub downstream from the root, classifies each hit as
`EXECUTABLE_JOB` / `DATASET` / `SERVING_ASSET` / `BI_ASSET` / `UNKNOWN`, and intersects with
the registered job set. Both lists are kept:

- `candidate_descendants` — everything impacted, **including** the Power BI report that
  cannot be rerun. Dropping it would understate the blast radius.
- `executable_descendants` — the subset that resolves to a job.
- `excluded_assets` — each with a reason.

---

## 4. Rerun only what is affected

This is the section people come here for. **Scope down in three dimensions: table, date,
and — where the tooling supports it — key or column.**

### 4a. One table, one COB — EOD

```bash
# Preview: what would close, and why it would or would not certify
bash scripts/eod-close.sh --table eod_oracle_coredb_corebank_account --cob 2026-09-28

# Then, with the operator gate
bash scripts/eod-close.sh --table eod_oracle_coredb_corebank_account --cob 2026-09-28 --execute
```

`--table ALL` exists and is almost never what you want during a recovery: it re-closes ten
tables to fix one, and the nine that were already certified become nine more things that
could go wrong.

> **A COB close sees only events dated to that COB.** The EOD close applies an `event_date`
> partition prune of ±1 day around the COB *on top of* the `source_commit_ts < cutoff`
> filter. If a table certifies with `rows=0`, check the event dates before assuming the
> close is broken.

### 4b. One table — REALTIME

```bash
# Normal incremental materialisation
python3 spark/jobs/realtime/realtime_engine.py --table rt_oracle_coredb_corebank_account

# Force a full window scan when the cursor is not trusted
python3 spark/jobs/realtime/realtime_engine.py \
  --table rt_oracle_coredb_corebank_account --force-window-scan

# Rebase the overlay after a COB certifies (drops what EOD now holds)
python3 spark/jobs/realtime/realtime_engine.py --table ALL --rebase-cob AUTO
```

### 4c. One mart, one interval — dbt

This is where "only the affected column or table" is most literal. dbt's graph operators do
the scoping for you.

```bash
cd dbt

# Just this model
dbt build --select mart_account_balance_daily --vars '{business_date: "2026-09-28"}'

# This model and everything downstream of it — the recovery case
dbt build --select mart_account_balance_daily+ --vars '{business_date: "2026-09-28"}'

# Everything downstream of a changed SOURCE, and nothing else
dbt build --select source:curated.fact_account_daily_snapshot+ --vars '{business_date: "2026-09-28"}'

# Only what changed versus the last good run — the narrowest scope dbt offers
dbt build --select state:modified+ --state ./target-last-good
```

**Column scope.** dbt reruns a model, not a column — a model is the smallest unit dbt can
rebuild. "Only the affected column" therefore means one of two things in practice:

1. **Find which models actually carry that column** and rerun only those, rather than the
   whole downstream cone:
   ```bash
   dbt ls --select 'mart_customer_360_daily+' --output json \
     | python3 -c "import sys,json;[print(json.loads(l)['name']) for l in sys.stdin if l.strip()]"
   ```
   Column lineage (DRP6) turns this from a grep into a query.
2. **Bound the rows instead**, which is usually what the incident actually needs — one COB,
   or one key set (§4d).

### 4d. Bounded keys and dates — AUTO_CORRECT and FULFILL

```bash
# Late events: recompute only the dates they touch, only the keys that changed
#   affected_date_policy: CHANGED_DATES_ONLY   affected_key_strategy: PRIMARY_KEY
airflow dags trigger datamart_auto_correct \
  --conf '{"business_date":"2026-09-28","affected_keys":["1001","1002"]}'

# A missing historical date
airflow dags trigger datamart_fulfill --conf '{"business_date":"2026-09-20"}'
```

`ALL_KEYS_IN_DATE` exists because a mart that aggregates across keys has totals that every
key's row depends on — one changed row moves the total. Use `PRIMARY_KEY` when the mart is
per-key, `ALL_KEYS_IN_DATE` when it aggregates.

### 4e. What NOT to do

| Tempting | Why it is wrong |
|---|---|
| reset the Kafka offsets / delete the checkpoint | it does not fix bad data, and it *hides* the zero-row ingest failure rather than repairing it. `scripts/streaming-reset.sh` exists for a rebuilt cluster, not for a DQ failure. |
| rebuild all history for a one-day defect | the defect is dated; the rebuild is not |
| rerun descendants before the root validates | produces a second wrong number on top of the first |
| write a correction back to Oracle/SQL Server | recovery repairs the lakehouse. `allow_source_writes` is refused unconditionally. |
| `dbt build` with no `--select` | rebuilds the warehouse to fix one mart |

### 4f. EMR Serverless reality

The application is capped at 100 GB disk (5 workers). **Submit one job at a time** — two
concurrent submissions fail `ApplicationMaxCapacityExceededException`, and since the VPC
rebuild each submission has taken 45–60 minutes to schedule. Plan a recovery as a sequence,
not a fan-out.

---

## 5. Validate the root, then the descendants

```
repair root → DQ + reconciliation on the ROOT → root CERTIFIED → descendants
```

Descendants do not start until the root's mandatory gates pass. After the descendants run,
DQ and reconciliation run again on them, and only then is the figure re-certified.

**Re-certification is not automatic.** The gates in `CERTIFICATION_MODEL.md` all have to
pass again: contract declared, DQ evaluated and non-blocking, reconciliation passed, cutoff
bounded, upstream ready, source closed.

---

## 6. The policy gate — when the platform may act without asking

From DRP8/DRP9, `decide_approval()` returns `AUTOMATIC` only when **all ten** hold:

1. root cause is known
2. the affected interval is bounded
3. the failure class is repairable by rerunning
4. **every lineage edge on the path is `observed` or `derived`** — one `declared` edge
   downgrades the whole plan
5. at least one affected asset resolves to a registered executable job
6. blast radius within `max_blast_radius`
7. cost class within `max_cost_class`
8. the attempt limit is not reached
9. the incident is not terminal
10. the plan targets this incident

Otherwise: **`INCIDENT_OPEN` + `RECOVERY_PLAN_REQUIRES_APPROVAL`**, and the plan is written
down rather than run. Every reason is reported, not just the first — an approver who fixes
one blocker only to hit the next stops reading the reasons.

**The loop terminates.** Attempts are capped; at the limit the incident goes `EXHAUSTED`
and no further automatic action follows. A reliability platform that can loop is an outage
generator with good intentions.

**Power BI is marked, never refreshed automatically.** There is no registered job, and
refreshing a report is an outward-facing action.

---

## 7. Close the incident

An incident closes as `RESOLVED` only when the figure is re-certified. Record:

plan id → approval (and by whom, if human) → turns → attempts → Airflow run ids → Spark app
ids → dbt invocation id → watermarks → Iceberg snapshots before and after → DQ and
reconciliation run ids → final certification tier.

`RESOLVED` without a resolution time is refused by the model: an incident that cannot be
aged is one that stops being visible.

---

## 8. Quick reference

| Situation | First command |
|---|---|
| "is anything broken?" | `python3 scripts/cdc-e2e-verify.py` |
| "did the close certify?" | query `ops.eod_run` for the COB |
| "did REALTIME actually read anything?" | query `ops.realtime_run` — look at `read_mode` and `input_rows` |
| "ingest says success but the table is empty" | `ops.streaming_batch_ledger` — check `batches` |
| "who owns this table?" | `compile_inventory()` — see §1e |
| "what breaks if I change this model?" | `dbt ls --select <model>+` |
| "rerun one mart for one day" | `dbt build --select <model> --vars '{business_date: …}'` |
| "rerun everything downstream of a source" | `dbt build --select source:<src>+` |
| "late data arrived" | trigger `datamart_auto_correct` with the affected keys |
| "a whole day is missing" | trigger `datamart_fulfill` for that date |
| "the overlay double-counts after a close" | `realtime_engine.py --table ALL --rebase-cob AUTO` |
