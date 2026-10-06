# TESTING DATA LINEAGE AND RECOVERY — IN AWS, END TO END

A hands-on guide. Every command is real, every table name is the deployed one, and every
expected output below was produced on this account (`111122223333`, `ap-southeast-1`).

**What this answers:** *how do I prove the lineage is right, and when a table or a column
goes wrong, how do I rerun only what was affected?*

- Companions: `LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md` (the loop), `END_TO_END_LINEAGE.md` (the
  graph), `DATA_QUALITY_MODEL.md` (the verdicts)
- Prerequisite for every AWS command: `export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1`

---

## 0. Ten-minute smoke: is the platform telling the truth?

```bash
# Who am I, and against what
aws sts get-caller-identity --output table

# 39 read-only checks across the whole chain
python3 scripts/cdc-e2e-verify.py

# The reliability ledgers exist and are the shape the model expects
bash scripts/create-reliability-tables.sh --verify
```

Expected:

```
PRESENT  dq_result_v2  (21 columns)
PRESENT  reconciliation_run  (19 columns)
PRESENT  data_incident  (22 columns)
PRESENT  recovery_plan  (21 columns)
PRESENT  recovery_execution  (11 columns)
```

---

## 1. Test the lineage graph — offline first, then in AWS

### 1a. The model (sub-second, $0, no AWS)

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.lineage_graph import build_static_graph, audit; \
  from cdc.urns import UrnMinter, default_catalog; \
  from cdc.metadata_plane import load; \
  g = build_static_graph(UrnMinter(load().environment, default_catalog())); \
  print(len(g.nodes),'nodes',len(g.edges),'edges',len(g.cycles()),'cycles'); \
  [print(' ',f) for f in audit(g)]"
```

Expected: `81 nodes 80 edges 0 cycles` and **no findings**.

If this is dirty, stop — everything below inherits the problem.

### 1b. Prove one critical field, source to mart

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.lineage_graph import build_static_graph; \
  from cdc.urns import UrnMinter, default_catalog; \
  from cdc.metadata_plane import load; from cdc.assets import AssetId; \
  m = UrnMinter(load().environment, default_catalog()); g = build_static_graph(m); \
  p = g.path(m.dataset_urn(AssetId.parse('src:oracle.coredb.corebank.account')), \
             m.dataset_urn(AssetId.parse('mart:mart_account_balance_daily'))); \
  [print(' ', m.parse_dataset(x)[1]) for x in p]; \
  print('weakest evidence:', g.weakest_evidence(p).value)"
```

Expected — **8 hops**:

```
coredb.corebank.account
cdc.oracle.COREBANK.ACCOUNT
kafka_dev_lab_dev_full_cdc.cdc_oracle_coredb_corebank_account
kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account
kafka_dev_lab_dev_curated.banking_account
kafka_dev_lab_dev_curated.fact_account_daily_snapshot
kafka_dev_lab_dev_mart.stg_fact_account_daily_snapshot
kafka_dev_lab_dev_mart.mart_account_balance_daily
weakest evidence: declared
```

> **Read `weakest evidence` carefully.** `declared` means a human asserted at least one hop.
> It is why an automatic recovery across this path is refused — see §5.

### 1c. Column level

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.lineage_graph import build_static_graph; \
  from cdc.urns import UrnMinter, default_catalog; \
  from cdc.metadata_plane import load; from cdc.assets import AssetId; \
  m = UrnMinter(load().environment, default_catalog()); g = build_static_graph(m); \
  u = m.dataset_urn(AssetId.parse('curated:banking_account')); \
  [print(' ',c.upstream_column,'->',c.downstream_column,'|',c.evidence.value,'|',c.source) \
     for c in g.column_upstreams(u,'balance')]"
```

Expected: `BALANCE -> balance | derived | curated/entities.yaml`

**Why `derived` and not a guess:** `curated_build.py` reads that same file to do the work.

### 1d. In AWS — the physical tables behind the graph

```bash
for db in full_cdc stream snapshot curated mart ops; do
  echo "=== $db ==="
  aws glue get-tables --database-name "kafka_dev_lab_dev_$db" \
    --query 'TableList[].Name' --output text | tr '\t' '\n' | head -20
done
```

Every name the graph prints must exist here. A name in the graph and not in Glue is a
lineage lie; a table in Glue and not in the graph is an **ungoverned asset** — the DRP0
finding.

### 1e. In DataHub (optional — costs ~10 GB RAM)

```bash
bash scripts/datahub-local.sh preflight
bash scripts/datahub-local.sh up --execute
export DATAHUB_GMS_URL=http://localhost:8080
# publish, then traverse — see DATAHUB_OPERATIONS_RUNBOOK.md §4
```

> **Wait for the index before trusting a traversal.** DataHub's graph index is *eventually
> consistent*. Measured here: **3 assets seconds after a publish, 18 minutes later**, with
> the stored aspects correct throughout. A traversal run too early does not fail — it
> returns a **smaller** blast radius, confidently.

---

### 1f. Is runtime lineage actually being emitted?

The difference between *declared* and *observed* lineage is whether anything emitted. Two
engines emit here, and they are configured differently — which is the whole point of ADR-091.

**Spark.** The conf is derived, never typed:

```bash
python3 -c "
from cdc.lineage_runtime import LineageRuntimeConfig, LineageEmitter, DEFAULT_CONFIG
from cdc.metadata_plane import load as load_plane
from cdc.urns import UrnMinter, default_catalog
cfg = LineageRuntimeConfig.load(DEFAULT_CONFIG); plane = load_plane(mode='local')
em = LineageEmitter(cfg, plane, UrnMinter(plane.environment, default_catalog()))
for k, v in em.spark_conf().items(): print(f'{k}={v}')"
```

Then submit with the listener attached — it is opt-in, so nothing changes unless you ask:

```bash
OPENLINEAGE=1 bash scripts/emr-submit.sh ...
```

**Airflow.** One command, dry-run by default:

```bash
scripts/airflow-enable-lineage.sh --verify              # read-only: image, env, listener
scripts/airflow-enable-lineage.sh                       # dry run: prints the plan
scripts/airflow-enable-lineage.sh --execute             # sets the config, rolls the image
scripts/airflow-enable-lineage.sh --rollback --execute  # helm rollback
```

It runs over SSM because there is no helm or kubectl on the workstation and installing
them would not help — the k3s API server has no public endpoint, so a local helm has
nothing to talk to. Both binaries are already on the node.

> **What `--verify` will tell you, and it surprised us.** The stock `apache/airflow:3.2.2`
> **already ships** `apache-airflow-providers-openlineage==2.17.0`, and the plugin is
> already registered. The provider was never missing. The deployed release simply carries
> **no** `AIRFLOW__OPENLINEAGE__*` variables, so nothing is configured to emit. The custom
> image exists to align versions — stock pairs the provider with `openlineage-python
> 1.47.1` while the Spark listener is pinned at **1.53.0**, and two integrations writing
> one graph on different client majors drift in producer strings and facet schema
> versions.

**What a healthy result looks like:**

| | |
|---|---|
| `airflow plugins` | lists `OpenLineageProviderPlugin` with the `OpenLineageListener` |
| event count | non-zero — 2 per task (START/COMPLETE) plus one DAG-level COMPLETE |
| `job.namespace` | `cdc-lakehouse-dev`, equal to what the module derives |
| `run.facets.parent` | **present** — this is what links an Airflow run to the Spark job it submitted |

> **The trap this exists for.** A green DAG run tells you nothing about lineage. Before the
> fix, `AIRFLOW__OPENLINEAGE__TRANSPORT` was the bare word `console`; the provider parses
> that key as JSON, threw during plugin import, and Airflow **silently ran without the
> listener** — `REGISTERED_LISTENERS = []`, every task successful, zero events. So do not
> check that the DAG succeeded. **Count the events.** If the count is 0, run
> `airflow plugins` — an empty listener column is the signature.

Afterwards, delete the pod: `kubectl -n airflow delete pod ol-probe`.

---

## 2. Check the data itself — is anything actually wrong?

All read-only, cents at most.

```sql
-- Did the EOD close certify, and on what?
SELECT cob_date, table_id, status, rows_written, certified, run_id, finished_at
FROM   kafka_dev_lab_dev_ops.eod_run
WHERE  cob_date = DATE '2026-09-28' ORDER BY table_id;

-- Did REALTIME read a real delta, or no-op?
SELECT table_id, operation, read_mode, fallback_reason,
       source_snapshot_before, input_rows, rows_written, attempt
FROM   kafka_dev_lab_dev_ops.realtime_run
WHERE  finished_at > current_timestamp - interval '1' day ORDER BY finished_at DESC;

-- THE TRAP: ingest reporting SUCCESS with zero rows
SELECT app_id, batches, rows_written, started_at, finished_at
FROM   kafka_dev_lab_dev_ops.streaming_batch_ledger ORDER BY finished_at DESC LIMIT 20;
```

> `batches = 0, rows = 0` with a green exit is not a quiet day. On 2026-09-29 it was a
> Structured Streaming checkpoint that had outlived its MSK cluster. Only a before/after row
> count reveals it.

### Table-level checks

```sql
-- EOD invariant: one active row per key
SELECT dv_pk_hash, count(*) n
FROM   kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account
WHERE  cob_date = DATE '2026-09-28'
GROUP  BY dv_pk_hash HAVING count(*) > 1;

-- Layer agreement for one COB
SELECT 'full_cdc' layer, count(*) n
FROM   kafka_dev_lab_dev_full_cdc.cdc_oracle_coredb_corebank_account
WHERE  event_date = DATE '2026-09-28'
UNION ALL
SELECT 'eod', count(*)
FROM   kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account
WHERE  cob_date = DATE '2026-09-28';
```

### Column-level checks

```sql
-- A measure that should never be null or negative
SELECT count(*) AS null_balance
FROM   kafka_dev_lab_dev_curated.banking_account WHERE balance IS NULL;

SELECT count(*) AS negative_balance
FROM   kafka_dev_lab_dev_curated.banking_account WHERE balance < 0;

-- Does the mart's measure still agree with its source column?
SELECT (SELECT sum(balance) FROM kafka_dev_lab_dev_curated.banking_account) AS curated_total,
       (SELECT sum(closing_balance) FROM kafka_dev_lab_dev_mart.mart_account_balance_daily
        WHERE cob_date = DATE '2026-09-28')                                 AS mart_total;
```

### Which Iceberg snapshot am I looking at?

```sql
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM   kafka_dev_lab_dev_snapshot."eod_oracle_coredb_corebank_account$snapshots"
ORDER  BY committed_at DESC LIMIT 10;
```

Snapshots are how a rerun is made reproducible, and how you prove which version a figure
came from.

---

## 3. Record the failure — the ledgers, in AWS

The five ledgers exist and round-trip. This is what a real DQ failure writes:

```sql
-- what blocked, and did it hold the watermark?
SELECT dataset_id, check_id, severity, status, observed_value, failed_count,
       rows_examined, sample_reference, config_version
FROM   kafka_dev_lab_dev_ops.dq_result_v2
WHERE  cob_date = DATE '2026-09-28';

-- do the layers agree?
SELECT source_dataset, target_dataset, metric, source_value, target_value,
       difference, tolerance_pct, status
FROM   kafka_dev_lab_dev_ops.reconciliation_run
WHERE  cob_date = DATE '2026-09-28';

-- the incident it opened
SELECT incident_id, dataset_id, failure_class, severity, status, root_cause_status,
       blast_radius_count, automatic_recovery_allowed, attempts
FROM   kafka_dev_lab_dev_ops.data_incident
WHERE  cob_date = DATE '2026-09-28';
```

### Produce real verdicts, without EMR

```bash
python3 scripts/run-dq-athena.py --business-date 2026-09-28                  # dry run
python3 scripts/run-dq-athena.py --business-date 2026-09-28 --layer EOD --execute
```

This runs the **generated** rule set — the same `build_rules()` output the Spark engine
takes — through Athena, and writes the same `DqResult` contract to the same ledger. Two
executors, one rule set, one result shape. A second rule set would be the drift this whole
platform exists to end.

Observed on 2026-09-28: **151 verdicts** in `ops.dq_result_v2`, `BLOCKS_PUBLISH True`.

> **Read `NOT_EVALUATED` correctly.** 25 of the EOD results are `NOT_EVALUATED` because
> those tables hold no rows for that COB — and the suite still blocks, because an
> unevaluated BLOCKER check is not a pass. *"We looked and there was nothing"* is a
> different answer from *"it is fine"*, and conflating them is the defect the whole
> three-verdict design exists to prevent.

### The severity that decides everything

| Severity | Certify? | Advance watermark? |
|---|---|---|
| `BLOCKER` | no | **no** — snapshot marked INVALID |
| `ERROR` | no | yes — correctable later |
| `WARN` / `INFO` | yes | yes |

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.dq_catalog import evaluate_publish; \
  from cdc.quality import DqResult, DataInterval, ResultStatus; \
  from cdc.contracts import CheckType, Severity; from datetime import date; \
  r = DqResult(dq_run_id='d',run_id='r',dataset_id='eod:x',check_id='c', \
      check_type=CheckType.UNIQUENESS,severity=Severity.BLOCKER, \
      interval=DataInterval(cob_date=date(2026,9,28)),status=ResultStatus.FAIL, \
      expected_rule='one row per key',engine='spark',config_version='v', \
      rows_examined=10,failed_count=1); \
  print(evaluate_publish(committed=True, dq_results=[r]).payload())"
```

Expected: `outcome INVALID · advance_watermark False · mark_snapshot_invalid True`.

---

## 4. Find the blast radius — what else is wrong?

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.impact import LineageImpactService; \
  from cdc.lineage_graph import build_static_graph; \
  from cdc.urns import UrnMinter, default_catalog; \
  from cdc.metadata_plane import load; from cdc.assets import AssetId; \
  m = UrnMinter(load().environment, default_catalog()); g = build_static_graph(m); \
  s = LineageImpactService(g, environment='dev', minter=m); \
  r = s.downstream(m.dataset_urn(AssetId.parse('eod:oracle_coredb_corebank_account'))); \
  print('impacted:', len(r.nodes), '| executable:', len(r.executable)); \
  [print(' ', k, len(v)) for k,v in r.by_class().items()]"
```

Expected: **14 impacted, 14 executable**.

**Column-scoped** (narrower, where column edges are known):

```python
r = s.downstream(urn, column="BALANCE")
```

It falls back to dataset scope where no column edge exists — narrowing on an unknown mapping
would *exclude* affected assets, which is the dangerous direction.

**Cross-check with dbt**, which is authoritative for model dependencies:

```bash
cd dbt && dbt ls --select 'source:curated.fact_account_daily_snapshot+' --resource-type model
```

---

## 5. Get the plan — and read why it refuses

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from datetime import date; \
  from cdc.impact import LineageImpactService; \
  from cdc.lineage_graph import build_static_graph; \
  from cdc.urns import UrnMinter, default_catalog; \
  from cdc.metadata_plane import load; from cdc.assets import AssetId; \
  from cdc.incidents import *; \
  m = UrnMinter(load().environment, default_catalog()); g = build_static_graph(m); \
  s = LineageImpactService(g, environment='dev', minter=m); \
  inc = Incident('inc-1','eod:oracle_coredb_corebank_account', \
        FailureClass.DQ_VIOLATION,'BLOCKER', \
        scope=AffectedScope(cob_dates=(date(2026,9,28),)), \
        root_cause_status=RootCauseStatus.CONFIRMED); \
  p,a,why = s.plan(inc, root_urn=m.dataset_urn(AssetId.parse('eod:oracle_coredb_corebank_account')), \
        policy=RecoveryPolicy(max_attempts=2,max_blast_radius=40), lineage_version='lin-1'); \
  print('plan', p.plan_id, '| radius', p.blast_radius, '| turns', [len(t) for t in p.turns]); \
  print('weakest', p.weakest_evidence.value, '| cost', p.cost_class.value); \
  print('approval', a.value); [print('  -', r) for r in why]"
```

Expected:

```
plan rp1:… | radius 14 | turns [1, 1, 2, 2, 5, 3]
weakest declared | cost medium
approval REQUIRES_APPROVAL
  - the weakest lineage edge on this path is declared; automatic recovery may only
    traverse derived, observed
```

**This refusal is the system working.** Every mart sits downstream of a Kimball fact whose
producer is declared only in Python, so a human approves. Closing that means giving
`spark/facts/` a config contract — *not* lowering the gate.

A `SOURCE_DEFECT` returns **no plan at all**: `WAITING_SOURCE_CORRECTION`, and nothing is
ever written back to Oracle or SQL Server.

### Inspect a stored plan — no scheduler required

```bash
python3 scripts/run-recovery.py --plan-id rp1:45b21f325ca10ec5            # gate + turns
python3 scripts/run-recovery.py --plan-id rp1:45b21f325ca10ec5 --status   # attempts so far
```

The gate, the turn ordering and the job resolution have nothing to do with scheduling, so
they do not need Airflow — and tying them to it once made the safety logic untestable
everywhere except the one cluster.

```
radius 14 · weakest=derived · cost=medium · approval=AUTOMATIC
turn 0: curated:banking_account          turn 3: 2 dbt staging models
turn 1: curated:dim_account              turn 4: 5 marts
turn 2: 2 curated facts                  turn 5: 3 marts
→ all 14 targets resolve to registered entrypoints
```

**`--execute` records the attempt; it does not rebuild.** The rebuild is an EMR submission,
and the execution row says `BLOCKED_REQUIRES_EMR` rather than `SUCCEEDED`. A coordinator
reporting success having run no turn is the failure this platform keeps finding.

> **Driving it through Airflow instead.** `airflow dags trigger recovery_coordinator` needs
> the DAG registered. Locally that means pointing the scheduler at this repository —
> `AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags airflow dags list` (verified: all 25 DAGs
> parse). Without it you get `DagNotFound`, which is the right answer to the wrong command.

---

## 6. Rerun ONLY what is affected

Scope down in three dimensions: **table, date, and key or column.**

### One table, one COB — EOD

```bash
bash scripts/eod-close.sh --table eod_oracle_coredb_corebank_account --cob 2026-09-28
bash scripts/eod-close.sh --table eod_oracle_coredb_corebank_account --cob 2026-09-28 --execute
```

`--table ALL` re-closes ten tables to fix one; the nine already certified become nine more
things that can go wrong.

> A COB close sees only events **dated to that COB** (an `event_date` prune of ±1 day around
> the cutoff). A certify with `rows=0` usually means the event dates, not a broken close.

### One table — REALTIME

```bash
python3 spark/jobs/realtime/realtime_engine.py --table rt_oracle_coredb_corebank_account
python3 spark/jobs/realtime/realtime_engine.py --table rt_oracle_coredb_corebank_account --force-window-scan
python3 spark/jobs/realtime/realtime_engine.py --table ALL --rebase-cob AUTO
```

### One mart, one interval — dbt (this is where "only the affected column" lives)

```bash
cd dbt
dbt build --select mart_account_balance_daily        --vars '{business_date: "2026-09-28"}'
dbt build --select mart_account_balance_daily+       --vars '{business_date: "2026-09-28"}'
dbt build --select source:curated.fact_account_daily_snapshot+ --vars '{business_date: "2026-09-28"}'
dbt build --select state:modified+ --state ./target-last-good
```

**Column scope, honestly.** dbt rebuilds a *model*, not a column — a model is the smallest
unit it can rebuild. So "only the affected column" means one of two things:

1. **Rerun only the models that carry it**, rather than the whole downstream cone:
   ```bash
   dbt ls --select 'mart_account_balance_daily+' --output json \
     | python3 -c "import sys,json;[print(json.loads(l)['name']) for l in sys.stdin if l.strip()]"
   ```
   Column lineage (§1c) turns that from a grep into a query.
2. **Bound the rows instead** — one COB, or one key set — which is usually what the incident
   actually needs.

### Bounded keys and dates — late data

```bash
airflow dags trigger datamart_auto_correct \
  --conf '{"business_date":"2026-09-28","affected_keys":["1001","1002"]}'
airflow dags trigger datamart_fulfill --conf '{"business_date":"2026-09-20"}'
```

`PRIMARY_KEY` when the mart is per-key; `ALL_KEYS_IN_DATE` when it aggregates, because one
changed row moves a total every key's row depends on.

### What NOT to do

| Tempting | Why it is wrong |
|---|---|
| reset Kafka offsets / delete the checkpoint | does not fix bad data, and **hides** the zero-row ingest failure |
| rebuild all history for a one-day defect | the defect is dated; the rebuild is not |
| rerun descendants before the root validates | a second wrong number on top of the first |
| write a correction back to Oracle/SQL Server | refused unconditionally |
| `dbt build` with no `--select` | rebuilds the warehouse to fix one mart |

### EMR reality

100 GB disk cap (5 workers). **Submit one job at a time** — two concurrent hit
`ApplicationMaxCapacityExceededException`, and since the VPC rebuild each takes 45–60 min to
schedule. Plan a recovery as a sequence, not a fan-out.

---

## 7. Validate, re-certify, close

```
repair root → DQ + reconciliation on the ROOT → root CERTIFIED → descendants
            → DQ + reconciliation → re-certify → close incident
```

Descendants do not start until the root's gates pass. Re-certification is not automatic: the
gates in `CERTIFICATION_MODEL.md` must all pass again.

```sql
UPDATE kafka_dev_lab_dev_ops.data_incident
SET status = 'RESOLVED', resolved_at = current_timestamp
WHERE incident_id = 'inc-…';   -- only after a re-certification
```

An incident closed without a re-certification is an incident nobody fixed.

---

## 8. One-page cheat sheet

| Question | Command |
|---|---|
| is the graph sound? | the `audit()` snippet, §1a |
| where did this number come from? | `g.path(src, tgt)` then `g.weakest_evidence(path)` |
| where did this **column** come from? | `g.column_upstreams(urn, 'balance')` |
| did the close certify? | `SELECT … FROM ops.eod_run` |
| did REALTIME read anything? | `SELECT … FROM ops.realtime_run` |
| ingest green but table empty? | `ops.streaming_batch_ledger` → check `batches` |
| what broke? | `SELECT … FROM ops.dq_result_v2` |
| what else is affected? | `svc.downstream(urn).by_class()` |
| may it auto-recover? | `svc.plan(...)` — read **every** reason |
| rerun one mart, one day | `dbt build --select <model> --vars '{business_date: …}'` |
| rerun everything downstream of a source | `dbt build --select source:<src>+` |
| late data arrived | trigger `datamart_auto_correct` with the keys |
| overlay double-counts after a close | `realtime_engine.py --table ALL --rebase-cob AUTO` |
