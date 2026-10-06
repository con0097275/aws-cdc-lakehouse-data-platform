# Adding a new CDC table, and a new dbt-Spark model with its dependencies

Two independent workflows. Do them in this order if the new model reads a new source table.

```
A.  new SOURCE TABLE  ->  Kafka topic  ->  FULL_CDC on S3
B.  new dbt MODEL + job YAML  ->  plan  ->  Airflow turn ordering  ->  mart on S3
```

**B is the one with the strong claim** (ADR-039): *a new mart needs a YAML and a SQL file
and nothing else — no Airflow DAG is written, no scheduler code changes, no turn is
hand-assigned.* That claim is now tested end to end, not asserted; see §B7.

---

# A. A new CDC source table into S3

## A1. Create it in the source, with CDC enabled

Oracle needs **per-table** supplemental logging. Database-level `MIN=YES` is not enough:
without `ALL COLUMN LOGGING` an UPDATE arrives with unchanged columns NULL, and every
before/after comparison downstream is silently wrong.

```sql
-- Oracle, as corebank
CREATE TABLE corebank.loan (
  loan_id      NUMBER(12)   NOT NULL,
  customer_id  NUMBER(12)   NOT NULL,
  principal    NUMBER(18,2) DEFAULT 0 NOT NULL,   -- NUMBER(p,s), never FLOAT
  status       VARCHAR2(20) DEFAULT 'ACTIVE' NOT NULL,
  updated_at   TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_loan PRIMARY KEY (loan_id)
);
-- as sysdba, in the PDB
ALTER TABLE corebank.loan ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

> `NUMBER(18,2)`, not `FLOAT`. Debezium maps a scaled NUMBER to a Kafka Connect `Decimal`;
> a float makes every downstream SUM non-reproducible.

SQL Server needs the capture instance:

```sql
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='loan',
                             @role_name=NULL, @supports_net_changes=0;
```

## A2. Create the topic BEFORE the connector produces to it

`auto.create.topics.enable=false`. Add the table to the loop in
`docker/cdc-runtime/create-topics.sh`:

```bash
for t in BRANCH CUSTOMER ACCOUNT TRANSACTION LOAN; do        # <- add LOAN
  create "cdc.oracle.COREBANK.$t" 3 "$RF" "--config cleanup.policy=delete --config retention.ms=86400000"
done
```

**Oracle folds unquoted identifiers to UPPERCASE**, so the topic is
`cdc.oracle.COREBANK.LOAN`. This has bitten before: lowercase topics were created, the
connector reported RUNNING, produced into the UPPERCASE names, and eight topics sat at
offset 0 while every health check stayed green. SQL Server includes the DATABASE in the
topic name: `cdc.sqlserver.digital.dbo.loan`.

```bash
bash scripts/cdc-runtime.sh create-topics --execute      # phrase: CREATE TOPICS
```

## A3. Register the table with the connector

`connectors/oracle/corebank-source.json.tmpl` — **two** places, and missing the second is
silent:

```jsonc
"table.include.list":  "COREBANK.CUSTOMER,COREBANK.ACCOUNT,COREBANK.TRANSACTION,COREBANK.BRANCH,COREBANK.LOAN",
"message.key.columns": "...;COREBANK.LOAN:LOAN_ID"
```

`message.key.columns` is what puts the canonical PK in the Kafka key. Omit it and Debezium
still produces — keyed by its own default — and **the same PK can land in different
partitions**, which breaks per-key ordering and every "last event wins" result downstream
(CLAUDE.md 5.1). Nothing errors.

```bash
bash scripts/register-connectors.sh render                 # check the rendered JSON first
bash scripts/register-connectors.sh register --execute     # phrase: REGISTER CONNECTORS
bash scripts/register-connectors.sh status                 # connector AND task RUNNING
```

## A4. Refresh the writer-schema export

A new table means a new Avro schema and a new `globalId`. The canonical ingest selects the
writer schema per record by that id; an id missing from the export is **quarantined, not
guessed**.

```bash
bash scripts/cdc-runtime.sh export-schemas --execute kafka-dev-lab-dev-lake-111122223333
```

## A5. Ingest into FULL_CDC

Add the topic to `--topics` on either the batch job (`full_cdc_job.py`) or the streaming one
(`full_cdc_stream_job.py`) — both share one decode path, so they behave identically. Full
submit commands: `docs/VERIFY_EVERY_FEATURE.md` §4.

```
FULL_CDC_DECODE cdc.oracle.COREBANK.LOAN globalId=<n>
FULL_CDC_TOPIC  cdc.oracle.COREBANK.LOAN rows=<n>
```

## A6. Verify it landed on S3

```bash
aws s3 ls s3://kafka-dev-lab-dev-lake-111122223333/warehouse/full_cdc/cdc_events/data/ --recursive | head
```

```sql
SELECT source_system, source_table, COUNT(*) rows
FROM kafka_dev_lab_dev_full_cdc.cdc_events
WHERE source_table='LOAN' GROUP BY 1,2;

-- the ordering guarantee, per key
WITH k AS (SELECT json_extract_scalar(payload_after,'$.LOAN_ID') pk, kafka_partition
           FROM kafka_dev_lab_dev_full_cdc.cdc_events WHERE source_table='LOAN')
SELECT SUM(CASE WHEN n>1 THEN 1 ELSE 0 END) AS pks_spanning_partitions
FROM (SELECT pk, COUNT(DISTINCT kafka_partition) n FROM k WHERE pk IS NOT NULL GROUP BY pk);
```

`pks_spanning_partitions` must be **0**. If it is not, `message.key.columns` is wrong (A3).

---

# B. A new dbt-Spark model with dependency-ordered deployment

Worked example: `mart_account_risk_daily`, added 2026-09-06, which reads
`mart_account_balance_daily` and therefore must run **after** it.

## B1. Write the model

`dbt/models/marts/mart_account_risk_daily.sql`

```sql
{{ config(**reporting_merge_config(
    unique_key=['account_sk', 'business_date'],
    partition_by=['business_date'])) }}

WITH source AS (
    SELECT account_sk, customer_sk, business_date, closing_balance
    FROM {{ ref('mart_account_balance_daily') }}      -- <- THIS registers the edge
    WHERE {{ incremental_filter('business_date') }}    -- <- the only per-mode branch
),
tiered AS (
    SELECT source.*,
           CASE WHEN closing_balance >= 10000000 THEN 'TIER_1_HIGH'
                WHEN closing_balance >=  1000000 THEN 'TIER_2_MEDIUM'
                WHEN closing_balance >=        0 THEN 'TIER_3_STANDARD'
                ELSE 'TIER_0_OVERDRAWN' END AS risk_tier,
           '{{ processing_status_for_mode() }}' AS processing_status,
           {{ input_cutoff() }} AS input_cutoff
    FROM source
)
SELECT _source.*, {{ audit_columns() }}, {{ reporting_audit_columns() }}
FROM tiered AS _source
WHERE {{ merge_guard(unique_key=['account_sk', 'business_date']) }}
```

Four things are not optional:

| macro | why |
|---|---|
| `reporting_merge_config()` | `MERGE_LADDER`, not `insert_overwrite`. AUTO_CORRECT and STREAM_BATCH also write marts; an overwrite erases the provisional rows the accuracy ladder protects |
| `incremental_filter()` | ONE model, FOUR modes. Separate SQL per mode makes a provisional-vs-certified variance ambiguous between "late data" and "divergent logic" |
| `processing_status_for_mode()` | the tier is decided by the MODE. Letting STREAM_BATCH stamp CERTIFIED defeats the ladder from inside |
| `merge_guard()` | drops rows that would DOWNGRADE an existing one, as a pre-filter. dbt-spark has no `WHEN MATCHED AND`, and putting the guard in the ON clause is actively wrong — a losing row falls to NOT MATCHED and gets INSERTED, duplicating the key |

## B2. Register the job — one YAML, and that is the whole registration

`reporting/jobs/mart_account_risk_daily.yaml`. Copy the closest existing job and change the
identity fields; every other block (per-mode schedules, retries, cost guards) is inherited
structure you rarely touch.

```yaml
job:
  job_id: mart_account_risk_daily
  dbt_model: mart_account_risk_daily
  dbt_selector: mart_account_risk_daily
  execution_engine: DBT_SPARK
  target:
    logical_layer: MART
    database: kafka_dev_lab_dev_mart
    table: mart_account_risk_daily
  write:
    primary_key: [account_sk, business_date]
    business_date_column: business_date
    partition_spec: [business_date]
    merge_strategy: MERGE_LADDER

dependencies:
  - dependency_type: JOB
    upstream_job_id: mart_account_balance_daily
    required: true
```

**No Airflow DAG is written. No turn is assigned.** The engine derives the turn.

Dependency types:

| type | use for |
|---|---|
| `JOB` | another reporting job must finish first — creates the turn ordering |
| `EOD_TABLE` | a GATE, not an ordering constraint: wait for `ops.eod_watermark` + the Iceberg tag on an upstream layer. There is no job to run before, only a condition |

## B3. Compile and check the turn placement — locally, before deploying

```bash
python3 - <<'PY'
import sys; sys.path.insert(0, "reporting")
from compile import compile_plan
from pathlib import Path
p = compile_plan(Path("reporting"), environment="dev",
                 lake_bucket="kafka-dev-lab-dev-lake-111122223333")
de = p["plan"]["dependency_engine"]["modes"]["ALL"]
print("config_version:", p["config_version"])
for t, jobs in sorted(de["waves"].items()):
    print(f"  wave {t}: {jobs}")
PY
```

```
config_version: cfg-0bbc686f6bf62904
  wave 1: ['mart_account_balance_daily', 'mart_channel_engagement_daily']
  wave 2: ['mart_account_balance_monthly', 'mart_account_risk_daily']   <- derived
```

`config_version` changes whenever the plan content changes. If it did not change, your YAML
was not picked up.

## B4. `ref()` alone is enough — the edges merge automatically (ADR-037)

`compile_plan(..., dbt_project=Path("dbt"))` runs `dbt parse` and merges the model's `ref()`
graph into the authored edges, so nobody maintains the same graph twice:

```bash
cd dbt && dbt parse --no-partial-parse --profiles-dir <dir>
python3 -c "
import json; m=json.load(open('target/manifest.json'))
n=[k for k in m['nodes'] if k.endswith('mart_account_risk_daily')][0]
print(m['nodes'][n]['depends_on']['nodes'])"
# ['model.aws_cdc_lakehouse.mart_account_balance_daily']
```

The repo has no `profiles.yml` — `emr_dbt_bootstrap.py` writes one at runtime. For a local
parse, any profile works because `dbt parse` does not connect. It needs a target named
**`local`** (`manifest_sync.generate_manifest` passes `--target local`):

```yaml
aws_cdc_lakehouse:
  target: local
  outputs:
    local:          {type: spark, method: session, schema: kafka_dev_lab_dev_mart, host: localhost}
    emr_serverless: {type: spark, method: session, schema: kafka_dev_lab_dev_mart, host: localhost}
```

Declaring the edge in the YAML **as well** is belt and braces: it documents intent for a
reader who has not run `dbt parse`, and it keeps the compiler usable where dbt is absent.

## B5. Deploy — two artifacts, both to S3

**Never edit files on the Airflow host.** `airflow-dag-sync.timer` re-syncs
`/opt/airflow/dags` from S3 **every 60 seconds with `--delete`**, so a host edit is reverted
within a minute while the pods keep running the old code.

```bash
LAKE=kafka-dev-lab-dev-lake-111122223333

# 1. the dbt project (the model)
#    rebuild FROM THE REPO so the zip cannot drift from version control (finding E3)
aws s3 cp <rebuilt>/dbt-project.zip s3://$LAKE/artifacts/dbt/dbt-project.zip

# 2. the compiled plan (the job + its turn)
aws s3 cp <compiled>/plan.json s3://$LAKE/bootstrap/airflow/reporting/plan.json
```

Back up both first — they are what every reporting run reads.

Confirm the node picked it up (≤60 s):

```bash
# on the airflow node
python3 -c "import json;print(json.load(open('/opt/airflow/dags/_reporting/plan.json'))['config_version'])"
# must print your new cfg-...
```

## B6. Run it

Through Airflow:

```bash
A="k3s kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- airflow"
$A dags trigger datamart_eod --run-id newjob_eod --logical-date 2026-09-06T06:00:00+00:00
```

Or the non-Airflow driver, which compiles the plan fresh from the repo:

```bash
python3 scripts/reporting-live-run.py --flow-mode EOD --business-date $(date -u +%F) --execute
```

> **These two read DIFFERENT plans.** `reporting-live-run.py` compiles from the repo at
> call time; Airflow reads the deployed `plan.json`. They drift the moment you change a YAML
> without deploying — observed here: the repo compiled `cfg-1682b193a20ce2ae` while the node
> ran `cfg-418d7158728c925c`. Deploy before comparing their results.

## B7. Verify — turn ordering, then the table

```
### datamart_eod  RUN_STATE=success
    resolve_plan           success
    turn_1.select_wave     success
    turn_1.gate_and_run  map=0  success     mart_account_balance_daily
    turn_1.gate_and_run  map=1  success     mart_channel_engagement_daily
    turn_2.select_wave     success
    turn_2.gate_and_run  map=0  success     mart_account_balance_monthly
    turn_2.gate_and_run  map=1  success     mart_account_risk_daily   <- the new job
    finalize               success
```

**turn_2 going from 1 mapped task to 2 is the proof.** No DAG file was edited; the mapped
task count comes from `resolve_plan` reading the deployed plan.

```bash
aws glue get-table --database-name kafka_dev_lab_dev_mart --name mart_account_risk_daily \
  --query 'Table.StorageDescriptor.Location' --output text
# s3://kafka-dev-lab-dev-lake-111122223333/warehouse/mart/mart_account_risk_daily
```

The table did not exist before this run. dbt created it, in the right database, with the
audit columns the framework macros add (`run_id`, `certified_at`, `built_at`,
`execution_id`, `coordinator_run_id`, `config_version`, `source_flow_mode`).

**Actual result of this worked example**, 2026-09-06:

```
risk_tier         accounts   balance             processing_status
TIER_1_HIGH             50     542,465,525.00    CERTIFIED
TIER_2_MEDIUM          270   1,489,414,635.00    CERTIFIED
TIER_3_STANDARD          3          15,623.99    CERTIFIED
                       ---   ----------------
                       323   2,031,895,783.99

parent mart_account_balance_daily : 323 rows / 2,031,895,783.99
child  mart_account_risk_daily    : 323 rows / 2,031,895,783.99   <- exact
```

`processing_status = CERTIFIED` on every row is the mode stamping working: an EOD run
certifies, and `processing_status_for_mode()` — not the model — decided that.

```sql
SELECT risk_tier, COUNT(*) accounts, ROUND(SUM(closing_balance),2) balance
FROM kafka_dev_lab_dev_mart.mart_account_risk_daily
WHERE business_date = DATE '2026-09-06'
GROUP BY risk_tier ORDER BY 1;
```

And the dbt log, which is where the grain tests appear:

```bash
aws s3 cp "s3://$LAKE/logs/emr/applications/<app>/jobs/<run>/SPARK_DRIVER/stdout.gz" - \
  | gunzip | grep -E "dbt=|PASS|WARN|ERROR|Completed"
# Done. PASS=n WARN=0 ERROR=0 SKIP=0
```

---

## Checklist

**New CDC table**

- [ ] table created; `ALL COLUMN LOGGING` (Oracle) / capture instance (SQL Server)
- [ ] topic added to `create-topics.sh` — **UPPERCASE** for Oracle — and created
- [ ] `table.include.list` **and** `message.key.columns` both updated; connectors re-registered
- [ ] `export-schemas` re-run so the new `globalId` is in the export
- [ ] ingested; `pks_spanning_partitions = 0`

**New dbt model**

- [ ] model uses `reporting_merge_config`, `incremental_filter`, `processing_status_for_mode`, `merge_guard`
- [ ] job YAML added with its `dependencies:` block
- [ ] `compile_plan` locally — `config_version` changed and the wave is what you expect
- [ ] `dbt parse` — the `ref()` edge is in `manifest.json`
- [ ] `dbt-project.zip` **and** `plan.json` deployed to S3 (backed up first)
- [ ] node's `_reporting/plan.json` shows the new `config_version`
- [ ] DAG run: the expected turn gained a mapped task
- [ ] table exists in Glue; dbt log shows `ERROR=0`
