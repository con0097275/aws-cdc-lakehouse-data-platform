# Verify every feature yourself — AWS, Airflow, datamart, streaming, AI

Every command here is **copy-pasteable** and **read-only** unless the heading says otherwise.
Each section states what to run, what you should see, and what it would mean if you saw
something else.

Written from the live window of **2026-09-06**. Job-run ids are from that window; if you
rebuild, the ids change but every path, table and command stays the same.

```
account   111122223333          profile   my-aws-profile          region  ap-southeast-1
lake      kafka-dev-lab-dev-lake-111122223333
EMR app   00g8i82doe9hl625      Athena WG kafka-dev-lab-dev-wg
lake CMK  d8ef38fe-c3d6-44b8-b3ae-4c1e910a9afd
```

> **Set your profile first, every time.** Without it boto3/CLI resolve through the default
> chain to a DIFFERENT account and you get `WorkGroup is not found` / `NoSuchBucket`, which
> reads as a missing resource rather than a wrong account (OPEN-13).
>
> ```bash
> export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1
> aws sts get-caller-identity      # must print 111122223333
> ```

---

## 0. Is the platform ready at all?

```bash
bash scripts/cdc-window-start.sh
```

Exit **0** and `READY - every component healthy`. It checks six things; the sixth is the one
that used to cost a 4-minute EMR failure:

```
1/6 MSK cluster              ACTIVE
2/6 CDC_WINDOW_START_TIME    re-stamped from MSK CreationTime if a stale one survived a destroy
3/6 EC2 hosts + SSM          toolbox / source-lab / cdc-runtime Online
4/6 EMR Serverless           application CREATED
5/6 Foundation intact        8 of 8 Glue databases present, by NAME
6/6 Lake CMK convergence     0 of N sampled artifacts/code/ objects on a STALE key
```

**If 6/6 reports stale objects, stop.** Every Spark job will fail with `kms:Decrypt` denied,
reported as an *S3* error, which sends you debugging the wrong service. Fix first:

```bash
bash scripts/reencrypt-lake-cmk.sh audit          # read-only
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute    # phrase: REENCRYPT LAKE
```

---

## 1. Source databases — CDC is genuinely enabled

```bash
bash scripts/source-lab.sh status
```

Expect **`ALL CHECKS PASSED - safe to deploy connectors`**:

```
[PASS] source-oracle healthy          [PASS] source-sqlserver healthy
[PASS] log_mode=ARCHIVELOG            [PASS] SQL Server Agent Running
[PASS] supplemental logging MIN=YES   [PASS] database CDC enabled
                                      [PASS] 4 tables tracked by CDC
```

Risk R15: a connector pointed at a CDC-disabled table starts **healthy** and produces
nothing. "Container is running" is not a sufficient check.

### 1a. Per-table supplemental logging (the one that silently corrupts updates)

```bash
aws ssm start-session --target $(cd terraform/envs/dev && terraform output -json source_lab | python3 -c 'import json,sys;print(json.load(sys.stdin)["instance_id"])')
# then, on the host:
set -a; . /opt/source-lab/.env; set +a
docker exec -i source-oracle sqlplus -S -L "sys/$ORACLE_PASSWORD@//localhost:1521/FREEPDB1 as sysdba" <<'SQL'
SET HEAD OFF PAGES 0 FEED OFF
SELECT table_name||' '||log_group_type FROM dba_log_groups WHERE owner='COREBANK';
EXIT
SQL
```

Expect **`ALL COLUMN LOGGING`** on ACCOUNT, BRANCH, CUSTOMER, TRANSACTION.

Database-level `MIN=YES` alone is **not** enough: without per-table ALL COLUMN LOGGING an
UPDATE arrives with unchanged columns NULL, every before/after comparison downstream is
wrong, and nothing errors.

### 1b. Credentials resolve from SSM, nothing is in Git

```bash
aws ssm describe-parameters --parameter-filters "Key=Name,Option=BeginsWith,Values=/kafka-dev-lab" \
  --query 'Parameters[].[Name,Type]' --output table
```

All **SecureString**. On the host, `/opt/source-lab/.env` is mode `0600` root-only with zero
unresolved `${ssm:...}` placeholders.

---

## 2. Kafka — CDC events are actually flowing

```bash
bash scripts/register-connectors.sh status     # both connectors + tasks RUNNING
bash scripts/cdc-runtime.sh topics             # 8 cdc.* topics + DLQ + schema-history
```

Count messages per topic (note the class name — it moved in Kafka 3.x, and the old one
fails in a way that `awk` sums to a convincing **0**):

```bash
aws ssm start-session --target <cdc-runtime instance id>
set -a; . /opt/cdc-runtime/.env; set +a
for t in cdc.oracle.COREBANK.ACCOUNT cdc.oracle.COREBANK.CUSTOMER \
         cdc.sqlserver.digital.dbo.app_user cdc.sqlserver.digital.dbo.merchant; do
  docker exec -e KAFKA_OPTS= -e KAFKA_JMX_OPTS= cdc-connect \
    kafka-run-class org.apache.kafka.tools.GetOffsetShell \
    --bootstrap-server "$BOOTSTRAP" --command-config /opt/cdc-runtime/client.properties \
    --topic "$t" | awk -F: -v t="$t" '{s+=$3} END{printf "%-42s %s\n", t, s}'
done
```

Seeded baseline was **5,728** across the eight topics, matching the source table-for-table.

---

## 3. FULL_CDC — the canonical layer, on S3

### 3a. In the catalog

```bash
aws glue get-table --database-name kafka_dev_lab_dev_full_cdc --name cdc_events \
  --query 'Table.StorageDescriptor.Location' --output text
```

→ `s3://kafka-dev-lab-dev-lake-111122223333/warehouse/full_cdc/cdc_events`

### 3b. On S3 — console links

| what | S3 console |
|---|---|
| FULL_CDC data | https://s3.console.aws.amazon.com/s3/buckets/kafka-dev-lab-dev-lake-111122223333?region=ap-southeast-1&prefix=warehouse/full_cdc/cdc_events/data/ |
| FULL_CDC metadata | …&prefix=warehouse/full_cdc/cdc_events/metadata/ |
| SNAPSHOT (curated) | …&prefix=warehouse/curated/fact_account_daily_snapshot/data/ |
| MART | …&prefix=warehouse/mart/ |
| RT stream layer | …&prefix=warehouse/stream/ |
| streaming checkpoints | …&prefix=checkpoints/ |
| quarantine payloads | …&prefix=quarantine-payloads/ |
| staged job code | …&prefix=artifacts/code/ |

```bash
aws s3 ls s3://kafka-dev-lab-dev-lake-111122223333/warehouse/full_cdc/cdc_events/data/
#   PRE source_system=oracle/
#   PRE source_system=sqlserver/          <- partitioned by source_system, then event_date
```

### 3c. Correctness in Athena

```sql
-- one row per delivered record, per table
SELECT source_system, source_table, COUNT(*) rows,
       SUM(CASE WHEN op='d' THEN 1 ELSE 0 END) deletes
FROM kafka_dev_lab_dev_full_cdc.cdc_events
GROUP BY 1,2 ORDER BY 1,2;

-- CLAUDE.md 5.1: the SAME PK must always land in the SAME partition
WITH k AS (SELECT json_extract_scalar(payload_after,'$.ACCOUNT_ID') pk, kafka_partition
           FROM kafka_dev_lab_dev_full_cdc.cdc_events WHERE source_table='ACCOUNT')
SELECT COUNT(*) distinct_pks,
       SUM(CASE WHEN nparts>1 THEN 1 ELSE 0 END) pks_spanning_partitions
FROM (SELECT pk, COUNT(DISTINCT kafka_partition) nparts FROM k WHERE pk IS NOT NULL GROUP BY pk);
```

`pks_spanning_partitions` **must be 0**. If it is not, per-key ordering is broken and every
"last event wins" result below it is unreliable.

---

## 4. Streaming FULL_CDC app — Kafka → S3 canonical, continuously

**This is the app that streams CDC into the FULL_CDC S3 layer.** It is
`spark/jobs/full_cdc/stream_job.py`; it supplies the streaming *lifecycle* and imports the
decode/quarantine/MERGE from the batch job so the two cannot diverge.

### Run it (costs money — it holds EMR capacity until the budget expires)

```bash
LAKE=kafka-dev-lab-dev-lake-111122223333
BOOT=$(cd terraform/envs/dev && terraform output -json kafka_platform \
       | python3 -c 'import json,sys;print(json.load(sys.stdin)["msk_bootstrap_brokers_sasl_iam"])')
JARS="/usr/lib/spark/connector/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/connector/lib/spark-token-provider-kafka-0-10.jar,/usr/lib/spark/connector/lib/spark-avro.jar,/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,s3://$LAKE/artifacts/code/aws-msk-iam-auth.jar,s3://$LAKE/artifacts/code/kafka-clients-3.4.1.jar,s3://$LAKE/artifacts/code/commons-pool2-2.11.1.jar"

aws emr-serverless start-job-run \
  --application-id 00g8i82doe9hl625 \
  --execution-role-arn arn:aws:iam::111122223333:role/kafka-dev-lab-dev-spark-stream \
  --name full-cdc-STREAM --execution-timeout-minutes 20 \
  --job-driver "{\"sparkSubmit\":{
     \"entryPoint\":\"s3://$LAKE/artifacts/code/full_cdc_stream_job.py\",
     \"entryPointArguments\":[\"--bootstrap\",\"$BOOT\",
       \"--topics\",\"cdc.oracle.COREBANK.ACCOUNT,cdc.oracle.COREBANK.CUSTOMER\",
       \"--schemas-uri\",\"s3://$LAKE/artifacts/code/schemas.json\",
       \"--warehouse\",\"s3://$LAKE/warehouse\",
       \"--table\",\"glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events\",
       \"--checkpoint\",\"s3://$LAKE/checkpoints/full_cdc/stream_$(date -u +%Y%m%d%H%M)\",
       \"--trigger-seconds\",\"20\",\"--run-seconds\",\"240\",\"--starting-offsets\",\"latest\"],
     \"sparkSubmitParameters\":\"--py-files s3://$LAKE/artifacts/code/full_cdc_job.py --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.session.timeZone=UTC --conf spark.jars=$JARS\"}}" \
  --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://$LAKE/logs/emr/\"}}}"
```

**The role must be `spark-stream`.** `spark-eod` has no MSK IAM permission and fails with
`SaslAuthenticationException: Access denied`.

### Read its log

```bash
APP=00g8i82doe9hl625; RUN=<jobRunId>
aws s3 cp "s3://$LAKE/logs/emr/applications/$APP/jobs/$RUN/SPARK_DRIVER/stdout.gz" - | gunzip
```

Expect (proven run `00g8icrr2u674o27`):

```
FULL_CDC_STREAM_STARTED  topics=4 trigger=20s budget=240s offsets=earliest
FULL_CDC_STREAM_TOPIC    ACCOUNT 326 | CUSTOMER 202 | app_user 152 | merchant 52
FULL_CDC_STREAM_BATCH    id=0 rows=732 tombstones=2 poison=0
FULL_CDC_STREAM_STOPPED  reason=run_seconds_budget_reached
FULL_CDC_COUNT           5740
```

**Reading 732 rows and the table growing by only 10 is correct, not a discrepancy** — the
MERGE on `event_id` is idempotent, so a replay adds only genuinely new events.

### The checkpoint must NOT be under warehouse/

```bash
aws s3 ls s3://$LAKE/checkpoints/full_cdc/          # <- here
aws s3 ls s3://$LAKE/warehouse/ --recursive | grep -c checkpoint    # must be 0
```

`remove_orphan_files` walks table locations and would delete streaming state — silently,
surfacing much later as a full replay. The app refuses at startup rather than allow it.

### Stopping it

You do not need to. `--run-seconds` is mandatory and the query stops itself
(`run_seconds_budget_reached`). To confirm nothing is left holding capacity:

```bash
aws emr-serverless list-job-runs --application-id 00g8i82doe9hl625 \
  --states RUNNING PENDING SCHEDULED --query 'jobRuns[].[id,name,state]' --output table
```

To cancel early: `aws emr-serverless cancel-job-run --application-id 00g8i82doe9hl625 --job-run-id <id>`

---

## 5. Simulate CDC events yourself and watch them land

**Mutating — this changes source data.** Run on the source-lab host.

```bash
set -a; . /opt/source-lab/.env; set +a
docker exec -i source-oracle sqlplus -S -L "corebank/$ORACLE_PASSWORD@//localhost:1521/FREEPDB1" <<'SQL'
SET HEAD OFF PAGES 0 FEED OFF
INSERT INTO corebank.account (account_id,customer_id,product_code,currency,open_date,status,balance)
VALUES (993001,100001,'CUR','USD',SYSDATE,'ACTIVE',1000.00);
UPDATE corebank.account SET balance=2500.55, status='DORMANT' WHERE account_id=993001;
INSERT INTO corebank.account (account_id,customer_id,product_code,currency,open_date,status,balance)
VALUES (993002,100002,'SAV','USD',SYSDATE,'ACTIVE',500.00);
COMMIT;
DELETE FROM corebank.account WHERE account_id=993002;
COMMIT;
EXIT
SQL
```

> Do **not** put a trailing `-- comment` after a SQL statement in a sqlplus heredoc. It
> swallows the `;` and the statement silently does not run (`ORA-03048`).

Then re-run the streaming app (section 4) or the batch job, and check in Athena:

```sql
SELECT op, kafka_offset,
       json_extract_scalar(payload_before,'$.BALANCE') bal_before,
       json_extract_scalar(payload_after ,'$.BALANCE') bal_after,
       json_extract_scalar(payload_after ,'$.STATUS')  status
FROM kafka_dev_lab_dev_full_cdc.cdc_events
WHERE source_table='ACCOUNT'
  AND COALESCE(json_extract_scalar(payload_after ,'$.ACCOUNT_ID'),
               json_extract_scalar(payload_before,'$.ACCOUNT_ID'))='993001'
ORDER BY CAST(position_primary AS DECIMAL(38,0));
```

Expect `c` then `u` with **before=1000.00, after=2500.55**. A NULL `bal_before` means
per-table supplemental logging is missing (section 1a).

Proven matrix from 2026-09-06:

| scenario | FULL_CDC | snapshot |
|---|---|---|
| INSERT → UPDATE (991001) | `c` off 108, `u` off 109, before 1000.00 → after 2500.55 | present, 2500.55 |
| INSERT → DELETE → RECREATE (991002) | `c` → `d` → `c` REOPENED 777.77 | present, 777.77 |
| INSERT → DELETE, never recreated (991003) | both events retained | **ABSENT** |
| SQL Server app_user / merchant | `c`+`u`, `c`+`d`+tombstone | n/a |

---

## 6. Snapshot layer (EOD) — one state per key, deletes excluded

```bash
LAKE=kafka-dev-lab-dev-lake-111122223333
aws emr-serverless start-job-run --application-id 00g8i82doe9hl625 \
  --execution-role-arn arn:aws:iam::111122223333:role/kafka-dev-lab-dev-spark-eod \
  --name eod --execution-timeout-minutes 30 \
  --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$LAKE/artifacts/code/eod_job.py\",
    \"entryPointArguments\":[\"--warehouse\",\"s3://$LAKE/warehouse\",
      \"--full-cdc\",\"glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events\",
      \"--curated\",\"glue_catalog.kafka_dev_lab_dev_curated.fact_account_daily_snapshot\",
      \"--ops-table\",\"glue_catalog.kafka_dev_lab_dev_ops.eod_watermark\",
      \"--business-date\",\"$(date -u +%F)\"],
    \"sparkSubmitParameters\":\"--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.session.timeZone=UTC\"}}" \
  --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://$LAKE/logs/emr/\"}}}"
```

Expect in the log:

```
EOD_SCOPED_TO 2026-09-06 full_cdc_rows=328
EOD_LAYER_CLOSED layer=EOD table=fact_account_daily_snapshot date=2026-09-06
EOD_TAG EOD_2026-09-06 -> snapshot 5804245257980612404
EOD_ROWS 323
```

**Both `EOD_LAYER_CLOSED` and `EOD_TAG` matter.** Producing the data is not the same as
declaring the day closed, and the downstream gate requires both — a half-finished table is
indistinguishable from a finished one by row count alone.

### Reconcile it independently (do not trust the job's own number)

```sql
WITH ranked AS (
  SELECT COALESCE(json_extract_scalar(payload_after ,'$.ACCOUNT_ID'),
                  json_extract_scalar(payload_before,'$.ACCOUNT_ID')) pk, op,
         ROW_NUMBER() OVER (PARTITION BY COALESCE(
             json_extract_scalar(payload_after ,'$.ACCOUNT_ID'),
             json_extract_scalar(payload_before,'$.ACCOUNT_ID'))
           ORDER BY CAST(position_primary AS DECIMAL(38,0)) DESC, kafka_offset DESC) rn
  FROM kafka_dev_lab_dev_full_cdc.cdc_events WHERE source_table='ACCOUNT')
SELECT (SELECT COUNT(*) FROM ranked WHERE rn=1 AND op<>'d') AS expected_from_full_cdc,
       (SELECT COUNT(*) FROM kafka_dev_lab_dev_curated.fact_account_daily_snapshot
        WHERE business_date = DATE '2026-09-06')          AS eod_rows;
```

Proven: **323 = 323**, and the permanently deleted key 991003 is absent while the recreated
991002 is present.

---

## 7. Schema evolution — two writer versions in one topic

**Mutating.** Add a column, produce rows, refresh the export, re-ingest.

```bash
# 1. on the source host
docker exec -i source-oracle sqlplus -S -L "corebank/$ORACLE_PASSWORD@//localhost:1521/FREEPDB1" <<'SQL'
ALTER TABLE corebank.account ADD (risk_tier VARCHAR2(20));
INSERT INTO corebank.account (account_id,customer_id,product_code,currency,open_date,status,balance,risk_tier)
VALUES (994001,100001,'SAV','USD',SYSDATE,'ACTIVE',4242.42,'HIGH');
COMMIT;
EXIT
SQL

# 2. refresh the writer-schema export (globalId-keyed, EVERY version)
bash scripts/cdc-runtime.sh export-schemas --execute kafka-dev-lab-dev-lake-111122223333

# 3. re-run FULL_CDC (batch or streaming)
```

Expect **two** decode lines for the one topic:

```
FULL_CDC_DECODE cdc.oracle.COREBANK.ACCOUNT globalId=29   <- new schema, has risk_tier
FULL_CDC_DECODE cdc.oracle.COREBANK.ACCOUNT globalId=6    <- old schema, does not
FULL_CDC_TOPIC  cdc.oracle.COREBANK.ACCOUNT rows=330
```

```sql
SELECT json_extract_scalar(payload_after,'$.ACCOUNT_ID') acct, op,
       json_extract_scalar(payload_after,'$.RISK_TIER')  risk_tier
FROM kafka_dev_lab_dev_full_cdc.cdc_events
WHERE source_table='ACCOUNT' AND json_extract_scalar(payload_after,'$.RISK_TIER') IS NOT NULL;
```

**If you see `LEGACY EXPORT, globalId=N unverifiable`,** the export is still the old
topic-keyed one — run step 2. The job falls back rather than quarantining every record, but
the schema-evolution test is not meaningful until the export can express versions.

**If you see `SchemaNotExported`,** a version is genuinely missing from the export. Those
records are quarantined, not guessed — refresh and re-run, and they will be picked up.

---

## 8. Poison record → quarantine

**Mutating.** Produce something that is not valid Avro:

```bash
set -a; . /opt/cdc-runtime/.env; set +a
printf 'this-is-not-avro-%s\n' "$(date -u +%s)" | \
  docker exec -i -e KAFKA_OPTS= -e KAFKA_JMX_OPTS= cdc-connect \
  kafka-console-producer --bootstrap-server "$BOOTSTRAP" \
  --producer.config /opt/cdc-runtime/client.properties --topic cdc.oracle.COREBANK.BRANCH
```

Re-run FULL_CDC on that topic. The run must **COMPLETE**, not die:

```
FULL_CDC_QUARANTINED cdc.oracle.COREBANK.BRANCH rows=1
FULL_CDC_TOPIC       cdc.oracle.COREBANK.BRANCH rows=4
```

```sql
SELECT error_class, source_topic, source_partition, source_offset, payload_s3_uri
FROM kafka_dev_lab_dev_quarantine.full_cdc_rejects;
```

Then confirm the referenced object **exists** and round-trips — this is the defect that was
fixed, a row pointing at an object nobody ever wrote:

```bash
aws s3 cp <payload_s3_uri> - | python3 -c "
import json,sys,base64; d=json.load(sys.stdin)
print(d['error_class'], d['offset'], 'truncated=',d['truncated'])
print(base64.b64decode(d['value_base64'])[:60])"
```

> A poison record is detected by `v IS NULL` **OR** `v.op IS NULL`. PERMISSIVE `from_avro`
> does not return a null struct for corrupt bytes — Avro has no magic bytes or checksum, so
> arbitrary input decodes into a struct with all-null *fields*. Testing only `v IS NULL`
> lets a corrupt row into the canonical layer silently, which is worse than crashing.

---

## 9. Reporting flows and the datamart

All five modes, through the framework's own code:

```bash
source <your reporting env>     # see docs/OPERATIONS_RUNBOOK.md
python3 scripts/reporting-live-run.py --flow-mode EOD          --business-date $(date -u +%F) --execute
python3 scripts/reporting-live-run.py --flow-mode AUTO_CORRECT --business-date $(date -u +%F) --execute
python3 scripts/reporting-live-run.py --flow-mode FULFILL      --business-date $(date -u +%F) \
        --from-date 2026-09-05 --to-date 2026-09-05 --execute
python3 scripts/reporting-live-run.py --flow-mode STREAM_BATCH --business-date $(date -u +%F) --execute
python3 scripts/reporting-live-run.py --flow-mode STREAMING_RT --business-date $(date -u +%F) --execute
```

Required environment, discovered the hard way:

```
REPORTING_JOB_ROLE_ARN     .../kafka-dev-lab-dev-reporting     # the INTENDED role; R4 is fixed
REPORTING_EMR_LOG_URI      s3://<lake>/logs/emr/               # required, CloudWatch is unreachable
REPORTING_FRAMEWORK_ZIP    s3://<lake>/artifacts/dbt/framework-flat.zip   # flat, NOT reporting-framework.zip
REPORTING_EOD_SOURCES      '["fact_account_daily_snapshot"]'   # BARE table name, not db-qualified
REPORTING_DBT_BOOTSTRAP_ARGS   a JSON ARRAY, not a shell string
```

Expect `"status": "SUCCEEDED"` and a `watermark_after`. AUTO_CORRECT's detail is the
certification ladder refusing to overwrite itself, and that is correct:

```
3 date(s), 0 key(s), FULL_WINDOW_FALLBACK;
1 date(s) ESCALATED (already certified; AUTO_CORRECT cannot overwrite CERTIFIED)
```

### dbt actually ran

```bash
aws s3 cp "s3://$LAKE/logs/emr/applications/00g8i82doe9hl625/jobs/00g8ib32jjq24g27/SPARK_DRIVER/stdout.gz" - | gunzip | grep -E "dbt=|PASS|Completed"
```

→ `Done. PASS=3 WARN=0 ERROR=0 SKIP=0` — one incremental model plus two grain tests
(uniqueness on `account_sk+business_date`, not-null on `account_sk`).

### The mart

```sql
SELECT business_date, COUNT(*) rows, COUNT(DISTINCT account_sk) pks,
       ROUND(SUM(closing_balance),2) total_balance
FROM kafka_dev_lab_dev_mart.mart_account_balance_daily
GROUP BY business_date ORDER BY 1;
```

---

## 10. Airflow

```bash
bash scripts/airflow-node.sh status
bash scripts/airflow-node.sh credentials     # username admin + password from SSM
bash scripts/airflow-node.sh ui              # -> http://localhost:8080
```

`ui` is the **only** path in — no ingress rule on 8080, no load balancer. It health-checks
the node first so the tunnel does not open onto nothing, and reconnects if SSM drops.
`AIRFLOW_UI_PORT=18080 bash scripts/airflow-node.sh ui` if 8080 is busy locally.

DAGs to look at: `datamart_eod`, `datamart_auto_correct`, `datamart_fulfill`,
`datamart_stream_batch`, `business_ai_insights`.

### Triggering from the CLI

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
A="k3s kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- airflow"
$A dags trigger datamart_fulfill --run-id my_run --logical-date 2026-09-05T12:00:00+00:00
```

Two traps, both of which cost time in this window:

* **The logical date must be in the PAST.** A future one is accepted, queued, and never
  scheduled — the run sits in `running` with every task `None` forever.
* **`(dag_id, logical_date)` is unique.** Re-using one fails with a SQLAlchemy
  `INSERT ... dag_run` error whose text does not mention uniqueness.

### Deploying a DAG change — edit S3, NOT the host

**The host filesystem is a derived copy.** `airflow-dag-sync.timer` runs
`/opt/airflow-dag-sync.sh` **every 60 seconds** and does:

```bash
aws s3 sync s3://<lake>/bootstrap/airflow/dags/ /opt/airflow/dags/ --delete ...
```

So anything you write into `/opt/airflow/dags/` — or `kubectl cp` into a pod — is silently
reverted within a minute, and the task pods keep running the old code while your edit
*appears* to have landed. This cost a full debugging cycle in this window: the fix was
verified on the host by checksum, and was gone before the next DAG run started.

```bash
# the ONLY correct way to deploy a DAG or DAG-library change
aws s3 cp airflow/dags/reporting_common.py \
  s3://kafka-dev-lab-dev-lake-111122223333/bootstrap/airflow/dags/reporting_common.py

# then wait for the sync (<=60s) and confirm the host agrees with the repo
sha256sum airflow/dags/reporting_common.py | cut -c1-16
# on the node:
sha256sum /opt/airflow/dags/reporting_common.py | cut -c1-16
```

### Clearing a stuck run

A DAG run created with a **future** logical date never schedules, holds
`max_active_runs=1`, and makes the scheduler log `Logical date is in future` on repeat —
which starves every later run of that DAG. `airflow dags delete-run` did not remove it in
this build; use the ORM:

```bash
k3s kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- python -c "
from airflow.settings import Session
from airflow.models import DagRun, TaskInstance
s=Session()
for rid in ('<run_id>',):
    for dr in s.query(DagRun).filter(DagRun.run_id==rid).all():
        s.query(TaskInstance).filter(TaskInstance.dag_id==dr.dag_id,
                                     TaskInstance.run_id==dr.run_id).delete()
        s.delete(dr)
s.commit(); print('purged')"
```

### Reading state (Airflow 3 removed `airflow tasks logs`)

`airflow tasks states-for-dag-run` prints blank state columns in this build. Use the ORM:

```bash
k3s kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- python -c "
from airflow.settings import Session
from airflow.models import DagRun, TaskInstance
s=Session()
for dr in s.query(DagRun).filter(DagRun.dag_id=='datamart_fulfill').all():
    print(dr.run_id, dr.state)
    for ti in s.query(TaskInstance).filter(TaskInstance.dag_id==dr.dag_id,
                                           TaskInstance.run_id==dr.run_id).all():
        print('   ', ti.task_id, ti.map_index, ti.state)"
```

Task logs are shipped to S3:

```
s3://<lake>/logs/airflow/dag_id=<dag>/run_id=<run>/task_id=<task>/[map_index=<n>/]attempt=1.log
```

### Re-pause when you are done

The DAGs are unpaused for testing and will keep creating scheduled runs (and EMR jobs):

```bash
for d in datamart_fulfill datamart_auto_correct; do $A dags pause $d; done
```

---

## 11. STREAMING_RT — the four-app realtime layer

Modelled on the plstream v9 production system. Design and evidence:
`docs/REALTIME_STREAMING_RT.md`.

```bash
LAKE=kafka-dev-lab-dev-lake-111122223333
# entryPoint is s3://$LAKE/artifacts/realtime/<app>.py, --py-files rtlib.zip
# roles: rt_stream_app needs spark-stream (MSK IAM); the rest use spark-eod
rt_ddl.py          --warehouse s3://$LAKE/warehouse --ddl s3://$LAKE/artifacts/realtime/ddl.sql
rt_eod_base.py     --warehouse s3://$LAKE/warehouse --business-date $(date -u +%F)
rt_stream_app.py   --bootstrap <brokers> --topic cdc.oracle.COREBANK.ACCOUNT \
                   --schemas-uri s3://$LAKE/artifacts/code/schemas.json \
                   --warehouse s3://$LAKE/warehouse \
                   --checkpoint s3://$LAKE/checkpoints/realtime/rt_stream_$(date -u +%Y%m%d) \
                   --trigger-seconds 30 --resolver-seconds 5 --run-seconds 240
rt_autocorrect.py  --warehouse s3://$LAKE/warehouse
rt_datamart_app.py --warehouse s3://$LAKE/warehouse --poll-seconds 45 --run-seconds 180
```

Expected markers:

```
RT_EOD_BASE_ROWS 320   RT_EOD_BASE_INCOMPLETE 0
RT_STREAM_SUMMARY {"batches":1,"facts":320,"full":320,"resolver_cycles":44}
RT_STREAM_STOPPED reason=run_seconds_budget_reached
RT_DATAMART_CYCLE {"cycle":1,...,"seconds":19.9} ... {"cycle":12,...,"seconds":3.5}
```

The datamart getting **19.9s → 3.5s** is the source cache warming — one materialisation per
cycle instead of one scan per section.

### The late-dimension loop (the heart of the v9 concept)

Create the gap the way production does — ingest ACCOUNT into FULL_CDC while CUSTOMER lags:

```bash
# insert a new customer + account in Oracle, then ingest ONLY the ACCOUNT topic
--topics cdc.oracle.COREBANK.ACCOUNT
# run rt_stream_app -> it publishes the balance and FLAGS the missing dimension
# then ingest cdc.oracle.COREBANK.CUSTOMER, and run rt_autocorrect
```

```sql
SELECT account_id, balance, segment_code, dim_complete
FROM kafka_dev_lab_dev_stream.rt_account_stream WHERE account_id = 990900;
SELECT account_id, missing_dims, retry_count, resolved_by, resolved_ts
FROM kafka_dev_lab_dev_ops.rt_pending_dim WHERE account_id = 990900;
SELECT account_id, segment_code, dim_complete
FROM kafka_dev_lab_dev_stream.rt_account_base WHERE account_id = 990900;
```

Proven sequence: row published with `dim_complete=false` and a pointer row
(`missing_dims=customer`, `retry_count=4`) → `RT_AUTOCORRECT_WORKLIST 1 → REPAIRED 1 →
STILL_INCOMPLETE 0` → BASE `segment_code=PRIORITY`, `dim_complete=true`, pointer
`resolved_by=AUTOCORRECT`.

**The balance is published, never dropped.** A report that silently omits an account is
wrong in a way nobody can see; a published row flagged incomplete is visible.

### RT-1 — the repaired row must win in the merge view

```sql
SELECT CASE WHEN build_ts < TIMESTAMP '2026-09-06 07:00:00' THEN 'before' ELSE 'after' END phase,
       dim_value, MAX(metric_value) balance, MAX(row_count) accounts
FROM kafka_dev_lab_dev_ops.rt_datamart_metrics
WHERE section='II_by_segment' AND dim_value='PRIORITY' GROUP BY 1,2 ORDER BY 1;
```

Proven: PRIORITY went **503,970,320.00 / 80 accounts → 503,982,665.67 / 81** — a difference
of exactly 12,345.67, account 990900's balance. Under the pre-RT-1 rule the view would still
serve the stale STREAM row.

---

## 12. AI features

### 12a. The copilot UI

```bash
python3 scripts/ai-ui.py          # then open the URL it prints
python3 scripts/ai-ask.py "What is FULL_CDC?"
```

### 12b. A governed KPI, and it must match SQL exactly

```bash
python3 - <<'PY'
import os,sys; sys.path.insert(0,"ai")
os.environ.setdefault("AWS_PROFILE","my-aws-profile"); os.environ.setdefault("AWS_REGION","ap-southeast-1")
from agent_tools.athena_tool import run_query
from business_agent.graph import ask_business
r = ask_business("what is the total closing balance on 2026-09-06", as_of="2026-09-06",
                 runner=lambda sql: run_query(sql, limit=1000))
a = r["answer"]
print(a["period"], a["actual_value"], a["data_status"])
PY
```

```sql
SELECT ROUND(SUM(closing_balance),2) FROM kafka_dev_lab_dev_mart.mart_account_balance_daily
WHERE business_date = DATE '2026-09-06';
```

Proven identical: **2,031,880,160.0** from the agent (`data_status CERTIFIED`) vs
`2031880160.00` from SQL. The `period.label` must be `2026-09-06`, **not** `yesterday` — an
explicitly named date has to win over the default (defect B1).

### 12c. It must REFUSE a blocked dimension, not substitute one

```bash
# blocked: product_code / currency require curated.dim_account, which is not materialised
ask_business("break down total closing balance by product_code", ...)
```

Expect `contract_ok = False` and a reason that **names the dimension you asked for**:

```
total_closing_balance cannot be broken down by 'product_code': dim_account is not
materialised (BAI-P0 gap 1). Refusing rather than answering by a different dimension.
```

Control — an available dimension (`processing_status`, `source_flow_mode`) gives
`contract_ok = True`. If a blocked dimension silently returns a breakdown by something else,
defect B2 has regressed.

### 12d. Security — it must refuse mutation and injection

```bash
make ai-security          # 23/23
python3 ai/eval/evaluate_business.py    # 25/25, $0.0000/question
python3 ai/eval/evaluate_rag.py ; python3 ai/eval/evaluate_agent.py
```

### 12e. Feature store + ML inference

```bash
python3 scripts/ai-feature-run.py --as-of 2026-09-06 --lookback-days 30 --top-k 10 --publish
```

Expect feature rows materialised from the live mart, anomaly scores from
`model:account_anomaly_v1`, and publication to
`s3://<lake>/ai/features/account_behavior/as_of=<date>/`.

**Features emitted as NULL with a recorded reason are correct, not a failure.** With one
business date of history a 7- or 30-day window cannot be computed, and a "7-day volatility"
over 1 day is a plausible number with the wrong name. The run refuses if any date's read
comes back at the row limit, because features built on a truncated history are not
detectably wrong.

### 12f. Bedrock narration — KNOWN NOT WORKING

`INVALID_PAYMENT_INSTRUMENT` on the account. Every number in the AI answers above is
**deterministic and computed**, never generated, so this blocks narration only — not any
KPI. It is a billing state, not a software defect.

---

## 13. Cost — check before you walk away

```bash
bash scripts/tf.sh verify        # what is billable right now
aws emr-serverless list-job-runs --application-id 00g8i82doe9hl625 \
  --states RUNNING PENDING SCHEDULED --query 'jobRuns[].[id,name]' --output table
```

Baseline **~$1.12/hr**, of which MSK is ~68% and **cannot be stopped, only destroyed**.

### What the $100/month budget actually buys

```
budget            $100.00/month     (ADR-030 as amended 2026-09-06; the account agrees)
remaining         $ 86.79           => ~77 h of uptime, or ~13 six-hour windows
24/7 for a month  $810              => 8x the budget
destroyed + idle  ~$0.30/day        => KMS CMKs + S3 only
```

**The hourly rate is not the risk — leaving it up is.** One forgotten weekend costs more
than a month of deliberate windows. Check the forecast, not just the actual:

```bash
aws budgets describe-budgets --account-id 111122223333 \
  --query 'Budgets[?BudgetName==`My Monthly Cost Budget`]|[0].CalculatedSpend' --output json
```

`ForecastedSpend` extrapolates the CURRENT run rate, so it spikes while the platform is up
and falls back after a destroy. A forecast near the limit while the lab is running is
expected; a forecast near the limit while it is destroyed is a real problem.

Alerts on `kafka-dev-lab-dev-monthly`: ACTUAL > $100, ACTUAL > $80, FORECASTED > $50.
The tag-filtered budget reports **hours late and cannot stop anything** — it is a
notification, not a control. The wall-clock alarm and `auto_destroy_after` are the control.

```bash
bash scripts/airflow-node.sh stop --execute    # cheaper: compute stops, EBS still bills
bash scripts/tf.sh destroy --execute           # the only thing that reaches $0
```

**After any destroy + apply, re-encrypt the lake before submitting Spark** — the new CMK does
not decrypt objects written under the old one. `cdc-window-start.sh` step 6/6 now catches
this in seconds instead of a failed EMR run.

---

## 14. Honest status

| # | Feature | Status |
|---|---|---|
| 1 | Source CDC (Oracle LogMiner + SQL Server CDC) | **PASS** |
| 2 | Kafka topics, per-PK partition stability | **PASS** |
| 3 | FULL_CDC canonical layer on S3 | **PASS** |
| 4 | **Streaming** Kafka → FULL_CDC | **PASS** — built 2026-09-06 |
| 5 | REALTIME window layer | **PASS** |
| 6 | EOD snapshot, deletes excluded, day declared closed | **PASS** |
| 7 | Reconciliation FULL_CDC ↔ EOD | **PASS** — 323 = 323 |
| 8 | Schema evolution, two writer versions | **PASS** — globalId 6 + 29 |
| 9 | Poison record → quarantine + payload object | **PASS** |
| 10 | Five reporting flow modes | **PASS** |
| 11 | dbt build + grain tests | **PASS** — 3/0/0/0 |
| 12 | STREAMING_RT four apps + late-dimension repair + RT-1 | **PASS** |
| 13 | Business AI KPI == SQL, blocked-dimension refusal | **PASS** |
| 14 | Feature store + anomaly inference | **PASS** |
| 15 | AI security gates | **PASS** — 23/23, 25/25 |
| 16 | **Airflow DAG end-to-end** | **PASS (with a caveat)** — see below |
| 17 | Bedrock narration | **CANNOT_FIX** — billing |

**Item 16 — a full green turn-ordered DAG run through the scheduler:**

```
### datamart_fulfill  run=s42f_fulfill   RUN_STATE=success
    resolve_plan           success
    turn_1.select_wave     success
    turn_1.gate_and_run    map=0  success
    turn_1.gate_and_run    map=1  success
    turn_2.select_wave     success
    turn_2.gate_and_run    map=0  success
    finalize               success
```

An earlier attempt showed `map=0  up_for_retry` with
`StaleWatermarkError: refusing to move the watermark ... backwards`. That was **correct**:
a CLI FULFILL had already set that job's watermark to 2026-09-05 and the DAG was triggered
with logical date 2026-09-04. The guard did its job; the test's choice of date was wrong.
Re-run with an advanceable date, every task is green.

**Pick a logical date >= the job's current watermark**, or FULFILL will legitimately refuse:

```bash
aws dynamodb scan --table-name kafka-dev-lab-dev-job-watermark-state \
  --query 'Items[].[watermark_key.S,last_success_date_of_data.S]' --output text
```

Three real obstacles were found and fixed along the way, all documented above:
`KeyError: 'business_date'` (the adapter read a key `JobTask.as_dict()` does not provide);
DAG edits reverted every 60 s because `/opt/airflow/dags` is a **derived copy** of an S3
prefix; and a future-dated run that never schedules while holding `max_active_runs=1` and
starving every later run.

Remaining caveat: the 2-vCPU node runs at ~85% CPU and the scheduler restarts under
task-pod contention, so runs are slow and `kubectl exec` is occasionally OOM-killed. That is
a sizing limit of the lab, not a pipeline defect.

**Item 17 does not affect any number.** Generation is off; every KPI is computed.

### Not yet applied

`terraform/modules/lake_iam/main.tf` gained a scoped policy so the CDC runtime host can write
`artifacts/code/schemas.json` (`cdc-runtime.sh export-schemas` currently fails with
`kms:GenerateDataKey` denied and needs the export uploaded from your workstation instead).
Planned clean: **2 to add, 0 to change, 0 to destroy**. Apply with your next
`bash scripts/tf.sh apply --execute`.
