# Verify the platform end to end — CDC → data mart, and the AI layer

How to check, with real commands, that every stage actually did what it claims. Written
2026-08-25 against the live account, and every number below was **observed, not estimated**.

```bash
export AWS_PROFILE=my-aws-profile
export AWS_DEFAULT_REGION=ap-southeast-1
export LAKE=kafka-dev-lab-dev-lake-111122223333
```

---

## 0. Read this before you verify the AI parts

**There is no ML model, no feature store, no vector index and no agent in this project yet.**

| Thing | State |
|---|---|
| `ai/` tier-1 assistant — router, BM25 retrieval, guards, eval | **EXISTS, runs, $0** |
| Tier-2 Bedrock generation | written, flag off, **never invoked** |
| Feature store / ML pilot / LangGraph agent / vector index | **NOT BUILT** — phases `AI-P1`…`AI-P16` |

So §6 verifies the assistant that exists. Anything else in the AI plane has nothing to
verify yet, and a guide that told you to check it would be inventing results.

---

## 1. Sixty-second health check

```bash
aws sts get-caller-identity --output table
aws kafka list-clusters-v2 --query 'ClusterInfoList[].{Name:ClusterName,State:State}' --output table
aws emr-serverless list-applications --query 'applications[].{Name:name,State:state,Id:id}' --output table
aws ec2 describe-instances --filters Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].{Name:Tags[?Key==`Name`]|[0].Value,Type:InstanceType,Id:InstanceId}' --output table
aws dynamodb list-tables --query TableNames --output table
aws glue get-databases --query 'DatabaseList[?starts_with(Name,`kafka_dev_lab`)].Name' --output table
```

Last observed: MSK `ACTIVE` · EMR Serverless `STARTED` · 4 EC2 running (toolbox `t3.small`,
cdc-runtime `t3.large`, source-lab `t3a.xlarge`, airflow `t3.large`) · 4 DynamoDB tables ·
7 Glue databases.

---

## 2. The data chain — CDC → data mart

This is the part that proves the pipeline. Run the counts and compare the shape, not just
the presence of a table.

```bash
aws glue get-tables --database-name kafka_dev_lab_dev_full_cdc --query 'TableList[].Name' --output text
aws glue get-tables --database-name kafka_dev_lab_dev_stream    --query 'TableList[].Name' --output text
aws glue get-tables --database-name kafka_dev_lab_dev_curated   --query 'TableList[].Name' --output text
aws glue get-tables --database-name kafka_dev_lab_dev_mart      --query 'TableList[].Name' --output text
```

### Counting through the layers with Athena

Save this helper; every query below uses it and it bills against the workgroup's enforced
10 GiB cutoff.

```bash
athenaq() {
  QID=$(aws athena start-query-execution --work-group kafka-dev-lab-dev-wg \
        --query-string "$1" --query QueryExecutionId --output text)
  while :; do
    ST=$(aws athena get-query-execution --query-execution-id "$QID" \
         --query 'QueryExecution.Status.State' --output text)
    case "$ST" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$QID" \
          --query 'QueryExecution.Status.StateChangeReason' --output text; return 1 ;;
    esac
    sleep 2
  done
  aws athena get-query-results --query-execution-id "$QID" \
    --query 'ResultSet.Rows[].Data[].VarCharValue' --output text
  echo "bytes scanned: $(aws athena get-query-execution --query-execution-id "$QID" \
        --query 'QueryExecution.Statistics.DataScannedInBytes' --output text)"
}
```

```bash
athenaq "SELECT count(*) FROM kafka_dev_lab_dev_full_cdc.cdc_events"
athenaq "SELECT count(*) FROM kafka_dev_lab_dev_stream.cdc_events_realtime"
athenaq "SELECT count(*) FROM kafka_dev_lab_dev_curated.fact_account_daily_snapshot"
athenaq "SELECT business_date, count(*) FROM kafka_dev_lab_dev_mart.mart_account_balance_daily
         GROUP BY business_date ORDER BY business_date"
```

**Observed 2026-08-25 — this is what a healthy chain looks like:**

| Layer | Table | Rows |
|---|---|---|
| FULL_CDC (canonical) | `cdc_events` | **20,390** |
| REALTIME | `cdc_events_realtime` | **20,390** |
| CURATED | `fact_account_daily_snapshot` | **1,280** |
| MART | `mart_account_balance_daily` | **320** for `business_date = 2026-08-22` |

The narrowing is the point: 20,390 raw CDC events collapse to 1,280 daily account states,
which roll up to 320 mart rows for one business date. A mart row count **equal to** the CDC
count means dedup or the cutoff is not working.

### Correctness spot-checks

```bash
# No duplicate business keys in the mart — must return 0 rows
athenaq "SELECT account_sk, business_date, count(*) c
         FROM kafka_dev_lab_dev_mart.mart_account_balance_daily
         GROUP BY account_sk, business_date HAVING count(*) > 1"

# CDC operations present — expect a mix of c/u/d (or r for snapshot reads)
athenaq "SELECT op, count(*) FROM kafka_dev_lab_dev_full_cdc.cdc_events GROUP BY op"

# Iceberg snapshot history — proves commits happened and when
athenaq "SELECT committed_at, snapshot_id, operation
         FROM kafka_dev_lab_dev_mart.\"mart_account_balance_daily\$snapshots\"
         ORDER BY committed_at DESC LIMIT 5"
```

---

## 3. THE CHECK THAT CATCHES A FALSE SUCCESS

**Run this one first when something looks fine but isn't.**

A job can be recorded `SUCCEEDED` and advance its watermark **without producing a table**.
dbt exits 0 on a model that selects zero rows, the coordinator sees a clean exit, and the
watermark moves. Nothing in the framework asserts the target table exists afterwards.

```bash
echo "--- watermarks say these marts are loaded:"
aws dynamodb scan --table-name kafka-dev-lab-dev-job-watermark-state \
  --query 'Items[].[watermark_key.S,last_success_date_of_data.S]' --output text

echo "--- Glue says these mart tables exist:"
aws glue get-tables --database-name kafka_dev_lab_dev_mart --query 'TableList[].Name' --output text

echo "--- S3 says these marts have data:"
aws s3 ls s3://$LAKE/warehouse/mart/
```

**The three lists must agree.** Observed 2026-08-25 — they do not:

| Job | Execution | Watermark | Glue table | S3 data |
|---|---|---|---|---|
| `mart_account_balance_daily` | SUCCEEDED | 2026-08-22 | **yes** | **yes** |
| `mart_channel_engagement_daily` | SUCCEEDED | 2026-08-22 | **NO** | **NO** |
| `mart_account_balance_monthly` | SUCCEEDED | 2026-08-22 | **NO** | **NO** |

Two of three marts advanced a watermark while producing no table and no data. Treat a
watermark as a claim, not as evidence. **This is an open defect.** The fix is
a post-write assertion that the target table
exists and has rows before the coordinator finalises SUCCEEDED. Not yet filed as a fix.

---

## 4. Airflow

```bash
AF=$(aws ec2 describe-instances \
  --filters "Name=tag:Component,Values=airflow" Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].InstanceId' --output text)
echo "$AF"

aws ssm describe-instance-information \
  --filters Key=InstanceIds,Values=$AF --query 'InstanceInformationList[].PingStatus' --output text
```

Open the UI through SSM — there is no public endpoint and there must not be one:

```bash
aws ssm start-session --target "$AF" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Then **http://localhost:8080**. Full detail and failure modes:
`docs/runbooks/airflow-access.md`.

### What to look at in the UI

| | Expect |
|---|---|
| DAGs | 4 flow DAGs — `datamart_eod`, `datamart_auto_correct`, `datamart_fulfill`, `datamart_stream_batch` |
| `datamart_eod` graph | `resolve_plan` → `turn_1.select_wave` → `turn_1.gate_and_run` (mapped) → `turn_2.…` |
| Task mapping | `turn_1.gate_and_run` expands to **2** mapped instances, `turn_2` to **1** |
| Pools | 4 pools exist and are not all-slots-used |
| Import errors | **zero** |

The wave layout is the framework working: adding a mart changes the waves and writes no DAG.

### From the node instead of the UI

```bash
aws ssm start-session --target "$AF"
# then, on the node:
sudo k3s kubectl get pods -A
sudo k3s kubectl -n airflow logs deploy/airflow-scheduler --tail=50
```

---

## 5. S3 and runtime state

```bash
aws s3 ls s3://$LAKE/
aws s3 ls s3://$LAKE/warehouse/
aws s3 ls s3://$LAKE/ --recursive --summarize | tail -3     # observed: 1,851 objects, ~175 MB

# checkpoints must be a SIBLING of warehouse/, never inside it
aws s3 ls s3://$LAKE/checkpoints/

# EMR + Airflow logs
aws s3 ls s3://$LAKE/logs/emr-serverless/ | tail -5
aws s3 ls s3://$LAKE/logs/airflow/       | tail -5
```

Runtime state (ADR-036):

```bash
aws dynamodb scan --table-name kafka-dev-lab-dev-job-execution \
  --query 'Items[].[execution_id.S,status.S]' --output text | sort
```

Statuses you should be able to explain: `SUCCEEDED`, `FAILED`, `WAITING_DEPENDENCY`,
`PLANNED`, `SKIPPED`, `BLOCKED`. A `WAITING_DEPENDENCY` on today's scheduled run is normal
when the upstream EOD gate has not been satisfied for today's date.

EMR job runs:

```bash
APP=$(aws emr-serverless list-applications --query 'applications[0].id' --output text)
aws emr-serverless list-job-runs --application-id "$APP" \
  --query 'jobRuns[].{Id:id,State:state,Created:createdAt}' --output table | head -20
```

---

## 6. The AI layer that exists today

All local, all free, no AWS call:

```bash
cd /path/to/aws-cdc-lakehouse

python3 ai/eval/evaluate.py
# expect: {'total': 14, 'passed': 14, 'routing_correct': 14, 'total_cost_usd': 0.0}

python3 ai/assistant.py "explain mart.fact_transaction"
python3 ai/assistant.py "what is the lineage of mart.dim_customer"
python3 ai/assistant.py "the KafkaConsumerLagGrowing alert is firing, what is the runbook"
```

Verify the safety boundary holds — these must all be **refused**, in code, not by wording:

```bash
python3 - <<'PY'
import sys; sys.path.insert(0, "ai")
from guards import assert_read_only_sql, GuardViolation
for sql in ["DROP TABLE mart.x",
            "SELECT 1; DROP TABLE mart.x",
            "/* SELECT */ DELETE FROM mart.x",
            "MSCK REPAIR TABLE mart.x"]:
    try:
        assert_read_only_sql(sql); print("NOT BLOCKED (bad):", sql)
    except GuardViolation as e:
        print("blocked  :", sql, "->", str(e)[:60])
PY
```

Repo-wide checks:

```bash
make check           # exit 0
make validate-docs   # 14 passed, 0 failed
make test            # full Python suite (needs Spark; slow)
```

---

## 7. Cost check

```bash
scripts/show-cost-resources.sh

aws ce get-cost-and-usage \
  --time-period Start=$(date -u -d '7 days ago' +%F),End=$(date -u +%F) \
  --granularity DAILY --metrics UnblendedCost \
  --query 'ResultsByTime[].{Day:TimePeriod.Start,USD:Total.UnblendedCost.Amount}' --output table
```

Budget of record is **$30/month** (ADR-030). With MSK + 4 EC2 + EMR running, the burn is
**~$1.12–1.53/hr — roughly the whole monthly budget per day.** If you are not actively using
it, stop it. See §8.

---

## 8. Stop for the night

Full procedure and the STOP-vs-DESTROY decision: `docs/runbooks/stop-and-resume.md`.

```bash
scripts/stop-ephemeral.sh              # dry run, always first
scripts/stop-ephemeral.sh --execute    # stops the 4 EC2 instances
```

**MSK cannot be stopped — only destroyed.** Stopping just the EC2 instances saves about
$0.42/hr and leaves ~$19.50/day of Kafka running. If you are not coming back tomorrow,
destroy instead:

```bash
scripts/tf.sh destroy --execute
scripts/verify-destroy.sh
```

> Never schedule deletion of the lake CMK. It makes every byte in the lake unreadable, and
> this project has already lived through that once.

---

## Verification summary — what to expect

| Check | Healthy result |
|---|---|
| MSK / EMR / EC2 | ACTIVE / STARTED / 4 running |
| `full_cdc.cdc_events` | 20,390 |
| `curated.fact_account_daily_snapshot` | 1,280 |
| `mart_account_balance_daily` | 320 rows, `business_date = 2026-08-22` |
| Duplicate business keys | **0 rows** |
| Watermarks vs Glue vs S3 | **must agree — currently 2 of 3 do not (§3)** |
| Airflow | 4 flow DAGs, 0 import errors, waves 2→1 |
| `ai/eval/evaluate.py` | 14/14, routing 14/14, $0.00 |
| Guard refusals | all 4 blocked |
| `make check` | exit 0 |
