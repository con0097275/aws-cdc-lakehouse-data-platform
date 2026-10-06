# Runbook — DynamoDB runtime-state investigation

Four tables. Execution history and watermarks are the two you will actually read.

| Table | Holds |
|---|---|
| `kafka-dev-lab-dev-job-execution` | one row per execution attempt |
| `kafka-dev-lab-dev-job-watermark-state` | one row per job+mode |
| `kafka-dev-lab-dev-streaming-app-state` | streaming deployments |
| `kafka-dev-lab-dev-summary-config` | compiled plan summaries |

## 1. Status distribution

```bash
python3 - <<'PY'
import boto3
from collections import Counter
t=boto3.resource("dynamodb", region_name="ap-southeast-1").Table("kafka-dev-lab-dev-job-execution")
i=t.scan().get("Items",[])
print("total", len(i), dict(Counter(str(x.get("status")) for x in i)))
for x in i:
    if str(x.get("status")) in ("RUNNING","SUBMITTED","PENDING","VALIDATING"):
        print("  NON-TERMINAL:", x["execution_id"])
PY
```

## 2. Non-terminal records are the thing to look for

`PLANNED`, `SUBMITTED`, `RUNNING`, `VALIDATING` are all legitimate **while a run is alive**.
A record stuck in one after its worker died blocks the next run of that job+mode — that guard
is deliberate ([stream-batch-recovery.md](stream-batch-recovery.md)).

**Is the worker alive?** Check the EMR job named by `spark_app_id`. If the job is terminal
and the record is not, the driver died between the two.

## 3. Finalising a stale record

```python
t.update_item(Key={"execution_id": eid},
  UpdateExpression="SET #s = :f, finalised_note = :n, finished_at = :ts",
  ExpressionAttributeNames={"#s": "status"},
  ExpressionAttributeValues={":f": "FAILED", ":n": "<why>", ":ts": <iso>})
```

**FAILED, never SUCCEEDED** — even when the EMR job demonstrably succeeded. The
orchestration run did not complete, and SUCCEEDED would advance a watermark behind work
nothing verified. Phase 14 produced four such records; all were finalised FAILED and the
dates rerun.

Always write a `finalised_note`. A status with no explanation is indistinguishable from a
genuine failure six months later.

## 4. Status transitions are enforced

`RUNNING -> SUCCEEDED` is **refused** — a run must pass through `VALIDATING`. If you find a
record that skipped it, something wrote to DynamoDB outside the framework.

## 5. Do not delete rows

Execution history is the audit trail. A missing row is not a clean slate; it is a run nobody
can account for. Finalise, do not delete.
