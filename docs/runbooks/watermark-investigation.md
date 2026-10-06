# Runbook — watermark investigation

**When:** a watermark looks stuck, wrong, or ahead of the data.

## The invariant

**A watermark advances only behind a run that completed AND validated.**
`ops_client.finish_successfully` is the only path to `advance_watermark`; it writes the
watermark (step 4) **before** SUCCEEDED (step 5), so a reader that sees SUCCEEDED can trust
the position behind it. `fail()` has no code path to it at all.

So a watermark that is *behind* is normal after a failure. A watermark that is *ahead* of
the data is a genuine defect — start with the execution that set it.

## 1. Read them

```bash
python3 - <<'PY'
import boto3
t=boto3.resource("dynamodb", region_name="ap-southeast-1").Table("kafka-dev-lab-dev-job-watermark-state")
for i in sorted(t.scan().get("Items",[]), key=lambda x: str(x.get("watermark_key",""))):
    print(i.get("watermark_key"), "->", i.get("last_success_date_of_data"),
          "| ts:", i.get("watermark_ts"), "|", i.get("last_success_execution_id"))
PY
```

Every watermark names the execution that set it. **Start there**, not with the watermark.

## 2. Read the two fields correctly

| Field | Meaning |
|---|---|
| `last_success_date_of_data` | the business date certified |
| `watermark_ts` | STREAM_BATCH only — the frozen upper bound of the last window |

`watermark_ts: None` on EOD/AUTO_CORRECT/FULFILL is **correct** — they are date-bounded, not
timestamp-bounded. `watermark_ts: None` on STREAM_BATCH means no batch has ever completed.

## 3. Common findings

| Symptom | Usually |
|---|---|
| behind after a failure | correct — the failed run did not advance it |
| unchanged after a "successful" run | the run failed validation; check for `VALIDATION_FAILED` |
| names an execution that is not SUCCEEDED | **real defect** — escalate, do not patch the watermark |
| STREAM_BATCH ts null but rows exist | rows came from another mode |

## 4. Do not hand-edit a watermark

Editing it makes the system claim work it never did. Rerun the date instead — the MERGE is
idempotent, so a rerun is cheap and leaves an audit trail a hand-edit does not.
