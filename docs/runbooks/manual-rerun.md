# Runbook — manual rerun of one date

**When:** a run failed, or a date must be recomputed. **Cost:** one EMR job (~$0.02).

## 1. Check nothing is in flight

```bash
python3 - <<'PY'
import boto3
t=boto3.resource("dynamodb", region_name="ap-southeast-1").Table("kafka-dev-lab-dev-job-execution")
for i in t.scan().get("Items",[]):
    if str(i.get("status")) in ("RUNNING","SUBMITTED","PENDING","VALIDATING"):
        print(i["execution_id"], i["status"])
PY
```

A non-terminal record blocks the rerun. If its worker is genuinely dead, finalise it first —
see [runtime-state-investigation.md](runtime-state-investigation.md). **Finalise it FAILED,
never SUCCEEDED.**

## 2. Rerun

```bash
set -a; . <env-file>; set +a
python3 scripts/reporting-live-run.py --flow-mode EOD --business-date 2026-08-22 \
  --coordinator-run-id "rerun-$(date +%s)" --evidence /tmp/rerun.json --execute
```

Omit `--execute` first — it prints the plan and submits nothing.

## 3. Verify

`watermark_after` must name **your** execution_id. If it still names the previous run, the
rerun did not advance it and something failed before validation.

## Safe to repeat?

Yes. Every write is an idempotent MERGE on the business key, so a rerun that duplicates work
does not duplicate rows. That is what makes this runbook safe to use when you are unsure
whether the previous attempt got anywhere.
