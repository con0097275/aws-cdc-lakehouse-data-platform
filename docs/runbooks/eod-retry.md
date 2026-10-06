# Runbook — EOD retry

**When:** the certified close failed, or the gate will not open.

## The gate needs BOTH

`check_eod_ready` requires two things, and produces the data is not one of them:

1. an `ops.eod_watermark` ledger row for `(EOD, <table>, <date>)`
2. an Iceberg tag `EOD_<date>` on the curated table

A half-finished curated table is indistinguishable from a finished one by row count alone.
The tag pins the **exact snapshot** the gate certified, so a later rerun cannot retroactively
change what EOD said was closed.

## 1. Which half is missing?

```bash
python3 scripts/... "SELECT * FROM eod_watermark WHERE business_date = DATE '<date>'" kafka_dev_lab_dev_ops
python3 scripts/... 'SELECT name, snapshot_id FROM "kafka_dev_lab_dev_curated"."fact_account_daily_snapshot$refs"'
```

If the gate query itself **errors**, it raises rather than returning "not ready" — by design.
An unevaluable gate is not an open gate.

## 2. Close the day

The EOD build writes both:

```bash
# spark/jobs/eod/job.py with --business-date and --ops-table
```

## 3. Retry the flow

See [manual-rerun.md](manual-rerun.md), `--flow-mode EOD`.

## If validation failed rather than the gate

The model output **is** committed — validation runs after the write. The row is present but
**uncertified**, and the watermark was not advanced. Do not publish it. Fix the data or the
rule, then rerun; the MERGE overwrites in place.
