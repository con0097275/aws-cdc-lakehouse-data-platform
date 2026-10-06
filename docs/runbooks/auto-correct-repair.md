# Runbook — AUTO_CORRECT repair

**When:** late CDC has moved a date that is already in the mart.

## Expect a refusal on a certified date

AUTO_CORRECT writes `PROVISIONAL_CORRECTED`, which ranks **below** `CERTIFIED`. Against a
certified date it returns:

```
0 date(s), 0 key(s), path=DERIVED; N date(s) ESCALATED (already certified;
AUTO_CORRECT cannot overwrite CERTIFIED)
```

with `spark_app_id: null` — it refuses **before submitting a job**, so it does not spend
money on work the model would reject. **This is success, not failure.**

## The sanctioned route for a certified date

Rerun **EOD**, which writes the same tier with a newer `input_cutoff` and therefore wins:

1. rebuild curated so the late event is present
2. rerun EOD for that date ([eod-retry.md](eod-retry.md))
3. confirm the mart value moved

Phase 14 did exactly this: a +999.99 late Oracle event moved the mart from
2,031,880,160.00 to 2,031,881,159.99.

## Do not reach for --allow-certified-repair

It makes the **flow** select the date, but the dbt model's `merge_guard` still drops the
rows in SQL. You get a new Iceberg snapshot and unchanged data — the worst of both, because
it looks like something happened.

## If the correction fails mid-window

Dates already merged keep their corrected rows; the MERGE is atomic per date, so no date is
half-written. The watermark did **not** advance, so a rerun re-derives the whole window.
No manual repair.
