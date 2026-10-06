# Runbook — FULFILL backfill

**When:** a historical date is missing from the mart.

## 1. Confirm the source can actually serve it

```bash
# mart rows at the date, curated rows at the date, FULL_CDC events at the date
```

If **FULL_CDC has zero events** for that date, no correct FULFILL can produce rows — the
data never existed. Stop here and investigate ingestion instead. Phase 14 case: 2026-08-18
had 0 everywhere, and that was the right answer, not a bug.

## 2. Run it

```bash
python3 scripts/reporting-live-run.py --flow-mode FULFILL --business-date <d> \
  --from-date <d0> --to-date <d1> --coordinator-run-id "bf-$(date +%s)" \
  --evidence /tmp/fulfill.json --execute
```

## 3. Read the per-date outcome

FULFILL returns **one result per date**, not one for the run:

```json
"succeeded_dates": ["2026-08-17"], "failed_dates": [], "remaining_dates": []
```

`remaining_dates` are dates the plan wanted but never attempted, because `stop_on_failure`
halted the run. **A resume starts there** — they are not lost.

## Tier

FULFILL stamps `RECONCILED`: above a routine correction, below a normally-closed day. It
will not overwrite a `CERTIFIED` date, and a later AUTO_CORRECT will not overwrite it.
