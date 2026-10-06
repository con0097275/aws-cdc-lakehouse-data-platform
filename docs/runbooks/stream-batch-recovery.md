# Runbook — STREAM_BATCH recovery

**When:** a micro-batch failed, or a run is blocked as already in flight.

## "already has a run in flight"

```
mart_x/STREAM_BATCH already has a run in flight: <execution_id> (RUNNING).
Two concurrent micro-batches would read overlapping windows and do the same work twice.
```

The run is **SKIPPED**, not queued. Airflow's `max_active_runs=1` does not cover a manual
trigger, a retry of a stuck task, or a worker that died leaving the record non-terminal —
hence this runtime guard.

**Is the in-flight run alive?**

- yes → wait. Two batches over the same window is the thing being prevented.
- no → finalise it FAILED ([runtime-state-investigation.md](runtime-state-investigation.md)),
  then rerun.

## A failed batch needs no repair

`watermark_ts` was **not** advanced, so the next batch re-reads the same window. If it had
advanced on failure, the next window would start after data that was never processed —
silent loss.

`safety_overlap_minutes` re-reads a margin **before** the old watermark on every run, which
is what makes the re-read self-healing. It must never be 0.

## The frozen upper bound

The batch owns `[watermark - overlap, frozen_upper)` and nothing later, so a row arriving
mid-run cannot be half-included. On success the watermark commits **at** `frozen_upper` —
check `watermark_ts` in the evidence record; a null there means no batch has ever completed.
