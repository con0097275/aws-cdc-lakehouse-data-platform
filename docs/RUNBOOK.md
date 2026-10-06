# Runbook

- Session: 15
- Date: 2026-08-15
- Status: **procedures written; only the locally-demonstrable ones have been executed**

Every alert in `observability/alerts/slo-alerts.yml` links to an anchor here. If you arrive
from a page, start at the matching section.

**The general rule:** every recovery in this pipeline is a *rerun*, not a repair. Every job
is idempotent by `(business_date, run_id)` and every MERGE is idempotent on a business key,
so the safe move is almost always to run it again rather than to hand-edit data.

---

## metrics-stale

**Alert:** `PipelineMetricsStale` · **page**

No metrics pushed for over an hour. **Assume the pipeline is dead, not healthy** — pushed
metrics are sticky, so every other panel is showing values from the last successful run.

1. `scripts/airflow-node.sh status` — is the orchestrator even running?
2. Check the last DAG run: is it failed, or did the schedule stop firing?
3. `SELECT max(recorded_at) FROM ops.data_certification;` — when did anything last complete?
4. If Airflow is down, everything downstream is stale by definition. Start there.

Do **not** silence this alert to "reduce noise". It is the only signal that separates a
healthy pipeline from an absent one.

---

## kafka-consumer-lag

**Alert:** `KafkaConsumerLagGrowing` · **page**

Lag is large **and rising**. The steady-state case is not an emergency; the growing case
ends in data loss.

**Kafka retention is 24 hours.** Events older than that are gone and require a Debezium
re-snapshot (RPO 24h).

1. Is the consumer alive? `bash scripts/cdc-runtime.sh smoke`
2. Is it *progressing*? Compare committed offsets a minute apart. A RUNNING consumer with a
   frozen offset is the common case — see `checkpoint-recovery`.
3. Estimate time-to-loss: `lag ÷ consumption_rate`. If that is under the remaining
   retention, this is now an incident, not a ticket.
4. If it will breach retention, the choice is to scale consumption or accept a re-snapshot.
   **Decide explicitly** — drifting into the re-snapshot is the worst of both.

---

## connector-restart

**Alert:** `CdcConnectorDown` · **page** · *drill 1*

Connect offsets live in Kafka internal topics, so a restart resumes from the committed
position. **No re-snapshot, no data loss** (RPO 0, RTO ~2 min).

```bash
bash scripts/cdc-runtime.sh restart-connect
bash scripts/cdc-runtime.sh smoke          # verify RUNNING and offsets advancing
```

Verify the consumer group resumes from its previous offset rather than 0. A restart that
resets to 0 replays the whole topic — harmless (the L1 write is idempotent on `event_id`)
but slow, and it usually means offset topics were lost.

---

## reconciliation-difference

**Alert:** `LayerReconciliationBroken` · **page** · *drill 7*

A nonzero difference is a **lost or duplicated event**, not a rounding artefact.

1. Which boundary? `SELECT * FROM ops.dq_result WHERE check_type='reconciliation' AND verdict='FAIL';`
2. Duplicates: `SELECT event_id, count(*) FROM full_cdc.<table> GROUP BY event_id HAVING count(*) > 1;`
3. If L2 < L1, events were dropped: re-run L2 for the date. The MERGE is idempotent on
   `event_id`, so a rerun converges rather than duplicating.
4. If L2 > L1, something wrote L2 twice with different ids — that is a defect in the writer,
   not a data problem. Do not "fix" it by deleting rows.

**Do not publish the certified partition until this is zero.** The DQ gate already blocks it
(exit 2); overriding that is a decision someone must own.

---

## checkpoint-recovery

**Alert:** `StreamingCheckpointStale` · **page** · *drills 3, 4*

The checkpoint has not advanced in 30 minutes. The job may be RUNNING and making no
progress — **the failure that looks healthiest**.

1. Restart the streaming job. Restarting from a stale checkpoint is safe: replay is
   idempotent on `event_id`.
2. If the checkpoint is **lost** (drill 4), restart from the earliest available Kafka offset
   within retention. RPO 0 *within* retention; beyond 24h, a re-snapshot is required.
3. Never point a new job at the old checkpoint path while the old job might still be
   running — two writers sharing a checkpoint corrupt the stream (`CLAUDE.md` §5.9).

---

## nrt-freshness

**Alert:** `NrtFreshnessSloBreached` · **ticket**

NRT output is **provisional**. Certified numbers come from EOD and are unaffected, which is
why this is a ticket.

Usual causes, in order of likelihood: consumer lag (see above), an NRT DAG run that is
paused, or a micro-batch running longer than its 5-minute cadence.

---

## eod-incomplete

**Alert:** `EodNotCompleted` · **ticket**

1. `SELECT * FROM ops.data_certification WHERE business_date = DATE '<date>' ORDER BY recorded_at;`
2. Find the failed task in the `eod_certified_pipeline` DAG.
3. Fix and **re-run the DAG for that date** — every task is idempotent, so a partial re-run
   is safe. Do not skip forward to the next stage: the ordering constraints in
   `docs/AIRFLOW_K8S.md` §4.3 are correctness, not preference.

---

## dq-failures

**Alert:** `DataQualityPassRateLow` · **ticket**

```sql
SELECT dataset, check_name, verdict, observed, threshold, detail
FROM   ops.dq_result
WHERE  business_date = DATE '<date>' AND verdict = 'FAIL';
```

Audit history is intact — quarantine holds **copies**, and L1/L2 keep every original.

---

## dq-not-evaluated

**Alert:** `DataQualityChecksNotEvaluated` · **ticket**

`NOT_EVALUATED` is **not** a failed check. It means the check had nothing to examine —
usually an empty partition, which means **the job produced nothing rather than producing
something wrong**.

Check whether the upstream job ran at all before investigating data. This alert usually
fires alongside `EodNotCompleted` and the upstream failure is the real story.

---

## iceberg-commit-conflicts

**Alert:** `IcebergCommitConflicts` · **ticket**

Almost always two writers on one table, or maintenance overlapping a live write. Iceberg
retries optimistically; sustained conflicts mean genuine contention.

Check that `iceberg_maintenance` is not running while a flow writes the same table — the
EOD DAG orders maintenance last for exactly this reason.

---

## airflow-failures / registry-outage / slow-batch

- **Airflow failures** — check the task log; the DAG is orchestration only, so the failure
  is almost always in the Spark job it submitted.
- **Registry outage** — Connect caches schemas, so CDC usually continues; only *new or
  changed* schemas fail. Ticket, not page, for that reason.
- **Slow batch** — compare against `sli:spark_batch_duration_seconds:max` history. A batch
  that suddenly doubles usually means small-file accumulation; check
  `mart."<table>$files"` and whether maintenance is keeping up.

---

## streaming-rt-stalled

**Symptom.** `streaming_rt_monitor` reports `healthy=false`, `status=DEGRADED`, and
`seconds_since_progress` climbing. No exception anywhere; the driver is up.

This is the streaming failure that does not announce itself. Only elapsed time since the
last committed batch shows it.

1. Confirm from the mirror, not from the UI: `ops.streaming_app_state` →
   `last_progress_at`, `last_batch_id`, `restart_count`.
2. `restart_count` climbing as well means the process is dying and being restarted, not
   stalling — read `error_msg` and treat it as a crash, not a stall.
3. A stall with `restart_count` flat is usually a source that stopped delivering or a batch
   that cannot finish. Check source lag first; a stream with nothing to read is idle, not
   stalled, and idle is not an incident.
4. Restart is the remedy: run `streaming_rt_stop`, then `streaming_rt_start`. It resumes
   from the checkpoint. **Do not delete the checkpoint** — see below.

## streaming-rt-checkpoint-reset

**Only when the checkpoint itself is the problem**, which is rare and is never the first
thing to try. A restart resumes; a reset changes where the stream reads from.

```
starting_offsets=latest    every event since the last committed batch is SKIPPED
starting_offsets=earliest  the source is REPLAYED from the beginning
```

Neither announces itself, and neither is undone by restarting again.

```bash
scripts/streaming-reset.sh --job-id <job> --environment dev --bucket <lake>            # dry run
scripts/streaming-reset.sh --job-id <job> --environment dev --bucket <lake> --execute  # for real
```

The script refuses non-interactively, requires a typed confirmation phrase, MOVES the
checkpoint to a timestamped backup rather than deleting it, and appends a `RECOVERY` row to
`artifacts/validation/streaming-resets.log`. Order is fixed and matters: **stop the
application → move the checkpoint → only then touch the target tables.** Reversed, a
still-running stream resumes into a table that has been replaced.

Nothing on the start path can delete a checkpoint, by construction: `CheckpointInspector`
has no delete method.

## streaming-rt-schema-change

**Symptom.** The application stopped with `SchemaEvolutionError` and `status=FAILED`.

That is the designed outcome for an incompatible change, and nothing was written. A dropped
column would have filled the target with NULLs for every row in the window; a narrowed type
would have been cast rather than rejected, putting the corruption in the data instead of the
logs.

1. Read `error_msg` — it names the column and the change.
2. Decide whether the upstream change is intended. If it is, evolve the target and the
   job's expected schema together, then start the application; it resumes from the
   checkpoint and reprocesses nothing.
3. Compatible changes — a new nullable column, a lossless widening — never stop the stream
   and need no action.

## Escalation

| Time | Who |
|---|---|
| 0–30 min | on-call data engineer |
| 30–60 min | + platform lead |
| > 60 min, or any data loss | + data owner from `governance/catalog/domains.yml` |

The registry names an owner and steward per domain, which is what makes "escalate to the
data owner" an actionable instruction rather than a gesture.
