# ADR-074 — The FULL_CDC ingest writes its own state; Airflow watches it, never drives it

* **Status**: Accepted (implementation: Phase B)
* **Date**: 2026-09-17
* **Extends**: ADR-073 (ingestion mode and checkpoint identity), ADR-045 (checkpoint reset),
  ADR-041 (a long-running app's lifecycle belongs to the orchestrator, its batches do not)
* **Evidence**: `cdc/streaming_state.py`, `spark/jobs/full_cdc/stream_job.py`,
  `airflow/dags/full_cdc_streaming_lifecycle.py`, `scripts/cdc-stream.py`,
  `artifacts/validation/phase-b/`

## Context

ADR-073 decided *how* the ingest runs. An audit of what it actually landed against Phase B's
brief found four gaps, and the first two meant ADR-073's central claims were not yet true:

1. **The derived checkpoint was dead code.** `stream_job.py` declared
   `--checkpoint` with `required=True`, and the plan-derived path was only read if that flag
   was absent, which argparse made impossible. Every run used whatever was typed.
2. **`ops.streaming_app_state` had a schema, a provisioned table and no writer.** It was
   verified live on 2026-09-17: the table exists and holds zero rows. An empty state table and
   a healthy app are indistinguishable from outside. That is the same failure Phase A
   measured, one layer up. Kafka `LOAN` held 124 events against FULL_CDC's 65 with every
   check green. By this ADR's measurement it held **1,668 against 65**.
3. **No lifecycle.** Nothing started, watched or restarted the ingest.
4. **`startingOffsets` defaulted to `latest`.** That default only matters for a checkpoint
   that does not exist yet, which is the first start under an identity-keyed path. The only
   FULL_CDC checkpoints in S3 today are date-keyed (`stream_w20260906`, `stream_w20260906b`).
   A first start under `full-cdc-oracle` would have silently discarded about 1,600 LOAN
   changes and reported success.

## Decision

### 1. The ingest writes one row per app, in place, throttled

`cdc/streaming_state.py` owns the row, the write, the legal transitions and the verdict. It
imports no Spark, so the Airflow monitor (which has no SparkSession) and the operator CLI
call the same functions the job does.

* **Update in place.** The job MERGEs on `app_id`, and the SQL is built from
  `ingestion.STREAMING_STATE_COLUMNS`, the same tuple the DDL reads. A column added to the
  contract therefore cannot be left unwritten.
* **Throttled.** "In place" describes rows, not commits. A write happens when it carries
  information (rows moved, status changed, an error) or when `heartbeat_interval` has
  elapsed. The default is 5 minutes: 288 state commits a day instead of 1,440.
* **Closed transitions.** `None→STARTING→RUNNING→{STOPPED,FAILED}→STARTING`. A
  `FAILED→RUNNING` move would mean two processes writing one app id, so it is refused.
* **`restart_count`** is read back from the stored row, not from the process. That makes a
  crash loop visible.
* **A failed state write never fails a batch.** Raising in `foreachBatch` tells Spark the
  batch did not happen. Lost visibility is survivable; lost rows are not.

### 2. The checkpoint is derived, and a subscription the plan cannot describe is refused

`--checkpoint` is optional. By default the path is
`s3://<lake>/checkpoints/full_cdc/<app_id>`, and `app_id` comes from the subscribed topics
through the plan's topic→engine map (`ingestion.apps`, plan schema **8**). If a subscription
includes a topic the plan does not describe, the job **refuses**. It does not key on the
topics it happens to know, because that would hand the process another app's checkpoint.
`--app-id` stays available for a deliberate one-off.

### 3. A fresh checkpoint starts from `earliest`

`dv_event_id` makes a replay idempotent (proven on Iceberg:
`test_a_replayed_micro_batch_adds_nothing`). Losing CDC events breaks the layer's contract
(CLAUDE.md 5.2). `latest` is still accepted as an explicit, documented choice.

### 4. Airflow deploys, watches and restarts. The trigger owns the micro-batch

`full_cdc_streaming_lifecycle.py` defines three DAGs: start (one mapped task per app from
the plan), monitor (every 10 minutes) and stop (cancels job runs and never touches the
checkpoint).

* A resident app is submitted as an **EMR Serverless `STREAMING` job run** with
  `retryPolicy.maxFailedAttemptsPerHour`. The service can restart a failed run itself,
  without waiting for the monitor's next tick, so the monitor's restart is only the backstop
  for what the service gave up on. The restart latency has **not** been measured.
  `SubmissionRequest` gained two optional fields. A caller that does not set them sends a
  byte-identical request to before.
* The monitor restarts only a verdict marked `restartable` (FAILED, or three missed
  heartbeats), and only when `FULL_CDC_AUTO_RESTART=true`, at most `FULL_CDC_MAX_RESTARTS`
  per run. It **never** restarts `STOPPED`, because an orchestrator that recovers a
  deliberate stop cannot be turned off. It fails its task on anything unhealthy, even after
  a restart, so a crash loop cannot show up as a green run.
* A test fails if the monitor's cadence gets within 5× of the trigger.

### 5. The checkpoint reset stays in one script

`scripts/streaming-reset.sh` gained `--app-id`. It keeps its guarantees: dry-run by
default, an identity guard, a typed phrase, a move to a timestamped backup rather than a
delete, and an audit line. A second script that also deleted checkpoints would make its
first sentence ("the only thing in the repository that deletes a checkpoint") false.

### 6. Exactly-once is claimed only as far as it is proven

| identity | what it is | what is guaranteed | evidence |
|---|---|---|---|
| `dv_event_id` | transport: topic, partition, offset | a replayed offset adds no row | Iceberg test; live: rows == distinct ids on 3 tables |
| `dv_src_event_id` | logical: the source change | a re-delivery at a **new** offset is **detectable** (two transport rows, one logical id) | Iceberg test `test_a_logical_replay_at_a_new_offset_stays_detectable` |

**Not claimed**: logical exactly-once at the application level. An offset-keyed MERGE cannot
deliver it, and the platform does not pretend to.

## Options

* **Append one state row per batch.** Rejected: 1,440 commits a day per app to record a
  timestamp.
* **Write state every batch, in place.** Rejected: the same commit count, just less visible.
* **Key the app on a hash of the topic set.** Rejected: reordering `--topics` would move the
  checkpoint.
* **Key on the known topics and ignore unknown ones.** Rejected: that shares another app's
  checkpoint.
* **Keep `latest`.** Rejected, see §3.
* **Monitor-only restart, no EMR STREAMING mode.** Rejected: the restart would come at the
  next 10-minute tick, with no per-hour bound owned by the service.
* **A new checkpoint-reset script for FULL_CDC.** Rejected, see §5.
* **Re-implement `health` in the DAG.** Rejected: two rules would agree until the night they
  did not.

## Consequences

* Plan schema **8**. A schema-7 consumer has no `ingestion.apps`, and `cdc-stream.py`
  refuses to run against such a plan.
* The first start under the new identity replays each topic from `earliest` into the MERGE.
  That costs more once than a steady-state start and is idempotent.
* The two date-keyed checkpoints under `checkpoints/full_cdc/` are now orphaned. They are
  left in place: removing them is a reset decision, not a side effect of this change.
* The Airflow node needs the `cdc` package on `CDC_PACKAGE_ROOT` (default `/opt/airflow`)
  and a botocore that knows `StartJobRun.mode`. Local 1.35.79 has it. The node's version is
  unverified.

## Cost

**$0 as shipped.** The registry stays `lab_low_cost`, the mode resolves to `available_now`,
every lifecycle DAG is paused and gated on `ENABLE_FULL_CDC_STREAMING`, and auto-restart is
off.

The lever this ADR prepares is priced in `docs/COST.md` §"FULL_CDC resident streaming".
Two resident apps at the submitter's default sizing come to about **$0.756/hr**, roughly
**$544/month**. That is eighteen times the $30 lab budget. It is a production decision and
stays off in the lab.

## Security

No IAM, network or credential change. Offsets are stored as coordinates, never payload. The
state query rejects an `app_id` containing a quote or a slash. The monitor needs Athena
SELECT, which the Airflow role already holds (`athena_query`, Session 36b). Cancelling job
runs needs `emr-serverless:ListJobRuns`/`CancelJobRun`, and the attached policy has
**not** been verified for them (open issue).

## Rollback

* Code: stop passing `--table-plan`, and the job requires `--checkpoint` again.
  `--state-table ''` disables the state write.
* Offsets: pass `--starting-offsets latest` explicitly.
* DAGs: already off by default. Deleting the file removes them.
* Submitter: the new fields default to `None`, and nothing that does not set them changes.

## Validation

1. `spark/tests/test_streaming_state.py`: **43**. Transitions, restart count, offsets JSON,
   MERGE shape and NULL typing, UTC, quoting, truncation, the heartbeat throttle and every
   health verdict.
2. `spark/tests/test_full_cdc_streaming_spark.py`: **20**, on real Iceberg. 100 heartbeats
   leave 1 row, two apps stay 2 rows, a restart count survives a read-back, a failure is
   readable, a replayed batch adds 0 rows, a logical replay is 2 transport rows with 1
   logical id, I/U/D all survive, an unknown topic is refused, and `resolve_policy` runs
   end to end (derived checkpoint, config change, warehouse refusal, production refusing a
   budget, an unknown topic refused).
3. `spark/tests/test_cdc_ingestion.py`: **61** (ADR-073's file, extended).
4. `airflow/tests/test_dags.py::TestFullCdcStreamingLifecycle`: **9**.
5. `test_governance_review_regressions.py`: the guard broken by ADR-073 is repaired. It
   was mutation-checked by inserting a swallowing `except` (fails) and restoring it (passes).
6. **Live, read-only** (`artifacts/validation/phase-b/`): the state table exists and holds
   0 rows, so `cdc-stream.py health` reports ABSENT and exits 1. Kafka vs FULL_CDC was
   measured, identity uniqueness was measured, and the checkpoint prefixes were listed.
7. **Not claimed, pending a live window**: a resident run on EMR, a kill and restart on the
   same checkpoint with a before/after offset comparison, and the EMR `STREAMING` mode with a
   `0` execution timeout. None of these has run. The commands are in
   `docs/FULL_CDC_STREAMING.md` §7.
