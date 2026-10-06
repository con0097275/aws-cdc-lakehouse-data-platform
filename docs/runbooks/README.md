# Operator runbooks

Twelve procedures. Each states **when it applies**, the **steps**, and — most importantly —
**what must be true before you are allowed to proceed**.

| Runbook | Use when |
|---|---|
| [new-mart.md](new-mart.md) | adding a datamart |
| [manual-rerun.md](manual-rerun.md) | a run must be repeated for one date |
| [eod-retry.md](eod-retry.md) | the certified close failed or the gate will not open |
| [auto-correct-repair.md](auto-correct-repair.md) | late CDC moved a date that is already published |
| [fulfill-backfill.md](fulfill-backfill.md) | a historical date is missing from the mart |
| [stream-batch-recovery.md](stream-batch-recovery.md) | a micro-batch failed, or two are in flight |
| [streaming-rt-restart.md](streaming-rt-restart.md) | the streaming app crashed or is stalled |
| [checkpoint-recovery.md](checkpoint-recovery.md) | the checkpoint is suspect — **read this before deleting anything** |
| [watermark-investigation.md](watermark-investigation.md) | a watermark looks wrong, stuck or ahead |
| [dependency-investigation.md](dependency-investigation.md) | a job is stuck WAITING_DEPENDENCY |
| [runtime-state-investigation.md](runtime-state-investigation.md) | DynamoDB execution/watermark state needs inspecting or repairing |
| [iceberg-rollback.md](iceberg-rollback.md) | a bad commit reached a table |
| [airflow-access.md](airflow-access.md) | reaching the Airflow UI, and what to check when the tunnel is dead |
| [stop-and-resume.md](stop-and-resume.md) | shutting the lab down to stop paying for it, and bringing it back correctly |

## Two rules that outrank every procedure here

**1. A run whose outcome was never confirmed is FAILED, never SUCCEEDED.**
If a driver died after its Spark job succeeded, the data may well be committed — but the
orchestration run did not complete, and marking it SUCCEEDED would advance a watermark
behind work nothing verified. This happened four times during Phase 14.

**2. Never delete a streaming checkpoint to "fix" a stream.**
Discarding it replays or skips data depending on where the offsets sat, and you cannot tell
which from the outside. See [checkpoint-recovery.md](checkpoint-recovery.md).
- [rebuild-from-scratch.md](rebuild-from-scratch.md) — apply from zero to CDC flowing, in the order that works, plus the seven defects a rebuild used to hit and what a regression looks like.
- [ai-platform-operations.md](ai-platform-operations.md) — ten AI failure modes: RAG sync, retrieval, model outage, Athena tool, runtime, feature lookup, inference, cost spike, unsafe-request investigation, version rollback.
