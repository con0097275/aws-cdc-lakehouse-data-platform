# Runbook — checkpoint recovery

**Read this before touching a checkpoint. It is the most destructive thing in the platform.**

## Why there is no delete button

Discarding a checkpoint **replays or skips** data depending on where the offsets sat, and
you cannot tell which from the outside. So:

- `CheckpointInspector` has **no `delete()`** and must not gain one
- no start or restart path can reach a delete
- the only tool is `scripts/streaming-reset.sh`: **dry-run by default**, requires a typed
  phrase, and **moves** rather than deletes

## 1. Read it first

```bash
aws s3 ls s3://<lake>/checkpoints/reporting/<env>/<job_id>/STREAMING_RT/offsets/ \
  --recursive --region ap-southeast-1 | tail
```

`S3CheckpointInspector` counts only **numeric** object names — Spark also writes `.crc` and
temp files, and counting those would make an empty stream look like it had run.

| Reading | Means |
|---|---|
| prefix absent | the stream never started |
| prefix present, 0 batches | started, never completed a batch |
| N batches, last id M | resumes after batch M |

Those are different situations. A stream that started and never committed is usually a
config or permissions problem, not a checkpoint problem.

## 2. Before considering a reset, rule out the cheap causes

Checkpoint corruption is rare. Far more common: the app cannot reach its source, the schema
changed incompatibly, or the sink is rejecting writes. Check the driver log first.

## 3. If a reset is genuinely required

```bash
bash scripts/streaming-reset.sh                 # dry run — shows what WOULD move
bash scripts/streaming-reset.sh --execute       # typed phrase required
```

It moves the checkpoint aside. **Keep the moved copy** until the replacement stream has run
and reconciled — it is the only record of where the old one stopped.

## 4. After a reset, reconcile

The stream will re-read from its configured start position. Compare its output against
`FULL_CDC` for the affected window before trusting it: a reset is exactly when duplicate or
missing rows appear, and the accuracy ladder will not catch them because the tier is
unchanged.
