# ADR-045 — STREAMING_RT checkpoint mechanism

- Status: **ACCEPTED** (Session 22) — applies when `enable_streaming_rt = true`
- Related: ADR-041, ADR-036, `CLAUDE.md` §5.9

## Context

Repo 3 keeps one stable checkpoint per application, config-driven
(`_shared_src/pipeline_config.yaml`: `checkpoint_dir:
"hdfs://.../warehouse/_checkpoint/pl_init_bcn_sodu"`), and its second long-running app
uses a separate one (`_checkpoint/pl_datamart`). Two hard-won lessons are recorded in that
codebase:

- `main.py:813 _warn_if_stale_checkpoint` inspects `offsets/` and warns that
  `starting_offsets: latest` applies **only** when the checkpoint is empty — and
  deliberately does **not** auto-delete, because deleting a checkpoint is an irreversible
  operator decision (`:822`).
- `0_phase_eod/config/eod_config.yaml:28-31` requires the checkpoint and monitor state
  directories to be removed **before** dropping the target tables, or a restarted stream
  resumes against a table that no longer exists.

This repository already encodes the S3-specific hazard: `checkpoints/` is a **top-level
sibling** of `warehouse/`, with a Terraform variable validation rejecting any prefix under
`warehouse/checkpoints` (`modules/data_lake/variables.tf:92-95`, `:110`) because Iceberg's
`remove_orphan_files` walks a table's location and would delete a checkpoint as
unreferenced. A 14-day noncurrent expiry is configured, with a validation that it exceeds
Kafka's retention (`variables.tf:46-56`, `envs/dev/terraform.tfvars`).

## Options

| Option | Verdict |
|---|---|
| **Derived S3 path under the top-level `checkpoints/` prefix; deletion is an operator action** | **CHOSEN** |
| Author `checkpoint_location` per job | Rejected |
| Checkpoint under `warehouse/<table>/_checkpoint/` | Rejected |
| Auto-delete a stale checkpoint on start | Rejected |
| Local/ephemeral disk on the executor | Rejected |

An authored path is a typo away from two apps sharing state. A path under `warehouse/` is
deleted by `remove_orphan_files` — the exact hazard `modules/data_lake/variables.tf:110`
already refuses to allow. Auto-deleting on start silently discards offsets and replays or
skips data; repo 3 explicitly warns and refuses to delete (`main.py:822`). Ephemeral disk
loses all state on restart, which §34 prohibits outright.

## Decision

**Checkpoint paths are derived, never authored:**

```
s3://<lake>/checkpoints/reporting/<environment>/<job_id>/<flow_mode>/
```

`job_flow_config.checkpoint_location` is a **computed** field. Config validation rejects
any literal path, any path outside the `checkpoints/` top-level prefix, and any path under
`warehouse/`. `job_id` is a stable string slug and never renumbered (ADR-035) precisely
because it appears here — a renumber would orphan streaming state.

The path is **stable across controlled restarts** and includes environment and job, as §34
requires. It does not include `code_version` or `deployment_id`: a checkpoint that moved
on every deploy would restart the stream from scratch every deploy.

**Deletion is an operator action, never automatic.** `streaming_rt_start` checks the
checkpoint and *reports* what it finds — empty, or resuming from batch N — into
`ops.streaming_app_state`. It never deletes. A separate, explicitly named script
`scripts/streaming-reset.sh` performs a reset, requires the interactive confirmation phrase
that `scripts/lib.sh:161 confirm_destructive()` already enforces for every irreversible
action in this repository, and writes a `RECOVERY` row to `ops.summary_config_hist_v1`.

**Teardown order is fixed** and encoded in `scripts/streaming-reset.sh`, following repo 3's
lesson: stop the application → delete the checkpoint → then touch the target tables. The
reverse order leaves a stream resuming into a table that has been replaced.

**State is mirrored, not owned.** The Spark checkpoint is authoritative for offsets.
`ops.streaming_app_state` records `last_batch_id`, `last_source_offset`, `last_progress_at`
and `restart_count` for observability and for the `streaming_rt_monitor` DAG — a mirror
that is allowed to lag and is never read to decide where to resume from.

**Lifecycle retention.** Checkpoints inherit the existing `expire-old-checkpoints`
lifecycle rule (`modules/data_lake/main.tf:203-210`, 14 days noncurrent). No new rule is
needed; the reporting prefix is added to `lake_prefixes` so the rule covers it.

## Consequences

- A controlled restart resumes exactly where it stopped.
- A rename of a job breaks its checkpoint continuity — which is why `job_id` is immutable
  and a compile check asserts no `job_id` in the plan has changed for a job whose
  STREAMING_RT mode is enabled.
- Two applications never share a checkpoint, because the path includes `flow_mode`.
- `remove_orphan_files` can never see a checkpoint.

## Cost

S3 storage for checkpoint state — kilobytes to low megabytes per app, with a 14-day
noncurrent expiry. Effectively zero.

## Security

Checkpoints live in the lake bucket: SSE-KMS with the lake CMK, TLS-only bucket policy,
Block Public Access — all already applied. The `reporting` role gets read/write on
`checkpoints/reporting/*` only, not the whole prefix.

## Rollback

Stop the app; the checkpoint persists. Re-enabling resumes. Discarding state is the
explicit reset script, never a side effect.

## Validation

- A test asserts the derived path starts with `s3://<lake>/checkpoints/` and never
  contains `warehouse/`.
- A test asserts a literal `checkpoint_location` in a job YAML fails compile.
- A restart-recovery test: kill mid-stream, restart, assert no committed batch is
  reprocessed and no uncommitted batch is lost.
- A test asserts `streaming_rt_start` never deletes a checkpoint.
- `scripts/streaming-reset.sh` refuses to run non-interactively (same assertion
  `scripts/validate-docs.py:515` already makes for the other destructive scripts).
