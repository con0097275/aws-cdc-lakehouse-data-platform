# ADR-041 — STREAM_BATCH vs STREAMING_RT

- Status: **ACCEPTED** (Session 22) — STREAMING_RT ships **flag-off**
- Related: ADR-040, ADR-045, ADR-027 (ephemeral lifecycle), `CLAUDE.md` §4

## Context

The two are different in kind and the references prove the confusion is real: repo 1 calls
an Airflow-triggered micro-batch `job_type='stream'` (`pipeline_builder.py:1460
build_stream_job_dag`, `schedule=None`), while repo 3's long-running application is also
called "stream" (`flow3_pl_stream_main`). Sharing a prefix is how an operator kills the
wrong thing.

This repository already recorded a decision against always-on streaming, with its reason:
"Structured Streaming with an always-on cluster would hold EMR Serverless capacity for
24h/day to serve a demo that runs for an hour" (`spark/common/flow_runner.py:12-18`).
`CLAUDE.md` §4.5/§4.6 forbid default always-on compute and require auto-stop.

## Options

| Option | Verdict |
|---|---|
| **Both modes, fully disjoint naming/pools/checkpoints; STREAMING_RT flag-off, bounded window** | **CHOSEN** |
| STREAM_BATCH only; drop STREAMING_RT | Rejected |
| STREAMING_RT always-on | Rejected |
| One "streaming" mode with a cadence parameter | Rejected |

Dropping STREAMING_RT would leave the framework unable to express a latency requirement it
is asked to support, and the contract is cheap to define even while the application is off.
Always-on contradicts `CLAUDE.md` §4.5/§4.6 and `flow_runner.py:12-18`. A single mode with
a cadence parameter is exactly the conflation that lets an operator kill the wrong thing —
repo 1 calls its micro-batch `stream` and repo 3 calls its long-running app the same.

## Decision

**Both exist. They share no name, no pool, no checkpoint root, and no status vocabulary.**

| | `STREAM_BATCH` | `STREAMING_RT` |
|---|---|---|
| Trigger | Airflow schedule | its own trigger loop |
| Lifetime | starts and exits per run | runs until stopped |
| Cadence | 5–15 min | seconds (`trigger_interval`) |
| Position | `job_watermark_state` | Spark checkpoint, mirrored to `streaming_app_state` |
| History | one `job_master_execution_hist` row per run | one row per **deployment**; progress in `streaming_app_state` |
| Failure | Airflow retry | app restart, checkpoint recovery |
| Airflow's role | runs the batch | starts, monitors, stops the app |
| Pool | `POOL_REPORTING` | `POOL_STREAMING` |
| Checkpoint | none | `checkpoints/reporting/<env>/<job_id>/STREAMING_RT/` |
| Prefix | `sb_` | `rt_` |

**STREAMING_RT ships behind `enable_streaming_rt`, default `false`, and runs in a bounded
window.** Repo 3 — the only reference that actually operates a long-running app — does not
run it 24/7 either: `configs/bcn_pipeline.yaml` starts it at 08:00 and kills it at 20:00,
six days a week, by app name. That is the pattern adopted: `streaming_rt_start` at window
open, `streaming_rt_stop` at window close, `streaming_rt_monitor` in between.

**`ops.streaming_app_state` is justified and will be created.** The §33 test is whether
`job_master_execution_hist + job_watermark_state` can represent the application cleanly.
They cannot: `restart_count`, `last_batch_id`, `last_progress_at`, `last_source_offset`
and "is the app up right now" are properties of a **process**, not of a run (which is
per-execution) or of a position (which is a single row per job+mode with no lifecycle).
Repo 3 needed three separate state tables for the same reason
(`dim_autocorrect_watermark`, `dim_pending_timeout_acctnbrs`, `dim_dim_change_audit`).

**Source policy.** `FULL_CDC_APPEND` is the default — an Iceberg incremental read of the
canonical layer, which keeps the lakehouse the single read path. `KAFKA_DIRECT` is
permitted only when a job's latency budget cannot tolerate the Iceberg commit interval,
must be justified in the job YAML's `source_justification` field (compile fails if
`KAFKA_DIRECT` is set without one), and carries two obligations: FULL_CDC remains the
canonical durable truth, and a reconciliation job against it is mandatory. Repo 3 chose
Kafka-direct and paid exactly that price — its correction pass exists to reconcile the
stream against the silver layer (`main_autocorrect.py`, step 2 "silver T-1 ∪ Kafka
window").

**The failure contract is normative**, taken verbatim in intent from
`flow3_pl_stream_main/src/main.py:299`, `:540-545`: with `foreachBatch`, returning quietly
means Spark commits the offsets, so a partial failure must **raise**. A test asserts the
handler raises on a seeded write failure.

## Consequences

- An operator cannot confuse the two: different prefixes, pools, DAGs and checkpoints.
- With the flag off, the framework still compiles, validates and tests STREAMING_RT
  config; only the application is not deployed.
- `docs/FOUR_FLOWS.md` and `docs/RUNBOOK.md` gain the distinction table above.

## Cost

`STREAM_BATCH`: seconds of EMR Serverless per run; frequency dominates.
`STREAMING_RT`: continuous EMR Serverless capacity for the window it is up — the single
largest cost lever in this framework, which is why it is off by default, bounded when on,
and covered by the existing $30 tag-filtered budget alert and `AutoDestroyAfter` tagging.

## Security

`KAFKA_DIRECT` requires MSK IAM auth through the existing `reporting` role; no new
credential. Certificates and bootstrap servers come from SSM/Secrets Manager at runtime,
never from config (`CLAUDE.md` §3.1/§3.6) — repo 3's `pipeline_config.yaml` commits SSL
keystore passwords in plaintext, which must not be copied.

## Rollback

`enable_streaming_rt=false` and run `streaming_rt_stop`. Checkpoints remain in S3 under
their own prefix with a 14-day noncurrent expiry; the app resumes from them when re-enabled.

## Validation

- A test asserts no shared identifier, pool or checkpoint prefix between the two modes.
- A test asserts `foreachBatch` raises on a seeded partial write failure.
- A test asserts a restart resumes from the checkpoint and re-emits no committed batch.
- A test asserts `KAFKA_DIRECT` without `source_justification` fails compile.
- `scripts/verify-destroy.sh` asserts no EMR Serverless job run remains after
  `streaming_rt_stop`.
