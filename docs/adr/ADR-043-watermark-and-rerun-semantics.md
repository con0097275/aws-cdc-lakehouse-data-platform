# ADR-043 — Watermark semantics and rerun behaviour

- Status: **ACCEPTED** (Session 22)
- Related: ADR-035, ADR-036, ADR-042

## Context

Repo 1 and repo 2 both advance position from inside business logic. The Spark job reads
`summary_config_v1`, and on an EOD-ready run writes the next working day back into it
(`f_acct_depo_by_day_level_acct_auto_correct_generate.py:1359-1372`). Repo 2's
micro-batch keeps its position in a single-row Iceberg table `d_0_<fact>` read at
`..._streaming_generate.py:186-203`, with a 3-minute rewind and a `-1` sentinel for a cold
start.

Two failure modes follow. First, a job that fails *after* writing position leaves the
position advanced past data that was never produced. Second — observed in repo 1 —
`handle_final_job_status` treats "the execution_date in summary_config moved" as evidence
that a FAILED Spark run actually succeeded (`pipeline_builder.py:690-697`): position was
being used as a success signal because nothing else was trustworthy.

This repository has no per-job watermark at all. `ops.layer_watermark` is per **layer**
(`spark/eod/ddl/ops_tables.sql:7`), and the NRT watermark is passed in as a function
argument (`run_flow.py:32`) with no persistent store.

## Options

| Option | Verdict |
|---|---|
| **Single framework-owned writer; advance only after validation; conditional write** | **CHOSEN** |
| Business logic advances position (repo 1, repo 2) | Rejected |
| Advance on engine success, validate afterwards | Rejected |
| No watermark; always reprocess a fixed window | Rejected |

Business-logic ownership is what produced repo 1's inference that a moved `execution_date`
proves a FAILED run succeeded (`pipeline_builder.py:690-697`). Advancing before validation
means a run that produced wrong data still moves the position, so the next run never
revisits it. A fixed reprocessing window with no watermark is correct but pays the full
scan every cadence, and `flow_runner.py:12-18` already records that frequency dominates
cost here.

## Decision

**One writer, one place, one moment.**

`ops.job_watermark_state`, keyed `(job_id, flow_mode)`, holds only the current successful
position. It is written by `spark/reporting/ops_client.py` and by nothing else — not by a
Spark job, not by a dbt model, not by an Airflow task other than `commit_watermark`.

The success path is ordered and total:

```
1. transform + write completes           (Spark/dbt returns success)
2. target commit confirmed               (Iceberg snapshot id captured)
3. required validation passes            (dbt tests + ops.dq_result has no ERROR FAIL)
4. watermark advanced                    (conditional write)
5. status → SUCCEEDED                    (only now)
```

**Failure at any step means the watermark does not move.** There is no partial advance and
no "advance anyway with a warning". Steps 4 and 5 are separate so that a crash between
them leaves a run that is *not* SUCCEEDED with a watermark already advanced — recoverable,
because step 4 is idempotent under the conditional write below, and detectable, because
the reconciler flags a watermark whose `last_success_execution_id` names a non-terminal
execution.

**The write is conditional**, on `last_success_execution_id` and on monotonicity:

```
UpdateItem  ConditionExpression:
    attribute_not_exists(last_success_execution_id)
    OR watermark_ts <= :new_watermark_ts
```

A stale retry that completes late therefore cannot move the watermark backwards. Repo 1
had no such guard.

**Per-mode semantics:**

| Mode | Watermark meaning | Advance |
|---|---|---|
| `EOD` | `last_success_date_of_data` = last certified COB | to the closed date |
| `AUTO_CORRECT` | correction upper bound + affected dates | to the run's `input_cutoff` |
| `FULFILL` | not used for position; each date is independent | records `max(date)` only |
| `STREAM_BATCH` | `watermark_ts` = source high-water | to `bounded_now` |
| `STREAMING_RT` | Spark checkpoint is authoritative; the row is a **mirror** | on each committed batch |

**Safety overlap is mandatory, not optional.** `STREAM_BATCH` reads from
`watermark_ts − safety_overlap_minutes`, defaulting to the 2 minutes this repo already
uses (`flow_runner.py:62 NRT_SAFETY_OVERLAP_MINUTES`) for the reason recorded there: an
event written microseconds before the recorded watermark otherwise falls through the gap
between two runs and is never picked up. Repo 2 independently chose 3 minutes. Overlap is
safe only because the write is an idempotent MERGE on the business key (ADR-042); compile
fails if `safety_overlap_minutes = 0`.

**Rerun behaviour:**

| Action | `attempt_number` | `date_of_data` | Watermark |
|---|---|---|---|
| Airflow task retry | unchanged (same execution) | unchanged | unchanged until success |
| Explicit rerun | **+1**, new `execution_id` | unchanged | re-advanced on success; conditional write makes a no-op safe |
| `FULFILL --force` on a past date | +1 | the target date | not moved backwards |
| `--reset-watermark` | +1 | as given | **rewound**, requires an explicit flag and writes a `RECOVERY` row to `summary_config_hist_v1` |

An Airflow *task* retry reuses the same `execution_id`, matching
`airflow/dags/common.py:86`'s deterministic `run_id` — chosen there over a UUID precisely
so a retry is recognisable as a repeat. An *operator* rerun is a new attempt, because it is
a new decision and must be visible as one.

**Position is never a success signal.** Status comes from the execution row alone. Repo
1's inference is explicitly prohibited, and a test asserts the framework never reads
`job_watermark_state` to decide whether a run succeeded.

## Consequences

- "Did day T actually produce data" is answerable from status, not inferred.
- A failed run leaves the next run reading the same window — reprocessing, not skipping.
- Backfills do not disturb streaming position.
- `ops.layer_watermark` stays as-is; it describes layers, this describes jobs.

## Cost

Two DynamoDB writes per successful run. Negligible.

## Security

None.

## Rollback

`--reset-watermark` is the supported manual correction and is audited. Reverting the
design would mean moving position back into business logic, which nothing requires.

## Validation

- **A failed run does not advance the watermark** — the single most important test.
- A stale retry with an older `watermark_ts` is rejected by the conditional write.
- Validation failure (an ERROR-severity `ops.dq_result` FAIL) blocks both the advance and
  the SUCCEEDED status.
- A crash simulated between steps 4 and 5 is detected by the reconciler.
- `safety_overlap_minutes = 0` fails compile.
- A test asserts no code path reads `job_watermark_state` to determine run success.
