# ADR-073 — Ingestion mode is declared, not inferred; checkpoint identity is stable

* **Status**: Accepted (implementation: Phase B)
* **Date**: 2026-09-17
* **Extends**: ADR-062/063 (per-table FULL_CDC), ADR-071 (orchestration)
* **Evidence**: `cdc/ingestion.py`, `spark/tests/test_cdc_ingestion.py` (61 tests after ADR-074)
* **Completed by**: ADR-074 (state writer, derived checkpoint, lifecycle)

## Context

Phase A found production behaviour being **inferred from a CLI flag**. `stream_job.py` opened
a real `processingTime` query and then killed it on a wall-clock budget:

```python
ap.add_argument("--run-seconds", type=int, default=240, ...)
...
while query.isActive and (time.monotonic() - started) < args.run_seconds:
```

A bounded streaming *session* wearing a streaming name — and with a **default**, so every run
was bounded whether anyone chose that or not. Worse, the job actually being submitted was the
**batch** one. Measured live 2026-09-17: Kafka `cdc.oracle.COREBANK.LOAN` held **124** events
while FULL_CDC held **65**, and nothing reported a problem, because a batch ingest that has
finished is indistinguishable from a stream that is caught up.

## Decision

### 1. The mode is a declared value; the profile picks its default

```yaml
ingestion:
  profile: lab_low_cost        # production | lab_low_cost
  trigger_interval: "1 minute"
```

| profile | default mode |
|---|---|
| `production` | `continuous_microbatch` — resident query |
| `lab_low_cost` | `available_now` — drain, commit, exit |

**This resolves a real tension rather than hiding it.** The production target is a resident
app; CLAUDE.md §4 says nothing runs 24/7 without a warning and `lab_low_cost` is the default
profile. Both are right. Hardcoding either makes the other a bug. The profile is the *cost
decision*, the mode its *consequence*, and turning on a resident application is then something
someone chose in a reviewed file.

### 2. Both modes share one transformation path

The only difference is the trigger:

```python
if mode == MODE_CONTINUOUS:
    writer = writer.trigger(processingTime=f"{trigger_s} seconds")
else:
    writer = writer.trigger(availableNow=True)
```

One `foreachBatch(process)`, one `writeStream`, asserted by test. A separate "batch ingestion
path" would be two implementations of one contract, and they would drift — which is the OPEN-28
defect this repository already has on record.

### 3. `--run-seconds` is TEST_ONLY and has no default

Renamed `--test-only-run-seconds`, default `None`. A resident run calls
`query.awaitTermination()` with **no timeout** — that single call is what makes the process
resident; with a timeout the loop decides when to stop, which is a session again. Taking the
test branch prints a line saying so.

### 4. The trigger is config, with a floor

Default **1 minute**, not 30 seconds. Every trigger is an Iceberg commit; Phase A measured a
mean file of **81 KB against a 128 MiB target**, and halving the interval doubles the
small-file problem the platform already has.

`parse_trigger` refuses anything ambiguous. `trigger_interval: "1"` is the dangerous case —
Spark reads a bare number as **milliseconds**, and a 1 ms trigger is only ever discovered from
a bill. Below **30 s** is refused outright: a table would produce more snapshots per day than
maintenance can expire.

### 5. Checkpoint identity is stable across config changes

```
s3://<lake>/checkpoints/full_cdc/full-cdc-<engine>
```

Outside the warehouse (CLAUDE.md §5.9 — a checkpoint under `warehouse/` is a file an Iceberg
maintenance job will eventually consider an orphan).

Keyed on the **engine**, not on `config_version` and not on a run id. Onboarding a table
changes `config_version`; if that changed the checkpoint, **adding one table would silently
re-ingest every other table's history from `earliest`**. The MERGE on `dv_event_id` makes that
non-destructive, but it is a full re-read that looks like normal operation. The config version
is recorded in state as *evidence* instead.

Nothing deletes a checkpoint automatically. A missing checkpoint is a **refusal**, not a
default.

### 6. `ops.streaming_app_state` is hot state, not an append log

One row per application, updated in place. A resident app heartbeating every trigger would
append **1,440 Iceberg commits a day** carrying nothing but a timestamp, on a table whose only
question is "is it alive and how far has it got". Unpartitioned: a handful of rows does not
need a directory per value.

Provisioned like every other target — a ledger the job creates on demand is one whose schema is
whatever the last version of the job thought it was.

## Options

* **Hardcode resident streaming as production.** Rejected — §1. It makes the lab's cost
  invariant a bug rather than a setting.
* **Keep `--run-seconds` with a smaller default.** Rejected: any default makes the lifecycle a
  property of the submit command. The problem was never the number.
* **Key the checkpoint on `config_version`.** Rejected — §5, it re-ingests everything whenever
  a table is onboarded.
* **Key it on a run id.** Rejected for the same reason, on every restart.
* **A separate batch ingestion job.** Rejected — §2.
* **Append a state row per heartbeat.** Rejected — §6.

## Consequences

* The plan is **schema 7**. A schema-6 consumer finds no `ingestion` block and falls back to
  its own default — which is precisely the inference this ADR removes, so the version moved.
* `ops.streaming_app_state` is a new OPS target; `plan_targets` emits it with the other three.
* Enabling `production` is a one-line registry change **and a cost decision** — Phase J prices
  the always-on figure before anyone flips it.

## Cost

**$0 as shipped**: the registry ships `profile: lab_low_cost`, so the mode stays
`available_now` and nothing runs between ingests. `streaming_app_state` holds one row per app.

The lever this ADR exposes is real: `continuous_microbatch` holds EMR Serverless capacity
continuously. That is the single largest cost change available in this platform, which is why
it is a declared profile rather than a flag, and why it is off.

## Security

No new resource, IAM change or credential. `kafka_offsets` stores topic/partition/offset
coordinates and `source_watermark_ts` a timestamp — **coordinates and counts, never payload**,
the same rule the event index and the run ledgers follow.

## Rollback

Remove the `ingestion:` block: the loader defaults to `lab_low_cost`/`available_now`, which is
the behaviour before this ADR. `stream_job.py` still accepts `--checkpoint` and
`--trigger-seconds` explicitly, so a run can bypass the plan entirely.

## Validation

1. `pytest spark/tests/test_cdc_ingestion.py` — **35 passed** as first written, **61** after ADR-074: profile→mode defaults, explicit
   override, unknown mode/profile refused, trigger read from config, `"1"`/`"1 min"`/bare
   numbers refused, sub-30s refused, checkpoint outside the warehouse, checkpoint stable across
   a config-version change, per-engine separation, path traversal in an app id refused, the
   plan carrying the *resolved* mode, compile-time refusal of a bad block, the state table
   provisioned and unpartitioned, and five assertions on the submit path (no `--run-seconds`
   default, mode from plan, one shared `foreachBatch`, `awaitTermination()` with no timeout,
   missing checkpoint refused).
2. **Not claimed**: no resident streaming run has executed on EMR. The `continuous_microbatch`
   path is `STATIC_PASS` — it parses, it is covered by tests on the submit path, and it has
   never held capacity. Runtime restart/duplicate evidence (§6, §9) is **pending** and is the
   first item of any Phase B follow-up.

### Addendum, 2026-09-17 (ADR-074)

Two claims above were **not yet true** when this ADR was accepted, and an audit against the
Phase B brief found both:

* §5 "checkpoint identity is stable" — `--checkpoint` was still `required=True`, so the
  plan-derived path could never be reached and every run used the typed one.
* §6 "`ops.streaming_app_state` is hot state" — the table was provisioned and nothing wrote
  to it (verified live: 0 rows).

Both are fixed in ADR-074, which also found that a fresh identity-keyed checkpoint with the
old `latest` default would have discarded ~1,600 LOAN events on first start.
