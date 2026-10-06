# ADR-072 — Downstream consumers follow the per-table cutover

* **Status**: Accepted (implementation: post-Phase-8 correction)
* **Date**: 2026-09-10
* **Extends**: ADR-062 (per-table FULL_CDC), ADR-065 (source-native ordering), ADR-067 (cutover)
* **Evidence**: `spark/realtime/cdc_source.py`,
  `spark/tests/test_realtime_rt.py::TestConsumersFollowTheCutover`

## Context

FULL_CDC became one Iceberg table per source table (ADR-062), and a per-table cutover mode
decides, **per table**, which physical table holds its data (ADR-067). Two consumers never
followed:

```python
# rt_stream_app.py and rt_autocorrect.py, in four places
spark.table("glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events")
     .filter((F.col("source_system") == "oracle")
             & (F.col("source_table") == "CUSTOMER"))
```

Three defects, and every one fails **quietly**.

1. **The monolith is named directly.** Once a table's mode is `PER_TABLE` the ingest stops
   writing it to `cdc_events`. This read then returns the rows that were there before cutover
   and **nothing since** — a dimension that silently stops updating while every job reports
   SUCCESS. `AUTO_CORRECT` is the worse of the two: it exists to fix wrong numbers, so
   re-deriving from a stale source is the one failure it must not have.
2. **`source_table` compared without `upper()`.** That column is written as the last segment
   of the *topic*, so it carries the engine's own spelling — Oracle folds unquoted identifiers
   to uppercase, SQL Server does not. The exact-case literal matched Oracle `CUSTOMER` and
   would match nothing for any SQL Server table. **This exact defect was measured live once
   already**: the legacy benchmark read returned zero rows, with no error, and scored it as
   "0 bytes scanned, very fast" (ADR-067).
3. **Oracle-only ordering that violates CLAUDE.md §5.4.**
   `ORDER BY CAST(position_primary AS DECIMAL(38,0)) DESC, kafka_offset DESC` — a SQL Server
   hex LSN casts to NULL, every row ties, and the ranking collapses onto `kafka_offset`, which
   is monotonic only *within* one partition.

## Decision

### 1. One module knows how to read FULL_CDC for a table

`spark/realtime/cdc_source.py`:

```python
full_cdc_for(spark, "oracle.coredb.corebank.customer")   # already filtered
latest_state_window("oracle", "j.CUSTOMER_ID")           # source-native ranking
```

A consumer names a **canonical table id**, never a physical table, and is handed a DataFrame
that is already correctly scoped. In `LEGACY` mode the resolver returns the monolith **with
its mandatory predicate**, which the helper applies — so a caller *cannot* forget it and read
eight tables' events as one. In `PER_TABLE` mode the table is the filter and no predicate is
needed.

### 2. The ordering is the EOD contract, not a second spelling

`latest_state_window` delegates to `cdc.eod.order_by_clause`. Two spellings of one ordering
contract is how they drift: EOD was corrected in ADR-065 and these consumers kept the old
form, which meant a streaming dimension could disagree with the certified snapshot about
which update was latest. A test asserts the EOD clause is literally contained in the window.

### 3. A missing plan is a refusal, not a default

`full_cdc_for` raises when no compiled plan is present. A consumer that fell back to a
hard-coded name would reintroduce exactly the defect this ADR removes.

The **cutover state** behaves oppositely and deliberately: when absent, `load_state` yields
all-`LEGACY`. An unknown mode must read the monolith, which holds everything, rather than a
per-table target that may never have been cut over.

### 4. The legacy REALTIME entry point refuses one combination

`spark/jobs/realtime/job.py` applies no `source_table` predicate — it predates per-table
FULL_CDC and its source is whatever the caller names. Safe when both sides were the monolith;
**not** safe now. Reading the monolith into a *per-table* REALTIME target writes every table's
events into one table's window and reports success.

It now refuses that cross combination and names `realtime_engine.py --table <id>` instead.
Legacy→legacy and per-table→per-table both stay allowed: a guard that refused too much would
be worked around, and then it would protect nothing. The deployed legacy targets
(`rt_account_stream`, `rt_account_base`) are exempted **by name**, because matching the `rt_`
prefix alone would refuse the configuration that is currently running.

### 5. The legacy EOD job needed no change — verified, not assumed

`spark/jobs/eod/job.py` already **refuses** a non-numeric source position rather than
mis-ranking it, and its own comment explains why. Its source is a `--full-cdc` parameter, so
it can be pointed at either layer. Checked and left alone.

## Options

* **Rewrite the two consumers to read per-table directly.** Rejected — it would break the
  moment a table's mode is anything but `PER_TABLE`, which is 8 of 10 today. The mode is the
  thing to consult, not to assume.
* **Pass the resolved table name in as a job argument.** Rejected: it moves the decision to
  whoever writes the submit command, which is where the original hard-coding came from.
* **Duplicate the ordering expression locally.** Rejected — §2.
* **Make the legacy REALTIME job apply a predicate.** Rejected: it has no table id to build
  one from, and inventing a default would make it quietly serve a different scope than the
  caller asked for. Refusing is honest; guessing is not.

## Consequences

* Both consumers now resolve at run time, so cutting a table over changes what they read with
  no code change — which is the property the cutover was built to have.
* They need `CDC_TABLE_PLAN` (and optionally `CDC_CUTOVER_STATE`) in their environment.
* `spark/realtime/cdc_source.py` imports from `cdc/`, so the framework zip must be on the
  path — it already is for every job that takes `--plan`.
* **Not live-tested.** The two consumers have not been re-run on EMR since the change; what
  is proven is that they resolve correctly against the shipped plan and that the guards fire.

## Cost

**$0.** Same reads, same volumes — the resolver changes *which* table is named, not how much
is scanned. In `PER_TABLE` mode the reads get cheaper for the same reason every other
per-table read did, and no new resource, job or schedule is introduced.

## Security

No new resource, no IAM change, no credential. The helper reads a compiled plan and a
Git-tracked cutover file, and resolves to tables the jobs' existing roles already read. It
cannot widen access: a table absent from the plan is a refusal, not a fallback.

## Rollback

Revert the two consumers to naming the monolith. That restores the prior behaviour including
its defects, and is only correct while every table they read is still in `LEGACY` mode.
`cdc_source.py` is additive and can stay.

## Validation

1. `pytest spark/tests/test_realtime_rt.py` — **34 passed**, including
   `TestConsumersFollowTheCutover` (no consumer names the monolith, none casts position to
   decimal, none tie-breaks on offset across partitions, all resolve through the shared
   reader, the shared ordering *is* the EOD contract, the two engines get opposite treatment,
   and a legacy read still carries its `upper()`-on-both-sides predicate) and
   `TestLegacyRealtimeRefusesTheUnsafeCombination`.
2. The guards were verified to **bite**: reverting `rt_autocorrect.py` to the decimal cast
   fails with `rt_autocorrect.py still casts position to decimal`, and restoring it passes.
