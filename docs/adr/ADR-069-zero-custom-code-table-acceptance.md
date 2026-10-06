# ADR-069 — Zero-custom-code table acceptance, and three defects it exposed

* **Status**: Accepted (implementation: Phase 8; live on AWS account 111122223333)
* **Date**: 2026-09-10
* **Extends**: ADR-062…ADR-068 (granularity, provisioning, routing, REALTIME, EOD,
  onboarding, cutover, operations)
* **Evidence**: `spark/tests/test_eod_engine_spark.py`, `spark/tests/test_cdc_operations.py`,
  `spark/tests/test_cdc_registry.py`, and the live runs cited below

## Context

Phases 1–7 built a config-driven platform and asserted, in tests and ADRs, that a new table
needs no new code. That claim had never been *executed*: every table in the registry
pre-dated the platform, so every code path had been exercised only against tables it was
designed around. An acceptance that reuses the tables the design was fitted to proves the
fit, not the generality.

Phase 8 onboards two genuinely new tables — one per engine — through the shipped commands
only, and treats anything that requires a keystroke outside the registry as a defect.

## Decision

### 1. The acceptance is two tables on two engines, not one

`oracle.coredb.corebank.loan` (JSON payload) and `sqlserver.digital.dbo.payment_method`
(**typed** payload, `micro_timestamp` encoding, `confidential`).

Two engines because SQL Server is what breaks Oracle-shaped assumptions: a hex LSN triplet
where Oracle has a numeric SCN, so a ranking that casts position to decimal yields NULL and
collapses silently onto `kafka_offset`, which CLAUDE.md §5.4 forbids as a comparator.

Two payload modes because they take different code paths through every layer — PK
extraction, ordering, DQ, the EOD grain. An acceptance that only ran the JSON path would
leave the typed one unproven while claiming coverage.

### 2. `scripts/cdc-deploy-code.sh` — the deploy is a command, not a memory

Through Phases 2–7 the `cdc-framework.zip` on S3 was built by hand. The code EMR ran was
whatever the last ad-hoc `zip` happened to include, so a job could fail on a fix that was
made, tested and committed but never uploaded, and nothing would say so.

The plan is **compiled** by that script rather than copied, so the plan on S3 always matches
the registry at the commit being deployed. Every job takes `--plan`; a stale one silently
runs the previous config.

**Three defects surfaced the moment the deploy became repeatable**, each of which had been
papered over by whatever the last hand-built artifact happened to contain:

* **Entrypoints flattened to their basename collided.** Three different jobs are called
  `job.py` (`full_cdc`, `eod`, `l1_stream`); uploading by basename silently overwrites one
  with another, and the deployed artifact is whichever the loop reached last. Names are now
  derived from the path (`full_cdc_job.py`).
* **A shared module was not in the zip.** `full_cdc/job.py` and `stream_job.py` both do a bare
  `import per_table`, and EMR Serverless downloads **only** the entrypoint — the sibling on S3
  is simply absent. The run acquires capacity, then dies with
  `ModuleNotFoundError: No module named 'per_table'`. `SHARED_MODULES` now go into the zip at
  its root, which is how they are imported.
* **The Kafka data source was missing for streaming jobs.** `readStream.format("kafka")` is
  not part of the EMR runtime, so the job died with
  `AnalysisException: Failed to find data source: kafka` — which reads like a typo in the
  format name and is a missing dependency. The five Kafka/MSK-IAM jars are now attached to
  the `stream` **role**, not to every job: an EOD close does not read Kafka and should not pay
  to ship 18 MB of jars it never opens.

### 3. A certification that cannot be recorded is not a certification

**Found live.** The first generic EOD close (`eod-account-0821`, run `00g8loum98plf027`) built
a correct snapshot — 320 rows, 320 distinct keys, matching an independent Athena computation
exactly — passed both gates, and printed:

```
EOD_LEDGER_WRITE_FAILED …ops.eod_run: AnalysisException: [TABLE_OR_VIEW_NOT_FOUND]
EOD_CLOSED … rows=320 deletes=0 dq=PASS recon=PASS status=CERTIFIED
EOD_SUMMARY closed=1 certified=1 rows=320
```

…and exited **0**. `write_ledger` was documented as "best-effort", with a real justification:
a ledger write that fails the run would report a good close as a failure and rebuild data
that is fine.

That justification is right about the **data** and wrong about the **claim**. The ledger row
*is* the completion marker (ADR-065 §6): it is what downstream reads to know a date is
certified, what a rerun reads to know the date was closed at all, and the only record of the
cutoff and position evidence. A run that prints `CERTIFIED` and records nothing has made an
unverifiable claim.

`STATUS_UNVERIFIED = "UNVERIFIED_NO_EVIDENCE"` is added, distinct from both `FAILED` (the
close broke) and a failed gate (the data is suspect). The snapshot stays written and
readable; the certification is withheld and the exit code is non-zero.

### 4. The root cause: `--table` silently skips the shared OPS targets

`plan_targets(..., include_event_index=True)` emits the event index and the two run ledgers
only when **no** `table_ids` filter is given. Correct — they are platform-wide and
re-emitting them per table is noise — but "not provisioned" and "provisioned elsewhere" look
identical from inside a filtered run, and every provisioning run in Phases 2–8 had used
`--table`. So the OPS tables had never been created at all.

The skip stays; the **silence** is the defect. A filtered run now prints
`CDC_PROVISION_NOTE` naming the unfiltered command.

### 5. `maintenance.actions: []` is refused, not obeyed

Carrying `maintenance` into the compiled plan (schema 6) serialised the "not declared"
sentinel `()` as `[]`. `_enabled_actions` reads a declared list as "permit **only** these", so
every table compiled to "permit nothing" and **maintenance became a no-op platform-wide**.
Nothing failed: the job ran, selected its batch, and did nothing — indistinguishable from a
platform with nothing to do.

Two changes, because either alone leaves the hole:

* the plan emits `null`, not `[]` — absence of a choice and a choice of nothing are different
  facts and the serialisation has to keep them different;
* an explicitly empty list is **refused**.

> **Completed later, and the first fix was incomplete.** The refusal above lived only in the
> maintenance *job*, and a systematic re-audit of every Phase 1 §13 rule found it could never
> fire from the registry: `tuple(raw.get("actions") or ())` collapsed *absent* and *declared
> empty* into one value at load, so `actions: []` compiled clean, serialised as `null`, and
> the runtime handed that table the **full default set**. The user asked for no maintenance
> and silently got all of it — the same silent-drop shape as `temperature`, in the same block.
>
> `MaintenancePolicy.actions` is now `None` for absent and a tuple for declared, and the empty
> declaration is refused **at compile time** (§13: fail before runtime). The runtime refusal
> stays as the backstop for a hand-edited plan. `[]` is what a mistake produces and is
  indistinguishable from a deliberate "never maintain this table", for which the platform has
  no setting. Obeying it disables compaction and snapshot expiry on a streaming table.

### 6. Fixtures may not name a real table as their counter-example

Onboarding `loan` broke eleven tests that used `loan` as "a table that does not exist" and
counts of `8` as "every table". Four of them *still passed* while asserting the opposite of
their intent — routing a now-**known** table and checking it was refused.

Counter-examples are now guarded names (`ABSENT_TABLE`, `UNREGISTERED_TOPIC`) with a test
asserting they stay absent, and set membership replaces counts: `SHIPPED_TABLE_IDS` says
*which* tables, so adding one shows up as a line in a diff rather than `8` becoming `9`.

## Options

* **Accept the platform on its existing tables.** Rejected — it proves the fit, not the
  generality, which is the entire claim.
* **One new table, one engine.** Rejected — §1. The Oracle-only ranking defect ADR-065 fixed
  is exactly the class of bug a single-engine acceptance cannot see.
* **Keep `write_ledger` best-effort and monitor for the log line.** Rejected: the run exits 0,
  so the scheduler is green and nobody reads the log. An outcome nothing checks is not a
  control.
* **Fail the whole run on a ledger write failure.** Rejected — it rebuilds correct data. The
  distinction between the data and the claim is the point.
* **Make `--table` also provision the OPS targets.** Rejected — it would re-emit shared
  infrastructure on every per-table run and make a filtered run non-obvious in the other
  direction. Saying what was skipped costs one line.
* **Treat `maintenance.actions: []` as "the default set".** Rejected — it makes a plausible
  deliberate reading unexpressible *and* silently overrides it.

## Consequences

* The compiled plan is **schema 6**. A schema-5 consumer would not carry `maintenance`, so it
  would fall back to inference and ignore an explicit temperature.
* **`spark/jobs/eod/eod_engine.py` can now exit non-zero on a close whose data is correct.**
  This is intended and is a behaviour change for any scheduler that treats non-zero as
  "rebuild": the correct response to `UNVERIFIED_NO_EVIDENCE` is to provision the ledger and
  re-run, which is convergent.
* Onboarding two tables raised the registry from 8 to 10 and the topic count with it. Cost
  impact is one Kafka topic, three Iceberg tables and one nightly close per table.
* `docs/CDC_TABLE_PLATFORM_ARCHITECTURE.md` and eight runbooks are the operator-facing
  surface; `CDC_TABLE_QUICKSTART.md` is the entry point.

## Cost

| item | impact |
|---|---|
| two new tables | 2 topics, 6 Iceberg tables, 2 nightly closes |
| `cdc-deploy-code.sh` | **$0** — S3 PUTs of ~200 KiB |
| the EOD/provisioning runs | bounded EMR Serverless runs, auto-stop on, 25-minute timeout |
| the OPS ledgers | coordinates, counts and outcomes; **no payload** |

Immaterial against the $100/month budget of record. Nothing here is always-on.

## Security

* **No new resource, no IAM change.** Both tables use roles that already read and write these
  layers.
* `payment_method` is classified `confidential` and stores **only** a masked last-four; the
  registry classification propagates into the provisioned `TBLPROPERTIES`.
* The ledgers carry coordinates, counts and outcomes — never payload — so the new
  `UNVERIFIED` path does not widen PII exposure.
* The connector update remains gated behind `confirm_destructive()`, which refuses to run
  non-interactively. Phase 8 did not bypass it.

## Rollback

* Remove the two registry entries and re-compile; the six Iceberg tables can be dropped (they
  hold only what capture produced) and the connector templates re-rendered.
* The legacy monolith grows by the new tables' events while they are in `dual_write`
  (20,390 -> 20,400), which is exactly what that mode means: both paths receive every event
  so the two can be compared. It is never dropped and remains the reconciliation baseline
  (ADR-062).
* The `STATUS_UNVERIFIED` behaviour is additive to an enum; a consumer that does not know the
  value sees a non-`CERTIFIED` status, which is the safe reading.

## Validation

1. `pytest spark/tests airflow/tests -q` — **2,283 passed, 0 failed** (3m54s), including four
   new regressions, each named for the defect it pins:
   `test_a_close_whose_ledger_cannot_be_written_is_not_certified`,
   `test_a_table_that_declares_no_actions_still_gets_the_default_set`,
   `test_an_explicitly_empty_action_list_is_refused_not_obeyed`,
   `test_an_explicit_temperature_survives_compilation`;
   plus three guards against the fixture rot in §6 —
   `test_the_absent_table_really_is_absent`,
   `test_the_unregistered_topic_really_is_unregistered`,
   `test_no_deployed_table_changed_its_window_boundary`.
2. **Live**: `oracle.coredb.corebank.loan` created in Oracle via SSM, precheck BLOCKED until
   declared (correct `declared`-evidence behaviour), all three targets provisioned on EMR.
3. **Live**: `sqlserver.digital.dbo.payment_method` validated 0-blocked and all three targets
   provisioned on EMR (run `00g8lp0dlt18h827`).
4. **Live**: EOD close of `oracle.coredb.corebank.account` for COB 2026-08-21 produced 320
   rows / 320 distinct keys, matching an **independently computed** Athena expectation of 320
   — computed from FULL_CDC below the same `cutoff_utc`, not by the job that wrote it.

**Also not claimed — the orchestration gap.** No Airflow DAG submits any generic entrypoint
(`eod_engine.py`, `realtime_engine.py`, `per_table.py`, `cdc_maintenance_job.py`); a grep over
`airflow/` returns nothing. `dag_eod_pipeline.py` hardcodes three table names, but they are the
**legacy** ones. So "a new table needs no new DAG" holds for a weaker reason than it appears
to: nothing schedules the per-table platform, and every run in this phase was a manual
`emr-submit.sh`. The engines take `--table`, so a DAG mapping over the compiled plan is small
work — it has not been written, and the acceptance would be dishonest to imply otherwise.

**Not claimed**: the live capture leg for either new table. Both connector templates are
rendered and committed, and applying them needs
`bash scripts/register-connectors.sh update --execute`, which requires a human to type the
confirmation phrase. That gate was not bypassed, so no row for either new table has traversed
Debezium → Kafka → FULL_CDC on live infrastructure.
