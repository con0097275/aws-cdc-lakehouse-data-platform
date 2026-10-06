# ADR-076 — The EOD control plane: current state, attempt history, and readiness

* **Status**: Accepted (implementation: Phase D)
* **Date**: 2026-09-20
* **Extends**: ADR-065 (config-driven EOD close), ADR-046 (rerun attempt semantics),
  ADR-073/074 (streaming state), ADR-075 (cadence and resource profile)
* **Evidence**: `cdc/eod.py`, `cdc/provision.py`, `spark/jobs/eod/eod_engine.py`,
  `spark/tests/test_eod_control_plane.py`, `airflow/tests/test_dags.py`

## Context

The EOD **builder** was already complete and covered by 124 tests: one generic application
taking `--table` and `--cob-date`, `resolve_cutoff` producing a half-open, DST-safe boundary
with no `23:59:59.999`, source-native ordering (Oracle SCN, SQL Server hex LSN) that
**refuses** rather than falling back to `kafka_offset`, delete policies, snapshot modes, DQ,
and certification withheld when validation fails. 15 of the brief's 18 test cases passed
before this phase began.

What did not exist was the control plane around it.

1. **`ops.eod_run` was doing two jobs.** It is an append log of every attempt. Asking it
   "what is the current certified state of this table for this date" meant "the latest row,
   by some ordering, that happens to be CERTIFIED" — a convention no column enforced and
   every reader had to re-implement.
2. **No attempt history with attempt numbers.** "This close has failed nine times today" was
   not answerable.
3. **No readiness.** A close fired by cron at 01:00 read whatever FULL_CDC happened to hold.
   Clock time cannot certify that the day's data arrived.
4. **No commit-then-crash handling.** A target could commit while the process died before
   recording success, leaving data no control row claimed.
5. **`eod.schedule` was inert** — the same defect Phase C fixed for REALTIME. The registry
   already declared `eod: {schedule: "30 2 * * *"}` for `loan`, and every table was closed on
   `CDC_EOD_CRON` regardless.

## Decision

### 1. `ops.eod_info` — current state, one row per `(table_id, cob_date)`

MERGEd in place on the logical key, carrying the **watermark pair**: `prev_watermark_ts`
(what the close moved *from*) and `watermark_ts` (what it moved *to*). A cutoff says where
the close looked; the pair says what it actually advanced. Unpartitioned — one row per table
per business date is thousands a year.

### 2. `ops.eod_run_hist` — one row per attempt, appended

`attempt` is counted from **history**, not from the process: an in-process counter calls
every retry attempt 1 forever. A failed attempt is never overwritten by its retry, and each
row carries the Spark application id and config version that find its log. Partitioned by
`days(cob_date)`, because history grows without bound and is queried per business date.

### 3. Readiness: two signals, either sufficient

Positive evidence that the day is complete, never a timeout:

* the **table's own** max `source_commit_ts` is at or past the cutoff, **or**
* the **platform's** ingest watermark (`ops.streaming_app_state`) is.

Requiring the first alone was the obvious rule and it is wrong: `channel` holds four rows and
may not change for weeks, so every reference table would report `LATE_SOURCE` every day. The
platform watermark answers the same question one level up — the ingest consumed past the
cutoff, so a table with nothing after it genuinely had nothing.

A failed or stale ingest, or unhealthy capture, blocks regardless of watermark: a quiet
source and a broken one are indistinguishable from the watermark alone.

Verdicts: `WAITING_SOURCE` before the SLA deadline (retry), `LATE_SOURCE` after it (a failure
distinct from a build error — nothing is wrong with the job). Neither builds anything and
neither advances a watermark.

`--skip-readiness` exists for a **historical rebuild**, where the source moved past the
cutoff long ago and the gate has nothing left to protect.

### 4. The watermark advances only after certification

`eod_info` is written **only** when the close reaches `CERTIFIED`. A DQ failure, a
reconciliation failure, an empty window or a crash all leave the previous state intact. If
the `eod_info` write itself fails, the status degrades to `UNVERIFIED_NO_EVIDENCE` — a
certification that cannot be recorded where downstream reads is not a certification.

### 5. Commit-then-crash is detected and reported, not silently re-run

A last attempt that never reached success, against a target whose current snapshot differs
from the one that attempt recorded, means data landed that no control row claims. The retry
**reports the orphan** and proceeds: the write overwrites the COB partition atomically, so
re-running from the same inputs produces the same snapshot. What must not happen is a human
comparing an EOD table against a control plane that never heard of the rows in it.

### 6. Legacy names live in a VIEW, applied through Athena

`pre_datelastmaint` / `datelastmaint` map onto the pair. They are **not** columns on
`eod_info`: "maintenance date" says nothing about whether it is the date a close moved from
or to, and making them canonical would bake that ambiguity into the control table. The view
is created in **Athena** rather than by the Spark provisioner, because Iceberg view support
is catalog-dependent — the Hadoop catalog the tests use refuses outright. `make
cdc-eod-legacy-view` prints the DDL.

### 7. Cadence and sizing, like REALTIME

`eod.schedule` is a validated cron (default `0 1 * * *`, closing yesterday), and EOD builds
**one DAG per cadence**: `cdc_eod` for the default nine tables and `cdc_eod_0230` for `loan`.
`resource_profile` and `source_sla_minutes` join the policy. Plan schema **10**.

## Options

* **Keep everything in `eod_run`.** Rejected — §1.
* **Count attempts in-process.** Rejected — §2.
* **Certify on a timeout alone.** Rejected: that is precisely "clock time certifies".
* **Require the table's own post-cutoff event.** Rejected — §3; it condemns every quiet
  reference table to `LATE_SOURCE`.
* **Skip the build when an orphan commit is found.** Rejected: the inputs and the write are
  deterministic, so re-running is safer than reasoning about what the dead process did.
* **`pre_datelastmaint`/`datelastmaint` as real columns.** Rejected — §6.
* **Provision the view through Spark.** Rejected: catalog-dependent, and it broke outright on
  the test catalog.

## Consequences

* A close now **requires** the control tables. `provision_cdc_tables.py` creates them; an
  older plan is refused at close time with a message naming the fix. Existing test fixtures
  were updated to provision them, because that is the configuration production runs.
* Builder-focused tests pass `skip_readiness=True`, which is the same path a historical
  rebuild takes.
* `loan` moves to its own DAG (`cdc_eod_0230`), honouring a registry declaration that had
  been inert since it was written.
* Plan schema **10**; a schema-9 consumer has one append ledger and no way to ask what the
  current certified state of a business date is.

## Cost

**$0 added.** Two OPS tables holding one row per table per COB and one per attempt; every
EOD DAG remains paused and flag-gated. The readiness gate *reduces* cost: a run that would
have built a snapshot from incomplete data now exits before acquiring capacity for the build.

## Security

No IAM, network or credential change. The control plane stores coordinates, counts and
statuses — never payload. `source_position_json` carries a max SCN/LSN, which is a position,
not data.

## Rollback

`skip_readiness=True` restores the pre-Phase-D close behaviour. Reverting the engine leaves
`eod_run` as it was — `eod_info` and `eod_run_hist` are additive and nothing else reads them
yet. Removing `eod.schedule` from the registry collapses EOD back to one DAG.

## Validation

1. `spark/tests/test_eod_control_plane.py` — **22 passed** on real Iceberg: readiness
   (waiting, late past SLA, post-cutoff event certifies, stalled ingest blocks, unhealthy
   capture blocks), `eod_info` (one row, MERGE not append, the watermark pair, two dates,
   no advance without certification), history (attempt numbering from history, failures
   retained, evidence to find the log), commit-then-crash (orphan detected, no double write,
   new attempt recorded), the legacy projection, and provisioning shape.
2. `airflow/tests/test_dags.py::TestEodCadenceComesFromConfig` — **5 passed**: `loan`'s
   declared cadence produces its own DAG, a DAG per cadence rather than per table, the
   canonical id belongs to the config default, all paused and bounded.
3. The brief's §12 matrix: 15 of 18 cases were already covered (Oracle and SQL Server
   ordering, single and composite PK, multiple updates, same timestamp, cross-partition
   offsets, delete, delete/recreate, late event, Asia/Ho_Chi_Minh cutoff, generic timezone
   boundary, same-COB rerun, DQ fail, reconciliation fail, historical rebuild). The three
   that were not — **source not ready**, **source late beyond SLA**, **commit-then-crash** —
   are covered above.
4. `make check-all` exit 0 — **2,551 passed**, 14/14 static, shellcheck clean, plan not
   stale.
5. **A latent bug this phase found and fixed**: PySpark converts stored timestamps to Python
   using the **machine's** zone, not the Spark session zone. Treating those naive datetimes
   as UTC when writing them back stored every watermark seven hours ahead of the events it
   described on this UTC+7 host. Caught by
   `test_the_watermark_PAIR_says_what_the_close_moved`.
6. **Not claimed**: no close has run against the live catalog (which holds 0 tables), and no
   EOD DAG has executed on the deployed Airflow.
