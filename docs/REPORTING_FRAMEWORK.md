# Reporting metadata framework — Phase 3 foundation

- Sessions: 22 (metadata) · 23 (dbt-Spark) · 24 (dependency engine) · 25 (coordinator) · 26 (EOD pilot)
- Date: 2026-08-19
- Status: **METADATA FOUNDATION READY.** Static-validated + unit-tested. **Nothing deployed.**
- Decisions: ADR-033 … ADR-045
- Design: [`REPORTING_TARGET_ARCHITECTURE.md`](REPORTING_TARGET_ARCHITECTURE.md)

Phase 3 built the metadata layer: models, contracts, the config compiler, validation and
tests. Phase 4 added the dbt-Spark datamart framework — macros, the execution wrapper, the
manifest sync and a pilot mart. **No flow executes yet** — no EOD, AUTO_CORRECT, FULFILL, STREAM_BATCH or
STREAMING_RT run, no DAG was written, no dbt model was invoked, no Terraform was applied.

**AWS mutations: none. Cost: $0.00.** The whole surface runs locally: the reporting tests
complete in about one second with no Spark session, no credential and no network.

---

## 1. What exists

```
reporting/                          the only editable config surface
├── layers.yaml                     logical → physical layer map (ADR-033)
├── legacy_field_mapping.yaml       ACB field dispositions — machine-checked
├── profiles/resource_profiles.yaml five named Spark sizings
├── jobs/mart_account_balance_daily.yaml   the pilot registration
├── schema/{job,layers,resource_profiles}.schema.json
└── compile.py                      the CLI

spark/reporting/
├── models.py            8 entities + 20 closed enums; config/position/runtime planes
├── status.py            10 statuses + the transition table
├── graph.py             closure, turns, cycle detection (networkx)
├── config_loader.py     YAML → schema → semantics → models
├── plan.py              deterministic payload, sha256, config_version
├── source_resolver.py   resolve_source_layer(table, flow_mode)
├── runtime_state.py     the repository contract + in-memory implementation
├── dynamodb_state.py    the DynamoDB adapter — CODE ONLY, no table exists
├── watermark.py         the advance rule, in Python and as a ConditionExpression
├── history.py           terminal execution → Iceberg audit row
├── ops_client.py        THE single writer; owns the ordered success path
└── ddl/reporting_ops_tables.sql   6 Iceberg tables — DDL only, not executed
```

## 2. The three planes

| Plane | Entities | Store | Mutability |
|---|---|---|---|
| **CONFIG** | `job_master`, `job_flow_config`, `job_dependency`, `resource_profile` | Git → `plan.json` → Iceberg | immutable per `config_version` |
| **POSITION** | `job_watermark_state`, `summary_config_v1` | DynamoDB | one row per key, framework-owned |
| **RUNTIME** | `job_master_execution_hist`, `streaming_app_state`, `summary_config_hist_v1` | DynamoDB live → Iceberg terminal | append-only audit |

The separation is enforced, not documented. `JobMaster.reject_runtime_keys()` fails the
compile if an authored job carries `date_of_data`, `status`, `spark_app_id`, `started_at`,
`ended_at`, `error_msg`, `turn`, `attempt_number`, `is_rerun`, `estimated_duration`,
`turn_watermark`, `is_skipped`, `non_eod`, `schedule` or `airflow_dag_id`.

## 3. Invariants, and where each is enforced

| Invariant | Enforced by | Test |
|---|---|---|
| A failed run never advances the watermark | `ops_client.finish_successfully` — steps 3/4/5 in one call, no reordering parameter | `test_failed_run_does_not_advance_the_watermark` |
| `SUCCEEDED` only from `VALIDATING` | `status._TRANSITIONS` | `test_succeeded_is_reachable_only_from_validating` |
| `FAILED → SUCCEEDED` is impossible | same table | `test_failed_cannot_become_succeeded` |
| The watermark never moves backwards | `watermark.may_advance` + the DynamoDB `ConditionExpression` | `test_watermark_cannot_move_backwards`, `test_condition_expression_itself_rejects_a_backwards_write` |
| One Iceberg append per completed execution | `history.assert_terminal` | `test_exactly_one_append_per_execution_not_one_per_transition` |
| `turn` ≠ `attempt_number` | separate fields; `graph` takes no attempt parameter | `test_turn_and_attempt_number_are_independent_fields` |
| Cycles never reach runtime | `graph.assert_acyclic`, on the full graph and every per-mode subgraph | `test_cycle_is_rejected_at_compile` |
| `plan.json` is deterministic | sorted keys, fixed separators, `generated_at` outside the hash | `test_two_compiles_of_identical_input_produce_identical_bytes` |
| Position is never a success signal | `watermark.assert_not_used_as_success_signal` | `test_watermark_must_not_be_read_as_a_success_signal` |

## 4. Using it

```bash
make reporting-validate    # schema + semantics; writes nothing
make reporting-compile     # → target/reporting/plan.json (gitignored, reproducible)
make reporting-verify      # recompute the hash and compare
make test-reporting        # 149 tests, ~1s, no Spark, no AWS
```

`make check` now runs `reporting-validate` alongside the doc and shell gates.

## 5. What Phase 4 needs, and what it does not

**Does not need designing** — the contract is settled and tested: the repository interface,
the status machine, the watermark rule, the history projection, the plan format.

**Still to build:** the coordinator, the four flow DAGs, the dbt execution wrapper, the
EOD/AUTO_CORRECT/FULFILL/STREAM_BATCH flows, the STREAMING_RT application, the dbt manifest
sync, and `modules/reporting_ops`.

## 6. Deliberately deferred

| Item | Why | Tracked as |
|---|---|---|
| DynamoDB tables | ADR-036 needs operator sign-off; no table exists | Phase 4 blocker |
| `enable_kafka_platform` discrepancy | tfvars says `true`; the applied tier ran `false`. EMR, IAM and Airflow are all gated on it | Phase 4 blocker — **not touched, as instructed** |
| `dim_date.is_working_day` | EOD/FULFILL date selection needs a working-day calendar; `mart.dim_date` has `is_weekend` only (`kimball.sql:84`) | Phase 4 prerequisite — **not solved, as instructed** |
| REALTIME layer | Does not exist. `layers.yaml` substitutes `full_cdc` with a bounded predicate and flags every read | ADR-033 |
| dbt manifest sync | Contract prepared (`SourceOfDefinition.DBT_MANIFEST`); the sync itself is Phase 4/5 | ADR-037 |

## 7. One approved decision corrected

ADR-042 originally said `partition_spec` may not contain **any** primary-key column. The
compiler rejected the pilot on its first run — and it was right to, under that rule.

`mart_account_balance_daily` has grain `(account_sk, business_date)` and is
`PARTITIONED BY (business_date)` in the repository's own deployed DDL
(`spark/common/ddl/kimball.sql:182`), as does every other daily mart here. The rule as
written rejected the existing design.

The rule was stated over the wrong attribute. What `CLAUDE.md` §6 protects against is a
partition whose cardinality approaches the row count — one file per row. Membership in a
composite PK is not that; being an identifier is. Corrected to:

- any column marked `high_cardinality_columns` → rejected, always;
- a PK column that is **not** `business_date_column` → rejected;
- `business_date_column` → allowed, even inside the PK.

ADR-042 records the original wording alongside the correction rather than replacing it.


---

# Phase 4 — dbt-Spark datamart framework

- Session: 23
- Status: **COMPILE_PASS / RUNTIME_PENDING_INFRA.** `dbt parse` and `dbt compile` run
  locally and produce real SQL; `dbt build` needs EMR Serverless, which does not exist.
- Decisions: ADR-037 (manifest sync), ADR-040/042 (modes, merge), ADR-044 (execution)

## What a new mart now costs

```
1. dbt/models/marts/<mart>.sql          the SQL
2. dbt/models/marts/schema.yml          tests + meta.reporting_job_id
3. reporting/jobs/<mart>.yaml           the job registration
```

No Airflow DAG, no Spark wrapper, no duplicated dependency definition. A `ref()` registers
its own edge.

## ONE model, four modes — the evidence

`make dbt-compile-modes` compiles the pilot under each mode. The filters differ; the
business logic does not:

| Mode | Compiled row filter |
|---|---|
| `EOD` | `business_date = CAST('2026-08-18' AS DATE)` |
| `AUTO_CORRECT` | `business_date > DATE_SUB(CAST('2026-08-18' AS DATE), 3) AND business_date <= …` |
| `FULFILL` | `business_date >= CAST('2026-08-01' AS DATE) AND business_date <= CAST('2026-08-05' AS DATE)` |
| `STREAM_BATCH` | `business_date >= CAST('2026-08-18 06:00:00' …) AND business_date < CAST('2026-08-18 06:10:00' …)` |

Four near-identical SQL files would make a provisional-vs-certified variance ambiguous
between "late data" and "divergent logic" — the argument `docs/FOUR_FLOWS.md` §1 already
makes for the Spark flows.

## Macros

| Macro | Purpose |
|---|---|
| `flow_mode()`, `cob_date()`, `window_*()`, `watermark_before()` | the variable contract, validated; placeholders during parse so local compile works with no runner |
| `resolve_source_layer(table, layer=)` | logical → physical; no mart SQL names a Glue database |
| `layer_is_substituted()`, `layer_bound_predicate()` | REALTIME-on-`full_cdc` stays bounded and visible |
| `incremental_filter(date_col, event_col=)` | the per-mode row filter — the only branch |
| `merge_guard(unique_key)` | the accuracy ladder as a pre-filter |
| `reporting_merge_config(unique_key, partition_by)` | shared materialisation settings |
| `processing_status_for_mode()` | the tier; a model never chooses its own |

**Why `merge_guard` is a pre-filter, not `WHEN MATCHED AND`:** dbt-spark exposes no such
clause. Its `incremental_predicates` land in the ON clause, where a losing row would fall
to NOT MATCHED and be **INSERTED**, duplicating the key. Filtering losing rows out of the
source is equivalent for this ladder and safe on every dbt-spark version.

## Config ownership

Duplication is permitted only where dbt structurally requires the value at parse time —
`unique_key` and `partition_by`. Everything else belongs to `reporting/jobs/*.yaml`. Drift
is prevented, not trusted: `test_model_config_matches_registered_job` fails when they
disagree. The full table is in `dbt/macros/reporting/reporting_mart.sql`.

## Manifest sync

`dbt parse` → `manifest.json` → `depends_on.nodes` → `JobDependency`.

- an **ephemeral** upstream is composition; the walk follows *through* it, so
  `mart_a → stg_x → mart_b` yields `mart_a → mart_b`;
- a **materialised** upstream is a scheduling edge and must be a registered job;
- a `ref()` to a materialised non-job **fails**, naming both fixes.

Reconciliation is asymmetric: inferred-only auto-registers silently, identical de-duplicates,
**conflicting fails**, manual-only survives.

## Execution wrapper — `spark/reporting/run_dbt_job.py`

`dbtRunner` + `method: session`, **`dbt build`** (not `run` — validation is a gate), and
**no in-process retry loop**. Repo 5's `while … number_of_retries < 8` is eight times the
EMR bill for one non-transient failure; retry is orchestration policy. `run_results.json`
is parsed for row metrics, which repo 5 discards. Tested for its argument vector and
failure semantics; **not executed** — no EMR application exists.

## Status

```
COMPILE_PASS            dbt parse, dbt compile, manifest, 4-mode compilation, 27 tests
RUNTIME_PENDING_INFRA   dbt build needs EMR Serverless; no application, no table
```


---

# Phase 5 — config compiler and dependency engine

- Session: 24
- Status: **DEPENDENCY ENGINE READY.** Static-validated + unit-tested. Nothing deployed.
- Decisions: ADR-034 (compile), ADR-037 (dbt sync), ADR-038 (turns)

## What was added

`spark/reporting/dependency_engine.py` and `summary_plan.py`. The primitives already
existed from Phase 3 — `topological_generations`, cycle detection with a readable path,
mode filtering, deterministic hashing — so Phase 5 is the composition plus the two
semantics the primitives did not settle.

## Inactive is not the same as skipped

The brief lists "inactive job" and "skipped job" separately, and they must behave
differently:

| State | Effect | Why |
|---|---|---|
| `job_master.is_active = false` | removed from **every** graph, **and so is anything that REQUIRES it, transitively** | a required edge onto a job that will never run can never be satisfied; scheduling the dependent produces a task that waits forever |
| an **optional** edge onto an inactive job | dependent **survives** | an optional upstream that is gone is an input that will not arrive, not a deadlock |
| `job_flow_config.is_enabled = false` | **bridged**: `A → B → C` with B disabled becomes `A → C` | dropping B outright makes both A and C turn 1, letting C start before A — C's data still ultimately depends on A's |

Repo 1 reaches the first conclusion via a `left anti join` over `after_full`
(`functions.py:106-115`); this states it as a rule. It cannot express the third at all —
its `is_skipped` is a single job-level flag, so pausing EOD also paused the micro-batch.

Bridging **adds** edges, so it can create a cycle the authored graph did not have.
`assert_acyclic` therefore runs *after* pruning, and a test covers exactly that.

## All eight derived legacy fields

`before_direct` / `after_direct` / `before_full` / `after_full` from the unfiltered graph;
the four `*_by_mode` variants as **MAPs keyed by mode**, not four more flat columns. Repo 1
stored each as one `VARCHAR(1000)` (`data_lake_init.sql:33-36`), which can hold only one
mode's answer and therefore silently holds the wrong one for the others.

A job absent from a mode's graph is **absent** from its `*_by_mode` entry, not empty:
"does not participate" and "participates with no dependencies" are different facts.

## Summary plan

`summary_plan.generate()` is pure — it takes `now` rather than reading a clock, so a test
asserts a plan rather than a shape. `execution_date` is derived from
`job_watermark_state`, never stored as mutable state on a config row, which is exactly what
repo 1's `job_master.date_of_data` is and why its scheduler must mutate the config table on
every sync.

`summary_config_v1` carries no field that is only in it; a test regenerates it and asserts
equality. It is a projection, not a source of truth.

## Compiler

`--with-dbt <project>` runs `dbt parse`, merges the `ref()` edges, and **re-validates** —
manifest edges can introduce a cycle the authored set did not have. Every compile now
prints the resolved waves, so a wrong order is visible in CI output rather than only inside
a JSON file.

```bash
make reporting-graph          # print the waves per mode
make reporting-compile-dbt    # compile with manifest sync
```

## One primitive corrected

`build_graph` conflated two conditions: an upstream that is **not registered at all** (a
typo — fatal) and one that is **registered but inactive** (a deliberate operational state).
It raised for both, which turned a routine deactivation into a compile failure and left the
engine unable to apply its own exclusion rules. Now separated; the graph test suite pins
both branches.


---

# Phase 7 — the EOD pilot datamart

- Session: 26
- Status: **EOD FLOW IMPLEMENTED AND TESTED END TO END AGAINST FAKES. Not run for real.**
- Scope: ONE mart — `mart_account_balance_daily`. No other mart migrated.

## The chain

```
approved COB
  → upstream EOD layer readiness   gates.check_eod_ready
  → reporting dependency resolution coordinator.gate
  → runtime execution record        already PLANNED
  → dbt build                       submitter → run_dbt_job.py
  → guarded Iceberg MERGE           merge_guard, inside the model
  → dbt / data validation           Validator
  → final history                   one Iceberg append
  → watermark advance               last, and only on success
```

`run_eod_job()` takes its gate reader, submitter, validator and clock as arguments, so the
whole chain runs in milliseconds against fakes — no Spark, no EMR, no AWS, no Airflow.
That is what makes the flow that matters most testable at all.

## The gate

Three conditions (ADR-040), selected by `eod_gate_mode`:

1. an `ops.eod_watermark` row — already written only after write **and** validation pass;
2. an Iceberg tag `EOD_<date>` — repo 1's best idea (`helpers.py:566-577`). Written in the
   same commit as the data, so it cannot exist for a day that was not written, and it hands
   the reader a snapshot id to pin;
3. every upstream reporting job's watermark has reached the date.

A closed gate leaves the run **non-terminal**, not FAILED: the next run re-evaluates, and
marking it failed would consume an attempt for a condition the job did not cause. Nothing is
submitted — the gate is a cost control as much as a correctness one.

## Three readers, one protocol

| Reader | Where | Status |
|---|---|---|
| `InMemoryLayerStateReader` | tests | used by all 24 |
| `AthenaLayerStateReader` | the Airflow task | code only |
| `SparkLayerStateReader` | inside a Spark job | code only |

**A `SparkSession` in a DAG file was caught by this repo's own guard.** Building one on the
2-vCPU scheduler node is compute on the orchestration node, even for a metadata read. The
Athena reader replaced it: both queries scan kilobytes of metadata, at run time rather than
the parse time ADR-039 rules out.

## What is NOT wired

AUTO_CORRECT, FULFILL and STREAM_BATCH resolve, gate and record, then raise before
submission — a mode that silently no-ops looks exactly like one that succeeded. The
lifecycle around submission is shared, so wiring each is small.

**Superseded for STREAM_BATCH in Phase 10 (Session 29):** it is now wired through
`_run_stream_batch`. AUTO_CORRECT and FULFILL still raise.

## Status

```
IMPLEMENTED_AND_UNIT_TESTED   gate, flow, submission shape, validation, history, watermark
RUNTIME_PENDING_INFRA         no EMR Serverless application, no Iceberg table, no DynamoDB
```

The DAG raises a clear error when `REPORTING_EMR_APPLICATION_ID` is unset rather than
pretending to run: a "successful" run that submitted nothing would advance a watermark for
data that was never written.


---

# Phase 7 addendum — the dim_date working-day gap

- Session: 26
- Closes the prerequisite carried open since Phase 3.

## What was missing

`mart.dim_date` had `is_weekend` but no `is_working_day`, so EOD and FULFILL could not pick
business dates. A weekday public holiday was labelled a business day, and the framework
would then wait for a close that never comes — an outage-shaped symptom with a calendar
cause. Repo 1 hit this and fixed it twice (`bcn_pipeline.yaml:52-68`, replacing a fixed
`run_date - N` offset with a `dim_times` lookup).

## Resolved by extending the existing dimension

`build_dim_date` already refused to invent holidays — *"a data feed, not a derivation;
inventing them here would silently mislabel business days"*. That was right, so the fix
supplies the feed rather than changing the rule:

```
reporting/calendar/holidays.yaml   the feed (jurisdiction, UTC, dated names)
        ↓
build_dim_date(spark, start, end, holidays=…)
        ↓
mart.dim_date + is_holiday, holiday_name, is_working_day
```

**No second calendar framework.** `spark/reporting/business_calendar.py` is the same rule in
pure Python so the coordinator can answer without a Spark session, and
`working_days_agree()` pins the two against each other — the pairing
`spark/common/flows.py:127/150` established, because a rule in two forms will otherwise hold
in only one.

Backwards compatible: `holidays` defaults to empty, so an existing caller behaves exactly as
before.

## Wired into the flow

- `summary_plan._next_business_date` advances to the next **working** day, not `+1 day`.
- `run_eod_job` **skips** a non-working COB rather than polling a gate that can never open —
  `SKIPPED`, not `FAILED`, because there was nothing to build.

## Evidence

`artifacts/validation/session-26/eod-pilot-run.json` — one pilot run, before and after.


---

# Phase 8 — AUTO_CORRECT

- Session: 27
- Status: **IMPLEMENTED AND UNIT-TESTED. Not run for real.**

## The finding: certified dates must be escalated, not corrected

AUTO_CORRECT stamps `PROVISIONAL_CORRECTED` (rank 2). The accuracy ladder will not let that
overwrite `CERTIFIED` (rank 4) — which is correct and is the entire point of the ladder.

But it means a correction for a date EOD has already certified would be **computed, billed,
and then silently discarded by the MERGE**. The run would report success and change nothing.

So affected dates are partitioned:

| | |
|---|---|
| **correctable** | not yet certified → AUTO_CORRECT handles it |
| **escalated** | already certified → recorded and surfaced; needs an EOD rerun or FULFILL, which stamp CERTIFIED and *can* replace it |

`allow_certified_repair` overrides it per job and defaults to `false`. Nothing is submitted
for an escalated date — `test_a_certified_date_produces_no_submission` pins that.

This was not visible until the two modes met.

## Impact analysis

```
new/late CDC → affected keys → affected dates → escalation
             → downstream marts (after_full) → bounded recomputation → guarded MERGE
```

Derived is the default; the full lookback window is the **recorded fallback**, taken when
the change feed cannot be read. Returning an empty affected set there would look like
"nothing changed" and skip a correction that was needed. `impact_analysis_path` says which
ran, because a silent fallback looks like a slow job rather than a missing capability.

## Config — no global 3-day assumption

| Field | Meaning |
|---|---|
| `lookback_days` | the correctable window. Per job; zero is refused |
| `late_arrival_days` | widens the READ window without widening policy |
| `correction_source` | `FULL_CDC` (canonical) or `REALTIME` (cheaper, retention-bound) |
| `affected_date_policy` | `CHANGED_DATES_ONLY` or `FULL_LOOKBACK` |
| `affected_key_strategy` | `PRIMARY_KEY` or `ALL_KEYS_IN_DATE` |
| `force_repair` | operator override; never a schedule |
| `allow_certified_repair` | off by default — see above |

## Evidence

`artifacts/validation/session-27/auto-correct-impact.json` — eight scenarios, one row each.


---

# Phase 9 — FULFILL

- Session: 28
- Status: **IMPLEMENTED AND UNIT-TESTED. Not run for real.**

No reference implementation has this. Repo 1's `load_hist` and repo 2's `_fulfilled` both
require an engineer to pick the dates by hand (discovery finding F5) — which is how a gap is
missed: the date nobody noticed is exactly the one nobody names.

## Four categories, not two

| | |
|---|---|
| `MISSING` | expected, never ran |
| `FAILED` | ran, terminal, not SUCCEEDED |
| **`INCOMPLETE`** | **SUCCEEDED but the target holds no rows** |
| `ALREADY_SUCCESSFUL` | skipped unless `force` |
| `DEPENDENCY_UNAVAILABLE` | no source can rebuild it — named, not omitted |

`INCOMPLETE` is the category a naive detector omits and the one that matters most: a run
that succeeded and wrote nothing is invisible to execution history alone. It needs a
separate `TargetInspector`, because history says a run succeeded and the inspector says the
data is there — and they disagree exactly here.

## Source per DATE, never silent

```
closed EOD  → EOD, snapshot-pinned      → stamps CERTIFIED
otherwise   → FULL_CDC at a cutoff      → stamps PROVISIONAL_CORRECTED
```

Every planned date carries `source` and `source_reason`. The tier follows the source so a
reconstruction cannot outrank a real close.

## Dry run is the default

`dry_run=True` by default and `execute=True` must be passed. FULFILL has the widest blast
radius in the framework, and a backfill that starts before anyone has read the plan is the
expensive mistake in this mode. A test asserts a dry run creates no execution record and no
submission even when `execute=True` is passed alongside it.

## Resumable

One execution record per date, not one per range. A partial failure halts (continuing widens
the hole while making it harder to see), reports `remaining`, and a resume re-plans — finding
the completed dates already successful and costing only the remainder.

**One collision found:** the coordinator already registers an execution for the run's own
business date, so FULFILL's per-date id collides by construction on the first date. A
PLANNED record is now adopted rather than duplicated; a terminal one is reported and left
alone, because overwriting it would erase that outcome.

## Evidence

`artifacts/validation/session-28/fulfill-plan.txt` — a five-date range with all four
categories and mixed sources.


---

# Phase 10 — STREAM_BATCH

- Session: 29
- Status: **IMPLEMENTED, UNIT-TESTED, AND WIRED TO AIRFLOW. Not run for real.**
- Scope: the same pilot mart. No new mart, no new DAG file per mart.

STREAM_BATCH is an Airflow-scheduled **finite** micro-batch. It starts, reads a bounded
delta from its watermark, writes, and exits. The long-running application is STREAMING_RT
(Phase 11, flag-off) and shares no name, pool, prefix or checkpoint with this — repo 1
called its micro-batch `stream` and repo 3 called its long-running app the same, which is
how an operator kills the wrong thing (ADR-041).

## The chain

```
Airflow schedule (*/10)
  → refuse to start alongside a live run   assert_no_overlap        single-writer
  → FREEZE the upper bound                 once, before any read
  → resolve the window                     [watermark − overlap, frozen)
  → resolve + RECORD the source layer      REALTIME, and whether it is a stand-in
  → bounded read                           in Spark, against the same frozen window
  → transform                              the same model file the other three modes run
  → guarded Iceberg MERGE                  merge_guard, stamping PROVISIONAL_NRT
  → validate                               dbt tests + ops.dq_result
  → advance the watermark                  to the FROZEN bound, conditionally
  → SUCCEEDED                              only now
```

## Why the upper bound is frozen

`frozen_upper` is captured once, before any read, and the same instant is used by the read,
the dbt vars and the watermark commit. Evaluating `now()` per step would let an event that
arrives mid-run fall inside the transform's window but outside the committed watermark — so
the next run, starting from that watermark, would skip it permanently. The freeze is what
makes the window a fact rather than a moving target.

The window is half-open, `[start, end)`: a closed upper bound puts a boundary event in two
windows and double-counts it (`DATA_CONTRACTS` §6.1).

The lower bound subtracts `safety_overlap_minutes`, mandatory and non-zero (2 for this job).
An event written microseconds before the recorded watermark otherwise falls through the gap
between two runs and is never picked up. This repository already applies the same rewind at
`spark/common/flow_runner.py:62`; repo 2 independently chose 3 minutes. It is safe only
because the write is an idempotent MERGE on the business key, and compile fails at zero.

A cold start reads a bounded 24 hours, not the whole history: a first run that scans
everything produces a "micro-batch" that takes hours and a watermark that skips the gap it
could not finish.

## The source, and the substitution that was not being recorded

`source_layer_policy: REALTIME`. FULL_CDC appears in this mode only through
`reconcile_against_full_cdc()`, which names keys FULL_CDC saw that REALTIME did not — a
correctness signal AUTO_CORRECT then picks up, never a second read path into the MERGE. A
micro-batch that scanned the canonical history every ten minutes would pay for the whole of
it to produce ten minutes of rows, and `flow_runner.py:12-18` already records that frequency
dominates cost here.

**A gap found and closed in this session.** `reporting/layers.yaml` states that REALTIME is
`SUBSTITUTED` onto the `full_cdc` database until the real layer exists, and that *every
execution that reads it records `source_layer_substituted=true`*. The field existed on
`ExecutionRecord`, the DynamoDB adapter read it back — and **nothing ever wrote it**. The
promise was documented, enforced nowhere, and would have been discovered the day someone
asked which rows predated the real layer, by which time the answer would be unrecoverable.

`record_source_layer()` now writes it, before the read, so a run that dies mid-read still
says which layer it was pointed at. A `SUBSTITUTED` binding with no `bounded_predicate` is
refused outright: an unbounded stand-in would scan the whole of full_cdc and call the result
a ten-minute window.

AUTO_CORRECT also touches REALTIME (`EOD_PLUS_REALTIME`) and does **not** yet record the
flag — Phase 8 work, listed under open items rather than folded in silently.

## A silent defect in the compiled SQL, found and fixed

The macro test proved the SQL was *written* a certain way. It could not prove the predicate
*selects* anything, and that is exactly where the mode failed:

```sql
-- before
WHERE business_date >= CAST('2026-08-18 06:00:00' AS TIMESTAMP)
  AND business_date <  CAST('2026-08-18 06:10:00' AS TIMESTAMP)
```

`business_date` is a `DATE`. Spark promotes it to midnight, so `00:00:00 >= 06:00:00` is
false for every row on the window's own date. The model selected **nothing, every run**,
succeeded, and advanced the watermark past data it had never written. Nothing raised: the
flow's quiet-window path treats zero rows as a legitimate outcome — which it is — and could
not tell the two cases apart.

The macro's no-event-column branch now projects the window onto the business dates it
overlaps and recomputes them in full:

```sql
-- after
WHERE business_date >= DATE(CAST('2026-08-18 06:00:00' AS TIMESTAMP))
  AND business_date <= DATE(CAST('2026-08-18 06:10:00' AS TIMESTAMP))
```

A whole date is a superset of the window. That is more work than an event-time filter and is
safe for one reason only: the write is an idempotent guarded MERGE on the business key, so
recomputing a date converges instead of double-counting. A model that *has* an event column
passes it and takes the precise half-open branch; this pilot's source
(`fact_account_daily_snapshot`) has no event timestamp, so it takes the date projection.

`spark/tests/test_reporting_stream_batch_sql.py` pins both predicates **by evaluating them
in real Spark**, including the defective one, so the bug cannot return unnoticed.

## Overlap is prevented, not tolerated

Two concurrent runs of one job would read overlapping windows and race to advance the same
watermark. The conditional write stops the position moving backwards; it cannot stop two
runs doing the same work twice and billing for it. So `assert_no_overlap` refuses to start
while another run of the same `(job_id, flow_mode)` is non-terminal — checked before
anything is submitted, and the refused run is **SKIPPED, not FAILED**: it was never
eligible, and failing it would burn a retry and make an ordinary schedule collision look
like a defect.

Airflow's `max_active_runs=1` says the same thing and is not enough on its own: a manual
trigger, a retry of a stuck task, or a worker that died leaving the record non-terminal all
get past it. No second lock system was introduced — the runtime-state records are the lock.

## Watermark

| | |
|---|---|
| Advance | to the **frozen** upper bound, never `now()` and never the newest event seen |
| When | after transform + target commit + validation, in that order, or not at all |
| Failure | the position does not move; the next run re-reads the same span |
| Stale retry | rejected by the conditional write on `last_success_execution_id` and monotonicity |
| Quiet window | advances — there is nothing to reprocess, and holding position would re-read a genuinely empty span forever |

`next_watermark()` returns `window.end` and nothing else. Using `now()` would advance past
events that arrived after the freeze and were never read; using the newest event seen would
never advance at all in a quiet window, so a job with no traffic would re-read the same span
forever.

## Airflow

`airflow/dags/datamart_stream_batch.py` is 21 lines and contains no job: the DAG is built by
`build_flow_dag("STREAM_BATCH", …)`, and registering a new mart for this mode is a YAML edit
with no DAG file at all. `_run_stream_batch` in `reporting_common.py` is the adapter — it
freezes the upper bound in the Airflow task, reads `safety_overlap_minutes` from the
compiled plan rather than a default, and refuses to run when `REPORTING_EMR_APPLICATION_ID`
is unset.

The production reader is `EngineSideBoundedReader`, which reads **nothing** in the Airflow
task: the bounded read happens inside the dbt/Spark job, against the same window this task
froze, and the model's `incremental_filter()` applies the identical predicate. Pulling
change rows through the scheduler node is what `airflow/tests/test_dags.py` forbids, and it
forbids it correctly. The trade is that the flow can no longer tell a quiet window from a
busy one before submitting, so it always submits — a few seconds of EMR Serverless against
an empty window, rather than guessing "probably empty" and advancing past real data.

## Evidence

| | |
|---|---|
| `artifacts/validation/session-29/stream-batch-run.json` | the six-step ladder: cold start, busy window, quiet window, engine failure, retry, overlap refusal — with the watermark before and after each |
| `artifacts/validation/session-29/stream-batch-compiled-sql.txt` | the four modes' compiled predicates, and the defect above |
| `make reporting-stream-batch-demo` | regenerates the trace; no AWS, $0 |

Six invariants are asserted by the demo itself and fail it if broken: a failed run did not
advance the watermark; the retry kept the same turn; the retry was a new attempt; the
overlapping run was SKIPPED not FAILED; every run recorded the substituted layer; the cold
start was bounded.

## Tests

```
45  spark/tests/test_reporting_stream_batch{,_sql}.py
    window freezing · half-open bounds · zero overlap refused · bounded cold start
    insert · update · delete · multiple updates per key · duplicate · out-of-order
    quiet window · failed read · failed engine write · failed validation · retry
    same-window rerun convergence · watermark recovery · overlap prevention (4)
    source layer recorded · unbounded substitute refused · missing binding
    the compiled predicate, evaluated in real Spark (5)
 5  airflow/tests/test_dags.py::TestStreamBatchAdapter
388 the reporting suite as a whole
821 spark/tests + airflow/tests
```

## Status

```
IMPLEMENTED_AND_UNIT_TESTED   window, watermark, overlap, dedup, MERGE shape, validation,
                              history, the Airflow adapter
LOCAL_PASS                    821 tests, dbt compile for all four modes, config compile
RUNTIME_PENDING_INFRA         no EMR Serverless application, no DynamoDB table, no Airflow
                              deployment, and no REALTIME layer — it is substituted
```


---

# Phase 11 — STREAMING_RT

- Session: 30
- Status: **IMPLEMENTED, UNIT-TESTED, AND SHIPPED OFF.** Never run for real.
- Scope: the same pilot mart, `is_enabled: false`, `enable_streaming_rt=false`.

## The source decision, reconfirmed and not reopened

ADR-041 chose **`FULL_CDC_APPEND`** as the default: an Iceberg incremental read of the
canonical layer, so the lakehouse stays the single read path and the stream's output can
always be reconciled against the truth it came from. That is what the pilot config declares
and what the application resolves.

`KAFKA_DIRECT` remains permitted and is **not** selected. It is expressible, and the two
obligations ADR-041 attaches to it are now enforced rather than documented:

| Obligation | Enforced by |
|---|---|
| a written `source_justification` | `JobFlowConfig.__post_init__` — compile fails without it |
| a reconciliation job against FULL_CDC | `resolve_stream_source()` — raises without it |

The second had nowhere to live before this phase. FULL_CDC remains canonical under either
choice; Kafka-direct is a faster path to the same truth, never a second truth.

## Application model

Long-running Spark Structured Streaming with its own trigger loop. Airflow deploys, starts,
monitors and stops it — three DAGs, no per-batch task:

```
streaming_rt_start    window open (01:00 UTC, Mon-Sat)   submit; report the checkpoint
streaming_rt_monitor  */5                                liveness from the mirror
streaming_rt_stop     window close (13:00 UTC)           stop; the checkpoint stays
```

A DAG that fired one task per streaming batch would be a micro-batch architecture wearing a
streaming name, and this repository already has that architecture under its own name.

The window is the cost control. This workload bills for as long as it is up, which makes it
the largest cost lever in the framework — hence `enable_streaming_rt=false`, a bounded
12-hour window, `is_paused_upon_creation=True`, and a second flag (`is_enabled: false`) in
the job config. Turning it on is two deliberate acts.

## Checkpoint

```
s3://<lake>/checkpoints/reporting/<environment>/<job_id>/<flow_mode>/
```

Derived by the compiler, never authored — an authored path is one typo away from two
applications sharing state. Top-level `checkpoints/`, a sibling of `warehouse/`, because
Iceberg's `remove_orphan_files` walks a table's location and would delete a checkpoint
stored under it as unreferenced.

**Nothing on the start path can delete a checkpoint.** `CheckpointInspector` has no delete
method and a test asserts it never grows one. The start path *inspects* and *reports* —
`EMPTY` or `RESUMING_FROM_BATCH_41` — into `ops.streaming_app_state`. A checkpoint that
cannot be read raises rather than being assumed empty: assuming empty silently skips every
event between the real offset and now.

Deletion is `scripts/streaming-reset.sh`: defaults to dry run, refuses non-interactively,
requires a typed confirmation phrase, **moves** the checkpoint to a timestamped backup
rather than deleting it, and appends an audited `RECOVERY` row. Teardown order is fixed —
stop the application, move the checkpoint, only then touch the target tables — because
reversed, a still-running stream resumes into a table that has been replaced.

The reference implementation reached the same conclusion independently and refuses to
auto-delete for the same reason: it warns, prints the manual command, and leaves the
decision to a person.

## The failure contract, which is the whole design

With `foreachBatch`, a handler that catches an exception and returns quietly tells Spark the
batch **succeeded**. Spark commits the offsets and the rows are gone — not in the target,
not in a queue, nowhere, with no error at a level anyone reads.

This is not hypothetical. The reference implementation records the incident in its own
source: a batch died on a broadcast timeout, returned quietly with `output_rows=0`, and
~100K offsets were committed and lost. Its correction pass could not recover them, because
those rows had never been written anywhere for a reconciler to find. That codebase's
mitigation is to leave a marker in an audit table before dropping the batch.

**ADR-041 takes the stronger option and this implementation raises.** Spark then does not
commit the offsets, retries the batch, and the data is still in the source. An audit marker
recovers a mistake; raising prevents it.

## Sink idempotency — two problems, two mechanisms

| Problem | Mechanism |
|---|---|
| a **replayed batch** after a crash between write and offset commit | a committed-batch ledger; the re-delivered batch is a no-op |
| a **duplicate or reordered event inside** a batch | MERGE on the business key, highest `event_order` wins |

A ledger alone lets the second through — it cannot see inside a batch. A MERGE alone
re-applies a whole batch's effects on replay, harmless for last-write-wins columns and wrong
for anything accumulated. The ledger is read from the target, not from process memory: an
in-process set is empty after a restart, which is exactly when replay detection matters.

**A defect found by its own test.** The first version dropped the row on a delete, so a
later-arriving but *older* update found nothing to lose to and resurrected the key. Deletes
are now soft — the tombstone keeps the key's `event_order` — which is what the job's
`delete_strategy: SOFT` already specified. The same fix was applied to the real
`MERGE INTO`, which had the identical flaw.

## State

`ops.streaming_app_state`, one row per **deployment**, is a MIRROR: allowed to lag, never
read to decide where to resume from. The checkpoint is authoritative for offsets.

Progress is written **after** the sink commits, so the mirror is behind the truth rather
than ahead of it — the safe direction for a mirror to be wrong in.

**Two counters, deliberately separate.** A failed batch raises and Spark retries it inside
the same query: the application did **not** restart, so it records an error and leaves
`restart_count` alone. Only an application restart increments it. Conflating them makes "one
bad batch" and "the process keeps dying" look identical, which is the one distinction the
counter exists to make. This required a new `record_streaming_error()` verb through the
repository contract and both adapters.

## Schema evolution

| Change | Verdict | Why |
|---|---|---|
| new nullable column | compatible | ordinary upstream evolution; stopping for it makes every schema change an outage |
| lossless widening (`int`→`long`, `date`→`timestamp`) | compatible | nothing is lost |
| dropped column | **refused** | the target still has it; the batch would write NULL over every row in the window |
| narrowed type | **refused** | Spark casts rather than failing, so the corruption lands in the data and not in the logs |
| key column type change | **refused** | the MERGE key stops matching and every event inserts a duplicate instead of updating |

A refusal raises before the write, so nothing partial is committed.

## Liveness

A stalled stream is the failure that does not announce itself: the process is up, the driver
is healthy, no exception is thrown, and nothing has moved for an hour. Only elapsed time
since the last committed batch shows it. The monitor's timeout is several trigger intervals
(300 s default) because a single slow batch is not an outage, and an alert that fires on one
is an alert people turn off. A cold start that has not yet produced a batch is `STARTING`,
not stale, until it exceeds the same timeout.

## Evidence

| | |
|---|---|
| `artifacts/validation/session-30/streaming-rt-lifecycle.json` | the ten-step lifecycle with twelve asserted invariants |
| `make reporting-streaming-rt-demo` | regenerates it; no AWS, no Spark, no Kafka, $0 |

## Tests

```
42  spark/tests/test_reporting_streaming_rt.py
    source decision (5) · identity and disjointness from STREAM_BATCH (3)
    checkpoint: derived path, warehouse refusal, no delete method, reported not assumed (6)
    initial start · controlled restart · crash restart · checkpoint recovery
    source interruption · sink interruption · retry · replayed batch · duplicate event
    out-of-order event · delete then late update · compatible and invalid schema (7)
    restart_count semantics (2) · progress mirror · monotonicity · liveness (5)
863 spark/tests + airflow/tests
```

## Status

```
IMPLEMENTED_AND_UNIT_TESTED   lifecycle, checkpoint policy, sink idempotency, schema
                              gate, failure contract, liveness, the three Airflow DAGs
LOCAL_PASS                    863 tests, config compile, validate-docs 14/14
RUNTIME_PENDING_INFRA         never submitted: no EMR Serverless application, no DynamoDB,
                              no Airflow deployment, no Kafka, and the flag is off
```

The application has never processed a real event. Everything above is proven against fakes,
which is the right proof for a failure contract and no proof at all of throughput, latency
or cost.
