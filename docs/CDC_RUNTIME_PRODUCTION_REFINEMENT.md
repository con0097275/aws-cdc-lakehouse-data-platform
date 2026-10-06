# CDC runtime production refinement — Phase A audit

**Status**: design only. **No code changed, no AWS mutated, no connector touched.**
**Date**: 2026-09-17 · account 111122223333 · `ap-southeast-1` · env `dev`
**Measured against the live platform**, not read off the source tree: every number below has
a query or a command behind it.

---

## 1. Current Kafka → FULL_CDC implementation

Two entry points exist, and **the one in use is not the streaming one**.

| file | mode | used today |
|---|---|---|
| `spark/jobs/full_cdc/job.py` | **batch** — `spark.read.format("kafka")`, `startingOffsets=earliest` | **yes** — every ingest this session |
| `spark/jobs/full_cdc/stream_job.py` | Structured Streaming — `readStream` + `processingTime` | no |

Both share `per_table.py` for the routed write, so the row contract is identical either way.

## 2. Is it actually long-running Structured Streaming?

**No — and `stream_job.py` is not either.**

`stream_job.py` uses a real `processingTime` trigger, but it is **time-boxed**:

```python
ap.add_argument("--run-seconds", ...,
                help="wall-clock budget; the query is stopped when it expires")
...
while query.isActive and (time.monotonic() - started) < args.run_seconds:
    query.awaitTermination(timeout=5)
```

So it is a *bounded streaming session*, not a resident application. That is a deliberate lab
cost choice, and §4 of the brief permits it **only when explicitly configured** — today it is
the only thing on offer, which is the gap.

The observable consequence, measured live this session: Kafka `cdc.oracle.COREBANK.LOAN`
reached **124** while FULL_CDC held **65**. The lake is only as fresh as the last manual
ingest.

## 3. Current trigger

`--trigger-seconds`, default **30 s**. Against the brief's production default of 1 minute this
commits roughly twice as often, and every commit is an Iceberg snapshot.

## 4. Current checkpoint

`--checkpoint` is a required argument with no default, written via
`.option("checkpointLocation", args.checkpoint)`. **Correctly outside the warehouse** — the
deployed convention is `checkpoints/…`, and `reporting/layers.yaml` keeps warehouse and
checkpoint prefixes disjoint (CLAUDE.md §5.9). Nothing deletes a checkpoint automatically.

**Gap**: checkpoint identity is caller-supplied, so restart stability depends on the submit
command rather than on the compiled plan.

## 5. Current table routing

`cdc/router.py::TableRouter`, driven by the compiled plan. An unregistered topic is **refused**
with a message naming the registration step — it never auto-creates. Verified live: the two
new Phase 8 tables routed with no code change.

## 6. Current FULL_CDC partition

```
partition_spec: [(event_date, identity)]
write: distribution_mode=hash · target_file_size_mb=128 · compression=zstd
```

`source_system`/`source_table` are **carried but not partitioned** — correct for one-table-per-
source-table (§7). Bucketing is supported in the schema and off for every shipped table.

**Divergence from the brief, deliberate**: the brief proposes `days(dv_src_ldt)`. This project
materialises `event_date` as a DATE column and partitions `identity` on it, which is the same
pruning with the transform applied at write. Renaming to `dv_src_ldt` would rewrite the row
contract and every table already written; ADR-062 records that choice.

## 7. Current file-size distribution — measured

| table | files | avg | min | max |
|---|---|---|---|---|
| `cdc_…_digital_event` | 1 | 489 KB | — | — |
| `cdc_…_loan` | 1 | 19 KB | — | — |
| `cdc_events` (monolith) | 10 | **81 KB** | 7.6 KB | 382 KB |

Against a 128 MiB target that is **three orders of magnitude under** — the same finding as the
Phase 0 audit (137 KiB mean), reproduced on a freshly rebuilt lab. This is the structural
argument for maintenance, and it gets worse under true streaming, not better.

Snapshots: monolith **10**, `loan` **1**. Low only because the lab is hours old.

## 8. Current REALTIME execution mode

Finite Spark (`spark.read`), not streaming — **matches the brief's §10 target**. Invoked as a
one-shot EMR job today; `cdc_realtime` DAG exists (ADR-071) but ships disabled.

## 9. Current REALTIME schedule / window

Window is entirely config-driven and the upper bound is frozen once per run. Measured live:

| table | boundary | logical lower bound |
|---|---|---|
| `loan` | `calendar_day` | **2026-09-13 00:00:00** (midnight, 5 days) |
| `payment_method` | `rolling_hours` | 72 h before the frozen upper bound |

`physical_retention >= lookback + grace` is enforced at compile (§12 satisfied).
**Gap**: `schedule` exists in config but nothing consumes it — cadence lives in the DAG cron.

## 10. Current EOD code

One generic builder, `spark/jobs/eod/eod_engine.py`, parameterised by `--table` + `--cob-date`.
No per-table snapshot code. The legacy Oracle-ACCOUNT-only `spark/jobs/eod/job.py` still
exists, still runs, and guards itself with a refusal on non-numeric positions.

## 11. Current EOD cutoff logic

`cdc/eod.py::resolve_cutoff` — `cutoff_local = start of (COB+1)` in the table's business
timezone, converted to UTC, predicate strictly `<`. Date arithmetic on the **local** date, so
DST cannot shift a business day. **No `23:59:59.999` anywhere.** §13 satisfied.

Measured live: `cutoff_utc = 2026-09-18 00:00:00` for COB 2026-09-17.

## 12. Current EOD control tables

**Only `ops.eod_run` exists.** It carries cutoff (both forms), both snapshot ids, position
evidence, counts, DQ and reconciliation outcomes — but it is an **append log**, not a current-
state table.

Missing against §18/§20:
* **`ops.eod_info`** — current authoritative state per `(table_id, cob_date)`, with the
  watermark pair the reference framework advances.
* **`ops.eod_run_hist`** — explicit per-attempt history with `attempt` numbering.

Today `eod_run` is doing both jobs, and "latest row wins" is implicit.

## 13. Current data-model readiness mechanism

`spark/reporting/coordinator.py` has gates and keeps ordering separate from readiness
("Gates do NOT change `turn`"). But there is **no EOD-backed gate**: nothing checks
`cob_date + status=SUCCESS + certification` before a mart model runs. §27–§29 unimplemented.

## 14. Current AUTO_CORRECT implementation

`spark/reporting/auto_correct.py` reads **FULL_CDC** as canonical (§31 satisfied) and bounds
itself with `correction_window(cob_date, lookback_days, late_arrival_days)` — per job, never a
global constant. It refuses `lookback_days <= 0`.

## 15. Current Airflow DAG architecture

13 DAGs. `cdc_table_platform.py` (ADR-071) provides `cdc_realtime` / `cdc_eod` /
`cdc_maintenance`, dynamic-mapped over the compiled plan — **no per-table DAG**, enforced by
test. `dag_eod_pipeline.py` hardcodes three table names but they are the **legacy** path.

**Gap**: no `full_cdc_streaming_lifecycle` DAG (§46), and the CDC DAGs have never run on the
deployed Airflow.

## 16. Current maintenance implementation

`cdc/maintenance.py` (pure planner) + `spark/ops/cdc_maintenance_job.py` (applier). Metric-
driven, not cron-driven; four actions; `remove_orphan_files` opt-in; cadence floor by
temperature. §33–§36 largely satisfied **as a planner**.

**Gap**: it plans per whole table. §37–§39 want *partition-scoped* compaction (recent event
days for FULL_CDC; active window for REALTIME; COB partitions for EOD rolling history).

## 17. Small-file metrics

See §7. Live: 1–10 files per table, 19 KB–489 KB, mean 81 KB on the monolith.
`cdc-maintenance.py measure` reads `$files` and feeds the planner; `min_small_files: 8` stops a
2-file table triggering a rewrite that costs more than it saves.

## 18. Current snapshot / manifest retention

`expire_snapshots_days: 7`, `remove_orphan_files_days: 3`, and the compiler enforces
`orphan <= expire`. **Business retention and Iceberg metadata retention are already separate
fields** (§41 satisfied): `eod.retention_days: 365` is business, `expire_snapshots_days` is
metadata.

## 19. What `pipeline_builder.py` provides

*(1,922 lines, Impala/Airflow, company-specific — read-only reference.)*

**Worth adapting**

| pattern | why |
|---|---|
| `impala_get_ready_eod(job_name, running_date)` | a *named readiness function* that resolves a job's declared dependencies against `dlpt.eod_info` for one `cob_date`, and returns ready/not — exactly the `EodTableReadyGate` §29 asks for |
| `eod_info` carrying `(pre_datelastmaint, datelastmaint, cob_date)` | a **watermark pair** per table+COB: what the close advanced *from* and *to*. Our `eod_run` has cutoff but no `prev_watermark`, so "what did this close actually move" is not answerable |
| separate `summary_config_v1` for dependencies | declared dependencies live in config, not in the DAG |
| explicit skip/processing branches (`check_is_processing`, `check_is_skipped`) | a re-run must distinguish "already running" from "already done" from "deliberately skipped" |
| status branch fan-out `[raise_exception, update_turn_watermark, handle_eod]` | the watermark advances on a **branch**, never unconditionally |

**Reject**

* `os.system(f'rm /home/dlpt/tmp/airflow/config/check_if_ready_eod_*_{job_name}.json')` —
  shelling out to `rm` with a glob and an interpolated job name, on the scheduler's local disk.
  Unsafe and un-testable; a cache belongs in the coordinator run, not in `/home/dlpt/tmp`.
* Impala/Hive coupling, `set timezone=UTC` session hacks, trust-store password reading.
* `signal`-based `TimeoutException` around SQL — replace with the client's own timeout.
* Company names (`dlpt`, `sg_acct`), hardcoded paths, per-job DAG construction.

## 20. What `f_acct…_auto_correct_generate.py` provides

*(1,871 lines, one business mart — read-only reference.)*

**Worth adapting**

| pattern | why |
|---|---|
| `latest_of_day = 1` flag + `row_number() over (partition by … order by etl_dt desc)` | an explicit "which row is the current state for this day" marker, materialised rather than recomputed by every reader |
| **carry-forward from `the_day_before_execution_date`** | a daily state mart seeds day D from day D-1's `latest_of_day = 1` rows, then applies the day's changes. This is what makes a *state* mart cheap — it never rescans history |
| `super_key = concat(hk,'|',system_time,'|',etl_date)` | a composite identity for the PIT row, distinct from the business key |
| `is_full_filled` flag | a row knows whether it came from a real event or from carry-forward |
| `break_lineage(df, storage_level)` | deliberate lineage truncation on long plans — a real Spark technique for wide pipelines |

**Reject**

* The 1,800 lines of `f_acct_depo_by_day` business columns (`bal_dau_ki`, `ma_cn`, `kyhan`, …)
  — these belong in a dbt model, never in framework core.
* String-interpolated SQL built from execution dates across hundreds of lines.
* `sync_wait(seconds, reason)` sleeps as a coordination primitive.

**The load-bearing idea**: our AUTO_CORRECT bounds a window and re-derives it. The reference
*carries forward* the previous day's state and applies only deltas. For a large state mart that
difference is the whole cost.

## 21. KEEP / ADAPT / REPLACE / REJECT

| area | verdict | note |
|---|---|---|
| `cdc/` config platform (registry, compiler, plan hash) | **KEEP** | 2,322 tests; §44–§45 already satisfied |
| `TableRouter` + refusal on unknown topic | **KEEP** | §26 satisfied |
| `eod_engine.py` generic builder | **KEEP** | §16 satisfied |
| `resolve_cutoff` half-open, timezone-safe | **KEEP** | §13 satisfied |
| Source-native ordering (SCN vs hex LSN) | **KEEP** | verified live both engines |
| `realtime_engine.py` frozen upper bound | **KEEP** | §11 satisfied |
| `cdc/maintenance.py` metric-driven planner | **KEEP** | §33–§36 satisfied |
| `auto_correct.py` FULL_CDC-canonical | **KEEP** | §31 satisfied |
| `cdc_table_platform.py` dynamic-mapped DAGs | **KEEP** | §24/§47 satisfied |
| `stream_job.py` time-box | **ADAPT** | make the budget a *config* (`lab` mode); production runs resident |
| trigger 30 s | **ADAPT** | default 1 minute from config, not a CLI flag |
| checkpoint path | **ADAPT** | derive from `table-plan.json` so restart identity is stable |
| `ops.eod_run` | **ADAPT** | split into `eod_info` (current) + `eod_run_hist` (attempts) |
| maintenance scope | **ADAPT** | add partition-scoped targets (§37–§39) |
| `dag_eod_pipeline.py` hardcoded 3 tables | **REPLACE** | superseded by `cdc_eod`; keep until its legacy path retires |
| batch `job.py` as the production ingest | **REPLACE** | production = resident streaming; batch stays the lab/backfill path |
| **no EOD readiness gate** | **BUILD** | `DependencyGate` + `EodTableReadyGate` (§29) |
| **no `full_cdc_streaming_lifecycle` DAG** | **BUILD** | §46 |
| Impala/`os.system`/`signal` patterns | **REJECT** | §19 |
| business columns from the reference mart | **REJECT** | §20 |

## 22. Implementation plan B → J

| phase | scope | acceptance |
|---|---|---|
| **B** FULL_CDC streaming hardening | `ingestion.{mode,trigger_interval}` in the registry; checkpoint derived from the plan; `stream_job` budget becomes lab-only config; `ops.streaming_app_state` wired | a resident run survives restart on the same checkpoint; Kafka→FULL_CDC lag measured, not inferred |
| **C** REALTIME config-driven cadence | `realtime.schedule` actually drives the DAG; per-table resource profile | changing `schedule` in YAML changes cadence with no DAG edit |
| **D** EOD control plane | `ops.eod_info` (current, unique on `table_id+cob_date`, watermark pair) + `ops.eod_run_hist` (append, `attempt`); status model §21; idempotent retry after commit (§23) | kill a close after the Iceberg commit → retry detects the committed snapshot and does not double-write |
| **E** Readiness + dbt gating | `DependencyGate` / `EodTableReadyGate` / `DatasetFreshnessGate`; batched resolution, cached per coordinator run (§49) | a mart model with an unclosed EOD dependency **waits**, and says which table it is waiting on |
| **F** AUTO_CORRECT integration | carry-forward from D-1 state (§20) instead of full window re-derivation; affected-key strategy from config | a late event repairs only affected keys/dates, measured |
| **G** Maintenance framework | partition-scoped compaction; per-layer policy (§37–§39); orphan retention exceeds max job duration (§40) | FULL_CDC compacts recent event days only; EOD uses its own policy, not FULL_CDC's |
| **H** Partition / file benchmark | small / medium / hot table, before-after (§43) | no partitioning claim without Athena bytes + file count + planning time |
| **I** Full E2E | source → resident streaming → FULL_CDC → REALTIME + EOD → readiness → dbt → MART, with the generator running | mart numbers reconcile against an independently computed count |
| **J** Docs + production review | ADRs for B–G; runbook updates; cost review of resident streaming vs scheduled batch | the always-on cost of §2's target is stated in `docs/COST.md` before it is enabled |

### The one decision that needs making before B

The brief's §4 says production default is a **long-running** streaming app. This project's
cost invariants (CLAUDE.md §4) say nothing runs 24/7 without a warning, and `lab_low_cost` is
the default profile. Those are in genuine tension.

**Recommendation**: `ingestion.mode` is config with **two supported values**, and the *profile*
picks the default — `lab_low_cost` → `available_now`, `production` → `continuous_microbatch`.
Neither is hardcoded, and enabling a resident app is then an explicit, costed decision rather
than a side effect of a code path. Phase J prices it.

---

**Phase A is audit only. Nothing was changed.**
