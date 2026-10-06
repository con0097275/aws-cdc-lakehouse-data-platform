# Reporting Implementation State

Durable repository state for the reporting/datamart platform. Rewritten by a context
rehydration pass on **2026-08-21** after Sessions 32 (infra apply + CDC bring-up) and 33
(Phase 14 execution) ran without updating any canonical document. Contains no
conversational memory and no secrets.

**Scope warning.** `PROJECT_STATE.md`, `DECISION_LOG.md` and `IMPLEMENTATION_REPORT.md`
are stale: they stop at **Session 31**. Sessions 32 and 33 — the live infrastructure apply,
the CDC bring-up, seventeen defect fixes and the entire Phase 14 run — exist only in
`artifacts/validation/session-32/` and `artifacts/validation/session-33/`. That is a
`CLAUDE.md` §9.8 violation carried forward, not a decision.

## Architecture checkpoint

`REPORTING_INFRA_APPLIED_AND_VERIFIED` → **Phase 14 executed, validation incomplete.**

Classification: **`PHASE14_ALL_SCENARIOS_EXECUTED_VALIDATION_INCOMPLETE`**.

No ADR conflict was found. ADR-033…ADR-045 all exist and the code matches them; in
particular `spark/reporting/run_dbt_job.py` still uses `dbtRunner` with `method: session`,
`dbt build` (not `run`), and no in-process retry loop — ADR-044 as accepted.

## HEAD and worktree

- Branch: `session-02-prerequisites`
- HEAD: `e9be2303c32b616d1013af72ff3d26e0ebd3bd73` — *Session 21: architecture and test guide*
- Worktree: **modified.** 39 modified, 66+ untracked paths. **Nothing has been committed
  since Session 21.** The entire reporting platform, every ADR from 033 up, all Phase 14
  evidence and every Session 32 infrastructure fix are uncommitted.

## Pre-Phase-14 phase status

| Phase | Mode | Status | Evidence |
|---|---|---|---|
| 3 | Metadata foundation | **PASS** | `spark/reporting/{models,config_loader,runtime_state,dynamodb_state,history,watermark,status,source_resolver,ops_client}.py`; `spark/reporting/ddl/reporting_ops_tables.sql` (job_master, job_flow_config, job_dependency, resource_profile, job_master_execution_hist, summary_config_hist_v1); 4 live DynamoDB tables; `models.py:695` documents `turn` ≠ `attempt_number` |
| 4 | dbt-Spark framework | **PARTIAL** — COMPILE_PASS / **RUNTIME_NEVER_EXECUTED** | `dbt/macros/reporting/*.sql`, `run_dbt_job.py`, `manifest_sync.py`, `scripts/dbt-compile-modes.sh`. `dbt build` has still never run anywhere — not locally against Glue, not on EMR |
| 5 | Config compiler + dependency engine | **PASS** | `reporting/compile.py`, `spark/reporting/{plan,graph,dependency_engine,summary_plan}.py`. Recompiled today: `cfg-9277073c543012d8`, **byte-identical to the plan.json Phase 14 ran against** |
| 6 | Airflow coordinator | **PARTIAL** | 4 flow DAGs + `reporting_common.py` + `coordinator.py` exist and are tested. `_gate_and_run_callable` dispatches **only EOD and STREAM_BATCH**; AUTO_CORRECT and FULFILL still `raise NotImplementedError` (`reporting_common.py:297`) — OPEN-28. No Airflow is deployed (`enable_airflow=false`) |
| 7 | EOD | **PASS (unit)** / module not exercised live — see below | `eod_flow.py`, `gates.py`, `business_calendar.py`; `artifacts/validation/session-26/` |
| 8 | AUTO_CORRECT | **PASS (unit)** / module not exercised live | `auto_correct.py`, `auto_correct_flow.py`; `artifacts/validation/session-27/` |
| 9 | FULFILL | **PASS (unit)** / module not exercised live | `fulfill.py`, `fulfill_flow.py`; `artifacts/validation/session-28/` |
| 10 | STREAM_BATCH | **PASS (unit)** / module not exercised live | `stream_batch.py`, `stream_batch_flow.py`; `artifacts/validation/session-29/` |
| 11 | STREAMING_RT | **PASS (unit)**; lifecycle exercised live, **no events ever processed** | `streaming_rt.py`, `streaming_rt_app.py`, `streaming_rt_lifecycle.py`; `artifacts/validation/session-30/` |
| 12 | Infra plan | **PASS** | `docs/REPORTING_INFRA_GAP_ANALYSIS.md`, `artifacts/validation/session-31/`, `terraform/modules/reporting_ops` |
| 13 | Infra apply | **INFRA_APPLIED_VERIFIED** | 214 resources in state, 12 of them `module.reporting_ops`; 4 DynamoDB tables live; EMR job `00g84kussh41e827` SUCCESS; `artifacts/validation/session-32/` |

Local suite on 2026-08-21: **875 passed**, `scripts/validate-docs.py` **14 passed / 0 failed**.

## Phase 14 — what actually ran

Executed 2026-08-20T11:19Z on live infrastructure. Two runs; the first
(`p14-s1-1787224544`, S4 = `NOT_A_GAP`) was discarded, the second
(`p14-s1-1787224742`) is the evidence of record. Both raw outputs are preserved at
`artifacts/validation/session-33/driver/`, and `raw-run2-clean.json` is content-identical
to `phase14-scenarios.json`.

| # | Scenario | Status | execution_id | Snapshot before → after | Watermark after |
|---|---|---|---|---|---|
| S1 | NORMAL EOD | **PASS** | `p14-s1-1787224742:EOD:mart_account_balance_daily:2026-08-20:1` | 6573451454893231677 → 1900945029509105984 | EOD = 2026-08-20 |
| S2 | LATE CDC | **PASS (with caveat)** | — (`SOURCE_MUTATED`) | — | — |
| S3 | AUTO_CORRECT | **PARTIAL** | `p14-s3-1787224757:AUTO_CORRECT:…:2026-08-20:1` | 1900945029509105984 → 4198656374970512297 | AUTO_CORRECT = 2026-08-20 |
| S4 | HISTORICAL GAP | **PASS** (`GAP_CONFIRMED`, 0 rows at 2026-08-17) | — | — | — |
| S5 | FULFILL | **PARTIAL** | `p14-s5-1787224763:FULFILL:…:2026-08-17:1` | 4198656374970512297 → 6929065822369863133 | FULFILL = 2026-08-17 |
| S6 | REALTIME CHANGE | **PASS (with caveat)** | — (`SOURCE_MUTATED`) | — | — |
| S7 | STREAM_BATCH | **PARTIAL** | `p14-s7-1787224768:STREAM_BATCH:…:2026-08-20:1` | 6929065822369863133 → 6264587913184777965 | STREAM_BATCH = 2026-08-20 |
| S8 | STREAMING_RT | **PARTIAL** (lifecycle only) | deployment `rt_dep-20260820T111931Z-phase14` | — | — |

All four watermarks and all 19 execution records were read back **from live DynamoDB on
2026-08-21** and still match. The four Iceberg tables still hold their data
(stream 8 objects, full_cdc 2, curated 15, mart 18).

### What the driver actually did — read this before trusting the matrix

The Phase 14 driver is `artifacts/validation/session-33/driver/scenarios.py` (also at
`s3://kafka-dev-lab-dev-lake-111122223333/artifacts/dbt/scenarios.py`). Reading it changes
the reading of the results:

1. **The mode flow modules never executed.** `scenario()` calls the real
   `coordinator.plan_run / gate / submit / finish` and then a **MERGE written inline in the
   driver**. `eod_flow.py`, `auto_correct.py`, `fulfill.py` and `stream_batch.py` were never
   invoked. What Phase 14 proved live is the *coordinator lifecycle + runtime state store +
   Iceberg MERGE*, not the five modes' own logic.
2. **The MERGE ladder that refused the downgrades was the driver's copy**, not
   `dbt/macros/reporting/reporting_mart.sql`. The semantics match ADR-042; the repo's
   implementation of them is still unproven at runtime.
3. **S2 and S6 mutate `curated.fact_account_daily_snapshot` directly** with an Iceberg
   MERGE. No CDC event traversed Oracle → Debezium → Kafka → L1 → L2 for either. The
   CDC hop itself is proven separately (topic counts match seed exactly, §1 of
   `PHASE14-RESULT.md`), but S2/S6 are curated-layer simulations of lateness.
4. **S5's "historical source" is fabricated** — the driver DELETEs the gap date from curated
   and INSERTs a copy of the current date's rows.
5. **S8's checkpoint reading came from `InMemoryCheckpointInspector()`.** The
   `checkpoint_state_at_start: EMPTY` value never touched S3. (S3 agrees — the
   `checkpoints/reporting/` prefix is empty — but that is corroboration, not the evidence.)
6. **`dbt build` never ran** — recorded honestly as `SUBSTITUTED_SPARK_SQL_MERGE`.
   dbt-core/dbt-spark are absent from the EMR image and the private subnets cannot reach
   PyPI.

`PHASE14-RESULT.md`'s header claim — *"nothing is simulated except the one substitution
named in §3"* — is **inaccurate** on points 1–5 and should be corrected rather than cited.

### What Phase 14 genuinely established

- Oracle and SQL Server CDC → Kafka: topic counts match the seed exactly (2000/320/200/4
  and 3000/150/50/4).
- Kafka → L1 → L2 FULL_CDC on EMR Serverless: jobs `00g84psb3ndrs827` and
  `00g84pvcfbbe1g27`, 8,932 events, idempotent on `event_id`.
- L2 → EOD curated: 320 rows / 320 distinct accounts, collapsing a genuine double snapshot
  (6000 L1 rows for 3000 keys) — the dedup contract proving itself on real duplicate data.
- Coordinator lifecycle end to end, with 19 durable execution records.
- Watermarks advancing only on success, each naming the execution that set it.
- The ADR-042 tier ladder refusing `PROVISIONAL_CORRECTED` and `PROVISIONAL_NRT`
  downgrades of a `CERTIFIED` row, against live Iceberg data.

### The five gaps that keep Phase 14 open

| # | Gap | Needs MSK? |
|---|---|---|
| G1 | `dbt build` never ran — the whole dbt-Spark layer is runtime-unproven | No |
| G2 | STREAM_BATCH's frozen upper bound and `[wm − overlap, frozen_upper)` window never computed against live data; `watermark_ts` is null | No |
| G3 | No correction has ever *landed* — S3 and S7 prove the refusal path only | No |
| G4 | Airflow orchestrates none of it (`enable_airflow=false`); DAGs, dynamic mapping and pools unproven. Blocked by OPEN-28 | No |
| G5 | STREAMING_RT processed zero events; checkpoint never written | No — `FULL_CDC_APPEND` reads Iceberg |
| G6 | The four mode flow modules were bypassed entirely by the driver (see above) | No |

**None of the six require MSK.** They all read the L2/curated data already in the lake.

## Infrastructure — live as of 2026-08-21T03:31Z

The platform was **rebuilt this morning at 02:51Z**, after the teardown described in
`artifacts/validation/session-33/RESTART-TOMORROW.md`. That document is now partly stale.

| Resource | State | Note |
|---|---|---|
| Terraform | 214 resources, `module.reporting_ops` = 12 | applied by the operator |
| MSK `kafka-dev-lab-dev` | **ACTIVE**, created 2026-08-21T02:51:36Z | **new cluster** — topics, connectors and Connect internal state are empty |
| EMR Serverless `00g84ii82onjs425` | STOPPED (idle, $0) | bills only while a job runs |
| Airflow | **not deployed** — `enable_airflow=false` | |
| DynamoDB | 4 tables, intact | 19 execution records, 4 watermarks, 3 streaming deployments |
| Glue/Iceberg | 7 databases; stream/full_cdc/curated/mart tables all hold data | Phase 14 lake state fully survives |
| Streaming checkpoint | `s3://…/checkpoints/reporting/` exists, **empty** | STREAMING_RT never ran |
| source-lab `i-087d247ae3fb4ac14` | running, launched 02:51Z | **NEW instance**; the old `i-0108f96e91b11b717` is terminated, so Oracle/SQL Server are **unseeded** and CDC is not enabled |
| cdc-runtime `i-00d1b513011cc492d` | running, launched 03:17Z | new; the manual Connect fixes are not applied |
| toolbox `i-01d2e57686ef6bd6b` | running | |

`scripts/cdc-window-start.sh` reports **READY** on all five checks.

Burn: **~$1.1244/hr** baseline (MSK is 68% of it), ~$1.4267/hr with an EMR job running.
Budget: ~$21.50 left of $30.

Two stale values to be aware of, neither safe to "fix" casually:

- `auto_destroy_after = "2026-08-16T18:00:00Z"` is in the past (OPEN-34). Changing it feeds
  provider `default_tags`, which makes the AZ data source unknown at plan time and
  **forces replacement of all three subnets, MSK, EMR and all three EC2 instances** —
  see `artifacts/validation/session-32/pre-phase14-blockers.txt`.
- `artifacts/validation/session-21/CDC_WINDOW_START_TIME.txt` still records
  `2026-08-16T09:53:12Z`; the gate keeps the original on re-run, so its cost arithmetic
  is for the wrong window. Today's window began 2026-08-21T02:51Z.

## Known blockers

1. **OPEN-28** — AUTO_CORRECT and FULFILL Airflow adapters raise `NotImplementedError`.
   Blocks G4.
2. **dbt is not installable on EMR** — no PyPI from private subnets. Blocks G1; needs
   either a custom image, a `--archives` virtualenv, or an S3-staged wheelhouse.
3. **OPEN-32** — EMR, `lake_iam` and `airflow_k3s` are `count`-gated on
   `enable_kafka_platform`, and `lake_iam` requires the MSK ARN. Reporting compute cannot
   exist without an MSK cluster it never reads. This is why closing G1/G2/G3/G5/G6 still
   costs $0.7650/hr for a cluster none of them use.
4. **MSK security-group flap** — `aws_security_group.msk` mixes inline `ingress` with
   separate rule resources; any untargeted plan wants to revoke EMR and CDC access on 9098.
5. **OPEN-27** — AUTO_CORRECT does not record `source_layer_substituted`.
6. **OPEN-31** — no reconciliation job for the KAFKA_DIRECT path (not needed while the
   pilot ships FULL_CDC_APPEND; the code refuses KAFKA_DIRECT without one).
7. **The REALTIME layer does not exist** — substituted onto `kafka_dev_lab_dev_full_cdc`
   with a mandatory bounded predicate.
8. Four CDC bring-up steps are gated on typed confirmation phrases and cannot be
   self-approved by an agent (`docs/APPROVAL_GATES.md`).

## Preserved test context

- Business date of record: **2026-08-20**; constructed historical gap: **2026-08-17**
- Job: `mart_account_balance_daily`; 320 accounts, 320 mart rows
- Mart balance sums: `2031884176.61` (S1/S3/S7), `2031885176.60` (S5)
- S2 mutated `min(account_sk)` by +999.99; S6 mutated `max(account_sk)` by +5.55 —
  the driver selects them by aggregate, so the literal keys were never recorded:
  **NOT_CAPTURED**
- Coordinator run ids: `p14-s1-1787224742`, `p14-s3-1787224757`, `p14-s5-1787224763`,
  `p14-s7-1787224768`
- Spark app ids: `inline-1` for every scenario — the driver's `InlineSubmitter`, not a real
  EMR application id. Real EMR job runs: `00g84kussh41e827` (smoke),
  `00g84psb3ndrs827` (L1), `00g84pvcfbbe1g27` (L2), `00g84qenvh31ig27` (scenarios)
- Iceberg mart snapshot chain: `6573451454893231677` → `1900945029509105984` →
  `4198656374970512297` → `6929065822369863133` → `6264587913184777965`
- STREAMING_RT deployments: `rt_dep-20260820T111306Z-phase14`,
  `…T111611Z-phase14`, `…T111931Z-phase14` — all STOPPED, restart_count 0
- Kafka offsets: **NOT_CAPTURED** at scenario level (topic totals only, and that cluster
  has since been destroyed and rebuilt)
- Config: `cfg-9277073c543012d8`, unchanged since the run

## Files that must not be lost

Everything below is untracked or unstaged on `session-02-prerequisites`. Do not run
`git reset --hard`, `git clean -fd`, `git checkout --` or `git stash drop` here.

- `spark/reporting/` (30 modules) and `spark/reporting/ddl/`
- `reporting/` (compiler, YAML, JSON Schemas, calendar, profiles)
- `dbt/macros/reporting/` (4 macros)
- `airflow/dags/datamart_{eod,auto_correct,fulfill,stream_batch}.py`,
  `reporting_common.py`, `streaming_rt_lifecycle.py`
- 18 `spark/tests/test_reporting_*.py` plus `fake_dynamodb.py`, `reporting_fixtures.py`,
  `reporting_common_shim.py`
- `docs/REPORTING_*.md` (5 documents), `docs/adr/ADR-033`…`ADR-045`, `docs/claude/`
- `terraform/modules/reporting_ops/`
- `artifacts/validation/session-26` … `session-33`, **including the newly preserved
  `session-33/driver/`** (the Phase 14 driver and both raw runs, recovered from a `/tmp`
  scratchpad that would not have survived)
- `scripts/dbt-compile-modes.sh`, `streaming-reset.sh`,
  `reporting-{stream-batch,streaming-rt}-demo.py`
- The Session 32 infrastructure fixes in `docker/cdc-runtime/`, `connectors/`,
  `terraform/modules/{lake_iam,emr_serverless,data_lake}`, `scripts/register-connectors.sh`

## Exact next action

**Resume Phase 14 at G6/G1, not at a scenario.** All eight scenarios have been executed and
none needs a blind rerun. The next concrete step is:

> Re-run S1 (EOD) through the framework's own `eod_flow.run_eod_job` and the repo's
> `reporting_mart.sql` MERGE macro — not the driver's inline copy — on a **non-certified**
> business date, so that G6 (module actually executes), G1 (dbt build) and G3 (a correction
> that lands) are closed by one run.

Do not rerun S2, S4, S6 or S8. Do not rebuild the CDC path for this: none of G1–G6 needs
MSK, and the CDC hop already passed with exact seed-to-topic counts.

Two decisions belong to the operator before that runs:

1. **MSK is billing $0.7650/hr for work that does not use it.** Either destroy the Kafka
   platform and continue on EMR + the existing lake data, or accept the burn. OPEN-32 means
   destroying it also removes EMR — that coupling needs deciding, not working around.
2. **How dbt gets onto EMR Serverless** (custom image / `--archives` venv / S3 wheelhouse).
   Without one, G1 cannot close and the dbt-Spark layer stays unproven.

Then: commit. Phases 3–14 are still entirely uncommitted.
