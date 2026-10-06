# FINAL TEST SUMMARY — 2026-09-03 live window

Window `09:06:04Z` → close. Platform live, 292 Terraform resources, ~$1.2340/hr.
**Nothing here is marked PASS without evidence in `artifacts/validation/final-e2e/`.**

## Scorecard

| # | Criterion | Status |
|---|---|---|
| 1 | Cost/safety gate: identity, backend, workspace, plan freshness | **PASS** |
| 2 | Saved plan: 248 create / 0 change / 0 destroy / 0 replace, 1 VPC, 1 MSK, 0 NAT/EKS/Redshift | **PASS** |
| 3 | Infrastructure bring-up + per-resource verification | **PASS** |
| 4 | CDC credential chain (SSM → .env → Connect secrets) | **PASS** |
| 5 | Source → Debezium → Kafka, 5 scenarios × 2 engines | **PASS** |
| 6 | Same-PK partition stability | **PASS** |
| 7 | Delete: `d` envelope + tombstone both reach Kafka | **PASS** |
| 8 | FULL_CDC canonical ingest, counts reconcile | **PASS** |
| 9 | Replay / rerun idempotency | **PASS** |
| 10 | REALTIME window materialisation | **PASS** |
| 11 | EOD one-state-per-PK, delete removed, recreate honoured | **PASS** (after fixing a P0) |
| 12 | Reconciliation FULL_CDC ↔ EOD | **PASS** |
| 13 | Dependency engine: manifest → plan hash → topological turns | **PASS** |
| 14 | Same-turn parallelism | **PASS** |
| 15 | **`dbt build` live on EMR** | **PASS** — first time in project history |
| 16 | EOD reporting flow + convergence on rerun | **PASS** |
| 17 | STREAM_BATCH frozen upper bound + watermark_ts | **PASS** |
| 18 | Failed run does not advance watermark | **PASS** |
| 19 | Datamart grain/keys (dbt uniqueness + not_null) | **PASS** |
| 20 | Athena serving queries, actual = expected | **PASS** |
| 21 | Business AI KPI == direct SQL | **PASS** |
| 22 | Agent refuses without evidence | **PASS** |
| 23 | RAG grounded citations; RAG does not supply KPIs | **PASS** |
| 24 | Agent security: mutation + injection refused | **PASS** |
| 25 | Reconciliation source → AI, zero drift | **PASS** |
| 26 | Data governance registry completeness | **PASS** |
| 27 | Security posture (no creds/SSH/public inbound, TLS, KMS) | **PASS** |
| 28 | Connect restart recovery (RPO 0, RTO ~30 s) | **PASS** |
| 29 | Forecast/comparison/anomaly refuse without history | **PASS** (correct NOT_APPLICABLE) |
| 30 | Driver analysis by requested dimension | **FIXED, AWAITING LIVE RE-TEST** — was FAIL; the contract node now refuses a blocked dimension by name (B2) |
| 31 | Reporting job under its intended IAM role | **FIXED IN TERRAFORM, AWAITING APPLY** — was FAIL; `logs/emr/*` added to the reporting role (R4) |
| 32 | Observability: reporting metrics published | **FIXED IN TERRAFORM, AWAITING APPLY** — was FAIL; `logs`/`monitoring` VPC endpoints added (G1/G2) |
| 33 | MSK controller alarm | **FIXED IN TERRAFORM, AWAITING APPLY** — was FAIL; the alarm is gated on `local.is_kraft` (G3) |
| 34 | AUTO_CORRECT flow | **PASS** — live, `4 date(s), 0 key(s), FULL_WINDOW_FALLBACK`, watermark advanced |
| 35 | FULFILL flow | **PASS** — live for gap date 2026-09-01, watermark advanced |
| 36 | STREAMING_RT lifecycle + checkpoint recovery | **PASS** — 4-app realtime layer built and run live; see `docs/REALTIME_STREAMING_RT.md` |
| 37 | Airflow DAG end-to-end execution | **FIXED, AWAITING LIVE RE-TEST** — was BLOCKED; AUTO_CORRECT and FULFILL wired into the coordinator (OPEN-28) |
| 38 | Schema evolution through the chain | **FIXED, AWAITING LIVE RE-TEST** — was BLOCKED; the writer schema is now chosen per record by `apicurio.value.globalId` (E2) |
| 39 | Poison record → quarantine | **FIXED, AWAITING LIVE RE-TEST** — was BLOCKED; PERMISSIVE decode + quarantine row + payload object (G-P1-1) |
| 40 | Proactive insight serving + idempotent rerun | **PARTIAL** — pipeline PASSES live (deterministic `insight:c4d1a236…`, rerun identical); the `serving` Glue database is now in Terraform, AWAITING APPLY |
| 41 | Feature materialisation + live ML inference | **NOT_TESTED** — runner built (`scripts/ai-feature-run.py`) and verified against a fake Athena; not yet run against the live mart |
| 42 | Generated narration / injection in generated text | **CANNOT_FIX** — Bedrock `INVALID_PAYMENT_INSTRUMENT`; a billing state, not a defect |

**28 PASS · 0 FAIL · 0 BLOCKED · 1 PARTIAL · 5 NOT_TESTED · 7 FIXED-AWAITING-RETEST · 1 CANNOT_FIX**

> **`FIXED, AWAITING …` IS NOT `PASS`.** Seven criteria that were FAIL or BLOCKED have had
> their cause removed in code or Terraform, each with regression tests. None of them has
> been re-run against AWS, so none of them is a pass. The PASS column is unchanged at 28
> and stays there until the live re-test in section "What to run tomorrow" produces
> evidence. Item 42 is CANNOT_FIX: the Bedrock account has no valid payment instrument,
> which is not a software defect.

## STREAMING_RT — added 2026-09-03, after the original sweep

A four-app realtime serving layer modelled on a production system (plstream v9), built and
run live end to end. Design and full evidence: `docs/REALTIME_STREAMING_RT.md`.

| Criterion | Status |
|---|---|
| Long-running stream app, own trigger loop | **PASS** — 4 micro-batches / 240s, 332 facts |
| Long-running datamart app, polling | **PASS** — 9 cycles / 180s, 17.8s -> 3.1s via source cache |
| Fast path publishes an unenriched row rather than dropping it | **PASS** — 990500 published with NULL dims |
| Pointer table records what the stream could not do | **PASS** — retry_count 5, resolved NULL |
| Resolver ages a pending row out INDEPENDENT of micro-batches | **PASS** — 43 resolver cycles, timed_out=1 |
| Correction pass repairs from a fresher source | **PASS** — worklist 1, repaired 1, still_incomplete 0 |
| Pointer resolved by the repair | **PASS** — resolved_by=AUTOCORRECT |
| EOD rebuild + watermark | **PASS** — 321 rows, watermark 2026-09-03 10:16:40 |
| Merge view, STREAM-wins-after-watermark | **PASS** — STREAM 4 / BASE 318, reconciles exactly |
| End-to-end numeric reconciliation | **PASS** — diff 13,250.17 fully explained |
| AUTOCORRECT advancing the watermark | **KNOWN GAP** — documented; identical to the reference |

## Defects found by running it, and fixed in-session

| Id | Finding | Proof of fix |
|---|---|---|
| G-P0-1 | UTC session zone missing on all 3 canonical EMR jobs | pinned + regression test |
| G-P0-2 | Unqualified table reference bypassed both SQL allow-lists | 7 regression cases |
| G-P1-2 | `assert_no_secrets(redact(x))` raised on correctly redacted text | idempotency tests |
| G-P1-4 | EOD ranking Oracle-only, would degrade to `kafka_offset` | fail-closed guard |
| **E1** | **Deleted accounts survived into the certified EOD snapshot** | **322 → 321, live** |
| B1 | An explicitly named date was silently replaced by "yesterday" | live re-query, 73 tests |
| R3 | `reporting-live-run.py` preflighted 9 of 14 env vars | all 17 now checked |

## Defects closed in code after the live window (2026-09-04), not yet re-tested live

| Id | Finding | The fix |
|---|---|---|
| B2 (P1) | Agent substitutes an available dimension for the one requested | the contract node refuses by name; `requested_dimension()` keys off the question |
| G1/G2/G3 (P1) | Observability blind | `logs`/`monitoring` VPC endpoints; the KRaft-invalid alarm gated on `local.is_kraft` |
| R4 (P1) | Reporting IAM role cannot emit logs | `logs/emr/*` added to the role policy |
| G-P1-1 (P1) | Canonical ingest has no quarantine path; payload never written | PERMISSIVE decode, quarantine row **and** the payload object, two error classes |
| E2 (P1) | Schema evolution unsupported — writer schemas pinned per topic | per-record selection by `apicurio.value.globalId`; unexported id ⇒ quarantine, never a guess |
| OPEN-28 | AUTO_CORRECT / FULFILL raised `NotImplementedError` in the scheduled path | wired through `_emr_submitter()` / `_run_correction_or_fulfill()` |
| item 40 | No `serving` Glue database, so insights could not persist | added to `terraform/envs/dev/main.tf` |

## Open defects, still not fixed

| Id | Finding | Why it is still open |
|---|---|---|
| E5 (P1) | EOD MERGE cannot retract a published row | the E1 fix clears the partition before rebuild, which covers the observed case; a general retract arm is a schema-and-semantics change, not a patch |
| G-P1-3 | Source-connector DLQ is inert; a record failing conversion **inside Connect** drops silently | `errors.deadletterqueue.*` is a sink-connector feature. The Spark-side control is now closed (G-P1-1), so this covers only records that never reach Kafka. Choosing `tolerance=none` trades availability for evidence — an operator decision |
| G4 (P2) | Workload hosts publicly addressable | consequence of running without NAT; no inbound is permitted and there is no key pair |
| item 42 | Bedrock generation unvalidated | `INVALID_PAYMENT_INSTRUMENT` on the account. Not a software defect and not fixable from here |

## What to run tomorrow, to convert FIXED-AWAITING into PASS

Ordered so each step's evidence is available to the next. Nothing here needs a rebuild
beyond the normal bring-up.

| # | Step | Turns which item into evidence |
|---|---|---|
| 1 | `bash scripts/tf.sh plan` → review → apply the saved plan (never `-auto-approve`) | 31, 32, 33, 40 |
| 2 | ~~restage the job code~~ **already done 2026-09-04** — `full_cdc_job.py`, `eod_job.py`, `realtime_job.py`, the STREAMING_RT apps and `framework-flat.zip` all match the repo; the previous copies are under `artifacts/*/.superseded-2026-09-03/` | prerequisite for 38, 39 |
| 3 | `bash scripts/cdc-runtime.sh export-schemas --execute <lake-bucket>` | prerequisite for 38 — the export must carry **every** version |
| 4 | Add a column to `COREBANK.ACCOUNT`, produce rows, rerun the FULL_CDC job; expect `FULL_CDC_DECODE … globalId=` twice for the topic | **38** |
| 5 | Produce one deliberately corrupt record to a topic; rerun; expect the run to COMPLETE, a row in `…_quarantine.full_cdc_rejects`, and the referenced S3 object to exist | **39** |
| 6 | Re-run the reporting DAG for AUTO_CORRECT and FULFILL through Airflow (not the CLI) | **37** |
| 7 | `python3 scripts/ai-ask.py` asking for a **blocked** dimension; expect a refusal naming it, not a substitution | **30** |
| 8 | `python3 scripts/ai-feature-run.py --as-of <business_date> --publish` | **41** |

Item 42 stays CANNOT_FIX until the Bedrock account has a valid payment instrument.

## Suite status

Re-run 2026-09-04, **with the platform torn down**. The AWS-dependent numbers are lower
than they were during the live window for that reason and no other; they are recorded as
run, not as they stood.

```
pytest spark/tests (ONE run, no exclusions)       1693 passed / 0 errors
validate-docs                                       14 passed / 0 failed
make lint-shell                                     exit 0
make ai-security                                    23/23
ai/eval/e2e_p14.py                                   8 PASS / 2 FAIL   <- AWS down
RAG gate / agent gate / business eval               PASSED / PASSED / 25-25
agent gate task_success_rate                      0.8636              <- AWS down
```

The two `e2e_p14` failures are S3 and S8, both of which need `query_athena`; the workgroup
was destroyed with the platform, so the call returns `WorkGroup is not found`. Same cause
for the agent gate's 6 tool failures (`structured_query_correctness 0.0`). During the live
window these were `10 PASS / 0 FAIL` and `PASSED`. Re-run step 1 of the table above before
reading anything into these two rows.

---

# Window 2026-09-06 — rebuilt platform, re-proven end to end

A full rebuild: new MSK cluster, new lake CMK, fresh seed. Nothing carried over. Every
number below was produced in this window.

## Criteria converted from FIXED-AWAITING to PASS

| # | Criterion | Evidence |
|---|---|---|
| 31 | Reporting job under its INTENDED IAM role | EOD flow ran under `kafka-dev-lab-dev-reporting`, not the `spark-eod` workaround. R4's `logs/emr/*` grant verified on the live role first |
| 30 | Driver analysis by requested dimension | `product_code` (blocked) → `contract_ok=False`, *"cannot be broken down by 'product_code' … Refusing rather than answering by a different dimension"*. Control (`processing_status`, available) → `contract_ok=True` |
| 39 | Poison record → quarantine | see "Two defects found by running it" below |
| 41 | Feature materialisation + ML inference | 320 feature rows from live Athena, 320 scored by `model:account_anomaly_v1`, published to `s3://<lake>/ai/features/`. 8 features correctly NULL-with-reason (1 day of history cannot support a 7/30-day window) |
| 36 | STREAMING_RT, all four apps + late-dimension repair | `docs/REALTIME_STREAMING_RT.md` §12 |

## Data correctness, source to AI — zero drift

```
Oracle ACCOUNT            320 rows
Kafka  8 topics         5,728 messages   (exact per-table match, 320/200/4/2000/150/4/3000/50)
FULL_CDC                5,728 rows       FULL_CDC_INGESTED == FULL_CDC_COUNT
  same PK -> same partition: 320 distinct PKs, 0 spanning >1 partition   (CLAUDE.md 5.1)
EOD 2026-09-06            320 rows       = 320 distinct account_sk, day CLOSED (watermark + Iceberg tag)
  independent Athena reconciliation: expected_from_full_cdc 320 == eod_rows 320
mart                      320 rows       2,031,880,160.00
Business AI               2,031,880,160.0  data_status CERTIFIED
```

`dbt build`: `PASS=3 WARN=0 ERROR=0 SKIP=0` — 1 incremental model, 2 grain tests.

All five reporting flow modes ran: EOD, AUTO_CORRECT, FULFILL, STREAM_BATCH, STREAMING_RT.
AUTO_CORRECT's detail is the certification ladder refusing to overwrite itself:
`3 date(s), 0 key(s), FULL_WINDOW_FALLBACK; 1 date(s) ESCALATED (already certified;
AUTO_CORRECT cannot overwrite CERTIFIED)`.

## Defects found by running it, and fixed in-window

| Id | Finding | How it was caught |
|---|---|---|
| **P39-1** | **PERMISSIVE `from_avro` does NOT return a null struct for a corrupt payload** — Avro has no magic bytes or checksum, so arbitrary bytes decode into a struct with all-null FIELDS. The quarantine predicate tested `v IS NULL`, so a poison record was INGESTED into the canonical layer with op/position/payload all NULL. Strictly worse than the FAILFAST crash it replaced: a crash is loud, a silently corrupt canonical row is not | injecting a real corrupt record |
| **P39-2** | Quarantine payloads were written with `ServerSideEncryption="aws:kms"` and no key id, which selects the AWS-MANAGED `aws/s3` key — not the lake CMK the Spark and AI roles are granted. The payload became unreadable by the jobs that need it: the dead-reference defect wearing a different hat | reading back the object's CMK |
| **P37-1** | The OPEN-28 DAG adapter read `task["business_date"]`; `JobTask.as_dict()` provides `date_of_data`. `KeyError` in the DAG while `reporting-live-run.py` stayed green — that script builds its FulfillPlan from `--from-date/--to-date` and never touches the task template. A CLI mirror only protects the paths it actually shares | triggering the DAG |
| **P-PY39** | `full_cdc/job.py` used PEP 604 (`str \| None`) with no `from __future__ import annotations`. EMR Serverless runs **Python 3.9**; the job died at IMPORT, 18 s in, before any logic ran | first EMR submission |
| **G-P2-9** | After destroy/apply the lake holds objects on the OLD CMK while roles are granted only the new one. Every Spark job fails with `kms:Decrypt` denied, reported as an S3 error | first EMR submission |
| **W-1** | `cdc-window-start.sh` kept a stamp from a DESTROYED window (21 days stale), making every cost figure below it nonsense | running the gate |
| **W-2** | The same gate asserted `Glue databases == 7`; a count cannot distinguish an ADDED database from a MISSING one, and the missing case is the expensive one | running the gate |

Each has a regression pin in `spark/tests/test_governance_review_regressions.py`, and each
pin was verified to FAIL without its fix.

`cdc-window-start.sh` now has a **6/6 Lake CMK convergence** check that catches G-P2-9 in
seconds instead of a 4-minute EMR cold start.

## Still not closed

| # | Criterion | Status |
|---|---|---|
| 38 | Schema evolution through the chain | **NOT_TESTED** — needs `cdc-runtime.sh export-schemas --execute` (an approval-gated command) to replace the legacy topic-keyed export. The legacy FALLBACK is proven: every topic logged `LEGACY EXPORT, globalId=N unverifiable`, decoded correctly, and did **not** quarantine 5,728 good records |
| 37 | Airflow DAG end-to-end | **PARTIAL** — `resolve_plan` and `turn_1.select_wave` succeeded; `turn_1.gate_and_run` pods reached `Completed` after the P37-1 fix, but no clean end-to-end run record was captured. The 2-vCPU node ran at 85% CPU with the scheduler restarting 3× under task-pod contention |
| 42 | Bedrock generation | **CANNOT_FIX** — `INVALID_PAYMENT_INSTRUMENT`, a billing state |

## Streaming FULL_CDC ingest — new 2026-09-06, tested and stopped

There was **no working continuous ingest into FULL_CDC**. `full_cdc/job.py` reads Kafka as a
bounded batch; `l1_stream/job.py` had the streaming shape and never worked (decodes Avro as
JSON, dies on the first tombstone, collects micro-batches to the driver);
`rt_stream_app.py` streams into the RT layer, not the canonical one.

`spark/jobs/full_cdc/stream_job.py` supplies the streaming **lifecycle only**. The decode,
quarantine and MERGE were EXTRACTED from the batch job into `decode_topic_to_rows()` and
`merge_into_full_cdc()` and are imported, not reimplemented — two paths that "look
equivalent" is exactly the OPEN-28 defect, where the DAG adapter and the CLI driver diverged
on one dict key and only live execution revealed it. A test asserts both callers use both
functions, and that the stream module contains no `from_avro` of its own.

### Live result

```
FULL_CDC_STREAM_STARTED  topics=4 trigger=20s budget=240s offsets=earliest
FULL_CDC_STREAM_TOPIC    ACCOUNT 326 | CUSTOMER 202 | app_user 152 | merchant 52
FULL_CDC_STREAM_BATCH    id=0 rows=732 tombstones=2 poison=0
FULL_CDC_STREAM_STOPPED  reason=run_seconds_budget_reached
FULL_CDC_COUNT           5740      (was 5730 -- read 732, added only the 10 NEW events)
```

Reading 732 rows and adding 10 is the append-only MERGE proving idempotent on the streaming
path, not a discrepancy.

### CDC event matrix, simulated in the source and traced through both layers

| scenario | FULL_CDC (stream layer) | SNAPSHOT layer |
|---|---|---|
| Oracle INSERT → UPDATE (991001) | `c` off 108, then `u` off 109 with **before=1000.00, after=2500.55** | present, `closing_balance=2500.55` |
| Oracle INSERT → DELETE → RECREATE (991002) | `c` ACTIVE 500.00 → `d` → `c` REOPENED 777.77 | present, `777.77` |
| Oracle INSERT → DELETE, never recreated (991003) | both events retained | **ABSENT** (CLAUDE.md 5.7) |
| SQL Server INSERT → UPDATE (app_user 991001) | `c` + `u` | n/a |
| SQL Server INSERT → DELETE (merchant 991002) | `c` + `d` + tombstone | n/a |

The UPDATE carrying a complete **before-image** is the evidence that per-table
`ALL COLUMN LOGGING` is working; without it that field reads NULL and every downstream
before/after comparison is silently wrong.

```
source accounts        323
FULL_CDC ACCOUNT       328 events   (every I/U/D kept -- append-only)
EOD / snapshot         323 rows     independent Athena reconciliation: expected 323 == 323
```

### S3 layout

```
warehouse/full_cdc/cdc_events/data/source_system=oracle|sqlserver/...   50 parquet, 259 metadata
warehouse/curated/fact_account_daily_snapshot/data/business_date=.../   27 parquet
checkpoints/full_cdc/stream_w*/                                          NOT under warehouse/  (CLAUDE.md 5.9)
quarantine-payloads/                                                     3 objects
warehouse/ total 1,405 objects, 0 on a stale CMK
```

### Defect found by running it

**S-1 — `foreachBatch` hands the callback a DataFrame owned by a CLONED SparkSession.**
`merge_into_full_cdc` registered `full_cdc_src` as a temp view on that clone and then ran
the MERGE through the session that *started the query*, which cannot see it:
`TABLE_OR_VIEW_NOT_FOUND: full_cdc_src`, on the first micro-batch containing rows, after the
stream was already running. Batch mode cannot reproduce it — there the two sessions are the
same object. Fixed by taking the session from the DataFrame (`spark = rows.sparkSession`),
which makes both callers correct by construction. Pinned.

### Cost

Every long-running app is stopped. `--run-seconds` is mandatory on both streaming apps and
each one exited on `run_seconds_budget_reached`; no query was left holding EMR capacity.
